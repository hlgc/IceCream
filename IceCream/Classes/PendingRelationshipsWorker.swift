//
//  PendingRelationshipsWorker.swift
//

import Foundation
import RealmSwift

/// 处理 iCloud 同步中尚未下达到 Realm 的关系引用，等所有记录到齐后统一回填
final class PendingRelationshipsWorker<Element: Object> {

    var realm: Realm?

    // MARK: - List 属性（一对多）
    // 用 Array 而非 Dictionary，避免多个 owner 共享同一 element 时的 key 碰撞。
    // 每个条目记录：属性名、宿主对象、CKRecord 里的完整有序 key 列表。
    // 回填时 removeAll + appendContentsOf 重建，同时保证顺序、杜绝重复。
    private struct ListEntry {
        let propName: String
        weak var owner: Object?
        let orderedKeys: [AnyHashable]
    }
    private var listEntries: [ListEntry] = []

    // MARK: - 直接引用（一对一，与 U/V/W 类型匹配的情况）
    // 仍用 Dictionary 是因为一对一引用不会因多 owner 共用产生碰撞
    private var directRefs: [AnyHashable: (propName: String, owner: Object)] = [:]

    // MARK: - API

    /// 记录 List 属性的待回填信息（含完整有序 key 列表）
    func addListEntry(propertyName: String, owner: Object, orderedKeys: [AnyHashable]) {
        listEntries.append(ListEntry(propName: propertyName, owner: owner, orderedKeys: orderedKeys))
    }

    /// 记录直接引用（一对一）的待回填信息（兼容旧接口）
    func addToPendingList(elementPrimaryKeyValue: AnyHashable, propertyName: String, owner: Object) {
        directRefs[elementPrimaryKeyValue] = (propName: propertyName, owner: owner)
    }

    func resolvePendingListElements() {
        guard let realm = realm else { return }

        let entries = listEntries
        let direct  = directRefs
        listEntries = []
        directRefs  = [:]

        guard !entries.isEmpty || !direct.isEmpty else { return }

        BackgroundWorker.shared.start {
            // 回填 List 属性：removeAll 后按 CKRecord 原始顺序重建，杜绝重复和乱序
            for entry in entries {
                guard let owner = entry.owner, !owner.isInvalidated else { continue }
                let orderedItems = entry.orderedKeys.compactMap {
                    realm.object(ofType: Element.self, forPrimaryKey: $0)
                }
                // 部分元素仍未到 Realm（极罕见）：写入已有的，下次 fetch 会补全
                guard !orderedItems.isEmpty else { continue }
                do {
                    try realm.write {
                        if let list = owner.value(forKey: entry.propName) as? List<Element> {
                            list.removeAll()
                            list.append(objectsIn: orderedItems)
                        }
                    }
                } catch {
                    print("IceCream: list entry resolution error:", error)
                }
            }

            // 回填一对一直接引用
            for (pk, ref) in direct {
                guard !ref.owner.isInvalidated else { continue }
                guard let element = realm.object(ofType: Element.self, forPrimaryKey: pk) else {
                    print("Cannot find existing resolving record in Realm")
                    continue
                }
                do {
                    try realm.write {
                        if let list = ref.owner.value(forKey: ref.propName) as? List<Element> {
                            if !list.contains(element) { list.append(element) }
                        } else {
                            ref.owner.setValue(element, forKey: ref.propName)
                        }
                    }
                } catch {
                    print("IceCream: direct ref resolution error:", error)
                }
            }
        }
    }
}
