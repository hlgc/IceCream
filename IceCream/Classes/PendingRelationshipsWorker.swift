//
//  PendingRelationshipsWorker.swift
//

import Foundation
import RealmSwift

/// 处理 iCloud 同步中尚未下达到 Realm 的关系引用，等所有记录到齐后统一回填。
/// 使用宿主对象的主键（而非 weak 引用）来持久追踪，避免 unmanaged Object 被释放后丢失引用。
final class PendingRelationshipsWorker<Element: Object> {

    var realm: Realm?
    /// 宿主对象的类型名（用于从 Realm 按主键查找），由 SyncObject 在 add(record:) 时设置
    var ownerTypeName: String?

    // MARK: - List 属性（一对多）
    private struct ListEntry {
        let propName: String
        let ownerPrimaryKey: AnyHashable
        let orderedKeys: [AnyHashable]
    }
    private var listEntries: [ListEntry] = []

    // MARK: - 直接引用（一对一，与 U/V/W 类型匹配的情况）
    private var directRefs: [AnyHashable: (propName: String, ownerPrimaryKey: AnyHashable)] = [:]

    // MARK: - API

    /// 记录 List 属性的待回填信息（含完整有序 key 列表）
    func addListEntry(propertyName: String, owner: Object, orderedKeys: [AnyHashable]) {
        guard let pk = ownerPrimaryKeyValue(of: owner) else { return }
        listEntries.append(ListEntry(propName: propertyName, ownerPrimaryKey: pk, orderedKeys: orderedKeys))
    }

    /// 记录直接引用（一对一）的待回填信息（兼容旧接口）
    func addToPendingList(elementPrimaryKeyValue: AnyHashable, propertyName: String, owner: Object) {
        guard let pk = ownerPrimaryKeyValue(of: owner) else { return }
        directRefs[elementPrimaryKeyValue] = (propName: propertyName, ownerPrimaryKey: pk)
    }

    func resolvePendingListElements() {
        guard let realm = realm, let typeName = ownerTypeName else { return }

        let entries = listEntries
        let direct  = directRefs
        listEntries = []
        directRefs  = [:]

        guard !entries.isEmpty || !direct.isEmpty else { return }

        let elementTypeName = Element.className()

        BackgroundWorker.shared.start {
            for entry in entries {
                guard let owner = realm.dynamicObject(ofType: typeName, forPrimaryKey: entry.ownerPrimaryKey),
                      !owner.isInvalidated else { continue }
                let orderedItems: [DynamicObject] = entry.orderedKeys.compactMap {
                    realm.dynamicObject(ofType: elementTypeName, forPrimaryKey: $0)
                }
                guard !orderedItems.isEmpty else { continue }
                do {
                    try realm.write {
                        let list = owner.dynamicList(entry.propName)
                        list.removeAll()
                        for item in orderedItems {
                            list.append(item)
                        }
                    }
                } catch {
                    print("IceCream: list entry resolution error:", error)
                }
            }

            for (pk, ref) in direct {
                guard let owner = realm.dynamicObject(ofType: typeName, forPrimaryKey: ref.ownerPrimaryKey),
                      !owner.isInvalidated else { continue }
                guard let element = realm.dynamicObject(ofType: elementTypeName, forPrimaryKey: pk) else {
                    print("Cannot find existing resolving record in Realm")
                    continue
                }
                do {
                    try realm.write {
                        owner.setValue(element, forKey: ref.propName)
                    }
                } catch {
                    print("IceCream: direct ref resolution error:", error)
                }
            }
        }
    }

    // MARK: - Private

    private func ownerPrimaryKeyValue(of owner: Object) -> AnyHashable? {
        guard let pkProp = owner.objectSchema.primaryKeyProperty,
              let value = owner.value(forKey: pkProp.name) as? AnyHashable else { return nil }
        return value
    }
}
