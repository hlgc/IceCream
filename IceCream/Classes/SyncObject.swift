//
//  SyncSource.swift
//  IceCream
//
//  Created by David Collado on 1/5/18.
//

import Foundation
import RealmSwift
import CloudKit

/// SyncObject用于您想要同步的每个模型。
/// 逻辑上，
/// 1.它负责CKRecordZone的操作。
/// 2.它检测领域数据库的变更集并直接与之对话。
/// 3.它移交给SyncEngine，以便可以与CloudKit对话。

public final class SyncObject<T, U, V, W> where T: Object & CKRecordConvertible & CKRecordRecoverable, U: Object, V: Object, W: Object {

    /// 只要持有对返回的通知令牌的引用，就会传递通知。我们应该在注册更新的类中保留对该令牌的强引用，因为当通知令牌被释放时，通知会自动取消注册。
    /// 更多，参考在这里:https://realm.io/docs/swift/latest/#notifications
    private var notificationToken: NotificationToken?

    public var pipeToEngine: ((_ recordsToStore: [CKRecord], _ recordIDsToDelete: [CKRecord.ID], _ completion: ((Error?) -> ())?) -> ())?
    /// 后台自动同步发生错误时的回调（如空间不足）
    public var backgroundSyncErrorCallback: ((Error) -> Void)?

    public let realmConfiguration: Realm.Configuration

    private let pendingUTypeRelationshipsWorker = PendingRelationshipsWorker<U>()
    private let pendingVTypeRelationshipsWorker = PendingRelationshipsWorker<V>()
    private let pendingWTypeRelationshipsWorker = PendingRelationshipsWorker<W>()
    /// 无法匹配 U/V/W 的一对一引用（如 AssetCategory），等所有记录下载完后统一回填
    /// key = 宿主对象主键，value = [(属性名, 被引用类型名, 被引用主键)]
    private var pendingDirectObjectRefs: [AnyHashable: [(propName: String, refType: String, refKey: AnyHashable)]] = [:]
    private let pendingDirectLock = NSLock()

    public init(
        realmConfiguration: Realm.Configuration = .defaultConfiguration,
        type: T.Type,
        uListElementType: U.Type? = nil,
        vListElementType: V.Type? = nil,
        wListElementType: W.Type? = nil
    ) {
        self.realmConfiguration = realmConfiguration
        pendingUTypeRelationshipsWorker.ownerTypeName = T.className()
        pendingVTypeRelationshipsWorker.ownerTypeName = T.className()
        pendingWTypeRelationshipsWorker.ownerTypeName = T.className()
    }

    deinit {
        // notificationToken 是在 BackgroundWorker 线程上创建的，必须在同一线程上销毁。
        // deinit 可能被调用于任意线程（如 CloudKit 回调队列），所以显式派发到 BackgroundWorker。
        let token = notificationToken
        notificationToken = nil
        if let t = token {
            SyncEngine.removeNotificationToken(t)
            BackgroundWorker.shared.start {
                t.invalidate()
            }
        }
    }

}

// MARK: - Zone information

extension SyncObject: Syncable {

    public var recordType: String {
        return T.recordType
    }

    public var zoneID: CKRecordZone.ID {
        return T.zoneID
    }

    public var zoneChangesToken: CKServerChangeToken? {
        get {
            /// 第一次启动时，令牌为零，服务器将把云上的所有内容都交给客户端
            /// 在其他情况下，只需将数据对象解归档
            guard let tokenData = UserDefaults.standard.object(forKey: T.className() + IceCreamKey.zoneChangesTokenKey.value) as? Data else { return nil }
            do {
                let token = try NSKeyedUnarchiver.unarchivedObject(ofClass: CKServerChangeToken.self, from: tokenData)
                // 使用 token
                return token
            } catch {
                // 处理解档错误
                print("Failed to unarchive CKServerChangeToken:", error)
                return nil
            }
        }
        set {
            guard let n = newValue else {
                UserDefaults.standard.removeObject(forKey: T.className() + IceCreamKey.zoneChangesTokenKey.value)
                return
            }
            do {
                let data = try NSKeyedArchiver.archivedData(withRootObject: n, requiringSecureCoding: true)
                UserDefaults.standard.set(data, forKey: T.className() + IceCreamKey.zoneChangesTokenKey.value)
            } catch {
                print("Failed to archive CKServerChangeToken:", error)
            }
        }
    }

    public var isCustomZoneCreated: Bool {
        get {
            guard let flag = UserDefaults.standard.object(forKey: T.className() + IceCreamKey.hasCustomZoneCreatedKey.value) as? Bool else { return false }
            return flag
        }
        set {
            UserDefaults.standard.set(newValue, forKey: T.className() + IceCreamKey.hasCustomZoneCreatedKey.value)
        }
    }

    /// 云端同步添加
    public func add(record: CKRecord) {
        BackgroundWorker.shared.start {
            guard let realm = try? Realm(configuration: self.realmConfiguration) else { return }

            // 冲突检测：本地存在且 updateAt 比云端 modificationDate 更新时，跳过覆盖（仅更新 ckSystemFields）
            let hasUpdateAtField = T.sharedSchema()?.properties.contains(where: { $0.name == "updateAt" && $0.type == .date }) ?? false
            if hasUpdateAtField, let primaryKey = T.primaryKeyForRecordID(recordID: record.recordID) {
                if let existingObject = realm.object(ofType: T.self, forPrimaryKey: primaryKey) {
                    let localUpdateAt = existingObject.value(forKey: "updateAt") as? Date
                    let cloudModDate = record.modificationDate
                    if let localDate = localUpdateAt, let cloudDate = cloudModDate, localDate > cloudDate {
                        if existingObject.objectSchema.properties.contains(where: { $0.name == "ckSystemFields" }),
                           let systemFieldsData = try? NSKeyedArchiver.archivedData(withRootObject: record, requiringSecureCoding: true) {
                            realm.beginWrite()
                            existingObject.setValue(systemFieldsData, forKey: "ckSystemFields")
                            do {
                                if let token = self.notificationToken {
                                    try realm.commitWrite(withoutNotifying: [token])
                                } else {
                                    try realm.commitWrite()
                                }
                            } catch {
                                print("IceCream: Realm write error in ckSystemFields update:", error)
                            }
                        }
                        print("IceCream: skip overwrite for \(T.className()) pk=\(String(describing: primaryKey)), local(\(localDate)) > cloud(\(cloudDate))")
                        return
                    }
                }
            }

            var localPendingDirectRefs: [(propName: String, refType: String, refKey: AnyHashable)] = []
            guard let object = T.parseFromRecord(
                record: record,
                realm: realm,
                notificationToken: self.notificationToken,
                pendingUTypeRelationshipsWorker: self.pendingUTypeRelationshipsWorker,
                pendingVTypeRelationshipsWorker: self.pendingVTypeRelationshipsWorker,
                pendingWTypeRelationshipsWorker: self.pendingWTypeRelationshipsWorker,
                pendingDirectRefs: &localPendingDirectRefs
            ) else {
                print("There is something wrong with the converson from cloud record to local object")
                return
            }
            if !localPendingDirectRefs.isEmpty,
               let pkName = T.sharedSchema()?.primaryKeyProperty?.name,
               let ownerKey = object.value(forKey: pkName) as? AnyHashable {
                self.pendingDirectLock.lock()
                var existing = self.pendingDirectObjectRefs[ownerKey] ?? []
                existing.append(contentsOf: localPendingDirectRefs)
                self.pendingDirectObjectRefs[ownerKey] = existing
                self.pendingDirectLock.unlock()
            }
            self.pendingUTypeRelationshipsWorker.realm = realm
            self.pendingVTypeRelationshipsWorker.realm = realm
            self.pendingWTypeRelationshipsWorker.realm = realm

            if object.objectSchema.properties.contains(where: { $0.name == "ckSystemFields" }),
               let systemFieldsData = try? NSKeyedArchiver.archivedData(withRootObject: record, requiringSecureCoding: true) {
                object.setValue(systemFieldsData, forKey: "ckSystemFields")
            }

            realm.beginWrite()
            realm.add(object, update: .modified)
            do {
                let tokens = SyncEngine.getNotificationTokens()
                if !tokens.isEmpty {
                    try realm.commitWrite(withoutNotifying: tokens)
                } else if let token = self.notificationToken {
                    try realm.commitWrite(withoutNotifying: [token])
                } else {
                    try realm.commitWrite()
                }
            } catch {
                print("IceCream: Realm write error in add:", error)
            }
        }
    }

    /// 云端同步删除
    public func delete(recordID: CKRecord.ID) {
        BackgroundWorker.shared.start {
            guard let realm = try? Realm(configuration: self.realmConfiguration) else { return }
            guard let object = realm.object(ofType: T.self, forPrimaryKey: T.primaryKeyForRecordID(recordID: recordID)) else {
                // 在本地数据库中找不到
                return
            }
            CreamAsset.deleteCreamAssetFile(with: recordID.recordName)
            realm.beginWrite()
            realm.delete(object)
            do {
                let tokens = SyncEngine.getNotificationTokens()
                if !tokens.isEmpty {
                    try realm.commitWrite(withoutNotifying: tokens)
                } else if let token = self.notificationToken {
                    try realm.commitWrite(withoutNotifying: [token])
                } else {
                    try realm.commitWrite()
                }
            } catch {
                print("IceCream: Realm write error in delete:", error)
            }
        }
    }

    /// 当您向一个realm提交写事务时，该realm的所有其他实例都将得到通知，并自动更新。
    /// 了解更多信息:https://realm.io/docs/swift/latest/#writes
    public func registerLocalDatabase() {
        BackgroundWorker.shared.start {
            guard let realm = try? Realm(configuration: self.realmConfiguration) else { return }
            self.notificationToken = realm.objects(T.self).observe({ [weak self](changes) in
                guard let self = self else { return }
                switch changes {
                case .initial(_):
                    break
                case .update(let collection, _, let insertions, let modifications):
                    let recordsToStore = (insertions + modifications).filter { $0 < collection.count }.map { collection[$0] }.filter{ !$0.isDeleted }.map { $0.record }
                    let recordIDsToDelete = modifications.filter { $0 < collection.count }.map { collection[$0] }.filter { $0.isDeleted }.map { $0.recordID }

                    guard recordsToStore.count > 0 || recordIDsToDelete.count > 0 else { return }
                    self.pipeToEngine?(recordsToStore, recordIDsToDelete, { [weak self] error in
                        if let error = error {
                            self?.backgroundSyncErrorCallback?(error)
                        }
                    })
                case .error(_):
                    break
                }
            })
            if let token = self.notificationToken {
                SyncEngine.addNotificationToken(token)
            }
        }
    }

    public func resolvePendingRelationships() {
        pendingUTypeRelationshipsWorker.resolvePendingListElements(notificationToken: notificationToken)
        pendingVTypeRelationshipsWorker.resolvePendingListElements(notificationToken: notificationToken)
        pendingWTypeRelationshipsWorker.resolvePendingListElements(notificationToken: notificationToken)
        resolveDirectObjectReferences()
    }

    private func resolveDirectObjectReferences() {
        pendingDirectLock.lock()
        let pending = pendingDirectObjectRefs
        pendingDirectObjectRefs = [:]
        pendingDirectLock.unlock()

        guard !pending.isEmpty else { return }

        let token = notificationToken
        BackgroundWorker.shared.start { [self] in
            guard let realm = try? Realm(configuration: realmConfiguration) else { return }
            for (ownerKey, refs) in pending {
                guard let owner = realm.object(ofType: T.self, forPrimaryKey: ownerKey),
                      !owner.isInvalidated else { continue }
                for ref in refs {
                    guard let refObj = realm.dynamicObject(ofType: ref.refType, forPrimaryKey: ref.refKey) else { continue }
                    do {
                        let tokens = SyncEngine.getNotificationTokens()
                        if !tokens.isEmpty || token != nil {
                            realm.beginWrite()
                            owner.setValue(refObj, forKey: ref.propName)
                            if !tokens.isEmpty {
                                try realm.commitWrite(withoutNotifying: tokens)
                            } else {
                                try realm.commitWrite(withoutNotifying: [token!])
                            }
                        } else {
                            try realm.write { owner.setValue(refObj, forKey: ref.propName) }
                        }
                    } catch {
                        print("IceCream: Failed to resolve pending direct reference:", error)
                    }
                }
            }
        }
    }

    public func localRecordCount() -> Int {
        guard let realm = try? Realm(configuration: realmConfiguration) else { return 0 }
        return realm.objects(T.self).filter { !$0.isDeleted }.count
    }

    public func offlineRecordCount(since date: Date) -> Int {
        guard let realm = try? Realm(configuration: realmConfiguration) else { return 0 }
        return realm.objects(T.self)
            .filter("isDeleted == false AND (createdAt > %@ OR updateAt > %@)", date as NSDate, date as NSDate)
            .count
    }

    public func pushOfflineObjectsToCloudKit(since date: Date, _ callback: ((Error?) -> Void)? = nil) {
        guard let realm = try? Realm(configuration: realmConfiguration) else { return }
        let recordsToStore: [CKRecord] = realm.objects(T.self)
            .filter("isDeleted == false AND (createdAt > %@ OR updateAt > %@)", date as NSDate, date as NSDate)
            .map { $0.record }
        guard !recordsToStore.isEmpty else { callback?(nil); return }
        pipeToEngine?(recordsToStore, [], callback)
    }

    public func cleanUp() {
        BackgroundWorker.shared.start {
            guard let realm = try? Realm(configuration: self.realmConfiguration) else { return }
            let objects = realm.objects(T.self).filter { $0.isDeleted }

            if objects.count <= 0 {
                return
            }

            var tokens: [NotificationToken] = []
            self.notificationToken.flatMap { tokens = [$0] }

            realm.beginWrite()
            objects.forEach({ realm.delete($0) })
            do {
                try realm.commitWrite(withoutNotifying: tokens)
            } catch {

            }
        }
    }

    public func pushLocalObjectsToCloudKit(_ callback: ((Error?) -> Void)? = nil) {
        guard let realm = try? Realm(configuration: self.realmConfiguration) else {
            callback?(NSError(domain: "IceCream", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to initialize Realm"]))
            return
        }
        let recordsToStore: [CKRecord] = realm.objects(T.self).filter { !$0.isDeleted }.map { $0.record }.filter { record in
            guard record.recordChangeTag != nil else { return true }
            return !record.changedKeys().isEmpty
        }
        guard !recordsToStore.isEmpty else {
            callback?(nil)
            return
        }
        pipeToEngine?(recordsToStore, [], callback)
    }

}
