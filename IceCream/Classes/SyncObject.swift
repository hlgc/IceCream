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
    
    public let realmConfiguration: Realm.Configuration
    
    private let pendingUTypeRelationshipsWorker = PendingRelationshipsWorker<U>()
    private let pendingVTypeRelationshipsWorker = PendingRelationshipsWorker<V>()
    private let pendingWTypeRelationshipsWorker = PendingRelationshipsWorker<W>()
    
    public init(
        realmConfiguration: Realm.Configuration = .defaultConfiguration,
        type: T.Type,
        uListElementType: U.Type? = nil,
        vListElementType: V.Type? = nil,
        wListElementType: W.Type? = nil
    ) {
        self.realmConfiguration = realmConfiguration
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
            let realm = try! Realm(configuration: self.realmConfiguration)
            guard let object = T.parseFromRecord(
                record: record,
                realm: realm,
                notificationToken: self.notificationToken,
                pendingUTypeRelationshipsWorker: self.pendingUTypeRelationshipsWorker,
                pendingVTypeRelationshipsWorker: self.pendingVTypeRelationshipsWorker,
                pendingWTypeRelationshipsWorker: self.pendingWTypeRelationshipsWorker
            ) else {
                print("There is something wrong with the converson from cloud record to local object")
                return
            }
            self.pendingUTypeRelationshipsWorker.realm = realm
            self.pendingVTypeRelationshipsWorker.realm = realm
            self.pendingWTypeRelationshipsWorker.realm = realm
            
            /// 如果您的模型类包含主键，您可以让Realm使用Realm()根据它们的主键值智能地更新或添加对象。添加(_:更新:)。
            /// https://realm.io/docs/swift/latest/#objects-with-primary-keys
            realm.beginWrite()
            realm.add(object, update: .modified)
            if let token = self.notificationToken {
                try! realm.commitWrite(withoutNotifying: [token])
            } else {
                try! realm.commitWrite()
            }
        }
    }
    
    /// 云端同步删除
    public func delete(recordID: CKRecord.ID) {
        BackgroundWorker.shared.start {
            let realm = try! Realm(configuration: self.realmConfiguration)
            guard let object = realm.object(ofType: T.self, forPrimaryKey: T.primaryKeyForRecordID(recordID: recordID)) else {
                // 在本地数据库中找不到
                return
            }
            CreamAsset.deleteCreamAssetFile(with: recordID.recordName)
            realm.beginWrite()
            realm.delete(object)
            if let token = self.notificationToken {
                try! realm.commitWrite(withoutNotifying: [token])
            } else {
                try! realm.commitWrite()
            }
        }
    }
    
    /// 当您向一个realm提交写事务时，该realm的所有其他实例都将得到通知，并自动更新。
    /// 了解更多信息:https://realm.io/docs/swift/latest/#writes
    public func registerLocalDatabase() {
        BackgroundWorker.shared.start {
            let realm = try! Realm(configuration: self.realmConfiguration)
            self.notificationToken = realm.objects(T.self).observe({ [weak self](changes) in
                guard let self = self else { return }
                switch changes {
                case .initial(_):
                    break
                case .update(let collection, _, let insertions, let modifications):
                    let recordsToStore = (insertions + modifications).filter { $0 < collection.count }.map { collection[$0] }.filter{ !$0.isDeleted }.map { $0.record }
                    let recordIDsToDelete = modifications.filter { $0 < collection.count }.map { collection[$0] }.filter { $0.isDeleted }.map { $0.recordID }
                    
                    guard recordsToStore.count > 0 || recordIDsToDelete.count > 0 else { return }
                    self.pipeToEngine?(recordsToStore, recordIDsToDelete, nil)
                case .error(_):
                    break
                }
            })
        }
    }
    
    public func resolvePendingRelationships() {
        pendingUTypeRelationshipsWorker.resolvePendingListElements()
        pendingVTypeRelationshipsWorker.resolvePendingListElements()
        pendingWTypeRelationshipsWorker.resolvePendingListElements()
    }
    
    public func cleanUp() {
        BackgroundWorker.shared.start {
            let realm = try! Realm(configuration: self.realmConfiguration)
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
        let realm = try! Realm(configuration: self.realmConfiguration)
        let recordsToStore: [CKRecord] = realm.objects(T.self).filter { !$0.isDeleted }.map { $0.record }
        pipeToEngine?(recordsToStore, [], callback)
    }
    
}

