//
//  PrivateDatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

#if os(macOS)
import Cocoa
#else
import UIKit
#endif

import CloudKit

final class PrivateDatabaseManager: DatabaseManager {
    
    let container: CKContainer
    let database: CKDatabase
    
    let syncObjects: [Syncable]
    
    var syncDateCallback: ((Date) -> Void)?
    
    // 是否正在清理云端数据
    private var isDeleteiCloudData: Bool = false
    
    public init(objects: [Syncable], container: CKContainer) {
        self.syncObjects = objects
        self.container = container
        self.database = container.privateCloudDatabase
    }
    
    /// 云端数据变化
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        let changesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseChangeToken)
        
        /// 仅在提取过程完成时更新changeToken
        changesOperation.changeTokenUpdatedBlock = { [weak self] newToken in
            self?.databaseChangeToken = newToken
        }
        
        changesOperation.fetchDatabaseChangesResultBlock = { [weak self] operationResult in
            guard let self = self else { return }
            switch operationResult {
            case .success((let newToken, _)):
                databaseChangeToken = newToken
                // 获取区域级别的更改
                fetchChangesInZones(callback)
                break
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    break
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                        self.fetchChangesInDatabase(callback)
                    })
                case .recoverableError(let reason, _):
                    switch reason {
                    case .changeTokenExpired:
                        /// previousServerChangeToken值太旧，客户端必须从头开始重新同步
                        self.databaseChangeToken = nil
                        self.fetchChangesInDatabase(callback)
                    default:
                        return
                    }
                default:
                    return
                }
                break
            }
        }
        
        database.add(changesOperation)
    }
    
    func createCustomZonesIfAllowed(_ callback: ((Error?) -> Void)?) {
        let zonesToCreate = syncObjects.filter { !$0.isCustomZoneCreated }.map { CKRecordZone(zoneID: $0.zoneID) }
        guard zonesToCreate.count > 0 else { return }
        
        let modifyOp = CKModifyRecordZonesOperation(recordZonesToSave: zonesToCreate, recordZoneIDsToDelete: nil)
        modifyOp.modifyRecordZonesResultBlock = { [weak self] operationResult in
            guard let self = self else { return }
            switch operationResult {
            case .success():
                self.syncObjects.forEach { object in
                    object.isCustomZoneCreated = true
                    
                    if self.isDeleteiCloudData {
                        return
                    }
                    // 当我们在第一步注册本地数据库时，我们必须强制推送本地对象
                    // 还没有被捕获到CloudKit中使数据同步
                    DispatchQueue.main.async {
                        object.pushLocalObjectsToCloudKit(callback)
                    }
                }
                if !self.isDeleteiCloudData {
                    return
                }
                callback?(nil)
                break
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    break
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                        self.createCustomZonesIfAllowed(callback)
                    })
                default:
                    return
                }
                break
            }
        }
        
        database.add(modifyOp)
    }
    
    func createDatabaseSubscriptionIfHaveNot() {
        #if os(iOS) || os(tvOS) || os(macOS)
        guard !subscriptionIsLocallyCached else { return }
        let subscription = CKDatabaseSubscription(subscriptionID: IceCreamSubscription.cloudKitPrivateDatabaseSubscriptionID.id)
        
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true // 无声推送
        
        subscription.notificationInfo = notificationInfo
        
        let createOp = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        createOp.modifySubscriptionsResultBlock = { operationResult in
            switch operationResult {
            case .success():
                self.subscriptionIsLocallyCached = true
            case .failure(_):
                break
            }
        }
        createOp.qualityOfService = .utility
        database.add(createOp)
        #endif
    }
    
    func startObservingTermination() {
        #if os(iOS) || os(tvOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willResignActiveNotification, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willTerminateNotification, object: nil)
        
        #elseif os(macOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willResignActiveNotification, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willTerminateNotification, object: nil)
        
        #endif
    }
    
    func registerLocalDatabase() {
        self.syncObjects.forEach { object in
            DispatchQueue.main.async {
                object.registerLocalDatabase()
            }
        }
    }
    
    private func fetchChangesInZones(_ callback: ((Error?) -> Void)? = nil) {
        let changesOp = CKFetchRecordZoneChangesOperation()
        changesOp.recordZoneIDs = zoneIds
        changesOp.configurationsByRecordZoneID = zoneIdOptions
        changesOp.fetchAllChanges = true
        
        changesOp.recordZoneChangeTokensUpdatedBlock = { [weak self] zoneId, token, _ in
            guard let self = self else { return }
            guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { return }
            syncObject.zoneChangesToken = token
        }
        
        changesOp.recordWasChangedBlock = { [weak self] recordID, recordResult in
            /// 云端会返回上次zoneChangesToken以来修改的记录，这里需要做本地缓存。
            /// 处理记录:
            guard let self = self else { return }
            switch recordResult {
            case .success(let record):
                guard let syncObject = self.syncObjects.first(where: { $0.recordType == record.recordType }) else { return }
                syncObject.add(record: record)
                /// 更新同步时间
                syncDateCallback?(Date())
            default:
                break
            }
        }
        
        changesOp.recordWithIDWasDeletedBlock = { [weak self] recordId, _ in
            guard let self = self else { return }
            guard let syncObject = self.syncObjects.first(where: { $0.zoneID == recordId.zoneID }) else { return }
            syncObject.delete(recordID: recordId)
            /// 更新同步时间
            syncDateCallback?(Date())
        }
        
        changesOp.recordZoneFetchResultBlock = { [weak self] zoneId, result in
            guard let self = self else { return }
            switch result {
            case .success((let token, _, _)):
                guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { return }
                syncObject.zoneChangesToken = token
                break
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    break
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                        self.fetchChangesInZones(callback)
                    })
                case .recoverableError(let reason, _):
                    switch reason {
                    case .changeTokenExpired:
                        /// previousServerChangeToken值太旧，客户端必须从头开始重新同步
                        guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { return }
                        syncObject.zoneChangesToken = nil
                        self.fetchChangesInZones(callback)
                    default:
                        return
                    }
                default:
                    return
                }
            }
        }
        
        changesOp.fetchRecordZoneChangesResultBlock = { [weak self] result in
            guard let self = self else { return }
            
            switch result {
            case .success():
                syncObjects.forEach {
                    $0.resolvePendingRelationships()
                }
                callback?(nil)
                break
            case .failure(let error):
                callback?(error)
                break
            }
        }
        
        database.add(changesOp)
    }
}

extension PrivateDatabaseManager {
    /// 更改令牌，更多信息请参考 https://developer.apple.com/videos/play/wwdc2016/231/
    var databaseChangeToken: CKServerChangeToken? {
        get {
            /// 第一次启动时，令牌为零，服务器将把云上的所有内容都交给客户端
            /// 在其他情况下，只需将数据对象解归档
            guard let tokenData = UserDefaults.standard.object(forKey: IceCreamKey.databaseChangesTokenKey.value) as? Data else { return nil }
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
                UserDefaults.standard.removeObject(forKey: IceCreamKey.databaseChangesTokenKey.value)
                return
            }
            
            do {
                let data = try NSKeyedArchiver.archivedData(withRootObject: n, requiringSecureCoding: true)
                UserDefaults.standard.set(data, forKey: IceCreamKey.databaseChangesTokenKey.value)
            } catch {
                print("Failed to archive CKServerChangeToken:", error)
            }
        }
    }
    
    var subscriptionIsLocallyCached: Bool {
        get {
            guard let flag = UserDefaults.standard.object(forKey: IceCreamKey.subscriptionIsLocallyCachedKey.value) as? Bool  else { return false }
            return flag
        }
        set {
            UserDefaults.standard.set(newValue, forKey: IceCreamKey.subscriptionIsLocallyCachedKey.value)
        }
    }
    
    private var zoneIds: [CKRecordZone.ID] {
        return syncObjects.map { $0.zoneID }
    }
    
    private var zoneIdOptions: [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneConfiguration] {
        return syncObjects.reduce([CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneConfiguration]()) { (dict, syncObject) -> [CKRecordZone.ID: CKFetchRecordZoneChangesOperation.ZoneConfiguration] in
            var dict = dict
            let zoneChangesOptions = CKFetchRecordZoneChangesOperation.ZoneConfiguration()
            zoneChangesOptions.previousServerChangeToken = syncObject.zoneChangesToken
            dict[syncObject.zoneID] = zoneChangesOptions
            return dict
        }
    }
    
    @objc func cleanUp() {
        for syncObject in syncObjects {
            syncObject.cleanUp()
        }
    }
    
    func deleteAllCloudKitData(completion: @escaping (Result<Void, Error>) -> Void) {
        guard zoneIds.count > 0 else {
            completion(.success(()))
            return
        }
        isDeleteiCloudData = true
        let deletesOp = CKModifyRecordZonesOperation(recordZonesToSave: nil, recordZoneIDsToDelete: zoneIds)
        deletesOp.modifyRecordZonesResultBlock = { result in
            switch result {
            case .success:
                self.syncObjects.forEach {
                    $0.isCustomZoneCreated = false
                }
                self.createCustomZonesIfAllowed { error in
                    if let error = error {
                        completion(.failure(error))
                        return
                    }
                    self.isDeleteiCloudData = false
                    completion(.success(()))
                }
            case .failure(let e):
                completion(.failure(e))
            }
        }
        database.add(deletesOp)
    }
}
