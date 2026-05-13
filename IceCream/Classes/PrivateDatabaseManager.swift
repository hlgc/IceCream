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
    var isBatchPushing: Bool = false

    // 是否正在清理云端数据
    private var isDeleteiCloudData: Bool = false

    // MARK: - Fetch 并发控制
    private let stateLock = NSLock()
    private var _isFetching = false
    private var _pendingFetchCallbacks: [((Error?) -> Void)?] = []
    private var _currentFetchOperation: CKFetchDatabaseChangesOperation?
    /// 每次启动新 fetch（含 cancelFetch 后重启）时递增。
    /// completeFetch 持有启动时的 generation，只有匹配才真正重置标志，
    /// 避免被取消的旧 operation 的回调干扰正在进行的新 fetch。
    private var _fetchGeneration = 0

    var recordFetchedCallback: ((Int) -> Void)?
    private var _fetchedRecordCount = 0

    var isFetching: Bool {
        stateLock.lock(); defer { stateLock.unlock() }
        return _isFetching
    }

    public init(objects: [Syncable], container: CKContainer) {
        self.syncObjects = objects
        self.container = container
        self.database = container.privateCloudDatabase
    }

    /// 云端数据变化（入口：有并发保护，重复触发时排队等待当前 fetch 完成后再执行一次）
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        stateLock.lock()
        if _isFetching {
            _pendingFetchCallbacks.append(callback)
            stateLock.unlock()
            return
        }
        _isFetching = true
        stateLock.unlock()
        performFetch(callback)
    }

    /// 实际执行 fetch，retry 路径直接调用此方法以保持 _isFetching = true 状态
    private func performFetch(_ callback: ((Error?) -> Void)?) {
        stateLock.lock()
        // 记录本轮 generation：cancelFetch 会递增该值，使旧 completeFetch 失效
        let myGeneration = _fetchGeneration
        let changesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseChangeToken)
        _currentFetchOperation = changesOperation
        stateLock.unlock()
        _fetchedRecordCount = 0

        changesOperation.changeTokenUpdatedBlock = { [weak self] newToken in
            self?.databaseChangeToken = newToken
        }

        changesOperation.fetchDatabaseChangesResultBlock = { [weak self] operationResult in
            guard let self = self else { return }
            switch operationResult {
            case .success((let newToken, _)):
                self.databaseChangeToken = newToken
                self.fetchChangesInZones { error in
                    self.completeFetch(callback, error: error, generation: myGeneration)
                }
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    self.completeFetch(callback, error: nil, generation: myGeneration)
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait) {
                        self.performFetch(callback)
                    }
                case .recoverableError(let reason, _):
                    switch reason {
                    case .changeTokenExpired:
                        self.databaseChangeToken = nil
                        self.performFetch(callback)
                    case .zoneNotFound:
                        self.syncObjects.forEach { $0.isCustomZoneCreated = false }
                        self.createCustomZonesIfAllowed { [weak self] _ in
                            self?.databaseChangeToken = nil
                            self?.performFetch(callback)
                        }
                    case .network:
                        print("IceCream: database fetch network error, will retry on next cycle")
                        self.completeFetch(callback, error: error, generation: myGeneration)
                    default:
                        self.completeFetch(callback, error: error, generation: myGeneration)
                    }
                default:
                    self.completeFetch(callback, error: error, generation: myGeneration)
                }
            }
        }

        database.add(changesOperation)
    }

    /// fetch 结束时调用：generation 不匹配（已被 cancelFetch 作废）则直接返回，避免干扰新 fetch
    private func completeFetch(_ callback: ((Error?) -> Void)?, error: Error?, generation: Int) {
        stateLock.lock()
        guard _fetchGeneration == generation else {
            stateLock.unlock()
            return
        }
        _isFetching = false
        _currentFetchOperation = nil
        let pending = _pendingFetchCallbacks
        _pendingFetchCallbacks = []
        stateLock.unlock()

        if error == nil && _fetchedRecordCount > 0 {
            syncDateCallback?(Date())
        }

        callback?(error)
        pending.forEach { $0?(error) }

        if !pending.isEmpty {
            fetchChangesInDatabase(nil)
        }
    }

    func createCustomZonesIfAllowed(_ callback: ((Error?) -> Void)?) {
        let zonesToCreate = syncObjects.filter { !$0.isCustomZoneCreated }.map { CKRecordZone(zoneID: $0.zoneID) }
        guard !zonesToCreate.isEmpty else {
            callback?(nil)
            return
        }

        let modifyOp = CKModifyRecordZonesOperation(recordZonesToSave: zonesToCreate, recordZoneIDsToDelete: nil)
        modifyOp.modifyRecordZonesResultBlock = { [weak self] operationResult in
            guard let self = self else { return }
            switch operationResult {
            case .success():
                self.syncObjects.forEach { $0.isCustomZoneCreated = true }
                callback?(nil)
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    callback?(nil)
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                        self.createCustomZonesIfAllowed(callback)
                    })
                default:
                    callback?(error)
                }
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
            guard let self = self else { return }
            switch recordResult {
            case .success(let record):
                guard let syncObject = self.syncObjects.first(where: { $0.recordType == record.recordType }) else { return }
                syncObject.add(record: record)
                self._fetchedRecordCount += 1
                self.recordFetchedCallback?(self._fetchedRecordCount)
            default:
                break
            }
        }

        changesOp.recordWithIDWasDeletedBlock = { [weak self] recordId, _ in
            guard let self = self else { return }
            guard let syncObject = self.syncObjects.first(where: { $0.zoneID == recordId.zoneID }) else { return }
            syncObject.delete(recordID: recordId)
        }

        var hasMoreComing = false
        let moreComingLock = NSLock()

        changesOp.recordZoneFetchResultBlock = { [weak self] zoneId, result in
            guard let self = self else { return }
            switch result {
            case .success((let token, _, let moreComing)):
                guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { return }
                syncObject.zoneChangesToken = token
                if moreComing {
                    moreComingLock.lock()
                    hasMoreComing = true
                    moreComingLock.unlock()
                }
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
                        guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { break }
                        syncObject.zoneChangesToken = nil
                        self.fetchChangesInZones(callback)
                    case .zoneNotFound:
                        guard let syncObject = self.syncObjects.first(where: { $0.zoneID == zoneId }) else { break }
                        syncObject.isCustomZoneCreated = false
                        self.createCustomZonesIfAllowed { [weak self] _ in
                            guard let syncObject = self?.syncObjects.first(where: { $0.zoneID == zoneId }) else { return }
                            syncObject.zoneChangesToken = nil
                            self?.fetchChangesInZones(callback)
                        }
                    case .network:
                        print("IceCream: zone fetch network error for \(zoneId), will retry on next fetch cycle")
                        callback?(error)
                    default:
                        callback?(error)
                    }
                default:
                    callback?(error)
                }
            }
        }

        changesOp.fetchRecordZoneChangesResultBlock = { [weak self] result in
            guard let self = self else { return }

            switch result {
            case .success():
                self.syncObjects.forEach {
                    $0.resolvePendingRelationships()
                }
                if hasMoreComing {
                    self.fetchChangesInZones(callback)
                } else {
                    callback?(nil)
                }
            case .failure(let error):
                callback?(error)
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

    func resetAllTokens() {
        databaseChangeToken = nil
        syncObjects.forEach { $0.zoneChangesToken = nil }
    }

    func cancelFetch() {
        stateLock.lock()
        _currentFetchOperation?.cancel()
        _currentFetchOperation = nil
        _isFetching = false
        _fetchGeneration += 1   // 使正在运行的 operation 的 completeFetch 失效
        let pending = _pendingFetchCallbacks
        _pendingFetchCallbacks = []
        stateLock.unlock()

        let cancelError = NSError(domain: "IceCream", code: NSUserCancelledError,
                                  userInfo: [NSLocalizedDescriptionKey: "Fetch cancelled"])
        pending.forEach { $0?(cancelError) }
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
