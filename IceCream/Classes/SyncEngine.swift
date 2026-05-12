//
//  SyncEngine.swift
//  IceCream
//
//  Created by 蔡越 on 08/11/2017.
//

import CloudKit

/// SyncEngine直接和CloudKit对话。
/// 逻辑上，
/// 1.它负责**CKDatabase**的操作
/// 2.它处理所有的CloudKit配置，比如订阅
/// 3.它把CKRecordZone的东西交给SyncObject，这样它就可以对本地领域数据库产生影响

public final class SyncEngine {

    // MARK: - Sync Date

    private static let syncDateKey = "icecream.sync.lastSyncDate"

    public var syncDateCallback: ((Date) -> Void)?
    public var syncDate: Date? {
        set {
            UserDefaults.standard.set(newValue, forKey: SyncEngine.syncDateKey)
        }
        get {
            UserDefaults.standard.object(forKey: SyncEngine.syncDateKey) as? Date
        }
    }

    // MARK: - Account Change

    public enum AccountChangeType {
        case signIn
        case signOut
        case switchAccount
    }
    /// iCloud 账户变化回调（登入/登出/切换账户）
    public var accountChangeCallback: ((AccountChangeType) -> Void)?
    private var lastKnownAccountID: String?

    // MARK: - Sync State

    private var isSyncAvailable: Bool = true {
        didSet {
            syncAvailableCallback?(isSyncAvailable)
        }
    }
    public var syncAvailableCallback: ((Bool) -> Void)?
    /// 后台自动同步出错时的回调（如空间不足、网络错误）
    public var backgroundSyncErrorCallback: ((Error) -> Void)?
    /// 初始 fetchChangesInDatabase 完成后的一次性回调（用于在拉取结束后再提示上传离线数据）
    public var onInitialFetchComplete: ((Error?) -> Void)?
    /// 初始 fetch 开始前的预处理 block。若设置，在 fetchChangesInDatabase 前调用；
    /// completion 必须在异步操作完成后调用以触发 fetch（例如：先推送再拉取）。
    public var beforeFetchAction: ((@escaping () -> Void) -> Void)?
    /// 是否正在执行 fetchChangesInDatabase
    public var isFetching: Bool { databaseManager.isFetching }
    /// 拉取记录时的进度回调，参数为已接收的记录数（下载时实时回调）
    public var fetchProgressCallback: ((Int) -> Void)? {
        didSet { databaseManager.recordFetchedCallback = fetchProgressCallback }
    }

    private let databaseManager: DatabaseManager
    private var hasCompletedInitialSetup = false

    private var isPushInProgress = false
    private let pushLock = NSLock()

    public convenience init(objects: [Syncable], databaseScope: CKDatabase.Scope = .private, container: CKContainer = .default()) {
        switch databaseScope {
        case .private:
            let privateDatabaseManager = PrivateDatabaseManager(objects: objects, container: container)
            self.init(databaseManager: privateDatabaseManager)
        case .public:
            let publicDatabaseManager = PublicDatabaseManager(objects: objects, container: container)
            self.init(databaseManager: publicDatabaseManager)
        default:
            fatalError("Shared database scope is not supported yet")
        }
    }

    private init(databaseManager: DatabaseManager) {
        self.databaseManager = databaseManager
        setup()
    }

    private func setup() {
        databaseManager.syncDateCallback = { [weak self] date in
            guard let self = self else { return }
            syncDate = date
            syncDateCallback?(date)
        }
        databaseManager.prepare()
        databaseManager.syncObjects.forEach { [weak self] syncable in
            syncable.backgroundSyncErrorCallback = { error in
                self?.backgroundSyncErrorCallback?(error)
            }
        }
        startObservingAccountChanges()
        checkAccountAndActivate()
    }

    // MARK: - Account Monitoring

    private func startObservingAccountChanges() {
        NotificationCenter.default.addObserver(
            self,
            selector: #selector(handleAccountChanged),
            name: .CKAccountChanged,
            object: nil
        )
    }

    @objc private func handleAccountChanged() {
        databaseManager.container.accountStatus { [weak self] status, _ in
            guard let self = self else { return }
            switch status {
            case .available:
                self.databaseManager.container.fetchUserRecordID { [weak self] recordID, _ in
                    guard let self = self else { return }
                    let newID = recordID?.recordName
                    if let lastID = self.lastKnownAccountID, let newID = newID, lastID != newID {
                        self.accountChangeCallback?(.switchAccount)
                    } else if self.lastKnownAccountID == nil, newID != nil {
                        self.accountChangeCallback?(.signIn)
                    }
                    self.lastKnownAccountID = newID

                    if !self.hasCompletedInitialSetup {
                        self.activateSync()
                    } else {
                        self.isSyncAvailable = true
                        self.databaseManager.fetchChangesInDatabase(nil)
                    }
                }
            case .noAccount, .restricted:
                if self.lastKnownAccountID != nil {
                    self.accountChangeCallback?(.signOut)
                    self.lastKnownAccountID = nil
                }
                guard self.databaseManager is PublicDatabaseManager else {
                    self.isSyncAvailable = false
                    return
                }
                self.isSyncAvailable = false
            case .temporarilyUnavailable, .couldNotDetermine:
                self.isSyncAvailable = false
            @unknown default:
                self.isSyncAvailable = false
            }
        }
    }

    private func checkAccountAndActivate() {
        databaseManager.container.accountStatus { [weak self] (status, _) in
            guard let self = self else { return }
            switch status {
            case .available:
                self.databaseManager.container.fetchUserRecordID { [weak self] recordID, _ in
                    self?.lastKnownAccountID = recordID?.recordName
                    self?.activateSync()
                }
            case .noAccount, .restricted:
                guard self.databaseManager is PublicDatabaseManager else {
                    self.isSyncAvailable = false
                    return
                }
                self.databaseManager.fetchChangesInDatabase(nil)
                self.databaseManager.resumeLongLivedOperationIfPossible()
                self.databaseManager.startObservingRemoteChanges()
                self.databaseManager.startObservingTermination()
                self.databaseManager.createDatabaseSubscriptionIfHaveNot()
            case .temporarilyUnavailable, .couldNotDetermine:
                self.isSyncAvailable = false
            @unknown default:
                self.isSyncAvailable = false
            }
        }
    }

    /// 账户可用时执行完整的同步激活流程（仅执行一次）
    private func activateSync() {
        isSyncAvailable = true
        if !hasCompletedInitialSetup {
            hasCompletedInitialSetup = true
            databaseManager.registerLocalDatabase()
        }
        databaseManager.createCustomZonesIfAllowed(nil)
        let doFetch = { [weak self] in
            guard let self = self else { return }
            self.databaseManager.fetchChangesInDatabase { [weak self] error in
                self?.onInitialFetchComplete?(error)
                self?.onInitialFetchComplete = nil
            }
        }
        if let action = self.beforeFetchAction {
            self.beforeFetchAction = nil
            action(doFetch)
        } else {
            doFetch()
        }
        databaseManager.resumeLongLivedOperationIfPossible()
        databaseManager.startObservingRemoteChanges()
        databaseManager.startObservingTermination()
        databaseManager.createDatabaseSubscriptionIfHaveNot()
    }

}

// MARK: Public Method
extension SyncEngine {

    /// 获取CloudKit上的数据并与local合并
    ///
    /// - 参数completionHandler:在"privateCloudDatabase"中受支持。当提取数据过程完成时，将调用completionHandler。当发生任何错误时，将返回错误。否则，误差将为零。
    public func pull(completionHandler: ((Error?) -> Void)? = nil) {
        databaseManager.fetchChangesInDatabase(completionHandler)
    }

    /// 重置所有变更令牌后全量拉取 CloudKit 数据，适用于：
    /// - 清除本地数据后需从 iCloud 完整恢复
    /// - 重新开启同步时确保获取全部云端记录
    public func fullPull(completionHandler: ((Error?) -> Void)? = nil) {
        databaseManager.cancelFetch()
        databaseManager.resetAllTokens()
        databaseManager.fetchChangesInDatabase(completionHandler)
    }

    /// 将所有现有的本地数据推送到CloudKit
    /// 内部有并发保护：若上一次推送尚未完成，立即以 IceCreamError.pushAlreadyInProgress 回调，不会重复提交
    public func pushAll(progress: @escaping (Int, Int, String) -> Void,
                        completion: @escaping (Result<Void, Error>) -> Void) {
        pushLock.lock()
        guard !isPushInProgress else {
            pushLock.unlock()
            completion(.failure(IceCreamError.pushAlreadyInProgress))
            return
        }
        isPushInProgress = true
        pushLock.unlock()

        let syncObjects = databaseManager.syncObjects
        if syncObjects.isEmpty {
            progress(0, 0, "无数据需要推送")
            pushLock.lock(); isPushInProgress = false; pushLock.unlock()
            completion(.success(()))
            return
        }

        let countPerObject = syncObjects.map { $0.localRecordCount() }
        let totalRecords = countPerObject.reduce(0, +)

        progress(0, totalRecords, "准备中")

        var completedRecords = 0

        func pushNext(_ index: Int) {
            if index >= syncObjects.count {
                progress(totalRecords, totalRecords, "推送完成")
                pushLock.lock(); isPushInProgress = false; pushLock.unlock()
                completion(.success(()))
                return
            }
            let syncObject = syncObjects[index]
            syncObject.pushLocalObjectsToCloudKit { error in
                if let error = error {
                    self.pushLock.lock(); self.isPushInProgress = false; self.pushLock.unlock()
                    completion(.failure(error))
                    return
                }
                completedRecords += countPerObject[index]
                progress(completedRecords, totalRecords, "推送中")
                pushNext(index + 1)
            }
        }

        pushNext(0)
    }

    /// 只推送 date 之后新增或修改的本地记录（用于重新开启同步后的离线数据上传）
    /// 与 pushAll 共用并发锁：两者不可同时运行，后调用的立即以 IceCreamError.pushAlreadyInProgress 回调
    public func pushOffline(since date: Date,
                            progress: @escaping (Int, Int, String) -> Void,
                            completion: @escaping (Result<Void, Error>) -> Void) {
        pushLock.lock()
        guard !isPushInProgress else {
            pushLock.unlock()
            completion(.failure(IceCreamError.pushAlreadyInProgress))
            return
        }
        isPushInProgress = true
        pushLock.unlock()

        let syncObjects = databaseManager.syncObjects
        let countPerObject = syncObjects.map { $0.offlineRecordCount(since: date) }
        let totalRecords = countPerObject.reduce(0, +)

        if totalRecords == 0 {
            progress(0, 0, "无离线数据")
            pushLock.lock(); isPushInProgress = false; pushLock.unlock()
            completion(.success(()))
            return
        }

        progress(0, totalRecords, "准备中")
        var completedRecords = 0

        func pushNext(_ index: Int) {
            if index >= syncObjects.count {
                progress(totalRecords, totalRecords, "推送完成")
                pushLock.lock(); isPushInProgress = false; pushLock.unlock()
                completion(.success(()))
                return
            }
            syncObjects[index].pushOfflineObjectsToCloudKit(since: date) { error in
                if let error = error {
                    self.pushLock.lock(); self.isPushInProgress = false; self.pushLock.unlock()
                    completion(.failure(error))
                    return
                }
                completedRecords += countPerObject[index]
                progress(completedRecords, totalRecords, "推送中")
                pushNext(index + 1)
            }
        }

        pushNext(0)
    }

    /// 本地所有类型的非删除记录总数（用于展示数据量）
    public func totalLocalRecordCount() -> Int {
        return databaseManager.syncObjects.reduce(0) { $0 + $1.localRecordCount() }
    }

    // 删除云端数据
    public func deleteAllCloudKitData(completion: @escaping (Result<Void, Error>) -> Void) {
        databaseManager.deleteAllCloudKitData { result in
            switch result {
            case .success():
                completion(.success(()))
                break
            case .failure(let error):
                completion(.failure(error))
                break
            }
        }
    }
}

public enum Notifications: String, NotificationName {
    case cloudKitDataDidChangeRemotely
}

public enum IceCreamKey: String {
    /// Tokens
    case databaseChangesTokenKey
    case zoneChangesTokenKey

    /// Flags
    case subscriptionIsLocallyCachedKey
    case hasCustomZoneCreatedKey

    var value: String {
        return "icecream.keys." + rawValue
    }
}

/// push 操作级错误
public enum IceCreamError: Error, LocalizedError {
    /// pushAll / pushOffline 已在进行中，重复调用被拒绝
    case pushAlreadyInProgress

    public var errorDescription: String? {
        switch self {
        case .pushAlreadyInProgress:
            return "A push operation is already in progress. Wait for it to complete before calling pushAll or pushOffline again."
        }
    }
}

/// 危险部分:
/// 在大多数情况下，您不应该更改字符串值，因为它与用户设置有关。
/// 例如:cloudKitSubscriptionID，如果不想使用"private_changes"而使用另一个字符串。你应该先删除旧的订阅。
/// 否则您的用户将不会再次保存同一个订阅。所以你有麻烦了。
/// 正确的方法是先删除旧订阅，然后保存新订阅。
public enum IceCreamSubscription: String, CaseIterable {
    case cloudKitPrivateDatabaseSubscriptionID = "private_changes"
    case cloudKitPublicDatabaseSubscriptionID = "cloudKitPublicDatabaseSubcriptionID"

    var id: String {
        return rawValue
    }

    public static var allIDs: [String] {
        return IceCreamSubscription.allCases.map { $0.rawValue }
    }
}
