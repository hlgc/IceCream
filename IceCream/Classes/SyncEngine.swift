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
    /// 同步时间
    public var syncDateCallback: ((Date) -> Void)?
    public var syncDate: Date? {
        set {
            UserDefaults.standard.set(newValue, forKey: #file+#function)
            UserDefaults.standard.synchronize()
        }
        
        get {
            UserDefaults.standard.object(forKey: #file+#function) as? Date
        }
    }
    
    /// 同步状态
    private var isSyncAvailable: Bool = true {
        didSet {
            syncAvailableCallback?(isSyncAvailable)
        }
    }
    public var syncAvailableCallback: ((Bool) -> Void)?
    
    private let databaseManager: DatabaseManager
    
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
            guard let self = self else {
                return
            }
            syncDate = date
            syncDateCallback?(date)
        }
        databaseManager.prepare()
        databaseManager.container.accountStatus { [weak self] (status, error) in
            guard let self = self else { return }
            switch status {
            case .available:
                // 可用
                isSyncAvailable = true
                self.databaseManager.registerLocalDatabase()
                self.databaseManager.createCustomZonesIfAllowed(nil)
                self.databaseManager.fetchChangesInDatabase(nil)
                self.databaseManager.resumeLongLivedOperationIfPossible()
                self.databaseManager.startObservingRemoteChanges()
                self.databaseManager.startObservingTermination()
                self.databaseManager.createDatabaseSubscriptionIfHaveNot()
            case .noAccount, .restricted:
                // 收限制的或者没有帐号
                guard self.databaseManager is PublicDatabaseManager else {
                    isSyncAvailable = false
                    break
                }
                self.databaseManager.fetchChangesInDatabase(nil)
                self.databaseManager.resumeLongLivedOperationIfPossible()
                self.databaseManager.startObservingRemoteChanges()
                self.databaseManager.startObservingTermination()
                self.databaseManager.createDatabaseSubscriptionIfHaveNot()
            case .temporarilyUnavailable:
                // 暂时不可用
                isSyncAvailable = false
                break
            case .couldNotDetermine:
                // 不能判断
                isSyncAvailable = false
                break
            @unknown default:
                isSyncAvailable = false
                break
            }
        }
    }
    
}

// MARK: Public Method
extension SyncEngine {
    
    /// 获取CloudKit上的数据并与local合并
    ///
    /// - 参数completionHandler:在“privateCloudDatabase”中受支持。当提取数据过程完成时，将调用completionHandler。当发生任何错误时，将返回错误。否则，误差将为零。
    public func pull(completionHandler: ((Error?) -> Void)? = nil) {
        databaseManager.fetchChangesInDatabase(completionHandler)
    }
    
    /// 将所有现有的本地数据推送到CloudKit
    /// 您不应该过于频繁地调用此方法
    public func pushAll(progress: @escaping (Double, String) -> Void,
                        completion: @escaping (Result<Void, Error>) -> Void) {
        let total = Double(databaseManager.syncObjects.count)
        if total == 0 {
            progress(1.0, "无数据需要推送")
            completion(.success(()))
            return
        }

        progress(0, "准备中")
        func pushLocalObjectToCloudKit(_ index: Int) {
            if index >= databaseManager.syncObjects.count {
                progress(1.0, "推送完成")
                completion(.success(()))
                return
            }
            let syncObject = databaseManager.syncObjects[index]
            
            syncObject.pushLocalObjectsToCloudKit { error in
                if let error = error {
                    completion(.failure(error))
                    return
                }
                
                let index = Double(index) + 1.0
                if index > total {
                    progress(1.0, "推送完成")
                    completion(.success(()))
                    return
                }
                let p = index/total
                progress(p, "推送中")
                pushLocalObjectToCloudKit(Int(index))
            }
        }
        
        pushLocalObjectToCloudKit(0)
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

/// 危险部分:
/// 在大多数情况下，您不应该更改字符串值，因为它与用户设置有关。
/// 例如:cloudKitSubscriptionID，如果不想使用“private_changes”而使用另一个字符串。你应该先删除旧的订阅。
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
