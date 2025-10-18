//
//  DatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

import CloudKit

protocol DatabaseManager: AnyObject {
    
    /// 用于访问应用程序容器的数据并对其执行操作的管道。
    var database: CKDatabase { get }
    
    /// 与应用程序相关的内容封装。
    var container: CKContainer { get }
    
    /// 更新同步时间
    var syncDateCallback: ((Date) -> Void)? { get set }
    
    var syncObjects: [Syncable] { get }
    
    init(objects: [Syncable], container: CKContainer)
    
    func prepare()
    
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?)
    
    /// cloud kit最佳实践已过时，现在使用:
    /// https://developer.apple.com/documentation/cloudkit/ckoperation
    /// 这个func解决的是哪个问题？例如:
    /// 1.(离线)你做了一个局部的改变，涉及到一个操作
    /// 2.应用程序退出或被用户弹出
    /// 3.再次返回应用程序
    /// 操作恢复！所有工作都像魔术一样！
    func resumeLongLivedOperationIfPossible()
    
    func createCustomZonesIfAllowed(_ callback: ((Error?) -> Void)?)
    func startObservingRemoteChanges()
    func startObservingTermination()
    func createDatabaseSubscriptionIfHaveNot()
    func registerLocalDatabase()
    
    func cleanUp()
    func deleteAllCloudKitData(completion: @escaping (Result<Void, Error>) -> Void)
}

extension DatabaseManager {
    
    func prepare() {
        syncObjects.forEach {
            $0.pipeToEngine = { [weak self] recordsToStore, recordIDsToDelete, completion in
                guard let self = self else { return }
                self.syncRecordsToCloudKit(recordsToStore: recordsToStore, recordIDsToDelete: recordIDsToDelete, completion: completion)
            }
        }
    }
    
    func resumeLongLivedOperationIfPossible() {
        container.fetchAllLongLivedOperationIDs { [weak self]( opeIDs, error) in
            guard let self = self, error == nil, let ids = opeIDs else { return }
            for id in ids {
                self.container.fetchLongLivedOperation(withID: id, completionHandler: { [weak self](ope, error) in
                    guard let self = self, error == nil else { return }
                    if let modifyOp = ope as? CKModifyRecordsOperation {
                        modifyOp.modifyRecordsResultBlock = { _ in
                            print("Resume modify records success!")
                            /// 更新同步时间
                            self.syncDateCallback?(Date())
                        }
//                        modifyOp.modifyRecordsCompletionBlock = { (_,_,_) in
//                            print("Resume modify records success!")
//                        }
                        // doc中的苹果示例代码(https://developer.apple.com/documentation/cloudkit/ckoperation/#1666033)
                        // 告诉我们在容器中添加操作。但无论如何，它在iOS 15测试版上崩溃了。
                        // 而崩溃日志告诉我们“CKDatabaseOperations必须提交给CKDatabase”。
                        // 所以我猜守护进程里肯定有什么东西变了。我们临时添加了这个可用性检查。
                        database.add(modifyOp)
                    }
                })
            }
        }
    }
    
    func startObservingRemoteChanges() {
        NotificationCenter.default.addObserver(forName: Notifications.cloudKitDataDidChangeRemotely.name, object: nil, queue: nil, using: { [weak self](_) in
            guard let self = self else { return }
            DispatchQueue.global(qos: .utility).async {
                // 收到云端变化
                self.fetchChangesInDatabase(nil)
            }
        })
    }
    
    /// 将本地数据同步到CloudKit
    /// 有关保存策略的更多信息: https://developer.apple.com/documentation/cloudkit/ckrecordsavepolicy
    public func syncRecordsToCloudKit(recordsToStore: [CKRecord], recordIDsToDelete: [CKRecord.ID], completion: ((Error?) -> ())? = nil) {
        let modifyOpe = CKModifyRecordsOperation(recordsToSave: recordsToStore, recordIDsToDelete: recordIDsToDelete)
        
        if #available(iOS 11.0, OSX 10.13, tvOS 11.0, watchOS 4.0, *) {
            let config = CKOperation.Configuration()
            config.isLongLived = true
            modifyOpe.configuration = config
        } else {
            // Fallback on earlier versions
            modifyOpe.isLongLived = true
        }
        
        // 我们使用。已更改密钥保存策略在此进行未锁定的更改，因为我的应用程序是有争议的，首先离线
        // 苹果建议使用。ifServerRecordUnchanged保存策略
        // 如需详细资讯，请参阅进阶云端套件(https://developer.apple.com/videos/play/wwdc2014/231/)
        modifyOpe.savePolicy = .changedKeys
        
        // 为了避免CKError.partialFailure，请使操作原子化(如果一条记录未能被修改，则所有记录都将失败)
        // 如果要处理部分失败，请设置。isAtomic为false并实现CKOperationResultType。失败(原因:。部分故障)
        modifyOpe.isAtomic = true
        
        modifyOpe.modifyRecordsResultBlock = {
            [weak self]
            (result) in
            
            guard let self = self else { return }
            switch result {
            case .success(_):
                /// 更新同步时间
                DispatchQueue.main.async {
                    self.syncDateCallback?(Date())
                    completion?(nil)
                }
                break
            case .failure(let error):
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    break
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait) {
                        self.syncRecordsToCloudKit(recordsToStore: recordsToStore, recordIDsToDelete: recordIDsToDelete, completion: completion)
                    }
                case .chunk:
                    /// CloudKit规定单个请求中的最大项目数为400。
                    /// 所以我觉得300应该是他们没问题的。
                    //                let chunkedRecords = recordsToStore.chunkItUp(by: 300)
                    //                for chunk in chunkedRecords {
                    //                    self.syncRecordsToCloudKit(recordsToStore: chunk, recordIDsToDelete: recordIDsToDelete, completion: completion)
                    //                }
                    let chunkedToStoreRecords = recordsToStore.chunkItUp(by: ErrorHandler.Constant.chunkSize)
                    let chunkedToDeleteRecordIDs = recordIDsToDelete.chunkItUp(by: ErrorHandler.Constant.chunkSize)
                    
                    if chunkedToStoreRecords.count >= chunkedToDeleteRecordIDs.count {
                        for (index, chunk) in chunkedToStoreRecords.enumerated() {
                            if index < chunkedToDeleteRecordIDs.count {
                                self.syncRecordsToCloudKit(recordsToStore: chunk, recordIDsToDelete: chunkedToDeleteRecordIDs[index], completion: completion)
                            } else {
                                self.syncRecordsToCloudKit(recordsToStore: chunk, recordIDsToDelete: [], completion: completion)
                            }
                        }
                    } else {
                        for (index, chunk) in chunkedToDeleteRecordIDs.enumerated() {
                            if index < chunkedToStoreRecords.count {
                                self.syncRecordsToCloudKit(recordsToStore: chunkedToStoreRecords[index], recordIDsToDelete: chunk, completion: completion)
                            } else {
                                self.syncRecordsToCloudKit(recordsToStore: [], recordIDsToDelete: chunk, completion: completion)
                            }
                        }
                    }
                default:
                    completion?(error)
                    return
                }
            }
        }
        database.add(modifyOpe)
    }
    
    private func modifyRecordsResult() {
        
    }
}
