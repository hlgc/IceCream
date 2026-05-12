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
    func resetAllTokens()
    func cancelFetch()

    /// 是否正在执行 fetchChangesInDatabase（用于避免 pushAll 与拉取并发）
    var isFetching: Bool { get }

    /// 每收到一条云端记录时的回调，参数为已累计接收数
    var recordFetchedCallback: ((Int) -> Void)? { get set }
}

extension DatabaseManager {
    var isFetching: Bool { false }
    func cancelFetch() {}
    var recordFetchedCallback: ((Int) -> Void)? {
        get { return nil }
        set { }
    }
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
                        // 而崩溃日志告诉我们"CKDatabaseOperations必须提交给CKDatabase"。
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

        // .changedKeys：只发送非 nil 字段，始终成功，无需 recordChangeTag，适合本架构。
        // 注：Apple 推荐 .ifServerRecordUnchanged，但需在 Realm 模型中持久化 recordChangeTag 才能正确做冲突检测；
        // 切换前须先将每次 fetch 返回的 CKRecord.recordChangeTag 写回对应 Realm 对象，否则所有已存在记录的更新
        // 因 changeTag 为 nil 而批量触发 serverRecordChanged，导致每条记录多一次服务端往返。
        modifyOpe.savePolicy = .changedKeys
        // 后台同步无需占用 userInitiated 资源
        modifyOpe.qualityOfService = .utility

        // 原子操作：一条失败则全批失败，确保数据库一致性
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
                    // 超出 CloudKit 单次限制时分批发送，用 DispatchGroup 确保 completion 只被调用一次
                    let chunkedToStoreRecords = recordsToStore.chunkItUp(by: ErrorHandler.Constant.chunkSize)
                    let chunkedToDeleteRecordIDs = recordIDsToDelete.chunkItUp(by: ErrorHandler.Constant.chunkSize)
                    let maxCount = max(chunkedToStoreRecords.count, chunkedToDeleteRecordIDs.count)

                    let group = DispatchGroup()
                    let errorLock = NSLock()
                    var firstError: Error? = nil

                    for i in 0..<maxCount {
                        group.enter()
                        let toStore = i < chunkedToStoreRecords.count ? chunkedToStoreRecords[i] : []
                        let toDelete = i < chunkedToDeleteRecordIDs.count ? chunkedToDeleteRecordIDs[i] : []
                        self.syncRecordsToCloudKit(recordsToStore: toStore, recordIDsToDelete: toDelete) { error in
                            if let e = error {
                                errorLock.lock()
                                if firstError == nil { firstError = e }
                                errorLock.unlock()
                            }
                            group.leave()
                        }
                    }
                    group.notify(queue: .main) {
                        completion?(firstError)
                    }
                case .recoverableError(let reason, _):
                    switch reason {
                    case .serverRecordChanged:
                        // 单条冲突：客户端字段合并到服务端记录后重试
                        let merged = resolveServerRecordConflict(error: error, clientRecords: recordsToStore)
                        guard !merged.isEmpty else { completion?(error); return }
                        self.syncRecordsToCloudKit(recordsToStore: merged, recordIDsToDelete: recordIDsToDelete, completion: completion)
                    case .partialFailure:
                        // 批量中含冲突：提取各条冲突合并后重试整批
                        let merged = resolvePartialConflicts(error: error, clientRecords: recordsToStore)
                        guard !merged.isEmpty else { completion?(error); return }
                        self.syncRecordsToCloudKit(recordsToStore: merged, recordIDsToDelete: recordIDsToDelete, completion: completion)
                    default:
                        completion?(error)
                    }
                default:
                    completion?(error)
                    return
                }
            }
        }
        database.add(modifyOpe)
    }

}

// MARK: - Conflict resolution helpers

/// 单条记录冲突（CKError.serverRecordChanged）：将客户端已改动的字段合并到服务端记录后返回，供调用方重试
private func resolveServerRecordConflict(error: Error, clientRecords: [CKRecord]) -> [CKRecord] {
    guard let ckError = error as? CKError,
          ckError.code == .serverRecordChanged,
          let serverRecord = ckError.serverRecord,
          let clientRecord = ckError.clientRecord else { return [] }
    // 将客户端变更的字段逐一写入服务端记录（服务端记录携带正确的 recordChangeTag）
    clientRecord.changedKeys().forEach { serverRecord[$0] = clientRecord[$0] }
    return [serverRecord]
}

/// 批量操作中含冲突（CKError.partialFailure 内嵌多条 serverRecordChanged）：
/// 对每条冲突记录执行字段合并，返回整批合并后的记录供调用方重试
private func resolvePartialConflicts(error: Error, clientRecords: [CKRecord]) -> [CKRecord] {
    guard let ckError = error as? CKError,
          ckError.code == .partialFailure,
          let partialErrors = ckError.partialErrorsByItemID else { return [] }

    var merged = clientRecords
    var hasConflict = false

    for (_, itemError) in partialErrors {
        guard let itemCKError = itemError as? CKError,
              itemCKError.code == .serverRecordChanged,
              let serverRecord = itemCKError.serverRecord,
              let clientRecord = itemCKError.clientRecord,
              let idx = merged.firstIndex(where: { $0.recordID == serverRecord.recordID })
        else { continue }
        hasConflict = true
        clientRecord.changedKeys().forEach { serverRecord[$0] = clientRecord[$0] }
        merged[idx] = serverRecord
    }
    // 无冲突条目时返回空，让调用方直接透传原始错误
    return hasConflict ? merged : []
}
