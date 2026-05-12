//
//  Syncable.swift
//  IceCream
//
//  Created by 蔡越 on 24/05/2018.
//

import Foundation
import CloudKit
import RealmSwift

/// 因为'sync'是'synchronize'的非正式版本，所以我们选择'syncable'一词来表示
/// 同步的能力。
public protocol Syncable: AnyObject {

    /// CKRecordZone相关
    var recordType: String { get }
    var zoneID: CKRecordZone.ID { get }

    /// 本地存储器
    var zoneChangesToken: CKServerChangeToken? { get set }
    var isCustomZoneCreated: Bool { get set }

    /// Realm数据库相关
    func registerLocalDatabase()
    func cleanUp()
    func add(record: CKRecord)
    func delete(recordID: CKRecord.ID)

    func resolvePendingRelationships()

    /// CloudKit相关-推送本地数据到iCloud
    func pushLocalObjectsToCloudKit(_ callback: ((Error?) -> Void)?)

    /// 只推送 date 之后新增或修改的本地记录（离线期间的变更）
    func pushOfflineObjectsToCloudKit(since date: Date, _ callback: ((Error?) -> Void)?)

    /// 返回本地非删除记录数，用于计算真实推送进度
    func localRecordCount() -> Int

    /// 返回 date 之后新增或修改的本地非删除记录数
    func offlineRecordCount(since date: Date) -> Int

    /// Callback
    var pipeToEngine: ((_ recordsToStore: [CKRecord], _ recordIDsToDelete: [CKRecord.ID], _ completion: ((Error?) -> ())? ) -> ())? { get set }
    /// 后台自动同步出错（如 quotaExceeded）时的回调
    var backgroundSyncErrorCallback: ((Error) -> Void)? { get set }

}

extension Syncable {
    public func localRecordCount() -> Int { return 0 }
    public func offlineRecordCount(since date: Date) -> Int { return 0 }
    public func pushOfflineObjectsToCloudKit(since date: Date, _ callback: ((Error?) -> Void)?) {
        pushLocalObjectsToCloudKit(callback)
    }
}
