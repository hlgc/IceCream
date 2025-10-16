//
//  Syncable.swift
//  IceCream
//
//  Created by 蔡越 on 24/05/2018.
//

import Foundation
import CloudKit
import RealmSwift

/// 因为‘sync’是‘synchronize’的非正式版本，所以我们选择‘syncable’一词来表示
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
    
    /// CloudKit相关
    func pushLocalObjectsToCloudKit()
    
    /// Callback
    var pipeToEngine: ((_ recordsToStore: [CKRecord], _ recordIDsToDelete: [CKRecord.ID]) -> ())? { get set }
    
}
