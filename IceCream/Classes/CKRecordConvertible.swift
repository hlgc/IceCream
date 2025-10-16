//
//  Object+CKRecord.swift
//  IceCream
//
//  Created by 蔡越 on 11/11/2017.
//

import Foundation
import CloudKit
import Realm
import RealmSwift

public protocol CKRecordConvertible {
    static var recordType: String { get }
    static var zoneID: CKRecordZone.ID { get }
    static var databaseScope: CKDatabase.Scope { get }
    
    var recordID: CKRecord.ID { get }
    var record: CKRecord { get }

    var isDeleted: Bool { get }
}

extension CKRecordConvertible where Self: Object {
    
    public static var databaseScope: CKDatabase.Scope {
        return .private
    }
    
    public static var recordType: String {
        return className()
    }
    
    public static var zoneID: CKRecordZone.ID {
        switch Self.databaseScope {
        case .private:
            return CKRecordZone.ID(zoneName: "\(recordType)sZone", ownerName: CKCurrentUserDefaultName)
        case .public:
            return CKRecordZone.default().zoneID
        default:
            /// 现在不支持共享数据库
            fatalError("Shared Database is not supported now")
        }
    }
    
    /// recordName:这是记录的唯一标识符，用于在数据库中定位记录。我们可以创建自己的ID，或者让CloudKit生成一个随机的UUID。
    /// 了解更多信息: https://medium.com/@guilhermerambo/synchronizing-data-with-cloudkit-94c6246a3fda
    public var recordID: CKRecord.ID {
        guard let sharedSchema = Self.sharedSchema() else {
            // 没有解决的模式。去Realm社区寻求更多帮助。
            fatalError("No schema settled. Go to Realm Community to seek more help.")
        }
        
        guard let primaryKeyProperty = sharedSchema.primaryKeyProperty else {
            // 您应该在领域对象上设置一个主键
            fatalError("You should set a primary key on your Realm object")
        }
        
        switch primaryKeyProperty.type {
        case .string:
            if let primaryValueString = self[primaryKeyProperty.name] as? String {
                // For more: https://developer.apple.com/documentation/cloudkit/ckrecord/id/1500975-init
                /// CKRecord名称的主键必须只包含ASCII字符
                assert(primaryValueString.allSatisfy({ $0.isASCII }), "Primary value for CKRecord name must contain only ASCII characters")
                /// CKRecord名称的主键不得超过255个字符
                assert(primaryValueString.count <= 255, "Primary value for CKRecord name must not exceed 255 characters")
                /// CKRecord名称的主键不得以下划线开头
                assert(!primaryValueString.starts(with: "_"), "Primary value for CKRecord name must not start with an underscore")
                return CKRecord.ID(recordName: primaryValueString, zoneID: Self.zoneID)
            } else {
                /// 值应为字符串类型
                assertionFailure("\(primaryKeyProperty.name)'s value should be String type")
            }
        case .int:
            if let primaryValueInt = self[primaryKeyProperty.name] as? Int {
                return CKRecord.ID(recordName: "\(primaryValueInt)", zoneID: Self.zoneID)
            } else {
                /// 值应该是Int类型
                assertionFailure("\(primaryKeyProperty.name)'s value should be Int type")
            }
        default:
            /// 主键应该是字符串或整数
            assertionFailure("Primary key should be String or Int")
        }
        /// 应该有合理的记录
        fatalError("Should have a reasonable recordID")
    }
    
    // 感谢这个家伙，用zoneID和recordID同时初始化CKRecord: https://stackoverflow.com/questions/45429133/how-to-initialize-ckrecord-with-both-zoneid-and-recordid
    public var record: CKRecord {
        let r = CKRecord(recordType: Self.recordType, recordID: recordID)
        let properties = objectSchema.properties
        for prop in properties {
            
            let item = self[prop.name]
            
            if prop.isArray {
                switch prop.type {
                case .int:
                    guard let list = item as? List<Int>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .string:
                    guard let list = item as? List<String>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .bool:
                    guard let list = item as? List<Bool>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .float:
                    guard let list = item as? List<Float>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .double:
                    guard let list = item as? List<Double>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .data:
                    guard let list = item as? List<Data>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .date:
                    guard let list = item as? List<Date>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .object:
                    /// 我们可以在这里得到列表<Cat>
                    /// 该项不能强制转换为List<Object>
                    /// 它可以在低级类型“RLMSwiftCollectionBase”上强制转换
                    guard let list = item as? RLMSwiftCollectionBase else { break }
                    if (list._rlmCollection.count > 0) {
                        var referenceArray = [CKRecord.Reference]()
                        let wrappedArray = list._rlmCollection
                        for index in 0..<wrappedArray.count {
                            guard let object = wrappedArray[index] as? Object, let primaryKey = object.objectSchema.primaryKeyProperty?.name else { continue }
                            switch object.objectSchema.primaryKeyProperty?.type {
                            case .string:
                                if let primaryValueString = object[primaryKey] as? String, let obj = object as? CKRecordConvertible, !obj.isDeleted {
                                    let referenceZoneID = CKRecordZone.ID(zoneName: "\(object.objectSchema.className)sZone", ownerName: CKCurrentUserDefaultName)
                                    referenceArray.append(CKRecord.Reference(recordID: CKRecord.ID(recordName: primaryValueString, zoneID: referenceZoneID), action: .none))
                                }
                            case .int:
                                if let primaryValueInt = object[primaryKey] as? Int, let obj = object as? CKRecordConvertible, !obj.isDeleted {
                                    let referenceZoneID = CKRecordZone.ID(zoneName: "\(object.objectSchema.className)sZone", ownerName: CKCurrentUserDefaultName)
                                    referenceArray.append(CKRecord.Reference(recordID: CKRecord.ID(recordName: "\(primaryValueInt)", zoneID: referenceZoneID), action: .none))
                                }
                            default:
                                break
                            }
                        }
                        r[prop.name] = referenceArray as CKRecordValue
                    }
                    else {
                        r[prop.name] = nil
                    }
                default:
                    break
                    /// 还不支持列表的其他内部类型
                }
                continue
            }
            
            switch prop.type {
            case .int, .string, .bool, .date, .float, .double, .data:
                r[prop.name] = item as? CKRecordValue
            case .object:
                guard let objectName = prop.objectClassName else { break }
                if objectName == CreamLocation.className(), let creamLocation = item as? CreamLocation {
                    r[prop.name] = creamLocation.location
                } else if objectName == CreamAsset.className(), let creamAsset = item as? CreamAsset {
                    // 如果对象是CreamAsset，则用其包装的CKAsset值设置记录
                    r[prop.name] = creamAsset.asset
                    if !creamAsset.shouldOverwrite {
                        r[ASSET_SHOULD_OVERWRITE] = false
                    }
                    if let fileExtension = creamAsset.fileExtension {
                        r[ASSET_EXTENSION] = fileExtension
                    }
                } else if let owner = item as? CKRecordConvertible {
                    // 处理一对一关系: https://realm.io/docs/swift/latest/#many-to-one
                    // 因此所有者对象必须符合CKRecordConvertible协议
                    r[prop.name] = CKRecord.Reference(recordID: owner.recordID, action: .none)
                } else {
                    /// 只是温馨提示:
                    /// 当我们将nil设置为CKRecord的属性时，该记录的属性将隐藏在CloudKit仪表板中
                    r[prop.name] = nil
                }
            default:
                break
            }
            
        }
        return r
    }
    
}
