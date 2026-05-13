//
//  CKRecordRecoverable.swift
//  IceCream
//
//  Created by 蔡越 on 26/05/2018.
//

import CloudKit
import RealmSwift

public protocol CKRecordRecoverable {
    
}

extension CKRecordRecoverable where Self: Object {
    static func parseFromRecord<U: Object, V: Object, W: Object>(
        record: CKRecord,
        realm: Realm,
        notificationToken: NotificationToken?,
        pendingUTypeRelationshipsWorker: PendingRelationshipsWorker<U>,
        pendingVTypeRelationshipsWorker: PendingRelationshipsWorker<V>,
        pendingWTypeRelationshipsWorker: PendingRelationshipsWorker<W>,
        pendingDirectRefs: inout [(propName: String, refType: String, refKey: AnyHashable)]
    ) -> Self? {
        let o = Self()
        
        var existingObj: Self? = nil
        if let pkValue = primaryKeyForRecordID(recordID: record.recordID, schema: o.objectSchema) {
            if let pkName = o.objectSchema.primaryKeyProperty?.name {
                o.setValue(pkValue, forKey: pkName)
            }
            existingObj = realm.object(ofType: Self.self, forPrimaryKey: pkValue)
        }
        
        for prop in o.objectSchema.properties {
            var recordValue: Any?
            
            if prop.isArray {
                switch prop.type {
                case .int:
                    guard let value = record.value(forKey: prop.name) as? [Int] else { break }
                    let list = List<Int>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .string:
                    guard let value = record.value(forKey: prop.name) as? [String] else { break }
                    let list = List<String>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .bool:
                    guard let value = record.value(forKey: prop.name) as? [Bool] else { break }
                    let list = List<Bool>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .float:
                    guard let value = record.value(forKey: prop.name) as? [Float] else { break }
                    let list = List<Float>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .double:
                    guard let value = record.value(forKey: prop.name) as? [Double] else { break }
                    let list = List<Double>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .data:
                    guard let value = record.value(forKey: prop.name) as? [Data] else { break }
                    let list = List<Data>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .date:
                    guard let value = record.value(forKey: prop.name) as? [Date] else { break }
                    let list = List<Date>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .object:
                    guard let value = record.value(forKey: prop.name) as? [CKRecord.Reference] else { break }

                    let uList = List<U>()
                    let vList = List<V>()
                    let wList = List<W>()
                    // 追踪每种类型的完整有序 key 列表及是否有缺失元素
                    var uAllKeys: [AnyHashable] = []; var uHasMissing = false
                    var vAllKeys: [AnyHashable] = []; var vHasMissing = false
                    var wAllKeys: [AnyHashable] = []; var wHasMissing = false

                    for reference in value {
                        if let objectClassName = prop.objectClassName,
                           let schema = realm.schema.objectSchema.first(where: { $0.className == objectClassName }),
                           let primaryKeyValue = primaryKeyForRecordID(recordID: reference.recordID, schema: schema) as? AnyHashable {
                            if schema.className == U.className() {
                                uAllKeys.append(primaryKeyValue)
                                if let existObject = realm.object(ofType: U.self, forPrimaryKey: primaryKeyValue) {
                                    uList.append(existObject)
                                } else { uHasMissing = true }
                            }
                            if schema.className == V.className() {
                                vAllKeys.append(primaryKeyValue)
                                if let existObject = realm.object(ofType: V.self, forPrimaryKey: primaryKeyValue) {
                                    vList.append(existObject)
                                } else { vHasMissing = true }
                            }
                            if schema.className == W.className() {
                                wAllKeys.append(primaryKeyValue)
                                if let existObject = realm.object(ofType: W.self, forPrimaryKey: primaryKeyValue) {
                                    wList.append(existObject)
                                } else { wHasMissing = true }
                            }
                        }
                    }

                    // 有缺失元素时记录完整有序 key 列表，回填时 removeAll + 重建保证顺序无重复
                    if uHasMissing && !uAllKeys.isEmpty {
                        pendingUTypeRelationshipsWorker.addListEntry(propertyName: prop.name, owner: o, orderedKeys: uAllKeys)
                    }
                    if vHasMissing && !vAllKeys.isEmpty {
                        pendingVTypeRelationshipsWorker.addListEntry(propertyName: prop.name, owner: o, orderedKeys: vAllKeys)
                    }
                    if wHasMissing && !wAllKeys.isEmpty {
                        pendingWTypeRelationshipsWorker.addListEntry(propertyName: prop.name, owner: o, orderedKeys: wAllKeys)
                    }

                    if prop.objectClassName == U.className() { recordValue = uList }
                    if prop.objectClassName == V.className() { recordValue = vList }
                    if prop.objectClassName == W.className() { recordValue = wList }
                    
                default:
                    break
                }
                o.setValue(recordValue, forKey: prop.name)
                continue
            }
            
            switch prop.type {
            case .int:
                recordValue = record.value(forKey: prop.name) as? Int
            case .string:
                recordValue = record.value(forKey: prop.name) as? String
            case .bool:
                recordValue = record.value(forKey: prop.name) as? Bool
            case .date:
                recordValue = record.value(forKey: prop.name) as? Date
            case .float:
                recordValue = record.value(forKey: prop.name) as? Float
            case .double:
                recordValue = record.value(forKey: prop.name) as? Double
            case .data:
                recordValue = record.value(forKey: prop.name) as? Data
            case .object:
                if let asset = record.value(forKey: prop.name) as? CKAsset {
                    if let parsed = CreamAsset.parse(from: prop.name, record: record, asset: asset) {
                        recordValue = parsed
                    } else {
                        // CKAsset 存在但 fileURL 无效或复制失败（下载未完成/临时文件已清理）
                        // 应该保留本地已有的图片，防止 nil 覆盖
                        if let existing = existingObj, let existingAsset = existing.value(forKey: prop.name) as? CreamAsset {
                            recordValue = existingAsset
                        } else {
                            continue
                        }
                    }
                } else if prop.objectClassName == CreamAsset.className() && record.value(forKey: prop.name) == nil {
                    // 云端该字段显式为 nil（用户主动删除图片），允许清空
                    recordValue = nil
                } else if let location = record.value(forKey: prop.name) as? CLLocation {
                    recordValue = CreamLocation.make(location: location)
                } else if let owner = record.value(forKey: prop.name) as? CKRecord.Reference,
                    let ownerType = prop.objectClassName,
                    let schema = realm.schema.objectSchema.first(where: { $0.className == ownerType })
                {
                    if let primaryKeyValue = primaryKeyForRecordID(recordID: owner.recordID, schema: schema) as? AnyHashable {
                        if let _recordValue = realm.dynamicObject(ofType: ownerType, forPrimaryKey: primaryKeyValue) {
                            recordValue = _recordValue
                        } else {
                            if schema.className == U.className() {
                                pendingUTypeRelationshipsWorker.addToPendingList(elementPrimaryKeyValue: primaryKeyValue, propertyName: prop.name, owner: o)
                            } else if schema.className == V.className() {
                                pendingVTypeRelationshipsWorker.addToPendingList(elementPrimaryKeyValue: primaryKeyValue, propertyName: prop.name, owner: o)
                            } else if schema.className == W.className() {
                                pendingWTypeRelationshipsWorker.addToPendingList(elementPrimaryKeyValue: primaryKeyValue, propertyName: prop.name, owner: o)
                            } else {
                                pendingDirectRefs.append((propName: prop.name, refType: ownerType, refKey: primaryKeyValue))
                            }
                        }
                    }
                }
            default:
                print("Other types will be supported in the future.")
            }
            if recordValue != nil || (recordValue == nil && prop.isOptional) {
                o.setValue(recordValue, forKey: prop.name)
            }
        }
        return o
    }
    
    /// Realm中的primaryKey可以是Int或String类型。然而,“记录名”是一个字符串类型，我们需要进行检查。
    /// 相反的过程发生在“CKRecordConvertible”协议的“recordID”属性中。
    ///
    /// - 参数recordID:cloud kit发送给我们的recordID
    /// - 返回:领域中primaryKey的特定值
    static func primaryKeyForRecordID(recordID: CKRecord.ID, schema: ObjectSchema? = nil) -> Any? {
        let schema = schema ?? Self().objectSchema
        guard let objectPrimaryKeyType = schema.primaryKeyProperty?.type else { return nil }
        switch objectPrimaryKeyType {
        case .string:
            return recordID.recordName
        case .int:
            return Int(recordID.recordName)
        default:
            // 对象primaryKey的类型应该是String或Int
            fatalError("The type of object primaryKey should be String or Int")
        }
    }
}
