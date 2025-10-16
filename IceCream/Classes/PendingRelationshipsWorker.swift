//
//  File.swift
//
//
//  Created by Soledad on 2021/2/7.
//

import Foundation
import RealmSwift

/// 当对象从CKRecord中恢复时，PendingRelationshipsWorker负责临时存储关系
final class PendingRelationshipsWorker<Element: Object> {
    
    var realm: Realm?
    
    var pendingListElementPrimaryKeyValue: [AnyHashable: (String, Object)] = [:]
    
    func addToPendingList(elementPrimaryKeyValue: AnyHashable, propertyName: String, owner: Object) {
        pendingListElementPrimaryKeyValue[elementPrimaryKeyValue] = (propertyName, owner)
    }
    
    func resolvePendingListElements() {
        guard let realm = realm, pendingListElementPrimaryKeyValue.count > 0 else {
            // 也许我们可以在这里添加一个日志
            return
        }
        BackgroundWorker.shared.start {
            for (primaryKeyValue, (propName, owner)) in self.pendingListElementPrimaryKeyValue {
                if let list = owner.value(forKey: propName) as? List<Element> {
                    if let existListElementObject = realm.object(ofType: Element.self, forPrimaryKey: primaryKeyValue) {
                        try! realm.write {
                            list.append(existListElementObject)
                        }
                        self.pendingListElementPrimaryKeyValue[primaryKeyValue] = nil
                    } else {
                        /// 在数据库中找不到现有的解析记录
                        print("Cannot find existing resolving record in Realm")
                    }
                } else if let existListElementObject = realm.object(ofType: Element.self, forPrimaryKey: primaryKeyValue) {
                    try! realm.write {
                        owner.setValue(existListElementObject, forKey: propName)
                    }
                    self.pendingListElementPrimaryKeyValue[primaryKeyValue] = nil
                }
            }
        }
    }
    
}
