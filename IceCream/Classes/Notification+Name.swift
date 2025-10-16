//
//  Notification+Name.swift
//  IceCream
//
//  Created by 蔡越 on 09/12/2017.
//

import Foundation

/// 我相信这应该是创建定制通知的最佳实践。
/// https://stackoverflow.com/questions/37899778/how-do-you-create-custom-notifications-in-swift-3

public protocol NotificationName {
    var name: Notification.Name { get }
}

extension RawRepresentable where RawValue == String, Self: NotificationName {
    public var name: Notification.Name {
        get {
            return Notification.Name(self.rawValue)
        }
    }
}
