//
//  LogConfig.swift
//  IceCream
//
//  Created by 蔡越 on 30/01/2018.
//

import Foundation

/// 这个文件是为冰淇淋框架设置一些开发配置。

public class IceCream {
    
    public static let shared = IceCream()
    
    /// 在冰淇淋源文件中有很多` print`s。
    /// 如果您不想在控制台中看到它们，只需将“enableLogging”属性设置为false。
    /// 默认值为true。
    public var enableLogging: Bool = true
    
}

/// 如果你想知道更多，
/// 这篇文章会有所帮助: https://medium.com/@maxcampolo/swift-conditional-logging-compiler-flags-54692dc86c5f
internal func print(_ items: Any..., separator: String = " ", terminator: String = "\n") {
    if (IceCream.shared.enableLogging) {
        #if DEBUG
        var i = items.startIndex
        repeat {
            Swift.print(items[i], separator: separator, terminator: i == (items.endIndex - 1) ? terminator : separator)
            i += 1
        } while i < items.endIndex
        #endif
    }
}
