//
//  File.swift
//
//
//  Created by Yue Cai on 2022/1/6.
//

import Foundation
import CloudKit
import RealmSwift
import CoreLocation

public class CreamLocation: Object {
    @Persisted public var latitude: CLLocationDegrees = 0
    @Persisted public var longitude: CLLocationDegrees = 0
    
    convenience public init(latitude: CLLocationDegrees, longitude: CLLocationDegrees) {
        self.init()
        self.latitude = latitude
        self.longitude = longitude
    }
    
    // MARK: - 用于CKRecordConvertible可改变的
    
    var location: CLLocation {
        get {
            return CLLocation(latitude: latitude, longitude: longitude)
        }
    }
    
    // MARK: - 用于CKRecordRecoverable可恢复的
    
    static func make(location: CLLocation) -> CreamLocation {
        return CreamLocation(latitude: location.coordinate.latitude, longitude: location.coordinate.longitude)
    }
}
