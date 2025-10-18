//
//  ErrorHandler.swift
//  IceCream
//
//  Created by @randycarney on 12/12/17.
//

import Foundation
import CloudKit

/// 此结构帮助您处理所有的CKErrors，并已更新到当前的Apple文档(2017年12月15日):
/// https://developer.apple.com/documentation/cloudkit/ckerror.code

struct ErrorHandler {
    
    static let shared = ErrorHandler()
    
    struct Constant {
        /// CloudKit规定单个请求中的最大项目数为400。
        /// 所以我觉得300应该是他们没问题的。
        static let chunkSize = 300
    }
    
    /// 我们可以将CKOperation返回的所有结果分为以下五种CKOperationResultTypes
    enum CKOperationResultType {
        case success
        case retry(afterSeconds: Double, message: String)
        case chunk
        case recoverableError(reason: CKOperationFailReason, message: String)
        case fail(reason: CKOperationFailReason, message: String)
    }
    
    /// CloudKit失败的原因可分为以下8种情况
    enum CKOperationFailReason {
        /// 更改令牌已过期
        case changeTokenExpired
        /// 网络错误
        case network
        /// 超过配额
        case quotaExceeded
        /// 部分失效
        case partialFailure
        /// 服务器记录已更改
        case serverRecordChanged
        /// 分享相关的
        case shareRelated
        /// 未处理的错误代码
        case unhandledErrorCode
        /// 未知
        case unknown
    }
    
    func resultType(with error: Error?) -> CKOperationResultType {
        guard error != nil else { return .success }
        
        guard let e = error as? CKError else {
            return .fail(reason: .unknown, message: "The error returned is not a CKError")
        }
        
        let message = returnErrorMessage(for: e.code)
        
        switch e.code {
            
        // 应重试
        case .serviceUnavailable,
             .requestRateLimited,
             .zoneBusy:
            
            // 如果在错误中指定了重试延迟，则使用该延迟。
            let userInfo = e.userInfo
            if let retry = userInfo[CKErrorRetryAfterKey] as? Double {
                print("ErrorHandler - \(message). Should retry in \(retry) seconds.")
                return .retry(afterSeconds: retry, message: message)
            } else {
                return .fail(reason: .unknown, message: message)
            }
            
        // 可恢复错误
        case .networkUnavailable,
             .networkFailure:
            print("ErrorHandler.recoverableError: \(message)")
            return .recoverableError(reason: .network, message: message)
        case .changeTokenExpired:
            print("ErrorHandler.recoverableError: \(message)")
            return .recoverableError(reason: .changeTokenExpired, message: message)
        case .serverRecordChanged:
            print("ErrorHandler.recoverableError: \(message)")
            return .recoverableError(reason: .serverRecordChanged, message: message)
        case .partialFailure:
            // Normally it shouldn't happen since if CKOperation `isAtomic` set to true
            if let dictionary = e.userInfo[CKPartialErrorsByItemIDKey] as? NSDictionary {
                print("ErrorHandler.partialFailure for \(dictionary.count) items; CKPartialErrorsByItemIDKey: \(dictionary)")
            }
            return .recoverableError(reason: .partialFailure, message: message)
            
        // 应该把它分块
        case .limitExceeded:
            print("ErrorHandler.Chunk: \(message)")
            return .chunk
            
        // 共享数据库相关
        case .alreadyShared,
             .participantMayNeedVerification,
             .referenceViolation,
             .tooManyParticipants:
            print("ErrorHandler.Fail: \(message)")
            return .fail(reason: .shareRelated, message: message)
        
        // 配额超出是一种特殊情况，用户必须在重试之前采取行动（例如在iCloud中预留更多空间）
        case .quotaExceeded:
            print("ErrorHandler.Fail: \(message)")
            return .fail(reason: .quotaExceeded, message: message)
            
        // 失败是最后的，我们真的不能再多做了
        default:
            print("ErrorHandler.Fail: \(message)")
            return .fail(reason: .unknown, message: message)

        }
        
    }
    
    func retryOperationIfPossible(retryAfter: Double, block: @escaping () -> ()) {
        
        let delayTime = DispatchTime.now() + retryAfter
        DispatchQueue.main.asyncAfter(deadline: delayTime, execute: {
            block()
        })
        
    }
    
    private func returnErrorMessage(for code: CKError.Code) -> String {
        var returnMessage = ""
        
        switch code {
        case .alreadyShared:
            /// 已共享：不能保存记录或共享，因为这样做会导致相同的记录层次结构存在于多个共享中。
            returnMessage = "Already Shared: a record or share cannot be saved because doing so would cause the same hierarchy of records to exist in multiple shares."
        case .assetFileModified:
            /// 资产文件已修改：指定的资产文件在保存过程中内容被修改。
            returnMessage = "Asset File Modified: the content of the specified asset file was modified while being saved."
        case .assetFileNotFound:
            /// 未找到资产文件：指定的资产文件未找到。
            returnMessage = "Asset File Not Found: the specified asset file is not found."
        case .badContainer:
            /// 坏容器：指定的容器是未知的或未经授权的。
            returnMessage = "Bad Container: the specified container is unknown or unauthorized."
        case .badDatabase:
            /// 数据库错误：无法在给定的数据库上完成操作。
            returnMessage = "Bad Database: the operation could not be completed on the given database."
        case .batchRequestFailed:
            /// Batch Request Failed：整个批请求被拒绝。
            returnMessage = "Batch Request Failed: the entire batch was rejected."
        case .changeTokenExpired:
            /// Change Token Expired：之前的服务器更改令牌太旧。
            returnMessage = "Change Token Expired: the previous server change token is too old."
        case .constraintViolation:
            /// 违反约束：服务器拒绝了请求，因为与唯一字段冲突。
            returnMessage = "Constraint Violation: the server rejected the request because of a conflict with a unique field."
        case .incompatibleVersion:
            /// 不兼容版本：您的应用程序版本比允许的最旧版本早。
            returnMessage = "Incompatible Version: your app version is older than the oldest version allowed."
        case .internalError:
            /// 内部错误：CloudKit遇到了不可恢复的错误。
            returnMessage = "Internal Error: a nonrecoverable error was encountered by CloudKit."
        case .invalidArguments:
            /// 无效参数：指定的请求包含错误信息。
            returnMessage = "Invalid Arguments: the specified request contains bad information."
        case .limitExceeded:
            /// 超出限制：对服务器的请求太大。
            returnMessage = "Limit Exceeded: the request to the server is too large."
        case .managedAccountRestricted:
            /// 受限制的托管帐户：由于受管理的帐户限制，请求被拒绝。
            returnMessage = "Managed Account Restricted: the request was rejected due to a managed-account restriction."
        case .missingEntitlement:
            /// 缺失的权利：应用程序缺少必要的权利。
            returnMessage = "Missing Entitlement: the app is missing a required entitlement."
        case .networkUnavailable:
            /// 网络不可用：网络连接似乎离线。
            returnMessage = "Network Unavailable: the internet connection appears to be offline."
        case .networkFailure:
            /// 网络故障：互联网连接似乎离线。
            returnMessage = "Network Failure: the internet connection appears to be offline."
        case .notAuthenticated:
            /// 未认证：要使用此应用程序，您必须启用iCloud同步。进入设备设置，登录到iCloud，然后在应用程序设置中，确保iCloud功能已启用。
            returnMessage = "Not Authenticated: to use this app, you must enable iCloud syncing. Go to device Settings, sign in to iCloud, then in the app settings, be sure the iCloud feature is enabled."
        case .operationCancelled:
            /// 操作已取消：操作被显式取消。
            returnMessage = "Operation Cancelled: the operation was explicitly canceled."
        case .partialFailure:
            /// 部分失败：部分项目失败，但整体操作成功。
            returnMessage = "Partial Failure: some items failed, but the operation succeeded overall."
        case .participantMayNeedVerification:
            /// 参与者可能需要验证：您不是共享的成员。
            returnMessage = "Participant May Need Verification: you are not a member of the share."
        case .permissionFailure:
            /// 权限失败：使用此应用程序，您必须启用iCloud同步。进入设备设置，登录到iCloud，然后在应用程序设置中，确保iCloud功能已启用。
            returnMessage = "Permission Failure: to use this app, you must enable iCloud syncing. Go to device Settings, sign in to iCloud, then in the app settings, be sure the iCloud feature is enabled."
        case .quotaExceeded:
            /// 配额超过：节省将超过您当前的iCloud存储配额。
            returnMessage = "Quota Exceeded: saving would exceed your current iCloud storage quota."
        case .referenceViolation:
            /// 引用冲突：没有找到记录的父引用或共享引用的目标。
            returnMessage = "Reference Violation: the target of a record's parent or share reference was not found."
        case .requestRateLimited:
            /// 请求速率限制：此时进出服务器的传输都受到速率限制。
            returnMessage = "Request Rate Limited: transfers to and from the server are being rate limited at this time."
        case .serverRecordChanged:
            /// Server Record Changed：由于服务器上的版本不同，记录被拒绝。
            returnMessage = "Server Record Changed: the record was rejected because the version on the server is different."
        case .serverRejectedRequest:
            /// 服务器拒绝请求
            returnMessage = "Server Rejected Request"
        case .serverResponseLost:
            /// 服务器响应丢失
            returnMessage = "Server Response Lost"
        case .serviceUnavailable:
            /// 服务不可用：请重试。
            returnMessage = "Service Unavailable: Please try again."
        case .tooManyParticipants:
            /// 过多参与者：由于已绑定的参与者过多，无法保存该共享。
            returnMessage = "Too Many Participants: a share cannot be saved because too many participants are attached to the share."
        case .unknownItem:
            /// Unknown Item：指定的记录不存在。
            returnMessage = "Unknown Item:  the specified record does not exist."
        case .userDeletedZone:
            /// 用户删除分区：用户已从配置界面中删除该分区。
            returnMessage = "User Deleted Zone: the user has deleted this zone from the settings UI."
        case .zoneBusy:
            /// 区域忙：服务器太忙，无法处理Zone操作。
            returnMessage = "Zone Busy: the server is too busy to handle the zone operation."
        case .zoneNotFound:
            /// 找不到区域：指定的记录区域在服务器上不存在。
            returnMessage = "Zone Not Found: the specified record zone does not exist on the server."
        default:
            /// 未处理的错误
            returnMessage = "Unhandled Error."
        }
        
        return returnMessage + "CKError.Code: \(code.rawValue)"
    }
    
}

extension Array {
    /// 根据给定的块大小，将大组分成较小的组
    /// 比如我们有一些狗(可以在操场上测试一下):
    ///
    /*  var dogs: [Dog] = []
        for i in 0...22 {
        var dog = Dog(age: i, name: "Dog \(i)")
            dogs.append(dog)
        }
        let chunkedDogs = dogs.chunkItUp(by: 5)
    */
    
    func chunkItUp(by chunkSize: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: chunkSize).map({ (startIndex) -> [Element] in
            let endIndex = (startIndex.advanced(by: chunkSize) > count) ? count : (startIndex + chunkSize)
            return Array(self[startIndex..<endIndex])
        })
    }
}

