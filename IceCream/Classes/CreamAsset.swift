//
//  CreamAsset.swift
//  IceCream
//
//  Created by Fu Yuan on 7/01/18.
//

import Foundation
import RealmSwift
import Realm
import CloudKit

let ASSET_EXTENSION = "asset_extension"
let ASSET_SHOULD_OVERWRITE = "asset_shouldOvertwrite"

/// 如果你想自动存储和同步大数据，那么使用CreamAsset可能是个不错的选择。
/// 根据苹果公司的说法:https://developer.apple.com/documentation/cloudkit/ckasset
/// "您还可以在要分配给字段的数据超过几千字节的地方使用资产。"
/// 根据刚铎realm的说法:https://realm.io/docs/objc/latest/#current-limitations
/// "数据和字符串属性不能容纳超过16MB的数据。要存储更大数量的数据，要么将其分成16MB的块，要么直接存储在文件系统上，在领域中存储这些文件的路径。如果您的应用程序试图在单个属性中存储超过16MB的内容，将在运行时引发异常。
/// 我们选择后者，即直接存储在文件系统中，在领域中存储这些文件的路径。
/// 所以这是交易。
public class CreamAsset: Object {
    @Persisted private var uniqueFileName = ""
    @Persisted var fileExtension: String? = nil
    @Persisted var shouldOverwrite = true
    
    override public static func ignoredProperties() -> [String] {
        return ["filePath"]
    }
    
    private convenience init(objectID: String, propName: String, shouldOverwrite: Bool, fileExtension: String? = nil) {
        self.init()
        self.shouldOverwrite = shouldOverwrite
        
        if let ext = fileExtension {
            self.fileExtension = ext
            self.uniqueFileName = "\(objectID)_\(propName).\(ext)"
        } else {
            self.uniqueFileName = "\(objectID)_\(propName)"
        }
    }
    
    /// 使用此方法获取CreamAsset的基础数据
    public func storedData() -> Data? {
        return try? Data(contentsOf: filePath)
    }
    
    /// 资产在文件系统中的位置
    public var filePath: URL {
        return CreamAsset.creamAssetDefaultURL().appendingPathComponent(uniqueFileName)
    }
    
    /// 将给定的数据保存到本地文件系统
    /// -参数:
    /// -数据:要保存的数据
    /// -路径:
    /// - shouldOverwrite:是否应该覆盖路径中存在的当前文件。
    private static func save(data: Data, to path: String, shouldOverwrite: Bool) throws {
        let url = CreamAsset.creamAssetDefaultURL().appendingPathComponent(path)
        guard shouldOverwrite || !FileManager.default.fileExists(atPath: url.path) else { return }
        try data.write(to: url)
    }
    
    // MARK: - CKRecordConvertible & CKRecordRecoverable
    
    /// 将资产包装为CKAsset以上传到CloudKit
    var asset: CKAsset {
        get {
            return CKAsset(fileURL: filePath)
        }
    }
    
    /// 将CKRecord和CKAsset解析回CreamAsset
    /// - 参数:
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - record:CKRecord，我们将从中提取记录ID来定位/存储文件
    /// - asset:CKAsset，我们将从中提取用于创建资产的URL
    /// - 返回:如果成功，则返回CreamAsset
    static func parse(from propName: String, record: CKRecord, asset: CKAsset) -> CreamAsset? {
        guard let url = asset.fileURL else { return nil }
        let fileExtension = record.value(forKey: ASSET_EXTENSION) as? String
        let shouldOverwrite = record.value(forKey: ASSET_SHOULD_OVERWRITE) as? Bool
        return CreamAsset.create(objectID: record.recordID.recordName,
                                 propName: propName,
                                 url: url,
                                 shouldOverwrite: shouldOverwrite ?? true,
                                 fileExtension: fileExtension)
    }
    
    // MARK: - Factory methods
    
    /// 用数据为给定的对象id创建新的CreamAsset
    ///
    /// - 参数:
    /// - objectID:标识资产的objectID(领域对象的关键属性)
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - 数据:文件数据
    /// - shouldOverwrite:即使存在具有相同对象ID的文件，是否尝试保存文件。
    /// - 返回:如果成功，则返回CreamAsset
    public static func create(objectID: String, propName: String, data: Data, shouldOverwrite: Bool = true, fileExtension: String? = nil) -> CreamAsset? {
        let creamAsset = CreamAsset(objectID: objectID,
                                    propName: propName,
                                    shouldOverwrite: shouldOverwrite,
                                    fileExtension: fileExtension)
        do {
            try save(data: data, to: creamAsset.uniqueFileName, shouldOverwrite: shouldOverwrite)
            return creamAsset
        } catch {
            // 此处出现Os.log错误
            return nil
        }
    }
    
    /// 为带有数据的给定对象创建新的CreamAsset
    ///
    /// - 参数:
    /// - 对象:资产将存在于其上的对象
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - 数据:文件数据
    /// - shouldOverwrite:即使同一个对象已有文件，是否尝试保存文件。
    /// - 返回:如果成功，则返回CreamAsset
    public static func create(object: CKRecordConvertible, propName: String, data: Data, shouldOverwrite: Bool = true, fileExtension: String? = nil) -> CreamAsset? {
        return create(objectID: object.recordID.recordName,
                      propName: propName,
                      data: data,
                      shouldOverwrite: shouldOverwrite,
                      fileExtension: fileExtension)
    }
    
    /// 为具有URL的给定对象创建新的CreamAsset
    ///
    /// - 参数:
    /// - 对象:资产将存在于其上的对象
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - url:文件所在的url
    /// - shouldOverwrite:即使同一个对象已有文件，是否尝试保存文件。
    /// - keepExtension:是否应该保留文件扩展名
    /// - 返回:如果成功，则返回CreamAsset
    public static func create(object: CKRecordConvertible, propName: String, url: URL, shouldOverwrite: Bool = true, keepExtension: Bool = false) -> CreamAsset? {
        return create(objectID: object.recordID.recordName,
                      propName: propName,
                      url: url,
                      shouldOverwrite: shouldOverwrite,
                      fileExtension: keepExtension ? url.pathExtension : nil)
    }
    
    /// 使用资产所在的URL为给定的objectID创建新的CreamAsset
    /// - 参数:
    /// - objectID:标识对象的键。通常它是CKRecord的recordName属性。从CloudKit恢复时的ID
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - url:资产所在的位置
    /// - shouldOverwrite:是否尝试保存文件，即使同一对象存在现有文件。
    /// - keepExtension:是否应该保留文件扩展名
    /// - 返回:如果创建成功，则返回CreamAsset
    public static func create(objectID: String, propName: String, url: URL, shouldOverwrite: Bool = true, keepExtension: Bool = false) -> CreamAsset? {
        return create(objectID: objectID,
                      propName: propName,
                      url: url,
                      shouldOverwrite: shouldOverwrite,
                      fileExtension: keepExtension ? url.pathExtension : nil)
    }
    
    /// 使用资产所在的URL为给定的objectID创建新的CreamAsset
    /// - 参数:
    /// - objectID:标识对象的键。通常它是CKRecord的recordName属性。从CloudKit恢复时的ID
    /// - propName:标识该资产的唯一属性名。例如:Dog对象可能有多个CreamAsset属性，因此我们需要唯一的“属性名”来标识这些属性。
    /// - url:资产所在的位置
    /// - shouldOverwrite:是否尝试保存文件，即使同一对象存在现有文件。
    /// - fileExtension:附加到文件名的文件扩展名
    /// - 返回:如果创建成功，则返回CreamAsset
    public static func create(objectID: String, propName: String, url: URL, shouldOverwrite: Bool = true, fileExtension: String? = nil) -> CreamAsset? {
        let creamAsset = CreamAsset(objectID: objectID, propName: propName, shouldOverwrite: shouldOverwrite, fileExtension: fileExtension)
        if shouldOverwrite {
            do {
                try FileManager.default.removeItem(at: creamAsset.filePath)
            } catch {
                // Os.log remove item failed error here
            }
        }
        if !FileManager.default.fileExists(atPath: creamAsset.filePath.path) {
            do {
                try FileManager.default.copyItem(at: url, to: creamAsset.filePath)
            } catch {
                /// Os.log copy item failed
                return nil
            }
        }
        return creamAsset
    }
}

extension CreamAsset {
    /// 存储CreamAsset的默认路径。那就是:
    /// xxx/Document/CreamAsset/
    public static func creamAssetDefaultURL() -> URL {
        let documentDir = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        let commonAssetPath = documentDir.appendingPathComponent(className())
        if !FileManager.default.fileExists(atPath: commonAssetPath.path) {
            do {
                try FileManager.default.createDirectory(atPath: commonAssetPath.path, withIntermediateDirectories: false, attributes: nil)
            } catch {
                /// Log: create directory failed
            }
        }
        return commonAssetPath
    }
    
    /// 获取所有CreamAsset文件的路径
    public static func creamAssetFilesPaths() -> [String] {
        do {
            return try FileManager.default.contentsOfDirectory(atPath: CreamAsset.creamAssetDefaultURL().path)
        } catch {
            
        }
        return [String]()
    }
    
    /// 执行删除
    private static func excecuteDeletions(in filesNames: [String]) {
        for fileName in filesNames {
            let absolutePath = CreamAsset.creamAssetDefaultURL().appendingPathComponent(fileName).path
            do {
                try FileManager.default.removeItem(atPath: absolutePath)
            } catch {
                /// Log: remove item failed at given path
            }
        }
    }
    
    /// 删除对象时。我们需要删除相关的CreamAsset文件
    public static func deleteCreamAssetFile(with id: String) {
        let needToDeleteCacheFiles = creamAssetFilesPaths().filter { $0.contains(id) }
        excecuteDeletions(in: needToDeleteCacheFiles)
    }
    
}
