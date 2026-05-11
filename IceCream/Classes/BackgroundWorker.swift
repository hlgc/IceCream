//
//  BackgroundWorker.swift
//  IceCream
//
//  Created by Kit Forge on 5/9/19.
//

import Foundation
import RealmSwift

// Based on https://academy.realm.io/posts/realm-notifications-on-background-threads-with-swift/
// Tweaked a little by Yue Cai

//class BackgroundWorker: NSObject {
//
//    static let shared = BackgroundWorker()
//
//    private var thread: Thread?
//    private var block: (() -> Void)?
//
//    func start(_ block: @escaping () -> Void) {
//        self.block = block
//
//        if thread == nil {
//            thread = Thread { [weak self] in
//                guard let self = self, let thread = self.thread else {
//                    Thread.exit()
//                    return
//                }
//                while !thread.isCancelled {
//                    RunLoop.current.run(
//                        mode: .default,
//                        before: Date.distantFuture)
//                }
//                Thread.exit()
//            }
//            thread?.name = "\(String(describing: self))-\(UUID().uuidString)"
//            thread?.start()
//        }
//
//        if let thread = thread {
//            perform(#selector(runBlock),
//                    on: thread,
//                    with: nil,
//                    waitUntilDone: true,
//                    modes: [RunLoop.Mode.default.rawValue])
//        }
//    }
//
//    func stop() {
//        thread?.cancel()
//    }
//
//    @objc private func runBlock() {
//        block?()
//    }
//}

#if canImport(UIKit)
import UIKit
#endif

final class BackgroundWorker: NSObject {
    static let shared = BackgroundWorker()

    private var thread: Thread?
    private let lock = NSLock()
    private var tasks: [() -> Void] = []

#if os(iOS) || os(tvOS)
    // 后台任务 ID
    private var bgTaskID: UIBackgroundTaskIdentifier = .invalid
#endif

    func start(_ block: @escaping () -> Void) {
        lock.lock()
        let wasEmpty = tasks.isEmpty
        tasks.append(block)
        lock.unlock()
        
        ensureThread()
        // 申请后台时间，避免挂起导致不执行
        beginBGTaskIfNeeded()
        
        if wasEmpty, let thread = thread {
            perform(#selector(processNext),
                    on: thread,
                    with: nil,
                    waitUntilDone: false,
                    modes: [RunLoop.Mode.default.rawValue])
        }
    }

    func stop() {
        lock.lock()
        tasks.removeAll()
        let t = thread
        thread = nil
        lock.unlock()
        
        t?.cancel()
        endBGTaskIfNeeded()
    }

    private func ensureThread() {
        // 用 lock 整体保护，避免多线程并发进入时创建多个后台线程
        lock.lock()
        guard thread == nil else {
            lock.unlock()
            return
        }
        let t = Thread { [weak self] in
            guard let self = self else { return }
            let rl = RunLoop.current
            rl.add(Port(), forMode: .default)
            while !Thread.current.isCancelled {
                rl.run(mode: .default, before: Date.distantFuture)
            }
            Thread.exit()
        }
        t.name = "com.icecream.BackgroundWorker.\(UUID().uuidString)"
        thread = t
        lock.unlock()
        t.start()
    }

    @objc private func processNext() {
        while true {
            lock.lock()
            let task = tasks.isEmpty ? nil : tasks.removeFirst()
            lock.unlock()
            
            guard let task = task else { break }
            
            autoreleasepool {
                task()
            }
        }
        // 所有任务完成后结束后台任务
        endBGTaskIfNeeded()
    }

    // MARK: - Background task

    private func beginBGTaskIfNeeded() {
#if os(iOS) || os(tvOS)
        lock.lock()
        defer { lock.unlock() }
        guard bgTaskID == .invalid else { return }
        // 注意：如果在 App Extension 中使用，UIApplication.shared 不可用，需视情况处理
        bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "IceCreamBackgroundWorker") { [weak self] in
            // 到期处理：结束任务，持久化未执行队列以便下次启动补偿
            self?.endBGTaskIfNeeded()
            // TODO: 将 tasks 保存到本地，App 下次启动时恢复执行
        }
#endif
    }

    private func endBGTaskIfNeeded() {
#if os(iOS) || os(tvOS)
        lock.lock()
        defer { lock.unlock() }
        if bgTaskID != .invalid {
            UIApplication.shared.endBackgroundTask(bgTaskID)
            bgTaskID = .invalid
        }
#endif
    }
}

// 说明（放到你的 CKOperation 创建处）：
// 对 CloudKit 上传操作设置长时操作，系统可在 App 退出后继续执行，并在下次启动恢复。
// 示例：
// let op = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: nil)
// op.configuration.isLongLived = true
// CKContainer.default().add(op)
// 下次启动：CKContainer.default().fetchAllLongLivedOperationIDs { ids, _ in /* 恢复 */ }

