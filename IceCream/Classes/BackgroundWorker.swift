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

import UIKit // 仅 iOS

final class BackgroundWorker: NSObject {
    static let shared = BackgroundWorker()

    private var thread: Thread?
    private let lock = NSLock()
    private var tasks: [() -> Void] = []

    // 后台任务 ID
    private var bgTaskID: UIBackgroundTaskIdentifier = .invalid

    func start(_ block: @escaping () -> Void) {
        enqueue(block)
        ensureThread()
        // 申请后台时间，避免挂起导致不执行
        beginBGTaskIfNeeded()
        if let thread = thread {
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
        lock.unlock()
        thread?.cancel()
        thread = nil
        endBGTaskIfNeeded()
    }

    private func enqueue(_ block: @escaping () -> Void) {
        lock.lock()
        tasks.append(block)
        lock.unlock()
    }

    private func ensureThread() {
        guard thread == nil else { return }
        let t = Thread { [weak self] in
            guard let self = self else { return }
            let rl = RunLoop.current
            rl.add(Port(), forMode: .default)
            while let th = self.thread, !th.isCancelled {
                rl.run(mode: .default, before: Date.distantFuture)
            }
            Thread.exit()
        }
        t.name = "com.icecream.BackgroundWorker.\(UUID().uuidString)"
        thread = t
        t.start()
    }

    @objc private func processNext() {
        var task: (() -> Void)?
        lock.lock()
        if !tasks.isEmpty { task = tasks.removeFirst() }
        lock.unlock()

        if let task = task {
            autoreleasepool {
                task()
            }
            // 若还有任务，继续调度
            lock.lock()
            let hasMore = !tasks.isEmpty
            lock.unlock()
            if hasMore, let thread = thread {
                perform(#selector(processNext),
                        on: thread,
                        with: nil,
                        waitUntilDone: false,
                        modes: [RunLoop.Mode.default.rawValue])
            } else {
                // 所有任务完成后结束后台任务
                endBGTaskIfNeeded()
            }
        } else {
            endBGTaskIfNeeded()
        }
    }

    // MARK: - Background task

    private func beginBGTaskIfNeeded() {
        guard bgTaskID == .invalid else { return }
        bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "IceCreamBackgroundWorker") { [weak self] in
            // 到期处理：结束任务，持久化未执行队列以便下次启动补偿
            self?.endBGTaskIfNeeded()
            // TODO: 将 tasks 保存到本地，App 下次启动时恢复执行
        }
    }

    private func endBGTaskIfNeeded() {
        if bgTaskID != .invalid {
            UIApplication.shared.endBackgroundTask(bgTaskID)
            bgTaskID = .invalid
        }
    }
}

// 说明（放到你的 CKOperation 创建处）：
// 对 CloudKit 上传操作设置长时操作，系统可在 App 退出后继续执行，并在下次启动恢复。
// 示例：
// let op = CKModifyRecordsOperation(recordsToSave: records, recordIDsToDelete: nil)
// op.configuration.isLongLived = true
// CKContainer.default().add(op)
// 下次启动：CKContainer.default().fetchAllLongLivedOperationIDs { ids, _ in /* 恢复 */ }

