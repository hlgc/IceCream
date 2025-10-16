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

//final class BackgroundWorker {
//    static let shared = BackgroundWorker()
//
//    // 串行队列，保证任务按顺序执行
//    private let queue: OperationQueue = {
//        let q = OperationQueue()
//        q.name = "com.icecream.BackgroundWorker"
//        q.maxConcurrentOperationCount = 1
//        return q
//    }()
//
//    // 幂等：每次 start 只入队，不覆盖其他调用
//    func start(_ block: @escaping () -> Void) {
//        queue.addOperation(block)
//    }
//
//    // 停止：取消并清空队列
//    func stop() {
//        queue.cancelAllOperations()
//    }
//}

// 串行任务 + 去抖合并：避免并发覆盖与频繁触发
final class BackgroundWorker {
    static let shared = BackgroundWorker()

    // 串行执行队列（OperationQueue 保证严格顺序）
    private let opQueue: OperationQueue = {
        let q = OperationQueue()
        q.name = "com.icecream.BackgroundWorker.queue"
        q.maxConcurrentOperationCount = 1
        q.qualityOfService = .utility
        return q
    }()

    // 去抖器：短时间多次 start 合并为一次
    private let debouncer = Debouncer(interval: 0.3, queue: DispatchQueue(label: "com.icecream.BackgroundWorker.debounce"))

    // 可选：外部查询是否有任务在跑
    var isRunning: Bool { !opQueue.operations.isEmpty }

    // 入队任务（幂等）：短时间内多次调用只触发一次
    func start(_ block: @escaping () -> Void) {
        debouncer.call { [weak self] in
            guard let self = self else { return }
            self.opQueue.addOperation {
                autoreleasepool { block() } // 每个任务独立内存域，适配 Realm
            }
        }
    }

    // 停止：取消并清空队列，清理去抖中的任务
    func stop() {
        debouncer.cancel()
        opQueue.cancelAllOperations()
    }
}

// 简单去抖器实现
private final class Debouncer {
    private let interval: TimeInterval
    private let queue: DispatchQueue
    private var workItem: DispatchWorkItem?
    private let lock = NSLock()

    init(interval: TimeInterval, queue: DispatchQueue) {
        self.interval = interval
        self.queue = queue
    }

    func call(_ block: @escaping () -> Void) {
        lock.lock()
        workItem?.cancel()
        let item = DispatchWorkItem(block: block)
        workItem = item
        lock.unlock()
        queue.asyncAfter(deadline: .now() + interval, execute: item)
    }

    func cancel() {
        lock.lock()
        workItem?.cancel()
        workItem = nil
        lock.unlock()
    }
}
