import Foundation
import RealmSwift

#if canImport(UIKit)
import UIKit
#endif

private class BlockWrapper: NSObject {
    let block: () -> Void
    init(_ block: @escaping () -> Void) {
        self.block = block
    }
    @objc func run() {
        autoreleasepool {
            block()
        }
    }
}

final class BackgroundWorker: NSObject {
    static let shared = BackgroundWorker()

    private var thread: Thread?
    private let lock = NSLock()

#if os(iOS) || os(tvOS)
    private var bgTaskID: UIBackgroundTaskIdentifier = .invalid
#endif

    func start(_ block: @escaping () -> Void) {
        ensureThread()
        beginBGTaskIfNeeded()
        
        if let thread = thread {
            let wrapper = BlockWrapper(block)
            perform(#selector(BlockWrapper.run),
                    on: thread,
                    with: nil,
                    waitUntilDone: true,
                    modes: [RunLoop.Mode.default.rawValue])
        }
        
        endBGTaskIfNeeded()
    }

    func stop() {
        lock.lock()
        let t = thread
        thread = nil
        lock.unlock()
        
        t?.cancel()
        endBGTaskIfNeeded()
    }

    private func ensureThread() {
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

    private func beginBGTaskIfNeeded() {
#if os(iOS) || os(tvOS)
        lock.lock()
        defer { lock.unlock() }
        guard bgTaskID == .invalid else { return }
        bgTaskID = UIApplication.shared.beginBackgroundTask(withName: "IceCreamBackgroundWorker") { [weak self] in
            self?.endBGTaskIfNeeded()
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
