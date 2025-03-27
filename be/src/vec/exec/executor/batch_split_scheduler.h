//#pragma once
//
//#include <atomic>
//#include <memory>
//#include <mutex>
//#include <condition_variable>
//#include <thread>
//#include <queue>
//
//#include "common/status.h"
//#include "vec/exec/scan/split_source_connector.h"
//#include "vec/exec/scan/vfile_scanner.h"
//#include "runtime/runtime_state.h"
//
//namespace doris::vectorized {
//
//// BatchSplitScheduler 负责在独立线程中获取文件分片并动态创建 VFileScanner
//class BatchSplitScheduler {
//public:
//    BatchSplitScheduler(std::shared_ptr<SplitSourceConnector> split_source,
//                       RuntimeState* state,
//                       const TFileScanNode& scan_node)
//        : _split_source(split_source)
//        , _state(state)
//        , _scan_node(scan_node)
//        , _running(false)
//        , _stop(false) {}
//
//    ~BatchSplitScheduler() {
//        stop();
//    }
//
//    Status start() {
//        if (_running) {
//            return Status::OK();
//        }
//        _running = true;
//        _stop = false;
//        _fetch_thread = std::thread(&BatchSplitScheduler::_fetch_loop, this);
//        return Status::OK();
//    }
//
//    void stop() {
//        if (!_running) {
//            return;
//        }
//        {
//            std::lock_guard<std::mutex> l(_lock);
//            _stop = true;
//            _cv.notify_one();
//        }
//        if (_fetch_thread.joinable()) {
//            _fetch_thread.join();
//        }
//        _running = false;
//    }
//
//    // 获取一个可用的 scanner
//    Status get_next_scanner(std::shared_ptr<VFileScanner>* scanner) {
//        std::unique_lock<std::mutex> l(_lock);
//        while (_scanners.empty() && !_stop && _running) {
//            _cv.wait(l);
//        }
//
//        if (_stop || !_running) {
//            return Status::Cancelled("BatchSplitScheduler stopped");
//        }
//
//        if (!_scanners.empty()) {
//            *scanner = _scanners.front();
//            _scanners.pop();
//            return Status::OK();
//        }
//
//        return Status::EndOfFile("No more scanners");
//    }
//
//    // 通知当前 scanner 已完成，可以获取新的分片
//    void notify_scanner_finished() {
//        std::lock_guard<std::mutex> l(_lock);
//        _scanner_finished = true;
//        _cv.notify_one();
//    }
//
//private:
//    void _fetch_loop() {
//        while (true) {
//            {
//                std::unique_lock<std::mutex> l(_lock);
//                _cv.wait(l, [this]() {
//                    return _stop || _scanner_finished || _scanners.empty();
//                });
//                if (_stop) {
//                    break;
//                }
//                _scanner_finished = false;
//            }
//
//            // 获取新的分片并创建 scanner
//            bool has_next = false;
//            TFileScanRange range;
//            auto status = _split_source->get_next(&has_next, &range);
//            if (!status.ok()) {
//                LOG(WARNING) << "Failed to get next split: " << status;
//                continue;
//            }
//
//            if (!has_next) {
//                stop();
//                break;
//            }
//
//            // 创建新的 scanner
//            auto scanner = std::make_shared<VFileScanner>(_state, _scan_node);
//            status = scanner->init(_state, _scan_node, {range}, nullptr, nullptr);
//            if (!status.ok()) {
//                LOG(WARNING) << "Failed to init scanner: " << status;
//                continue;
//            }
//
//            {
//                std::lock_guard<std::mutex> l(_lock);
//                _scanners.push(scanner);
//                _cv.notify_one();
//            }
//        }
//    }
//
//private:
//    std::shared_ptr<SplitSourceConnector> _split_source;
//    RuntimeState* _state;
//    TFileScanNode _scan_node;
//    std::atomic<bool> _running;
//    std::atomic<bool> _stop;
//    std::atomic<bool> _scanner_finished{true}; // 初始为 true 以获取第一批分片
//    std::mutex _lock;
//    std::condition_variable _cv;
//    std::thread _fetch_thread;
//    std::queue<std::shared_ptr<VFileScanner>> _scanners;
//};
//
//} // namespace doris::vectorized
