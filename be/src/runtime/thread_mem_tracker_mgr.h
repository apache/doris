// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <fmt/format.h>

#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/mem_tracker.h"

namespace doris {

typedef void (*ERRCALLBACK)();

struct ConsumeErrCallBackInfo {
    std::string cancel_msg;
    bool cancel_task; // Whether to cancel the task when the current tracker exceeds the limit
    ERRCALLBACK cb_func;

    ConsumeErrCallBackInfo() { init(); }

    ConsumeErrCallBackInfo(const std::string& cancel_msg, bool cancel_task, ERRCALLBACK cb_func)
            : cancel_msg(cancel_msg), cancel_task(cancel_task), cb_func(cb_func) {}

    void init() {
        cancel_msg = "";
        cancel_task = false;
        cb_func = nullptr;
    }
};

// If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
// Note: After the tracker is stopped, the memory alloc in the consume method should be released in time,
// otherwise the MemTracker statistics will be inaccurate.
// In some cases, we want to turn off thread automatic memory statistics, manually call consume.
// In addition, when ~RootTracker, TCMalloc delete hook release RootTracker will crash.
inline thread_local bool start_thread_mem_tracker = false;

// TCMalloc new/delete Hook is counted in the memory_tracker of the current thread.
//
// In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior. Therefore, when alloc for some large memory,
// need to manually call cosume after stop_mem_tracker, and then start_mem_tracker.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() {
        _mem_trackers["process"] = MemTracker::get_process_tracker();
        _untracked_mems["process"] = 0;
        _tracker_id = "process";
        start_thread_mem_tracker = true;
    }
    ~ThreadMemTrackerMgr() {
        clear_untracked_mems();
        start_thread_mem_tracker = false;
    }

    void clear_untracked_mems() {
        for (const auto& untracked_mem : _untracked_mems) {
            if (untracked_mem.second != 0) {
                DCHECK(_mem_trackers[untracked_mem.first]);
                _mem_trackers[untracked_mem.first]->consume(untracked_mem.second);
            }
        }
        _mem_trackers[_tracker_id]->consume(_untracked_mem);
        _untracked_mem = 0;
    }

    // After attach, the current thread TCMalloc Hook starts to consume/release task mem_tracker
    void attach_task(const std::string& cancel_msg, const std::string& task_id,
                     const TUniqueId& fragment_instance_id,
                     const std::shared_ptr<MemTracker>& mem_tracker);

    void detach_task();

    // Must be fast enough!
    // Thread update_tracker may be called very frequently, adding a memory copy will be slow.
    std::string update_tracker(const std::shared_ptr<MemTracker>& mem_tracker);

    void update_tracker_id(const std::string& tracker_id) {
        if (tracker_id != _tracker_id) {
            _untracked_mems[_tracker_id] += _untracked_mem;
            _untracked_mem = 0;
            _tracker_id = tracker_id;
        }
    }

    inline ConsumeErrCallBackInfo update_consume_err_cb(const std::string& cancel_msg,
                                                        bool cancel_task, ERRCALLBACK cb_func) {
        _temp_consume_err_cb = _consume_err_cb;
        _consume_err_cb.cancel_msg = cancel_msg;
        _consume_err_cb.cancel_task = cancel_task;
        _consume_err_cb.cb_func = cb_func;
        return _temp_consume_err_cb;
    }

    inline void update_consume_err_cb(const ConsumeErrCallBackInfo& consume_err_cb) {
        _consume_err_cb = consume_err_cb;
    }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void cache_consume(int64_t size);

    void noncache_consume();

    bool is_attach_task() { return _task_id != ""; }

    std::shared_ptr<MemTracker> mem_tracker() {
        DCHECK(_mem_trackers[_tracker_id]);
        return _mem_trackers[_tracker_id];
    }

private:
    // If tryConsume fails due to task mem tracker exceeding the limit, the task must be canceled
    void exceeded_cancel_task(const std::string& cancel_details);

    void exceeded(int64_t mem_usage, Status st);

private:
    // Cache untracked mem, only update to _untracked_mems when switching mem tracker.
    // Frequent calls to unordered_map _untracked_mems[] in cache_consume will degrade performance.
    int64_t _untracked_mem = 0;

    // May switch back and forth between multiple trackers frequently. If you use a pointer to save the
    // current tracker, and consume the current untracked mem each time you switch, there is a performance problem:
    //  1. The frequent change of the use-count of shared_ptr has a huge cost; (it can also be solved by using
    //  raw pointers, which requires uniform replacement of the pointers of all mem trackers in doris)
    //  2. The cost of calling consume for the current untracked mem is huge;
    // In order to reduce the cost, during an attach task, the untracked mem of all switched trackers is cached,
    // and the untracked mem is consumed only after the upper limit is reached or when the task is detached.
    std::unordered_map<std::string, std::shared_ptr<MemTracker>> _mem_trackers;
    std::string _tracker_id;
    std::unordered_map<std::string, int64_t> _untracked_mems;

    // Avoid memory allocation in functions and fall into an infinite loop
    std::string _temp_tracker_id;
    ConsumeErrCallBackInfo _temp_consume_err_cb;
    std::shared_ptr<MemTracker> _temp_task_mem_tracker;

    std::string _task_id;
    TUniqueId _fragment_instance_id;
    ConsumeErrCallBackInfo _consume_err_cb;
};

inline std::string ThreadMemTrackerMgr::update_tracker(
        const std::shared_ptr<MemTracker>& mem_tracker) {
    DCHECK(mem_tracker);
    _temp_tracker_id = mem_tracker->id();
    if (_temp_tracker_id == _tracker_id) {
        return _tracker_id;
    }
    if (_mem_trackers.find(_temp_tracker_id) == _mem_trackers.end()) {
        _mem_trackers[_temp_tracker_id] = mem_tracker;
        _untracked_mems[_temp_tracker_id] = 0;
    }
    _untracked_mems[_tracker_id] += _untracked_mem;
    _untracked_mem = 0;
    std::swap(_tracker_id, _temp_tracker_id);
    return _temp_tracker_id; // old tracker_id
}

inline void ThreadMemTrackerMgr::cache_consume(int64_t size) {
    _untracked_mem += size;
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    if (_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
        _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) {
        DCHECK(_mem_trackers.find(_tracker_id) != _mem_trackers.end());
        // When switching to the current tracker last time, the remaining untracked memory.
        if (_untracked_mems[_tracker_id] != 0) {
            _untracked_mem += _untracked_mems[_tracker_id];
            _untracked_mems[_tracker_id] = 0;
        }
        // Avoid getting stuck in infinite loop if there is memory allocation in noncache_consume.
        // For example: GC function when try_consume; mem_limit_exceeded.
        start_thread_mem_tracker = false;
        noncache_consume();
        start_thread_mem_tracker = true;
    }
}

inline void ThreadMemTrackerMgr::noncache_consume() {
    DCHECK(_mem_trackers[_tracker_id]);
    Status st = _mem_trackers[_tracker_id]->try_consume(_untracked_mem);
    if (!st) {
        // The memory has been allocated, so when TryConsume fails, need to continue to complete
        // the consume to ensure the accuracy of the statistics.
        _mem_trackers[_tracker_id]->consume(_untracked_mem);
        exceeded(_untracked_mem, st);
    }
    _untracked_mem = 0;
}

} // namespace doris
