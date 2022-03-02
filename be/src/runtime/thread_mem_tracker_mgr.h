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
    std::string action_type;
    bool cancel_task; // Whether to cancel the task when the current tracker exceeds the limit
    ERRCALLBACK call_back_func;

    ConsumeErrCallBackInfo() {
        init();
    }

    ConsumeErrCallBackInfo(std::string action_type, bool cancel_task, ERRCALLBACK call_back_func)
            : action_type(action_type), cancel_task(cancel_task), call_back_func(call_back_func) {}

    void update(std::string new_action_type, bool new_cancel_task, ERRCALLBACK new_call_back_func) {
        action_type = new_action_type;
        cancel_task = new_cancel_task;
        call_back_func = new_call_back_func;
    }

    void init() {
        action_type = "";
        cancel_task = false;
        call_back_func = nullptr;
    }
};

// If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
// Note: After the tracker is stopped, the memory alloc in the consume method should be released in time,
// otherwise the MemTracker statistics will be inaccurate.
// In some cases, we want to turn off thread automatic memory statistics, manually call consume.
// In addition, when ~RootTracker, TCMalloc delete hook release RootTracker will crash.
inline thread_local bool thread_mem_tracker_mgr_init = false;

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
        thread_mem_tracker_mgr_init = true;
    }
    ~ThreadMemTrackerMgr() {
        clear_untracked_mems();
        thread_mem_tracker_mgr_init = false;
    }

    void clear_untracked_mems() {
        for(auto untracked_mem : _untracked_mems) {
            // auto tracker = _mem_trackers[untracked_mem.first].lock();
            if (untracked_mem.second != 0) {
                // if (_mem_trackers[untracked_mem.first]) {
                _mem_trackers[untracked_mem.first]->consume(untracked_mem.second);
                // _mem_trackers[untracked_mem.first]->consume(untracked_mem.second);
                // } else {
                //     DCHECK(_tracker_id == "process");
                //     _root_mem_tracker->consume(untracked_mem.second);
                // }
                // if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
                //     ExecEnv::GetInstance()->new_process_mem_tracker()->consume(untracked_mem.second);
                // }
            }
        }
        _mem_trackers[_tracker_id]->consume(_untracked_mem);
        _untracked_mem = 0;
        // if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
        //     ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem);
        // }
        if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
            ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem2);
            _untracked_mem2 = 0;
        }
    }

    // After attach, the current thread TCMalloc Hook starts to consume/release task mem_tracker
    void attach_task(const std::string& action_type, const std::string& task_id,
                     const TUniqueId& fragment_instance_id,
                     const std::shared_ptr<MemTracker>& mem_tracker);

    void detach_task();

    // Must be fast enough!!!
    // Thread update_tracker may be called very frequently, adding a memory copy will be slow.
    std::string update_tracker(const std::shared_ptr<MemTracker>& mem_tracker);
    std::string update_trackerP(const std::shared_ptr<MemTracker>& mem_tracker);

    void set_tracker_id(const std::string& tracker_id) {
        // DCHECK(_untracked_mem == 0);
        if (tracker_id != _tracker_id) {
            // _untracked_mems[_tracker_id] += _untracked_mem;
            _mem_trackers[_tracker_id]->consume_cache(_untracked_mem);
            // if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
            //     ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem2);
            // }
            _untracked_mem = 0;
            _tracker_id = tracker_id;
        }
        // std::swap(_untracked_mems[_tracker_id], _untracked_mem);

        // _untracked_mem = _untracked_mems[_tracker_id];
        // _untracked_mems[_tracker_id] = 0;
    }

    void set_tracker_idP(const std::string& tracker_id) {
        if (tracker_id != _tracker_id) {
            _mem_trackers[_tracker_id]->consume_cache(_untracked_mem);
            _untracked_mem = 0;
            _tracker_id = tracker_id;
        }
    }

    inline ConsumeErrCallBackInfo update_consume_err_call_back(
        const std::string& action_type, bool cancel_task, ERRCALLBACK call_back_func);

    inline void update_consume_err_call_back(ConsumeErrCallBackInfo& consume_err_call_back) {
        _consume_err_call_back = consume_err_call_back;
    }

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void cache_consume(int64_t size);

    void noncache_consume();

    // Frequent weak_ptr.lock() is expensive
    std::shared_ptr<MemTracker> mem_tracker() {
        // if (_shared_mem_tracker == nullptr || _shared_mem_tracker->id() != _tracker_id) {
        //     _shared_mem_tracker = _mem_trackers[_tracker_id];
        // }
        // if (_mem_trackers[_tracker_id]) {
        return _mem_trackers[_tracker_id];
        // } else {
        //     DCHECK(_tracker_id == "process");
        //     return MemTracker::get_root_tracker();
        // }
    }

private:
    // If tryConsume fails due to task mem tracker exceeding the limit, the task must be canceled
    void exceeded_cancel_task(const std::string& cancel_details);

    void exceeded(int64_t mem_usage, Status st);

private:
    // 避免shared ptr use count 费
    std::unordered_map<std::string, std::shared_ptr<MemTracker>> _mem_trackers;
    // lable + timestamp
    std::string _tracker_id;
    // MemTracker* _process_mem_tracker;

    // Consume size smaller than mem_tracker_consume_min_size_bytes will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    std::unordered_map<std::string, int64_t> _untracked_mems;
    // Cache untracked mem, only update to _untracked_mems when switching mem tracker.
    // Frequent calls to unordered_map _untracked_mems[] in cache_consume will degrade performance.
    int64_t _untracked_mem = 0;
    int64_t _untracked_mem2 = 0;

    ConsumeErrCallBackInfo _consume_err_call_back;

    // Avoid memory allocation in functions and fall into an infinite loop
    std::string _temp_tracker_id;
    ConsumeErrCallBackInfo _temp_consume_err_call_back;
    std::shared_ptr<MemTracker> _temp_task_mem_tracker;

    std::string _task_id;
    TUniqueId _fragment_instance_id;
};

inline std::string ThreadMemTrackerMgr::update_tracker(const std::shared_ptr<MemTracker>& mem_tracker) {
    DCHECK(mem_tracker != nullptr);
    DCHECK(_mem_trackers[_tracker_id]);
    _temp_tracker_id = mem_tracker->id();
    if (_temp_tracker_id == _tracker_id) {
        return _tracker_id;
    }
    if (_mem_trackers.find(_temp_tracker_id) == _mem_trackers.end()) {
        _mem_trackers[_temp_tracker_id] = mem_tracker;
        _untracked_mems[_temp_tracker_id] = 0;
    }
    // _untracked_mems[_tracker_id] += _untracked_mem;
    _mem_trackers[_tracker_id]->consume(_untracked_mem);
    // if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
    //     ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem2);
    // }
    _untracked_mem = 0;
    std::swap(_tracker_id, _temp_tracker_id);
    // if (_mem_trackers.find(_temp_tracker_id) == _mem_trackers.end()) {
    //     _mem_trackers[_temp_tracker_id] = mem_tracker;
    //     _untracked_mems[_tracker_id] += _untracked_mem;
    //     _untracked_mem = 0;
    //     // std::swap(_untracked_mems[_tracker_id], _untracked_mem);
    //     // DCHECK(_untracked_mem == 0);
    //     std::swap(_tracker_id, _temp_tracker_id);
    //     _untracked_mems[_tracker_id] = 0;
    // } else {
    //     // std::swap(_untracked_mems[_tracker_id], _untracked_mem);
    //     // DCHECK(_untracked_mem == 0);
    //     _untracked_mems[_tracker_id] += _untracked_mem;
    //     _untracked_mem = 0;
    //     std::swap(_tracker_id, _temp_tracker_id);
    //     // std::swap(_untracked_mems[_tracker_id], _untracked_mem);
    // }
    DCHECK(_mem_trackers[_temp_tracker_id]);
    return _temp_tracker_id; // old tracker_id
    // return _tracker_id;
}

inline std::string ThreadMemTrackerMgr::update_trackerP(const std::shared_ptr<MemTracker>& mem_tracker) {
    _temp_tracker_id = mem_tracker->id();
    if (_temp_tracker_id == _tracker_id) {
        return _tracker_id;
    }
    if (_mem_trackers.find(_temp_tracker_id) == _mem_trackers.end()) {
        _mem_trackers[_temp_tracker_id] = mem_tracker;
        _untracked_mems[_temp_tracker_id] = 0;
    }
    _mem_trackers[_tracker_id]->consume(_untracked_mem);
    _untracked_mem = 0;
    std::swap(_tracker_id, _temp_tracker_id);
    DCHECK(_mem_trackers[_temp_tracker_id]);
    return _temp_tracker_id; // old tracker_id
    // return _tracker_id;
}

inline ConsumeErrCallBackInfo ThreadMemTrackerMgr::update_consume_err_call_back(
    const std::string& action_type, bool cancel_task, ERRCALLBACK call_back_func) {
    _temp_consume_err_call_back = _consume_err_call_back;
    _consume_err_call_back.update(action_type, cancel_task, call_back_func);
    return _temp_consume_err_call_back;
}

inline void ThreadMemTrackerMgr::cache_consume(int64_t size) {
    // _untracked_mems[_tracker_id] += size;
    _untracked_mem += size;
    _untracked_mem2 += size;
    // When some threads `0 < _untracked_mem < config::mem_tracker_consume_min_size_bytes`
    // and some threads `_untracked_mem <= -config::mem_tracker_consume_min_size_bytes` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    if (_untracked_mem >= config::mem_tracker_consume_min_size_bytes ||
        _untracked_mem <= -config::mem_tracker_consume_min_size_bytes) {
        // DCHECK(_mem_trackers.find(_tracker_id) != _mem_trackers.end());
        thread_mem_tracker_mgr_init = false;
        if (_untracked_mems[_tracker_id] != 0) {
            _untracked_mem += _untracked_mems[_tracker_id];
            _untracked_mems[_tracker_id] = 0;
        }
        noncache_consume();
        // _untracked_mem = 0;
        // _untracked_mem2 = 0;
        thread_mem_tracker_mgr_init = true;
    }

    
    if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
        if (_untracked_mem2 >= config::mem_tracker_consume_min_size_bytes ||
            _untracked_mem2 <= -config::mem_tracker_consume_min_size_bytes) {
            ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem2);
            _untracked_mem2 = 0;
        }
    }
}

inline void ThreadMemTrackerMgr::noncache_consume() {
    // if (ExecEnv::GetInstance()->new_process_mem_tracker()) {
    //     ExecEnv::GetInstance()->new_process_mem_tracker()->consume(_untracked_mem2);
    // }
    // Ensure thread safety
    // auto tracker = _mem_trackers[_tracker_id].lock();
    if (_mem_trackers[_tracker_id]) {
        Status st = _mem_trackers[_tracker_id]->try_consume(_untracked_mem);
        if (!st) {
            // The memory has been allocated, so when TryConsume fails, need to continue to complete
            // the consume to ensure the accuracy of the statistics.
            _mem_trackers[_tracker_id]->consume(_untracked_mem);
            exceeded(_untracked_mem, st);
        }
        _untracked_mem = 0;
    } 
    // else {
    //     DCHECK(_tracker_id == "process");
    //     _mem_trackers["process"] = ExecEnv::GetInstance()->process_mem_tracker();
    //     _root_mem_tracker->consume(_untracked_mems[_tracker_id]);
    // }
}

} // namespace doris
