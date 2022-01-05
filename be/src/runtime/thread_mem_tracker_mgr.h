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
    std::string action_name;
    bool cancel_task;
    ERRCALLBACK call_back_func;

    ConsumeErrCallBackInfo(std::string action_name, bool cancel_task, ERRCALLBACK call_back_func)
            : action_name(action_name), cancel_task(cancel_task), call_back_func(call_back_func) {}
};

// TCMalloc new/delete Hook is counted in the memory_tracker of the current thread.
//
// In the original design, the MemTracker consume method is called before the memory is allocated.
// If the consume succeeds, the memory is actually allocated, otherwise an exception is thrown.
// But the statistics of memory through TCMalloc new/delete Hook are after the memory is actually allocated,
// which is different from the previous behavior. Therefore, when alloc for some large memory,
// need to manually call cosume after stop_mem_tracker, and then start_mem_tracker.
class ThreadMemTrackerMgr {
public:
    ThreadMemTrackerMgr() : _mem_tracker(default_mem_tracker()) {}
    ~ThreadMemTrackerMgr() { detach(); }

    std::shared_ptr<MemTracker> default_mem_tracker();

    // After attach, the current thread TCMalloc Hook starts to consume/release query mem_tracker
    void attach_query(const std::string& query_id, const TUniqueId& fragment_instance_id);

    void detach();

    std::weak_ptr<MemTracker> update_tracker(std::weak_ptr<MemTracker> mem_tracker);
    std::shared_ptr<ConsumeErrCallBackInfo> update_consume_err_call_back(
            const std::string& action_name, bool cancel_task, ERRCALLBACK call_back_func);
    std::shared_ptr<ConsumeErrCallBackInfo> update_consume_err_call_back(
            std::shared_ptr<ConsumeErrCallBackInfo> consume_err_call_back);

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void cache_consume(int64_t size);

    void noncache_consume();

    std::weak_ptr<MemTracker> mem_tracker() { return _mem_tracker; }
    void stop_mem_tracker() { _stop_mem_tracker = true; }
    void start_mem_tracker() { _stop_mem_tracker = false; }

private:
    void exceeded_cancel_query();

    void exceeded(Status st, int64_t mem_usage);

private:
    std::weak_ptr<MemTracker> _mem_tracker;

    // Consume size smaller than _tracker_consume_cache_size will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    int64_t _untracked_mem = 0;
    int64_t _tracker_consume_cache_size = config::mem_tracker_consume_min_size_bytes;

    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    // Note: After the tracker is stopped, the memory alloc in the consume method should be released in time,
    // otherwise the MemTracker statistics will be inaccurate.
    // In some cases, we want to turn off thread automatic memory statistics, manually call consume.
    // In addition, when ~RootTracker, TCMalloc delete hook release RootTracker will crash.
    bool _stop_mem_tracker = false;

    std::shared_ptr<ConsumeErrCallBackInfo> _consume_err_call_back;

    std::string _query_id;
    TUniqueId _fragment_instance_id;
};

} // namespace doris
