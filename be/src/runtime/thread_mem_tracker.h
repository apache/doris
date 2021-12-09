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

// TCMalloc new/delete Hook is counted in the memory_tracker of the current thread
class ThreadMemTracker {
public:
    ThreadMemTracker() : _global_hook_tracker(MemTracker::GetGlobalHookTracker()) {}
    ~ThreadMemTracker() { detach_query(); }

    // After attach, the current thread TCMalloc Hook starts to consume/release query mem_tracker
    void attach_query(const std::string& query_id, const TUniqueId& fragment_instance_id);

    void detach_query();

    void update_query_mem_tracker(std::weak_ptr<MemTracker> mem_tracker);

    void try_consume(int64_t size);

    void stop_mem_tracker() { _stop_mem_tracker = true; }

private:
    void query_mem_limit_exceeded(int64_t mem_usage);

    void global_mem_limit_exceeded(int64_t mem_usage);

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void consume();

private:
    std::string _query_id;
    TUniqueId _fragment_instance_id;

    std::weak_ptr<MemTracker> _query_mem_tracker;
    std::shared_ptr<MemTracker> _global_hook_tracker = nullptr;

    // Consume size smaller than _tracker_consume_cache_size will continue to accumulate
    // to avoid frequent calls to consume/release of MemTracker.
    int64_t _untracked_mem = 0;
    int64_t _tracker_consume_cache_size = config::mem_tracker_consume_min_size_bytes;

    // If there is a memory new/delete operation in the consume method, it may enter infinite recursion.
    // Note: After the tracker is stopped, the memory alloc in the consume method should be released in time,
    // otherwise the MemTracker statistics will be inaccurate.
    bool _stop_query_mem_tracker = false;
    bool _stop_global_mem_tracker = false;

    // In some cases, we want to turn off memory statistics.
    // For example, when ~GlobalHookTracker, TCMalloc delete hook
    // release GlobalHookTracker will crash.
    bool _stop_mem_tracker = false;

    // Control the interval of printing Log.
    int64_t global_exceeded_interval = 0;
};

} // namespace doris
