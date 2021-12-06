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

#include <string>
#include <thread>

#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"

namespace doris {

// The thread context saves some info about a working thread.
// 2 requried info:
//   1. thread_id:   Current thread id, Auto generated.
//   2. type:        The type is a enum value indicating which type of task current thread is running.
//                   For example: QUERY, LOAD, COMPACTION, ...
//   3. task id:     A unique id to identify this task. maybe query id, load job id, etc.
//
// There may be other optional info to be added later.
class ThreadContext {
public:
    enum TaskType {
        UNKNOWN = 0,
        QUERY = 1,
        LOAD = 2,
        COMPACTION = 3
        // to be added ...
    };

public:
    ThreadContext()
            : _thread_id(std::this_thread::get_id()),
              _type(TaskType::UNKNOWN),
              _global_hook_tracker(MemTracker::GetGlobalHookTracker()) {}
    ~ThreadContext() {}

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id = TUniqueId()) {
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        update_query_mem_tracker(task_id);
    }

    void detach() {
        _type = TaskType::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
        update_query_mem_tracker();
    }

    const std::string type() const;
    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() const { return _thread_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    void update_query_mem_tracker(const std::string& query_id) {}
    void update_query_mem_tracker(
            std::weak_ptr<doris::MemTracker> mem_tracker = std::weak_ptr<doris::MemTracker>()) {}
    void query_mem_limit_exceeded(int64_t mem_usage) {}
    void global_mem_limit_exceeded(int64_t mem_usage) {}

    // Note that, If call the memory allocation operation in TCMalloc new/delete Hook,
    // such as calling LOG/iostream/sstream/stringstream/etc. related methods,
    // must increase the control to avoid entering infinite recursion, otherwise it may cause crash or stuck,
    void consume() {}
    void try_consume(int64_t size) {}

    void consume_mem(int64_t size) { try_consume(size); }

    void release_mem(int64_t size) { try_consume(-size); }

    void stop_mem_tracker() { _stop_mem_tracker = true; }

private:
    std::thread::id _thread_id;
    TaskType _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;

    std::weak_ptr<MemTracker> _query_mem_tracker;
    std::shared_ptr<MemTracker> _global_hook_tracker = nullptr;

    // The memory size that is not tracker is used to control batch trackers,
    // avoid frequent consume/release.
    int64_t _untracked_mem = 0;
    int64_t _untracked_mem_limit = config::mem_tracker_consume_min_size_mbytes;

    // Memory size of tracker failure after mem limit exceeded,
    // expect to be successfully consumed later.
    int64_t _missed_query_tracker_mem = 0;
    int64_t _missed_global_tracker_mem = 0;

    // When memory is being consumed, avoid entering infinite recursion.
    bool _query_mem_consuming = false;
    bool _global_mem_consuming = false;

    // In some cases, we want to turn off memory statistics.
    // For example, when ~GlobalHookTracker, TCMalloc delete hook
    // release GlobalHookTracker will crash.
    bool _stop_mem_tracker = false;
};

inline thread_local ThreadContext thread_local_ctx;

inline const std::string task_type_string(ThreadContext::TaskType type) {
    switch (type) {
    case ThreadContext::TaskType::QUERY:
        return "QUERY";
    case ThreadContext::TaskType::LOAD:
        return "LOAD";
    case ThreadContext::TaskType::COMPACTION:
        return "COMPACTION";
    default:
        return "UNKNOWN";
    }
}

inline const std::string ThreadContext::type() const {
    return task_type_string(_type);
}

} // namespace doris
