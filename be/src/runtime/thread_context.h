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
#include "runtime/thread_mem_tracker.h"

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
    ThreadContext() : _thread_id(std::this_thread::get_id()), _type(TaskType::UNKNOWN) {
        _thread_mem_tracker.reset(new ThreadMemTracker());
    }
    ~ThreadContext() {}

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id = TUniqueId()) {
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
        if (type == TaskType::QUERY) {
            _thread_mem_tracker->attach_query(task_id, fragment_instance_id);
        }
    }

    void detach() {
        if (_type == TaskType::QUERY) {
            _thread_mem_tracker->detach_query();
        }
        _type = TaskType::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
    }

    const std::string type() const;
    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() const { return _thread_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

    void consume_mem(int64_t size) {
        if (_thread_mem_tracker != nullptr) {
            _thread_mem_tracker->try_consume(size);
        }
    }
    void release_mem(int64_t size) {
        if (_thread_mem_tracker != nullptr) {
            _thread_mem_tracker->try_consume(-size);
        }
    }
    void stop_mem_tracker() { _thread_mem_tracker->stop_mem_tracker(); }

private:
    std::thread::id _thread_id;
    TaskType _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;

    // After _thread_mem_tracker is initialized,
    // the current thread TCMalloc Hook starts to consume/release mem_tracker
    std::unique_ptr<ThreadMemTracker> _thread_mem_tracker;
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
