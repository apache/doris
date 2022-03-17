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

#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "runtime/threadlocal.h"

#define SCOPED_ATTACH_TASK_THREAD(type, ...) \
    auto VARNAME_LINENUM(attach_task_thread) = AttachTaskThread(type, ## __VA_ARGS__)

namespace doris {

class TUniqueId;

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
    ThreadContext() : _thread_id(std::this_thread::get_id()), _type(TaskType::UNKNOWN) {}

    void attach(const TaskType& type, const std::string& task_id,
                const TUniqueId& fragment_instance_id) {
        DCHECK(_type == TaskType::UNKNOWN && _task_id == "");
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
    }

    void detach() {
        _type = TaskType::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
    }

    const std::string type() const;
    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() const { return _thread_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }

private:
    std::thread::id _thread_id;
    TaskType _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
};

// Using gcc11 compiles thread_local variable on lower versions of GLIBC will report an error,
// see https://github.com/apache/incubator-doris/pull/7911
//
// If we want to avoid this error,
// 1. For non-trivial variables in thread_local, such as std::string, you need to store them as pointers to
//    ensure that thread_local is trivial, these non-trivial pointers will uniformly call destructors elsewhere.
// 2. The default destructor of the thread_local variable cannot be overridden.
//
// This is difficult to implement. Because the destructor is not overwritten, it means that the outside cannot
// be notified when the thread terminates, and the non-trivial pointers in thread_local cannot be released in time.
// The func provided by pthread and std::thread doesn't help either.
//
// So, kudu Class-scoped static thread local implementation was introduced. Solve the above problem by
// Thread-scopedthread local + Class-scoped thread local.
//
// This may look very trick, but it's the best way I can find.
//
// refer to:
//  https://gcc.gnu.org/onlinedocs/gcc-3.3.1/gcc/Thread-Local.html
//  https://stackoverflow.com/questions/12049684/
//  https://sourceware.org/glibc/wiki/Destructor%20support%20for%20thread_local%20variables
//  https://www.jianshu.com/p/756240e837dd
//  https://man7.org/linux/man-pages/man3/pthread_tryjoin_np.3.html
class ThreadContextPtr {
public:
    ThreadContextPtr();

    ThreadContext* get();

private:
    DECLARE_STATIC_THREAD_LOCAL(ThreadContext, thread_local_ctx);
};

inline thread_local ThreadContextPtr thread_local_ctx;

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

class AttachTaskThread {
public:
    explicit AttachTaskThread(const ThreadContext::TaskType& type, const std::string& task_id = "",
                              const TUniqueId& fragment_instance_id = TUniqueId()) {
        thread_local_ctx.get()->attach(type, task_id, fragment_instance_id);
    }

    ~AttachTaskThread() { thread_local_ctx.get()->detach(); }
};

} // namespace doris
