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

#include <bthread/bthread.h>

#include "io/fs/file_system.h"
#include "olap/olap_define.h"
#include "work_thread_pool.hpp"

namespace doris {

struct AsyncIOCtx {
    int nice;
};

/**
 * Separate task from bthread to pthread, specific for IO task.
 */
class AsyncIO {
public:
    AsyncIO() {
        _io_thread_pool = new PriorityThreadPool(config::doris_scanner_thread_pool_thread_num,
                                                 config::doris_scanner_thread_pool_queue_size,
                                                 "async_io_thread_pool");
        _remote_thread_pool = new PriorityThreadPool(
                config::doris_remote_scanner_thread_pool_thread_num,
                config::doris_remote_scanner_thread_pool_queue_size, "async_remote_thread_pool");
    }

    ~AsyncIO() {
        SAFE_DELETE(_io_thread_pool);
        SAFE_DELETE(_remote_thread_pool);
    }

    AsyncIO& operator=(const AsyncIO&) = delete;
    AsyncIO(const AsyncIO&) = delete;

    static AsyncIO& instance() {
        static AsyncIO instance;
        return instance;
    }

    // This function should run on the bthread, and it will put the task into
    // thread_pool and release the bthread_worker at cv.wait. When the task is completed,
    // the bthread will continue to execute.
    static void run_task(std::function<void()> fn, io::FileSystemType file_type) {
        DCHECK(bthread_self() != 0);
        std::mutex mutex;
        std::condition_variable cv;
        std::unique_lock l(mutex);

        AsyncIOCtx* ctx = static_cast<AsyncIOCtx*>(bthread_getspecific(btls_io_ctx_key));
        int nice = -1;
        if (ctx == nullptr) {
            nice = 18;
        } else {
            nice = ctx->nice;
        }

        PriorityThreadPool::Task task;
        task.priority = nice;
        task.work_function = [&] {
            fn();
            std::unique_lock l(mutex);
            cv.notify_one();
        };

        if (file_type == io::FileSystemType::LOCAL) {
            AsyncIO::instance().io_thread_pool()->offer(task);
        } else {
            AsyncIO::instance().remote_thread_pool()->offer(task);
        }
        cv.wait(l);
    }

    inline static bthread_key_t btls_io_ctx_key;

    static void io_ctx_key_deleter(void* d) { delete static_cast<AsyncIOCtx*>(d); }

private:
    PriorityThreadPool* _io_thread_pool = nullptr;
    PriorityThreadPool* _remote_thread_pool = nullptr;

private:
    PriorityThreadPool* io_thread_pool() { return _io_thread_pool; }
    PriorityThreadPool* remote_thread_pool() { return _remote_thread_pool; }
};

} // end namespace doris
