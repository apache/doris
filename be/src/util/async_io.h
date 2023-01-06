#pragma once

#include <bthread/bthread.h>

#include "io/fs/file_system.h"
#include "olap/olap_define.h"
#include "priority_thread_pool.hpp"
#include "runtime/threadlocal.h"
#include "service/brpc_conflict.h"
#include "util/lock.h"

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
        _io_thread_pool =
                new PriorityThreadPool(config::doris_scanner_thread_pool_thread_num,
                                       config::doris_scanner_thread_pool_queue_size, "AsyncIo");
        _remote_thread_pool = new PriorityThreadPool(
                config::doris_remote_scanner_thread_pool_thread_num,
                config::doris_remote_scanner_thread_pool_queue_size, "AsyncIo");
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
        doris::Mutex mutex;
        doris::ConditionVariable cv;
        std::unique_lock<doris::Mutex> l(mutex);

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
            std::unique_lock<doris::Mutex> l(mutex);
            cv.notify_one();
        };

        if (file_type == io::FileSystemType::S3) {
            AsyncIO::instance().remote_thread_pool()->offer(task);
        } else {
            AsyncIO::instance().io_thread_pool()->offer(task);
        }
        cv.wait(l);
    }

    inline static bthread_key_t btls_io_ctx_key;

    static void io_ctx_key_deleter(void* d) { delete static_cast<AsyncIOCtx*>(d); }

private:
    PriorityThreadPool* _io_thread_pool = nullptr;
    PriorityThreadPool* _remote_thread_pool = nullptr;

    PriorityThreadPool* io_thread_pool() { return _io_thread_pool; }
    PriorityThreadPool* remote_thread_pool() { return _remote_thread_pool; }
};

} // end namespace doris