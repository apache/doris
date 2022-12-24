#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "service/brpc_conflict.h"

namespace doris {
class BthreadSharedMutex;
#if !defined(USE_BTHREAD_SCANNER)
using Mutex = std::mutex;
using ConditionVariable = std::condition_variable;
using SharedMutex = std::shared_mutex;
#else
using Mutex = bthread::Mutex;
using ConditionVariable = bthread::ConditionVariable;
using SharedMutex = BthreadSharedMutex;
#endif

class BthreadSharedMutex {
public:
    BthreadSharedMutex() : _reader_nums(0), _is_writing(false) {}
    ~BthreadSharedMutex() = default;

    void lock_shared() {
        std::unique_lock<doris::Mutex> lock(_mutex);
        while (_is_writing) {
            _cv.wait(lock);
        }
        ++_reader_nums;
    }

    void unlock_shared() {
        std::unique_lock<doris::Mutex> lock(_mutex);
        --_reader_nums;
        _cv.notify_one();
    }

    void lock() {
        std::unique_lock<doris::Mutex> lock(_mutex);
        while (_reader_nums != 0 || _is_writing == true) {
            _cv.wait(lock);
        }
        _is_writing = true;
    }

    void unlock() {
        std::unique_lock<doris::Mutex> lock(_mutex);
        _is_writing = false;
        _cv.notify_all();
    }

    void try_lock_shared_until() {
        // not support yet
        assert(false);
    }

    void try_lock_shared() {
        // not support yet
        assert(false);
    }

    void try_lock_shared_for() {
        // not support ye
        assert(false);
    }

private:
    int64_t _reader_nums;
    bool _is_writing;

    doris::Mutex _mutex;
    doris::ConditionVariable _cv;

private:
    DISALLOW_COPY_AND_ASSIGN(BthreadSharedMutex);
};

} // end namespace doris