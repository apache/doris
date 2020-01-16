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

#include <atomic>
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>
#include <utility>

#include "util/blocking_queue.hpp"
#include "util/counter_cond_variable.hpp"
#include "util/spinlock.h"
#include "util/thread_pool.hpp"
#include "olap/olap_define.h"

namespace doris {

class DataDir;
class DeltaWriter;
class ExecEnv;
class MemTable;

// The context for a memtable to be flushed.
class FlushHandler;
struct MemTableFlushContext {
    // memtable to be flushed
    std::shared_ptr<MemTable> memtable;
    // flush handler from a delta writer.
    // use shared ptr because flush_handler may be deleted before this
    // memtable being flushed. so we need to make sure the flush_handler
    // is alive until this memtable being flushed.
    std::shared_ptr<FlushHandler> flush_handler;
};

// the flush result of a single memtable flush
struct FlushResult {
    OLAPStatus flush_status;
    int64_t flush_time_ns = 0;
    int64_t flush_size_bytes = 0;
};

// the statistic of a certain flush handler.
// use atomic because it may be updated by multi threads
struct FlushStatistic {
    std::atomic<std::int64_t> flush_time_ns = {0};
    std::atomic<std::int64_t> flush_count= {0};
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat);

class MemTableFlushExecutor;

// flush handler is for flushing memtables in a delta writer
// This class must be wrapped by std::shared_ptr, or you will get bad_weak_ptr exception
// when calling submit();
class FlushHandler : public std::enable_shared_from_this<FlushHandler> {
public:
    FlushHandler(int32_t flush_queue_idx, MemTableFlushExecutor* flush_executor):
        _flush_queue_idx(flush_queue_idx),
        _last_flush_status(OLAP_SUCCESS),
        _counter_cond(0),
        _flush_executor(flush_executor),
        _is_cancelled(false) {
    }

    // submit a memtable to flush. return error if some previous submitted MemTable has failed
    OLAPStatus submit(std::shared_ptr<MemTable> memtable);
    // wait for all memtables submitted by itself to be finished.
    OLAPStatus wait();
    // get flush operations' statistics
    const FlushStatistic& get_stats() const { return _stats; }
    // called when a memtable is finished by executor.
    void on_flush_finished(const FlushResult& res);
    // called when a flush memtable execution is cancelled
    void on_flush_cancelled() {
        _counter_cond.dec();
    }

    bool is_cancelled() { return _last_flush_status.load() != OLAP_SUCCESS || _is_cancelled.load(); }

    void cancel() { _is_cancelled.store(true); }

private:
    // flush queue idx in memtable flush executor
    int32_t _flush_queue_idx;
    // the flush status of last memtable
    std::atomic<OLAPStatus> _last_flush_status;
    // used to wait/notify the memtable flush execution
    CounterCondVariable _counter_cond;

    FlushStatistic _stats;
    MemTableFlushExecutor* _flush_executor;

    // the caller of the flush handler can set this variable to notify that the
    // uppper application is already cancelled.
    std::atomic<bool> _is_cancelled;
};

// MemTableFlushExecutor is responsible for flushing memtables to disk.
// Each data directory has a specified number of worker threads and each thread will correspond
// to a queue. The only job of each worker thread is to take memtable from its corresponding
// flush queue and writes the data to disk.
//
// NOTE: User SHOULD NOT call method of this class directly, use pattern should be:
//      ...
//      std::shared_ptr<FlushHandler> flush_handler;
//      memTableFlushExecutor.create_flush_handler(path_hash, &flush_handler);
//      ...
//      flush_handler->submit(memtable)
//      ...
class MemTableFlushExecutor {
public:
    MemTableFlushExecutor() {}
    ~MemTableFlushExecutor();

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    void init(const std::vector<DataDir*>& data_dirs);

    // create a flush handler to access the flush executor
    OLAPStatus create_flush_handler(size_t path_hash, std::shared_ptr<FlushHandler>* flush_handler);

private:
    friend class FlushHandler;

    // given the path hash, return the next idx of flush queue.
    // eg.
    // path A is mapped to idx 0 and 1, so each time get_queue_idx(A) is called,
    // 0 and 1 will returned alternately.
    size_t _get_queue_idx(size_t path_hash);

    // push the memtable to specified flush queue
    OLAPStatus _push_memtable(int32_t queue_idx, MemTableFlushContext& ctx);

    void _flush_memtable(int32_t queue_idx);

    int32_t _thread_num_per_store;
    int32_t _num_threads;
    ThreadPool* _flush_pool;
    // the size of this vector should equal to _num_threads
    std::vector<BlockingQueue<MemTableFlushContext>*> _flush_queues;
    // lock to protect _path_map
    SpinLock _lock;
    // path hash -> queue idx of _flush_queues;
    std::unordered_map<size_t, size_t> _path_map;
};

} // end namespace
