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
#include <future>
#include <memory>
#include <vector>
#include <unordered_map>
#include <utility>

#include "olap/olap_define.h"
#include "util/blocking_queue.hpp"
#include "util/spinlock.h"
#include "util/thread_pool.hpp"

namespace doris {

class ExecEnv;
class DeltaWriter;
class MemTable;

// The context for a memtable to be flushed.
// It does not own any objects in it.
struct MemTableFlushContext {
    std::shared_ptr<MemTable> memtable;
    DeltaWriter* delta_writer;
    std::atomic<OLAPStatus>* flush_status;
};

// MemTableFlushExecutor is for flushing memtables to disk.
// Each data directory has a specified number of worker threads and a corresponding number of flush queues.
// Each worker thread only takes memtable from the corresponding flush queue and writes it to disk.
class MemTableFlushExecutor {
public:
    MemTableFlushExecutor(ExecEnv* exec_env);
    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    void init();

    ~MemTableFlushExecutor();

    // given the path hash, return the next idx of flush queue.
    // eg.
    // path A is mapped to idx 0 and 1, so each time get_queue_idx(A) is called,
    // 0 and 1 will returned alternately.
    int32_t get_queue_idx(size_t path_hash);

    // push the memtable to specified flush queue, and return a future
    std::future<OLAPStatus> push_memtable(int32_t queue_idx, const MemTableFlushContext& ctx);

private:
    void _flush_memtable(int32_t queue_idx);

private:
    ExecEnv* _exec_env;
    int32_t _thread_num_per_store;
    int32_t _num_threads;
    ThreadPool* _flush_pool;
    // the size of this vector should equals to _num_threads
    std::vector<BlockingQueue<MemTableFlushContext>*> _flush_queues;

    // lock to protect path_map
    SpinLock _lock;
    // path hash -> queue idx of _flush_queues;
    std::unordered_map<size_t, int32_t> _path_map;
};

} // end namespace
