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

#include "runtime/memtable_flush_executor.h"

#include "olap/memtable.h"
#include "olap/delta_writer.h"
#include "runtime/exec_env.h"

namespace doris {

MemTableFlushExecutor::MemTableFlushExecutor(ExecEnv* exec_env):
    _exec_env(exec_env) {
}

void MemTableFlushExecutor::init() {
#ifndef BE_TEST
    int32_t data_dir_num = _exec_env->store_paths().size();
    _thread_num_per_store = std::max(1, config::flush_thread_num_per_store);
    _num_threads = data_dir_num * _thread_num_per_store;
#else
    int32_t data_dir_num = 1;
    _thread_num_per_store = std::max(1, config::flush_thread_num_per_store);
    _num_threads = data_dir_num * _thread_num_per_store;
#endif

    // create flush queues
    for (int i = 0; i < _num_threads; ++i) {
        BlockingQueue<MemTableFlushContext>* queue = new BlockingQueue<MemTableFlushContext>(10);
        _flush_queues.push_back(queue);
    }
    // create thread pool
    _flush_pool = new ThreadPool(_num_threads, 1);
    for (int32_t i = 0; i < _num_threads; ++i) {
       _flush_pool->offer(boost::bind<void>(&MemTableFlushExecutor::_flush_memtable, this, i));
    }

    // _path_map saves the path hash to current idx of flush queue.
    // eg.
    // there are 4 data stores, each store has 2 work thread.
    // so there are 8(= 4 * 2) queues in _flush_queues.
    // and the path hash of the 4 paths are mapped to idx 0, 2, 4, 6.
    int32_t group = 0;
    for (auto store : _exec_env->storage_engine()->get_stores<true>()) {
        _path_map[store->path_hash()] = group;
        group += _thread_num_per_store;
    }
}

MemTableFlushExecutor::~MemTableFlushExecutor() {
    // shutdown queues
    for (auto queue : _flush_queues) {
        queue->shutdown();
    }

    // shutdown thread pool
    _flush_pool->shutdown();
    _flush_pool->join();

    // delete queue    
    for (auto queue : _flush_queues) {
        delete queue;
    }
    _flush_queues.clear();

    delete _flush_pool;
}

int32_t MemTableFlushExecutor::get_queue_idx(size_t path_hash) {
    std::lock_guard<SpinLock> l(_lock);
    int32_t cur_idx = _path_map[path_hash];
    int32_t group = cur_idx / _thread_num_per_store;
    int32_t next_idx = group * _thread_num_per_store + ((cur_idx + 1) % _thread_num_per_store);
    DCHECK(next_idx < _num_threads);
    _path_map[path_hash] = next_idx;
    return cur_idx;
}

std::future<OLAPStatus> MemTableFlushExecutor::push_memtable(int32_t queue_idx, const MemTableFlushContext& ctx) {
    _flush_queues[queue_idx]->blocking_put(ctx);
    return ctx.memtable->get_flush_future();
}

void MemTableFlushExecutor::_flush_memtable(int32_t queue_idx) {
    while(true) {
        MemTableFlushContext ctx;
        if (!_flush_queues[queue_idx]->blocking_get(&ctx)) {
            // queue is empty and shutdown, end of thread
            return;
        }

        DeltaWriter* delta_writer = ctx.delta_writer;
        // if last flush of this tablet already failed, just skip
        if (delta_writer->get_flush_status() != OLAP_SUCCESS) {
            continue;
        }

        // flush the memtable
        MonotonicStopWatch timer;
        timer.start();
        OLAPStatus st = ctx.memtable->flush(delta_writer->rowset_writer());
        if (st == OLAP_SUCCESS) {
            delta_writer->update_flush_time(timer.elapsed_time());
        } else {
            // if failed, update the flush status, this staus will be observed
            // by delta writer.
            ctx.flush_status->store(st);
        }
        // set the promise's value
        ctx.memtable->set_flush_status(st);
    }
}

} // end of namespace
