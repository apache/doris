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

#include "olap/memtable_flush_executor.h"

#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/memtable.h"
#include "runtime/exec_env.h"

namespace doris {

OLAPStatus FlushHandler::submit(std::shared_ptr<MemTable> memtable) {
    RETURN_NOT_OK(_last_flush_status.load());
    MemTableFlushContext ctx;
    ctx.memtable = memtable;
    ctx.flush_handler = this->shared_from_this();
    _flush_futures.push(_flush_executor->_push_memtable(_flush_queue_idx, ctx));
    return OLAP_SUCCESS; 
}

OLAPStatus FlushHandler::wait() {
    if (_last_flush_status.load() != OLAP_SUCCESS) {
        return _last_flush_status.load();
    }

    while(!_flush_futures.empty()) {
        std::future<OLAPStatus>& fu = _flush_futures.front();
        OLAPStatus st = fu.get();
        if (st != OLAP_SUCCESS) {
            _last_flush_status.store(st);
            return st;
        }
        _flush_futures.pop();
    }
    return OLAP_SUCCESS;
}

const FlushStatistic& FlushHandler::get_stats() {
    return _stats;
}

void FlushHandler::on_flush_finished(const FlushResult& res) {
    if (res.flush_status != OLAP_SUCCESS) {
        _last_flush_status.store(res.flush_status);
    } else {
        _stats.flush_time_ns.fetch_add(res.flush_time_ns);
        _stats.flush_count.fetch_add(1);
    }
}

OLAPStatus MemTableFlushExecutor::create_flush_handler(int64_t path_hash, FlushHandler** flush_handler) {
    int32_t flush_queue_idx = _get_queue_idx(path_hash); 
    *flush_handler = new FlushHandler(flush_queue_idx, this);
    return OLAP_SUCCESS;
}

void MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    _thread_num_per_store = std::max(1, config::flush_thread_num_per_store);
    _num_threads = data_dir_num * _thread_num_per_store;

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
    for (auto store : data_dirs) {
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

int32_t MemTableFlushExecutor::_get_queue_idx(size_t path_hash) {
    std::lock_guard<SpinLock> l(_lock);
    int32_t cur_idx = _path_map[path_hash];
    int32_t group = cur_idx / _thread_num_per_store;
    int32_t next_idx = group * _thread_num_per_store + ((cur_idx + 1) % _thread_num_per_store);
    DCHECK(next_idx < _num_threads);
    _path_map[path_hash] = next_idx;
    return cur_idx;
}

std::future<OLAPStatus> MemTableFlushExecutor::_push_memtable(int32_t queue_idx, MemTableFlushContext& ctx) {
    ctx.flush_id = _id_generator.fetch_add(1);
    std::promise<OLAPStatus> promise;
    std::future<OLAPStatus> fu = promise.get_future();
    {
        std::lock_guard<SpinLock> l(_lock);
        _flush_promises[ctx.flush_id] = std::move(promise);
    }

    _flush_queues[queue_idx]->blocking_put(ctx);
    return fu;
}

void MemTableFlushExecutor::_flush_memtable(int32_t queue_idx) {
    while(true) {
        MemTableFlushContext ctx;
        if (!_flush_queues[queue_idx]->blocking_get(&ctx)) {
            // queue is empty and shutdown, end of thread
            return;
        }

        // if last flush of this tablet already failed, just skip
        if (ctx.flush_handler->last_flush_status() != OLAP_SUCCESS) {
            continue;
        }

        // flush the memtable
        FlushResult res;
        MonotonicStopWatch timer;
        timer.start();
        res.flush_status = ctx.memtable->flush();
        res.flush_time_ns = timer.elapsed_time();
        ctx.flush_handler->on_flush_finished(res);

        {
            std::lock_guard<SpinLock> l(_lock);
            _flush_promises[ctx.flush_id].set_value(res.flush_status);
            _flush_promises.erase(ctx.flush_id);
        }
    }
}

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000
       << ", flush count=" << stat.flush_count << ")";
    return os;
}

} // end of namespac
