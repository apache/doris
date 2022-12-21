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

#include <functional>

#include "olap/memtable.h"
#include "runtime/thread_context.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

class MemtableFlushTask final : public Runnable {
public:
    MemtableFlushTask(FlushToken* flush_token, std::unique_ptr<MemTable> memtable,
                      int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(std::move(memtable)),
              _submit_task_time(submit_task_time) {}

    ~MemtableFlushTask() override = default;

    void run() override {
        _flush_token->_flush_memtable(_memtable.get(), _submit_task_time);
        _memtable.reset();
    }

private:
    FlushToken* _flush_token;
    std::unique_ptr<MemTable> _memtable;
    int64_t _submit_task_time;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / NANOS_PER_MILLIS
       << ", flush wait time(ms)=" << stat.flush_wait_time_ns / NANOS_PER_MILLIS
       << ", running flush count=" << stat.flush_running_count
       << ", finish flush count=" << stat.flush_finish_count
       << ", flush bytes: " << stat.flush_size_bytes
       << ", flush disk bytes: " << stat.flush_disk_size_bytes << ")";
    return os;
}

Status FlushToken::submit(std::unique_ptr<MemTable> mem_table) {
    auto s = _flush_status.load();
    if (s != OK) {
        return Status::Error(s);
    }
    int64_t submit_task_time = MonotonicNanos();
    auto task = std::make_shared<MemtableFlushTask>(this, std::move(mem_table), submit_task_time);
    _stats.flush_running_count++;
    return _flush_token->submit(std::move(task));
}

void FlushToken::cancel() {
    _flush_token->shutdown();
}

Status FlushToken::wait() {
    _flush_token->wait();
    auto s = _flush_status.load();
    return s == OK ? Status::OK() : Status::Error(s);
}

void FlushToken::_flush_memtable(MemTable* memtable, int64_t submit_task_time) {
    _stats.flush_wait_time_ns += (MonotonicNanos() - submit_task_time);
    // If previous flush has failed, return directly
    if (_flush_status.load() != OK) {
        return;
    }

    MonotonicStopWatch timer;
    timer.start();
    Status s = memtable->flush();
    if (!s) {
        LOG(WARNING) << "Flush memtable failed with res = " << s;
        // If s is not ok, ignore the code, just use other code is ok
        _flush_status.store(ErrorCode::INTERNAL_ERROR);
    }
    if (_flush_status.load() != OK) {
        return;
    }

    VLOG_CRITICAL << "flush memtable cost: " << timer.elapsed_time()
                  << ", running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << memtable->memory_usage()
                  << ", disk size: " << memtable->flush_size();
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_finish_count++;
    _stats.flush_running_count--;
    _stats.flush_size_bytes += memtable->memory_usage();
    _stats.flush_disk_size_bytes += memtable->flush_size();
}

void MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    size_t min_threads = std::max(1, config::flush_thread_num_per_store);
    size_t max_threads = data_dir_num * min_threads;
    ThreadPoolBuilder("MemTableFlushThreadPool")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool);

    min_threads = std::max(1, config::high_priority_flush_thread_num_per_store);
    max_threads = data_dir_num * min_threads;
    ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_high_prio_flush_pool);
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::unique_ptr<FlushToken>* flush_token,
                                                 RowsetTypePB rowset_type, bool should_serial,
                                                 bool is_high_priority) {
    if (!is_high_priority) {
        if (rowset_type == BETA_ROWSET && !should_serial) {
            // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
            flush_token->reset(
                    new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT)));
        } else {
            // alpha rowset do not support flush in CONCURRENT.
            flush_token->reset(
                    new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
        }
    } else {
        if (rowset_type == BETA_ROWSET && !should_serial) {
            // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
            flush_token->reset(new FlushToken(
                    _high_prio_flush_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT)));
        } else {
            // alpha rowset do not support flush in CONCURRENT.
            flush_token->reset(new FlushToken(
                    _high_prio_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
        }
    }
    return Status::OK();
}

} // namespace doris
