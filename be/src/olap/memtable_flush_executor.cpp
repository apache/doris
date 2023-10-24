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

#include <gen_cpp/olap_file.pb.h>
#include <stddef.h>

#include <algorithm>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "olap/memtable.h"
#include "olap/rowset/rowset_writer.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

class MemtableFlushTask final : public Runnable {
public:
    MemtableFlushTask(FlushToken* flush_token, std::unique_ptr<MemTable> memtable,
                      int32_t segment_id, int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(std::move(memtable)),
              _segment_id(segment_id),
              _submit_task_time(submit_task_time) {}

    ~MemtableFlushTask() override = default;

    void run() override {
        _flush_token->_flush_memtable(_memtable.get(), _segment_id, _submit_task_time);
        _memtable.reset();
    }

private:
    FlushToken* _flush_token;
    std::unique_ptr<MemTable> _memtable;
    int32_t _segment_id;
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
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }

    if (mem_table == nullptr || mem_table->empty()) {
        return Status::OK();
    }
    int64_t submit_task_time = MonotonicNanos();
    auto task = std::make_shared<MemtableFlushTask>(
            this, std::move(mem_table), _rowset_writer->allocate_segment_id(), submit_task_time);
    _stats.flush_running_count++;
    return _flush_token->submit(std::move(task));
}

void FlushToken::cancel() {
    _flush_token->shutdown();
}

Status FlushToken::wait() {
    _flush_token->wait();
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }
    return Status::OK();
}

Status FlushToken::_do_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t* flush_size) {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << memtable->tablet_id()
                  << ", memsize: " << memtable->memory_usage()
                  << ", rows: " << memtable->stat().raw_rows;
    int64_t duration_ns;
    SCOPED_RAW_TIMER(&duration_ns);
    signal::set_signal_task_id(_rowset_writer.load_id());
    std::unique_ptr<vectorized::Block> block = memtable->to_block();
    {
        SCOPED_CONSUME_MEM_TRACKER(memtable->flush_mem_tracker());
        SKIP_MEMORY_CHECK(RETURN_IF_ERROR(
                _rowset_writer->flush_memtable(block.get(), segment_id, flush_size)));
    }
    _memtable_stat += memtable->stat();
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << memtable->tablet_id()
                  << ", flushsize: " << *flush_size;
    return Status::OK();
}

void FlushToken::_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t submit_task_time) {
    uint64_t flush_wait_time_ns = MonotonicNanos() - submit_task_time;
    _stats.flush_wait_time_ns += flush_wait_time_ns;
    // If previous flush has failed, return directly
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }

    MonotonicStopWatch timer;
    timer.start();
    size_t memory_usage = memtable->memory_usage();

    int64_t flush_size;
    Status s = _do_flush_memtable(memtable, segment_id, &flush_size);

    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        LOG(WARNING) << "Flush memtable failed with res = " << s;
        _flush_status = s;
        return;
    }

    VLOG_CRITICAL << "flush memtable wait time:" << flush_wait_time_ns
                  << "(ns), flush memtable cost: " << timer.elapsed_time()
                  << "(ns), running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << memory_usage << ", disk size: " << flush_size;
    _stats.flush_time_ns += timer.elapsed_time();
    _stats.flush_finish_count++;
    _stats.flush_running_count--;
    _stats.flush_size_bytes += memtable->memory_usage();
    _stats.flush_disk_size_bytes += flush_size;
}

void MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    size_t min_threads = std::max(1, config::flush_thread_num_per_store);
    size_t max_threads = data_dir_num * min_threads;
    static_cast<void>(ThreadPoolBuilder("MemTableFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_flush_pool));

    min_threads = std::max(1, config::high_priority_flush_thread_num_per_store);
    max_threads = data_dir_num * min_threads;
    static_cast<void>(ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_high_prio_flush_pool));
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::unique_ptr<FlushToken>& flush_token,
                                                 RowsetWriter* rowset_writer, bool should_serial,
                                                 bool is_high_priority) {
    if (!is_high_priority) {
        if (rowset_writer->type() == BETA_ROWSET && !should_serial) {
            // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
            flush_token.reset(
                    new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT)));
        } else {
            // alpha rowset do not support flush in CONCURRENT.
            flush_token.reset(
                    new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
        }
    } else {
        if (rowset_writer->type() == BETA_ROWSET && !should_serial) {
            // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
            flush_token.reset(new FlushToken(
                    _high_prio_flush_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT)));
        } else {
            // alpha rowset do not support flush in CONCURRENT.
            flush_token.reset(new FlushToken(
                    _high_prio_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
        }
    }
    flush_token->set_rowset_writer(rowset_writer);
    return Status::OK();
}

} // namespace doris
