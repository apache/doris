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

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "olap/memtable.h"
#include "olap/rowset/rowset_writer.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(flush_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(flush_thread_pool_thread_num, MetricUnit::NOUNIT);

bvar::Adder<int64_t> g_flush_task_num("memtable_flush_task_num");

class MemtableFlushTask final : public Runnable {
    ENABLE_FACTORY_CREATOR(MemtableFlushTask);

public:
    MemtableFlushTask(std::shared_ptr<FlushToken> flush_token, std::unique_ptr<MemTable> memtable,
                      int32_t segment_id, int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(std::move(memtable)),
              _segment_id(segment_id),
              _submit_task_time(submit_task_time) {
        g_flush_task_num << 1;
    }

    ~MemtableFlushTask() override { g_flush_task_num << -1; }

    void run() override {
        auto token = _flush_token.lock();
        if (token) {
            token->_flush_memtable(std::move(_memtable), _segment_id, _submit_task_time);
        } else {
            LOG(WARNING) << "flush token is deconstructed, ignore the flush task";
        }
    }

private:
    std::weak_ptr<FlushToken> _flush_token;
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
        DBUG_EXECUTE_IF("FlushToken.submit_flush_error", {
            _flush_status = Status::IOError<false>("dbug_be_memtable_submit_flush_error");
        });
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }

    if (mem_table == nullptr || mem_table->empty()) {
        return Status::OK();
    }
    int64_t submit_task_time = MonotonicNanos();
    auto task = MemtableFlushTask::create_shared(shared_from_this(), std::move(mem_table),
                                                 _rowset_writer->allocate_segment_id(),
                                                 submit_task_time);
    Status ret = _thread_pool->submit(std::move(task));
    if (ret.ok()) {
        // _wait_running_task_finish was executed after this function, so no need to notify _cond here
        _stats.flush_running_count++;
    }
    return ret;
}

// NOTE: FlushToken's submit/cancel/wait run in one thread,
// so we don't need to make them mutually exclusive, std::atomic is enough.
void FlushToken::_wait_running_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _cond.wait(lock, [&]() { return _stats.flush_running_count.load() == 0; });
}

void FlushToken::cancel() {
    _shutdown_flush_token();
    _wait_running_task_finish();
}

Status FlushToken::wait() {
    _wait_running_task_finish();
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
    SCOPED_ATTACH_TASK(memtable->query_thread_context());
    signal::set_signal_task_id(_rowset_writer->load_id());
    signal::tablet_id = memtable->tablet_id();
    {
        std::unique_ptr<vectorized::Block> block;
        // During to block method, it will release old memory and create new block, so that
        // we could not scoped it.
        RETURN_IF_ERROR(memtable->to_block(&block));
        memtable->flush_mem_tracker()->consume(block->allocated_bytes());
        SCOPED_CONSUME_MEM_TRACKER(memtable->flush_mem_tracker());
        RETURN_IF_ERROR(_rowset_writer->flush_memtable(block.get(), segment_id, flush_size));
    }
    _memtable_stat += memtable->stat();
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << memtable->tablet_id()
                  << ", flushsize: " << *flush_size;
    return Status::OK();
}

void FlushToken::_flush_memtable(std::unique_ptr<MemTable> memtable_ptr, int32_t segment_id,
                                 int64_t submit_task_time) {
    Defer defer {[&]() {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.flush_running_count--;
        if (_stats.flush_running_count == 0) {
            _cond.notify_one();
        }
    }};
    if (_is_shutdown()) {
        return;
    }
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
    size_t memory_usage = memtable_ptr->memory_usage();

    int64_t flush_size;
    Status s = _do_flush_memtable(memtable_ptr.get(), segment_id, &flush_size);

    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        LOG(WARNING) << "Flush memtable failed with res = " << s
                     << ", load_id: " << print_id(_rowset_writer->load_id());
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
    _stats.flush_size_bytes += memtable_ptr->memory_usage();
    _stats.flush_disk_size_bytes += flush_size;
}

void MemTableFlushExecutor::init(int num_disk) {
    num_disk = std::max(1, num_disk);
    int num_cpus = std::thread::hardware_concurrency();
    int min_threads = std::max(1, config::flush_thread_num_per_store);
    int max_threads = num_cpus == 0 ? num_disk * min_threads
                                    : std::min(num_disk * min_threads,
                                               num_cpus * config::max_flush_thread_num_per_cpu);
    static_cast<void>(ThreadPoolBuilder("MemTableFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_flush_pool));

    min_threads = std::max(1, config::high_priority_flush_thread_num_per_store);
    max_threads = num_cpus == 0 ? num_disk * min_threads
                                : std::min(num_disk * min_threads,
                                           num_cpus * config::max_flush_thread_num_per_cpu);
    static_cast<void>(ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_high_prio_flush_pool));
    _register_metrics();
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 bool is_high_priority) {
    switch (rowset_writer->type()) {
    case ALPHA_ROWSET:
        // alpha rowset do not support flush in CONCURRENT.  and not support alpha rowset now.
        return Status::InternalError<false>("not support alpha rowset load now.");
    case BETA_ROWSET: {
        // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
        ThreadPool* pool = is_high_priority ? _high_prio_flush_pool.get() : _flush_pool.get();
        flush_token = FlushToken::create_shared(pool);
        flush_token->set_rowset_writer(rowset_writer);
        return Status::OK();
    }
    default:
        return Status::InternalError<false>("unknown rowset type.");
    }
}

Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 ThreadPool* wg_flush_pool_ptr) {
    if (rowset_writer->type() == BETA_ROWSET) {
        flush_token = FlushToken::create_shared(wg_flush_pool_ptr);
    } else {
        return Status::InternalError<false>("not support alpha rowset load now.");
    }
    flush_token->set_rowset_writer(rowset_writer);
    return Status::OK();
}

void MemTableFlushExecutor::_register_metrics() {
    REGISTER_HOOK_METRIC(flush_thread_pool_queue_size,
                         [this]() { return _flush_pool->get_queue_size(); });
    REGISTER_HOOK_METRIC(flush_thread_pool_thread_num,
                         [this]() { return _flush_pool->num_threads(); })
}

void MemTableFlushExecutor::_deregister_metrics() {
    DEREGISTER_HOOK_METRIC(flush_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(flush_thread_pool_thread_num);
}

} // namespace doris
