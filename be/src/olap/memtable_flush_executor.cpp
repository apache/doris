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
#include "olap/storage_engine.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/pretty_printer.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<int64_t> g_flush_task_num("memtable_flush_task_num");

class MemtableFlushTask final : public Runnable {
    ENABLE_FACTORY_CREATOR(MemtableFlushTask);

public:
    MemtableFlushTask(std::shared_ptr<FlushToken> flush_token, std::shared_ptr<MemTable> memtable,
                      int32_t segment_id, int64_t submit_task_time)
            : _flush_token(flush_token),
              _memtable(memtable),
              _segment_id(segment_id),
              _submit_task_time(submit_task_time) {
        g_flush_task_num << 1;
    }

    ~MemtableFlushTask() override { g_flush_task_num << -1; }

    void run() override {
        auto token = _flush_token.lock();
        if (token) {
            token->_flush_memtable(_memtable, _segment_id, _submit_task_time);
        } else {
            LOG(WARNING) << "flush token is deconstructed, ignore the flush task";
        }
    }

private:
    std::weak_ptr<FlushToken> _flush_token;
    std::shared_ptr<MemTable> _memtable;
    int32_t _segment_id;
    int64_t _submit_task_time;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / NANOS_PER_MILLIS
       << ", flush wait time(ms)=" << stat.flush_wait_time_ns / NANOS_PER_MILLIS
       << ", flush submit count=" << stat.flush_submit_count
       << ", running flush count=" << stat.flush_running_count
       << ", finish flush count=" << stat.flush_finish_count
       << ", flush bytes: " << stat.flush_size_bytes
       << ", flush disk bytes: " << stat.flush_disk_size_bytes << ")";
    return os;
}

Status FlushToken::submit(std::shared_ptr<MemTable> mem_table) {
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
    auto task = MemtableFlushTask::create_shared(
            shared_from_this(), mem_table, _rowset_writer->allocate_segment_id(), submit_task_time);
    // NOTE: we should guarantee WorkloadGroup is not deconstructed when submit memtable flush task.
    // because currently WorkloadGroup's can only be destroyed when all queries in the group is finished,
    // but not consider whether load channel is finish.
    std::shared_ptr<WorkloadGroup> wg_sptr = _wg_wptr.lock();
    ThreadPool* wg_thread_pool = nullptr;
    if (wg_sptr) {
        wg_thread_pool = wg_sptr->get_memtable_flush_pool();
    }
    Status ret = wg_thread_pool ? wg_thread_pool->submit(std::move(task))
                                : _thread_pool->submit(std::move(task));
    if (ret.ok()) {
        // _wait_running_task_finish was executed after this function, so no need to notify _cond here
        _stats.flush_submit_count++;
    }
    return ret;
}

// NOTE: FlushToken's submit/cancel/wait run in one thread,
// so we don't need to make them mutually exclusive, std::atomic is enough.
void FlushToken::_wait_submit_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _submit_task_finish_cond.wait(lock, [&]() { return _stats.flush_submit_count.load() == 0; });
}

void FlushToken::_wait_running_task_finish() {
    std::unique_lock<std::mutex> lock(_mutex);
    _running_task_finish_cond.wait(lock, [&]() { return _stats.flush_running_count.load() == 0; });
}

void FlushToken::cancel() {
    _shutdown_flush_token();
    _wait_running_task_finish();
}

Status FlushToken::wait() {
    _wait_submit_task_finish();
    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return _flush_status;
        }
    }
    return Status::OK();
}

Status FlushToken::_try_reserve_memory(const std::shared_ptr<ResourceContext>& resource_context,
                                       int64_t size) {
    auto* thread_context = doris::thread_context();
    auto* memtable_flush_executor =
            ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    Status st;
    int32_t max_waiting_time = config::memtable_wait_for_memory_sleep_time_s;
    do {
        // only try to reserve process memory
        st = thread_context->thread_mem_tracker_mgr->try_reserve(
                size, ThreadMemTrackerMgr::TryReserveChecker::CHECK_PROCESS);
        if (st.ok()) {
            memtable_flush_executor->inc_flushing_task();
            break;
        }
        if (_is_shutdown() || resource_context->task_controller()->is_cancelled()) {
            st = Status::Cancelled("flush memtable already cancelled");
            break;
        }
        // Make sure at least one memtable is flushing even reserve memory failed.
        if (memtable_flush_executor->check_and_inc_has_any_flushing_task()) {
            // If there are already any flushing task, Wait for some time and retry.
            LOG_EVERY_T(INFO, 60) << fmt::format(
                    "Failed to reserve memory {} for flush memtable, retry after 100ms",
                    PrettyPrinter::print_bytes(size));
            std::this_thread::sleep_for(std::chrono::seconds(1));
            max_waiting_time -= 1;
        } else {
            st = Status::OK();
            break;
        }
    } while (max_waiting_time > 0);
    return st;
}

Status FlushToken::_do_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t* flush_size) {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << memtable->tablet_id()
                  << ", memsize: " << PrettyPrinter::print_bytes(memtable->memory_usage())
                  << ", rows: " << memtable->stat().raw_rows;
    memtable->update_mem_type(MemType::FLUSH);
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        SCOPED_ATTACH_TASK(memtable->resource_ctx());
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
        SCOPED_CONSUME_MEM_TRACKER(memtable->mem_tracker());

        DEFER_RELEASE_RESERVED();

        auto reserve_size = memtable->get_flush_reserve_memory_size();
        if (memtable->resource_ctx()->task_controller()->is_enable_reserve_memory() &&
            reserve_size > 0) {
            RETURN_IF_ERROR(_try_reserve_memory(memtable->resource_ctx(), reserve_size));
        }

        Defer defer {[&]() {
            ExecEnv::GetInstance()->storage_engine().memtable_flush_executor()->dec_flushing_task();
        }};
        std::unique_ptr<vectorized::Block> block;
        RETURN_IF_ERROR(memtable->to_block(&block));
        RETURN_IF_ERROR(_rowset_writer->flush_memtable(block.get(), segment_id, flush_size));
        memtable->set_flush_success();
    }
    _memtable_stat += memtable->stat();
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << memtable->tablet_id()
                  << ", flushsize: " << PrettyPrinter::print_bytes(*flush_size);
    return Status::OK();
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable_ptr, int32_t segment_id,
                                 int64_t submit_task_time) {
    signal::set_signal_task_id(_rowset_writer->load_id());
    signal::tablet_id = memtable_ptr->tablet_id();
    Defer defer {[&]() {
        std::lock_guard<std::mutex> lock(_mutex);
        _stats.flush_submit_count--;
        if (_stats.flush_submit_count == 0) {
            _submit_task_finish_cond.notify_one();
        }
        _stats.flush_running_count--;
        if (_stats.flush_running_count == 0) {
            _running_task_finish_cond.notify_one();
        }
    }};
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_before_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    if (_is_shutdown()) {
        return;
    }
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_first_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
    _stats.flush_running_count++;
    // double check if shutdown to avoid wait running task finish count not accurate
    if (_is_shutdown()) {
        return;
    }
    DBUG_EXECUTE_IF("FlushToken.flush_memtable.wait_after_second_shutdown",
                    { std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000)); });
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

    VLOG_CRITICAL << "flush memtable wait time: "
                  << PrettyPrinter::print(flush_wait_time_ns, TUnit::TIME_NS)
                  << ", flush memtable cost: "
                  << PrettyPrinter::print(timer.elapsed_time(), TUnit::TIME_NS)
                  << ", submit count: " << _stats.flush_submit_count
                  << ", running count: " << _stats.flush_running_count
                  << ", finish count: " << _stats.flush_finish_count
                  << ", mem size: " << PrettyPrinter::print_bytes(memory_usage)
                  << ", disk size: " << PrettyPrinter::print_bytes(flush_size);
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
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 bool is_high_priority,
                                                 std::shared_ptr<WorkloadGroup> wg_sptr) {
    switch (rowset_writer->type()) {
    case ALPHA_ROWSET:
        // alpha rowset do not support flush in CONCURRENT.  and not support alpha rowset now.
        return Status::InternalError<false>("not support alpha rowset load now.");
    case BETA_ROWSET: {
        // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
        ThreadPool* pool = is_high_priority ? _high_prio_flush_pool.get() : _flush_pool.get();
        flush_token = FlushToken::create_shared(pool, wg_sptr);
        flush_token->set_rowset_writer(rowset_writer);
        return Status::OK();
    }
    default:
        return Status::InternalError<false>("unknown rowset type.");
    }
}

} // namespace doris
