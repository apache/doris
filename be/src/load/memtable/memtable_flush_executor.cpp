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

#include "load/memtable/memtable_flush_executor.h"

#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "common/metrics/system_metrics.h"
#include "common/signal_handler.h"
#include "exec/sink/autoinc_buffer.h"
#include "load/memtable/memtable.h"
#include "runtime/thread_context.h"
#include "storage/binlog.h"
#include "storage/rowset/group_rowset_writer.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet_info.h"
#include "util/debug_points.h"
#include "util/pretty_printer.h"
#include "util/stopwatch.hpp"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

bvar::Adder<int64_t> g_flush_task_num("memtable_flush_task_num");

class MemtableFlushTask : public Runnable {
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

protected:
    std::weak_ptr<FlushToken> _flush_token;
    std::shared_ptr<MemTable> _memtable;
    int32_t _segment_id;
    int64_t _submit_task_time;
};

class PartOfGroupMemtableFlushTask final : public MemtableFlushTask {
    ENABLE_FACTORY_CREATOR(PartOfGroupMemtableFlushTask);

public:
    PartOfGroupMemtableFlushTask(std::shared_ptr<FlushToken> flush_token,
                            std::shared_ptr<SharedMemtable> shared_memtable,
                           WriteRequestType write_req_type, int64_t submit_task_time)
            : MemtableFlushTask(flush_token, nullptr, 0, submit_task_time),
              _shared_memtable(std::move(shared_memtable)),
              _write_req_type(write_req_type) {}

    void run() override {
        auto token = _flush_token.lock();
        if (token) {
            token->_flush_group_memtable(_shared_memtable, _write_req_type, _submit_task_time);
        } else {
            LOG(WARNING) << "flush token is deconstructed, ignore the flush task";
        }
    }

private:
    std::shared_ptr<SharedMemtable> _shared_memtable;
    WriteRequestType _write_req_type;
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

SharedMemtable::~SharedMemtable() {
    if (block == nullptr) {
        return;
    }
    DCHECK(memtable != nullptr);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(memtable->mem_tracker());
    block.reset();
}

Status FlushToken::_submit_sub_tasks(ThreadPool* pool, std::vector<std::shared_ptr<Runnable>> sub_tasks) {
    for (int i = 0; i < sub_tasks.size(); ++i){
        {
            std::shared_lock rdlk(_flush_status_lock);
            DBUG_EXECUTE_IF("FlushToken.submit_sub_task_error", {
                if (i != 0) {
                    // only affect flush binlog task
                    _flush_status = Status::IOError<false>("dbug_be_memtable_submit_flush_error");
                }
            });
            if (!_flush_status.ok()) {
                return _flush_status;
            }
        }
        Status submit_st = pool->submit(std::move(sub_tasks[i]));
        if (UNLIKELY(!submit_st.ok())) {
            {
                std::lock_guard wrlk(_flush_status_lock);
                if (_flush_status.ok()) {
                    _flush_status = submit_st;
                }
            }
            _shutdown_flush_token();
            return submit_st;
        }
        _stats.flush_submit_count++;
    }
    return Status::OK();
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
    auto* group_rowset_writer = typeid_cast<GroupRowsetWriter*>(_rowset_writer.get());
    std::shared_ptr<SharedMemtable> shared_memtable;
    std::vector<std::shared_ptr<Runnable>> tasks;
    if (group_rowset_writer != nullptr) {
        auto data_writer = group_rowset_writer->data_writer();
        auto binlog_writer = group_rowset_writer->row_binlog_writer();
        DCHECK(data_writer != nullptr);
        DCHECK(binlog_writer != nullptr);

        shared_memtable = std::make_shared<SharedMemtable>();
        shared_memtable->memtable = mem_table;
        // Keep data/binlog segment_id allocators in sync.
        auto segment_id = data_writer->allocate_segment_id();
        auto binlog_segment_id = binlog_writer->allocate_segment_id();
        DCHECK_EQ(segment_id, binlog_segment_id);
        shared_memtable->segment_id = segment_id;

        if (binlog_writer->context().write_binlog_opt().need_build_binlog()) {
            if (_row_binlog_lsn_buffer == nullptr) {
                std::unique_lock<std::mutex> lock(_mutex);
                if (_row_binlog_lsn_buffer == nullptr) {
                    if (_table_schema_param == nullptr) {
                        return Status::InternalError<false>("need binlog but table_schema_param is null");
                    }
                    _row_binlog_lsn_buffer = GlobalAutoIncBuffers::GetInstance()->get_auto_inc_buffer(
                            _table_schema_param->db_id(), _table_schema_param->table_id(),
                            kBinlogLsnAutoIncId);
                }
            }
            std::shared_ptr<std::vector<int128_t>> lsn;
            RETURN_IF_ERROR(allocate_binlog_lsn(_row_binlog_lsn_buffer, mem_table->raw_rows(), &lsn));
            DCHECK(lsn != nullptr && !lsn->empty());
            const_cast<RowsetWriterContext&>(binlog_writer->context())
                    .write_binlog_opt()
                    .write_binlog_config()
                    .insert_seg_lsn(shared_memtable->segment_id, lsn);
        }

        tasks.emplace_back(PartOfGroupMemtableFlushTask::create_shared(shared_from_this(), shared_memtable,
                                                                WriteRequestType::DATA_IN_GROUP,
                                                                submit_task_time));
        tasks.emplace_back(PartOfGroupMemtableFlushTask::create_shared(shared_from_this(), shared_memtable,
                                                                WriteRequestType::BINLOG_IN_GROUP,
                                                                submit_task_time));
    } else {
        tasks.emplace_back(MemtableFlushTask::create_shared(shared_from_this(), mem_table,
                                                                _rowset_writer->allocate_segment_id(),
                                                                submit_task_time));
    }
    // NOTE: we should guarantee WorkloadGroup is not deconstructed when submit memtable flush task.
    // because currently WorkloadGroup's can only be destroyed when all queries in the group is finished,
    // but not consider whether load channel is finish.
    std::shared_ptr<WorkloadGroup> wg_sptr = _wg_wptr.lock();
    ThreadPool* wg_thread_pool = nullptr;
    if (wg_sptr) {
        wg_thread_pool = wg_sptr->get_memtable_flush_pool();
    }
    ThreadPool* pool = wg_thread_pool ? wg_thread_pool : _thread_pool;

    return _submit_sub_tasks(pool, std::move(tasks));
}

void FlushToken::_flush_group_memtable(std::shared_ptr<SharedMemtable> shared_memtable,
                                       WriteRequestType write_req_type,
                                       int64_t submit_task_time) {
    DCHECK(shared_memtable != nullptr);
    DCHECK(shared_memtable->memtable != nullptr);
    DCHECK(write_req_type == WriteRequestType::DATA_IN_GROUP ||
           write_req_type == WriteRequestType::BINLOG_IN_GROUP);

    auto* group_rowset_writer = typeid_cast<GroupRowsetWriter*>(_rowset_writer.get());
    DCHECK(group_rowset_writer != nullptr);
    auto flush_writer = write_req_type == WriteRequestType::DATA_IN_GROUP
                                ? group_rowset_writer->data_writer()
                                : group_rowset_writer->row_binlog_writer();
    DCHECK(flush_writer != nullptr);
    _flush_memtable_impl(flush_writer.get(), shared_memtable->memtable.get(),
                         shared_memtable->segment_id, submit_task_time, shared_memtable.get());
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

Status FlushToken::_memtable2block(MemTable* memtable,
                                   SharedMemtable* shared_memtable,
                                   std::shared_ptr<Block>& flush_block) {
    DCHECK(memtable != nullptr);

    if (shared_memtable == nullptr) {
        std::unique_ptr<Block> block;
        RETURN_IF_ERROR(memtable->to_block(&block));
        flush_block.reset(block.release());
        return Status::OK();
    }

    std::call_once(shared_memtable->block_once, [&]() {
        std::unique_ptr<Block> block;
        shared_memtable->block_status = memtable->to_block(&block);
        if (shared_memtable->block_status.ok()) {
            shared_memtable->block.reset(block.release());
        }
    });
    if (!shared_memtable->block_status.ok()) {
        return shared_memtable->block_status;
    }
    flush_block = shared_memtable->block;
    DCHECK(flush_block != nullptr);
    return Status::OK();
}

void FlushToken::_flush_memtable_impl(RowsetWriter* flush_writer, MemTable* memtable,
                                      int32_t segment_id, int64_t submit_task_time,
                                      SharedMemtable* shared_memtable) {
    DCHECK(flush_writer != nullptr);
    DCHECK(memtable != nullptr);

    signal::set_signal_task_id(flush_writer->load_id());
    signal::tablet_id = memtable->tablet_id();
    // Count the task as running before registering the deferred cleanup so
    // cancel/shutdown paths keep flush_running_count symmetric on every exit.
    _stats.flush_running_count++;
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
    size_t memory_usage = memtable->memory_usage();

    int64_t flush_size = 0;
    Status s;
    memtable->update_mem_type(MemType::FLUSH);
    int64_t duration_ns = 0;
    {
        s = [&]() {
            SCOPED_RAW_TIMER(&duration_ns);
            SCOPED_ATTACH_TASK(memtable->resource_ctx());
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
                    memtable->resource_ctx()->memory_context()->mem_tracker()->write_tracker());
            SCOPED_CONSUME_MEM_TRACKER(memtable->mem_tracker());

            // DEFER_RELEASE_RESERVED();

            // auto reserve_size = memtable->get_flush_reserve_memory_size();
            // if (memtable->resource_ctx()->task_controller()->is_enable_reserve_memory() &&
            //     reserve_size > 0) {
            //     RETURN_IF_ERROR(_try_reserve_memory(memtable->resource_ctx(), reserve_size));
            // }

            // Defer defer {[&]() {
            //     ExecEnv::GetInstance()->storage_engine().memtable_flush_executor()->dec_flushing_task();
            // }};
            std::shared_ptr<Block> flush_block;
            RETURN_IF_ERROR(_memtable2block(memtable, shared_memtable, flush_block));
            RETURN_IF_ERROR(flush_writer->flush_memtable(flush_block.get(), segment_id, &flush_size));
            memtable->set_flush_success();
            
            return Status::OK();
        }();

        if (s.ok()) {
            bool record_memtable_stat = shared_memtable == nullptr;
            if (shared_memtable != nullptr) {
                auto finished_sub_task_count = shared_memtable->add_finished_sub_task() + 1;
                record_memtable_stat =
                        finished_sub_task_count == shared_memtable->total_sub_task_count.load();
            }
            if (record_memtable_stat) {
                _memtable_stat += memtable->stat();
            }
            DorisMetrics::instance()->memtable_flush_total->increment(1);
            DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
        }
    }

    {
        std::shared_lock rdlk(_flush_status_lock);
        if (!_flush_status.ok()) {
            return;
        }
    }
    if (!s.ok()) {
        std::lock_guard wrlk(_flush_status_lock);
        if (_flush_status.ok()) {
            LOG(WARNING) << "Flush memtable failed with res = " << s
                         << ", load_id: " << print_id(flush_writer->load_id());
            _flush_status = s;
        }
        _shutdown_flush_token();
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
    _stats.flush_size_bytes += memtable->memory_usage();
    _stats.flush_disk_size_bytes += flush_size;
}

void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable_ptr, int32_t segment_id,
                                 int64_t submit_task_time) {
    _flush_memtable_impl(_rowset_writer.get(), memtable_ptr.get(), segment_id, submit_task_time);
}

std::pair<int, int> MemTableFlushExecutor::calc_flush_thread_count(int num_cpus, int num_disk,
                                                                   int thread_num_per_store) {
    if (config::enable_adaptive_flush_threads && num_cpus > 0) {
        int min = std::max(1, (int)(num_cpus * config::min_flush_thread_num_per_cpu));
        int max = std::max(min, num_cpus * config::max_flush_thread_num_per_cpu);
        return {min, max};
    }
    int min = std::max(1, thread_num_per_store);
    int max = num_cpus == 0
                      ? num_disk * min
                      : std::min(num_disk * min, num_cpus * config::max_flush_thread_num_per_cpu);
    return {min, max};
}

void MemTableFlushExecutor::init(int num_disk) {
    _num_disk = std::max(1, num_disk);
    int num_cpus = std::thread::hardware_concurrency();

    auto [min_threads, max_threads] =
            calc_flush_thread_count(num_cpus, _num_disk, config::flush_thread_num_per_store);
    static_cast<void>(ThreadPoolBuilder("MemTableFlushThreadPool")
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_flush_pool));

    auto [hi_min, hi_max] = calc_flush_thread_count(
            num_cpus, _num_disk, config::high_priority_flush_thread_num_per_store);
    static_cast<void>(ThreadPoolBuilder("MemTableHighPriorityFlushThreadPool")
                              .set_min_threads(hi_min)
                              .set_max_threads(hi_max)
                              .build(&_high_prio_flush_pool));
}

void MemTableFlushExecutor::update_memtable_flush_threads() {
    int num_cpus = std::thread::hardware_concurrency();

    auto [min_threads, max_threads] =
            calc_flush_thread_count(num_cpus, _num_disk, config::flush_thread_num_per_store);
    // Update max_threads first to avoid constraint violation when increasing min_threads
    static_cast<void>(_flush_pool->set_max_threads(max_threads));
    static_cast<void>(_flush_pool->set_min_threads(min_threads));

    auto [hi_min, hi_max] = calc_flush_thread_count(
            num_cpus, _num_disk, config::high_priority_flush_thread_num_per_store);
    // Update max_threads first to avoid constraint violation when increasing min_threads
    static_cast<void>(_high_prio_flush_pool->set_max_threads(hi_max));
    static_cast<void>(_high_prio_flush_pool->set_min_threads(hi_min));
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
Status MemTableFlushExecutor::create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                                                 std::shared_ptr<RowsetWriter> rowset_writer,
                                                 bool is_high_priority,
                                                 std::shared_ptr<WorkloadGroup> wg_sptr,
                                                 std::shared_ptr<OlapTableSchemaParam> table_schema_param) {
    switch (rowset_writer->type()) {
    case ALPHA_ROWSET:
        // alpha rowset do not support flush in CONCURRENT.  and not support alpha rowset now.
        return Status::InternalError<false>("not support alpha rowset load now.");
    case BETA_ROWSET: {
        // beta rowset can be flush in CONCURRENT, because each memtable using a new segment writer.
        ThreadPool* pool = is_high_priority ? _high_prio_flush_pool.get() : _flush_pool.get();
        flush_token = FlushToken::create_shared(pool, wg_sptr);
        flush_token->set_rowset_writer(rowset_writer);
        flush_token->set_table_schema_param(std::move(table_schema_param));
        return Status::OK();
    }
    default:
        return Status::InternalError<false>("unknown rowset type.");
    }
}

} // namespace doris
