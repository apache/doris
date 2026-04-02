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

#include <bthread/types.h>
#include <stdint.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "concurrentqueue.h"
#include "core/block/block.h"
#include "exec/common/memory.h"
#include "exec/scan/scanner.h"
#include "exec/scan/task_executor/split_runner.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
class TupleDescriptor;
class WorkloadGroup;

class ScanLocalStateBase;
class Dependency;

class Scanner;
class ScannerDelegate;
class ScannerScheduler;
class TaskExecutor;
class TaskHandle;

// Adaptive processor for dynamic scanner concurrency adjustment
struct ScannerAdaptiveProcessor {
    ENABLE_FACTORY_CREATOR(ScannerAdaptiveProcessor)
    ScannerAdaptiveProcessor() = default;
    ~ScannerAdaptiveProcessor() = default;
    // Expected scanners in this cycle

    int expected_scanners = 0;
    // Timing metrics
    // int64_t context_start_time = 0;
    // int64_t scanner_total_halt_time = 0;
    // int64_t scanner_gen_blocks_time = 0;
    // std::atomic_int64_t scanner_total_io_time = 0;
    // std::atomic_int64_t scanner_total_running_time = 0;
    // std::atomic_int64_t scanner_total_scan_bytes = 0;

    // Timestamps
    // std::atomic_int64_t last_scanner_finish_timestamp = 0;
    // int64_t check_all_scanners_last_timestamp = 0;
    // int64_t last_driver_output_full_timestamp = 0;
    int64_t adjust_scanners_last_timestamp = 0;

    // Adjustment strategy fields
    // bool try_add_scanners = false;
    // double expected_speedup_ratio = 0;
    // double last_scanner_scan_speed = 0;
    // int64_t last_scanner_total_scan_bytes = 0;
    // int try_add_scanners_fail_count = 0;
    // int check_slow_io = 0;
    // int32_t slow_io_latency_ms = 100; // Default from config
};

class ScanTask {
public:
    enum class State : int {
        PENDING,   // not scheduled yet
        IN_FLIGHT, // scheduled and running
        COMPLETED, // finished with result or error, waiting to be collected by scan node
        EOS,       // finished and no more data, waiting to be collected by scan node
    };
    ScanTask(std::weak_ptr<ScannerDelegate> delegate_scanner) : scanner(delegate_scanner) {
        _resource_ctx = thread_context()->resource_ctx();
        DorisMetrics::instance()->scanner_task_cnt->increment(1);
    }

    ~ScanTask() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_resource_ctx->memory_context()->mem_tracker());
        DorisMetrics::instance()->scanner_task_cnt->increment(-1);
        cached_block.reset();
    }

private:
    // whether current scanner is finished
    Status status = Status::OK();
    std::shared_ptr<ResourceContext> _resource_ctx;
    State _state = State::PENDING;

public:
    std::weak_ptr<ScannerDelegate> scanner;
    BlockUPtr cached_block = nullptr;
    bool is_first_schedule = true;
    // Use weak_ptr to avoid circular references and potential memory leaks with SplitRunner.
    // ScannerContext only needs to observe the lifetime of SplitRunner without owning it.
    // When SplitRunner is destroyed, split_runner.lock() will return nullptr, ensuring safe access.
    std::weak_ptr<SplitRunner> split_runner;

    void set_status(Status _status) {
        if (_status.is<ErrorCode::END_OF_FILE>()) {
            // set `eos` if `END_OF_FILE`, don't take `END_OF_FILE` as error
            _state = State::EOS;
        }
        status = _status;
    }
    Status get_status() const { return status; }
    bool status_ok() { return status.ok() || status.is<ErrorCode::END_OF_FILE>(); }
    bool is_eos() const { return _state == State::EOS; }
    void set_state(State state) {
        switch (state) {
        case State::PENDING:
            DCHECK(_state == State::PENDING || _state == State::IN_FLIGHT) << (int)_state;
            DCHECK(cached_block == nullptr);
            break;
        case State::IN_FLIGHT:
            DCHECK(_state == State::COMPLETED || _state == State::PENDING ||
                   _state == State::IN_FLIGHT)
                    << (int)_state;
            DCHECK(cached_block == nullptr);
            break;
        case State::COMPLETED:
            DCHECK(_state == State::IN_FLIGHT) << (int)_state;
            DCHECK(cached_block != nullptr);
            break;
        case State::EOS:
            DCHECK(_state == State::IN_FLIGHT || status.is<ErrorCode::END_OF_FILE>())
                    << (int)_state;
            break;
        default:
            break;
        }

        _state = state;
    }
};

// ScannerContext is responsible for recording the execution status
// of a group of Scanners corresponding to a ScanNode.
// Including how many scanners are being scheduled, and maintaining
// a producer-consumer blocks queue between scanners and scan nodes.
//
// ScannerContext is also the scheduling unit of ScannerScheduler.
// ScannerScheduler schedules a ScannerContext at a time,
// and submits the Scanners to the scanner thread pool for data scanning.
class ScannerContext : public std::enable_shared_from_this<ScannerContext>,
                       public HasTaskExecutionCtx {
    ENABLE_FACTORY_CREATOR(ScannerContext);
    friend class ScannerScheduler;

public:
    ScannerContext(RuntimeState* state, ScanLocalStateBase* local_state,
                   const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<std::shared_ptr<ScannerDelegate>>& scanners, int64_t limit_,
                   std::shared_ptr<Dependency> dependency, std::atomic<int64_t>* shared_scan_limit,
                   std::shared_ptr<MemShareArbitrator> arb, std::shared_ptr<MemLimiter> limiter,
                   int ins_idx, bool enable_adaptive_scan
#ifdef BE_TEST
                   ,
                   int num_parallel_instances
#endif
    );

    ~ScannerContext() override;
    Status init();

    // TODO(gabriel): we can also consider to return a list of blocks to reduce the scheduling overhead, but it may cause larger memory usage and more complex logic of block management.
    BlockUPtr get_free_block(bool force);
    void return_free_block(BlockUPtr block);
    void clear_free_blocks();
    inline void inc_block_usage(size_t usage) { _block_memory_usage += usage; }

    int64_t block_memory_usage() { return _block_memory_usage; }

    // Caller should make sure the pipeline task is still running when calling this function
    void update_peak_running_scanner(int num);
    void reestimated_block_mem_bytes(int64_t num);

    // Get next block from blocks queue. Called by ScanNode/ScanOperator
    // Set eos to true if there is no more data to read.
    Status get_block_from_queue(RuntimeState* state, Block* block, bool* eos, int id);

    [[nodiscard]] Status validate_block_schema(Block* block);

    // submit the running scanner to thread pool in `ScannerScheduler`
    // set the next scanned block to `ScanTask::current_block`
    // set the error state to `ScanTask::status`
    // set the `eos` to `ScanTask::eos` if there is no more data in current scanner
    Status submit_scan_task(std::shared_ptr<ScanTask> scan_task, std::unique_lock<std::mutex>&);

    // Push back a scan task.
    void push_back_scan_task(std::shared_ptr<ScanTask> scan_task);

    // Return true if this ScannerContext need no more process
    bool done() const { return _is_finished || _should_stop; }

    std::string debug_string();

    std::shared_ptr<TaskHandle> task_handle() const { return _task_handle; }

    std::shared_ptr<ResourceContext> resource_ctx() const { return _resource_ctx; }

    RuntimeState* state() { return _state; }

    void stop_scanners(RuntimeState* state);

    int batch_size() const { return _batch_size; }

    // During low memory mode, there will be at most 4 scanners running and every scanner will
    // cache at most 1MB data. So that every instance will keep 8MB buffer.
    bool low_memory_mode() const;

    // TODO(yiguolei) add this as session variable
    int32_t low_memory_mode_scan_bytes_per_scanner() const {
        return 1 * 1024 * 1024; // 1MB
    }

    int32_t low_memory_mode_scanners() const { return 4; }

    ScanLocalStateBase* local_state() const { return _local_state; }

    // the unique id of this context
    std::string ctx_id;
    TUniqueId _query_id;

    bool _should_reset_thread_name = true;

    int32_t num_scheduled_scanners() {
        std::lock_guard<std::mutex> l(_transfer_lock);
        return _in_flight_tasks_num;
    }

    Status schedule_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                              std::unique_lock<std::mutex>& transfer_lock,
                              std::unique_lock<std::shared_mutex>& scheduler_lock);

protected:
    /// Four criteria to determine whether to increase the parallelism of the scanners
    /// 1. It ran for at least `SCALE_UP_DURATION` ms after last scale up
    /// 2. Half(`WAIT_BLOCK_DURATION_RATIO`) of the duration is waiting to get blocks
    /// 3. `_free_blocks_memory_usage` < `_max_bytes_in_queue`, remains enough memory to scale up
    /// 4. At most scale up `MAX_SCALE_UP_RATIO` times to `_max_thread_num`
    void _set_scanner_done();

    RuntimeState* _state = nullptr;
    ScanLocalStateBase* _local_state = nullptr;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const RowDescriptor* _output_row_descriptor = nullptr;

    Status _process_status = Status::OK();
    std::atomic_bool _should_stop = false;
    std::atomic_bool _is_finished = false;

    // Lazy-allocated blocks for all scanners to share, for memory reuse.
    moodycamel::ConcurrentQueue<BlockUPtr> _free_blocks;

    int _batch_size;
    // The limit from SQL's limit clause
    int64_t limit;
    // Points to the shared remaining limit on ScanOperatorX, shared across all
    // parallel instances and their scanners. -1 means no limit.
    std::atomic<int64_t>* _shared_scan_limit = nullptr;
    // Atomically acquire up to `desired` rows. Returns actual granted count (0 = exhausted).
    int64_t acquire_limit_quota(int64_t desired);
    int64_t remaining_limit() const { return _shared_scan_limit->load(std::memory_order_acquire); }

    int64_t _max_bytes_in_queue = 0;
    // _transfer_lock protects _completed_tasks, _pending_tasks, and all other shared state
    // accessed by both the scanner thread pool and the operator (get_block_from_queue).
    std::mutex _transfer_lock;

    // Together, _completed_tasks and _in_flight_tasks_num represent all "occupied" concurrency
    // slots.  The scheduler uses their sum as the current concurrency:
    //
    //   current_concurrency = _completed_tasks.size() + _in_flight_tasks_num
    //
    // Lifecycle of a ScanTask:
    //   _pending_tasks  --(submit_scan_task)--> [thread pool]  --(push_back_scan_task)-->
    //   _completed_tasks  --(get_block_from_queue)--> operator
    //   After consumption: non-EOS task goes back to _pending_tasks; EOS increments
    //   _num_finished_scanners.

    // Completed scan tasks whose cached_block is ready for the operator to consume.
    // Protected by _transfer_lock.  Written by push_back_scan_task() (scanner thread),
    // read/popped by get_block_from_queue() (operator thread).
    std::list<std::shared_ptr<ScanTask>> _completed_tasks;

    // Scanners waiting to be submitted to the scheduler thread pool.  Stored as a stack
    // (LIFO) so that recently-used scanners are re-scheduled first, which is more likely
    // to be cache-friendly.  Protected by _transfer_lock.  Populated in the constructor
    // and by schedule_scan_task() when the concurrency limit is reached; drained by
    // _pull_next_scan_task() during scheduling.
    std::stack<std::shared_ptr<ScanTask>> _pending_tasks;

    // Number of scan tasks currently submitted to the scanner scheduler thread pool
    // (i.e. in-flight).  Incremented by submit_scan_task() before submission and
    // decremented by push_back_scan_task() when the thread pool returns the task.
    // Declared atomic so it can be read without _transfer_lock in non-critical paths,
    // but must be read under _transfer_lock whenever combined with _completed_tasks.size()
    // to form a consistent concurrency snapshot.
    std::atomic_int _in_flight_tasks_num = 0;
    // Scanner that is eos or error.
    int32_t _num_finished_scanners = 0;
    // weak pointer for _scanners, used in stop function
    std::vector<std::weak_ptr<ScannerDelegate>> _all_scanners;
    std::shared_ptr<RuntimeProfile> _scanner_profile;
    // This counter refers to scan operator's local state
    RuntimeProfile::Counter* _scanner_memory_used_counter = nullptr;
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    RuntimeProfile::Counter* _scale_up_scanners_counter = nullptr;
    std::shared_ptr<ResourceContext> _resource_ctx;
    std::shared_ptr<Dependency> _dependency = nullptr;
    std::shared_ptr<doris::TaskHandle> _task_handle;

    std::atomic<int64_t> _block_memory_usage = 0;

    // adaptive scan concurrency related

    ScannerScheduler* _scanner_scheduler = nullptr;
    MOCK_REMOVE(const) int32_t _min_scan_concurrency_of_scan_scheduler = 0;
    // The overall target of our system is to make full utilization of the resources.
    // At the same time, we dont want too many tasks are queued by scheduler, that is not necessary.
    // Each scan operator can submit _max_scan_concurrency scanner to scheduelr if scheduler has enough resource.
    // So that for a single query, we can make sure it could make full utilization of the resource.
    int32_t _max_scan_concurrency = 0;
    MOCK_REMOVE(const) int32_t _min_scan_concurrency = 1;

    std::shared_ptr<ScanTask> _pull_next_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                                   int32_t current_concurrency);

    int32_t _get_margin(std::unique_lock<std::mutex>& transfer_lock,
                        std::unique_lock<std::shared_mutex>& scheduler_lock);

    // Memory-aware adaptive scheduling
    std::shared_ptr<MemLimiter> _scanner_mem_limiter = nullptr;
    std::shared_ptr<MemShareArbitrator> _mem_share_arb = nullptr;
    std::shared_ptr<ScannerAdaptiveProcessor> _adaptive_processor = nullptr;
    const int _ins_idx;
    const bool _enable_adaptive_scanners = false;

    // Adjust scan memory limit based on arbitrator feedback
    void _adjust_scan_mem_limit(int64_t old_scanner_mem_bytes, int64_t new_scanner_mem_bytes);

    // Calculate available scanner count for adaptive scheduling
    int _available_pickup_scanner_count();

    // TODO: Add implementation of runtime_info_feed_back
    // adaptive scan concurrency related end
};
} // namespace doris
