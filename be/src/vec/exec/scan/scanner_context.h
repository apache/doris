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
#include "common/status.h"
#include "concurrentqueue.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/executor/split_runner.h"
#include "vec/exec/scan/scanner.h"

namespace doris {

class RuntimeState;
class TupleDescriptor;
class WorkloadGroup;

namespace pipeline {
class ScanLocalStateBase;
class Dependency;
} // namespace pipeline

namespace vectorized {

class Scanner;
class ScannerDelegate;
class ScannerScheduler;
class SimplifiedScanScheduler;
class TaskExecutor;
class TaskHandle;

class ScanTask {
public:
    ScanTask(std::weak_ptr<ScannerDelegate> delegate_scanner) : scanner(delegate_scanner) {
        _resource_ctx = thread_context()->resource_ctx();
        DorisMetrics::instance()->scanner_task_cnt->increment(1);
    }

    ScanTask(std::shared_ptr<ResourceContext> resource_ctx,
             std::weak_ptr<ScannerDelegate> delegate_scanner)
            : _resource_ctx(std::move(resource_ctx)), scanner(delegate_scanner) {}

    ~ScanTask() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_resource_ctx->memory_context()->mem_tracker());
        cached_blocks.clear();
        DorisMetrics::instance()->scanner_task_cnt->increment(-1);
    }

private:
    // whether current scanner is finished
    bool eos = false;
    Status status = Status::OK();
    std::shared_ptr<ResourceContext> _resource_ctx;

public:
    std::weak_ptr<ScannerDelegate> scanner;
    std::list<std::pair<vectorized::BlockUPtr, size_t>> cached_blocks;
    bool is_first_schedule = true;
    // Use weak_ptr to avoid circular references and potential memory leaks with SplitRunner.
    // ScannerContext only needs to observe the lifetime of SplitRunner without owning it.
    // When SplitRunner is destroyed, split_runner.lock() will return nullptr, ensuring safe access.
    std::weak_ptr<SplitRunner> split_runner;

    void set_status(Status _status) {
        if (_status.is<ErrorCode::END_OF_FILE>()) {
            // set `eos` if `END_OF_FILE`, don't take `END_OF_FILE` as error
            eos = true;
        }
        status = _status;
    }
    Status get_status() const { return status; }
    bool status_ok() { return status.ok() || status.is<ErrorCode::END_OF_FILE>(); }
    bool is_eos() const { return eos; }
    void set_eos(bool _eos) { eos = _eos; }
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
    friend class SimplifiedScanScheduler;

public:
    ScannerContext(RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
                   const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                   int64_t limit_, std::shared_ptr<pipeline::Dependency> dependency,
                   int num_parallel_instances);

    ~ScannerContext() override;
    Status init();

    vectorized::BlockUPtr get_free_block(bool force);
    void return_free_block(vectorized::BlockUPtr block);
    void clear_free_blocks();
    inline void inc_block_usage(size_t usage) { _block_memory_usage += usage; }

    int64_t block_memory_usage() { return _block_memory_usage; }

    // Caller should make sure the pipeline task is still running when calling this function
    void update_peak_running_scanner(int num);

    // Get next block from blocks queue. Called by ScanNode/ScanOperator
    // Set eos to true if there is no more data to read.
    Status get_block_from_queue(RuntimeState* state, vectorized::Block* block, bool* eos, int id);

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

    SimplifiedScanScheduler* get_scan_scheduler() { return _scanner_scheduler; }

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

    pipeline::ScanLocalStateBase* local_state() const { return _local_state; }

    // the unique id of this context
    std::string ctx_id;
    TUniqueId _query_id;

    bool _should_reset_thread_name = true;

    int32_t num_scheduled_scanners() {
        std::lock_guard<std::mutex> l(_transfer_lock);
        return _num_scheduled_scanners;
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
    Status _try_to_scale_up();

    RuntimeState* _state = nullptr;
    pipeline::ScanLocalStateBase* _local_state = nullptr;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const RowDescriptor* _output_row_descriptor = nullptr;

    std::mutex _transfer_lock;
    std::list<std::shared_ptr<ScanTask>> _tasks_queue;

    Status _process_status = Status::OK();
    std::atomic_bool _should_stop = false;
    std::atomic_bool _is_finished = false;

    // Lazy-allocated blocks for all scanners to share, for memory reuse.
    moodycamel::ConcurrentQueue<vectorized::BlockUPtr> _free_blocks;

    int _batch_size;
    // The limit from SQL's limit clause
    int64_t limit;

    int64_t _max_bytes_in_queue = 0;
    doris::vectorized::ScannerScheduler* _scanner_scheduler_global = nullptr;
    SimplifiedScanScheduler* _scanner_scheduler = nullptr;
    // Using stack so that we can resubmit scanner in a LIFO order, maybe more cache friendly
    std::stack<std::weak_ptr<ScannerDelegate>> _pending_scanners;
    // Scanner that is submitted to the scheduler.
    std::atomic_int _num_scheduled_scanners = 0;
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
    std::shared_ptr<pipeline::Dependency> _dependency = nullptr;
    const int _parallism_of_scan_operator;
    std::shared_ptr<doris::vectorized::TaskHandle> _task_handle;

    std::atomic<int64_t> _block_memory_usage = 0;

    // adaptive scan concurrency related

    int32_t _min_scan_concurrency_of_scan_scheduler = 0;
    int32_t _min_scan_concurrency = 1;
    int32_t _max_scan_concurrency = 0;

    std::shared_ptr<ScanTask> _pull_next_scan_task(std::shared_ptr<ScanTask> current_scan_task,
                                                   int32_t current_concurrency);

    int32_t _get_margin(std::unique_lock<std::mutex>& transfer_lock,
                        std::unique_lock<std::shared_mutex>& scheduler_lock);

    // TODO: Add implementation of runtime_info_feed_back
    // adaptive scan concurrency related end
};
} // namespace vectorized
} // namespace doris
