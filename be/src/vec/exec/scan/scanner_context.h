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
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "concurrentqueue.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {

class ThreadPoolToken;
class RuntimeState;
class TupleDescriptor;
class WorkloadGroup;

namespace pipeline {
class ScanLocalStateBase;
class Dependency;
} // namespace pipeline

namespace vectorized {

class VScanner;
class ScannerDelegate;
class VScanNode;
class ScannerScheduler;
class SimplifiedScanScheduler;

class ScanTask {
public:
    ScanTask(std::weak_ptr<ScannerDelegate> delegate_scanner) : scanner(delegate_scanner) {
        _query_thread_context.init();
        DorisMetrics::instance()->scanner_task_cnt->increment(1);
    }

    ~ScanTask() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_thread_context.query_mem_tracker);
        cached_blocks.clear();
        DorisMetrics::instance()->scanner_task_cnt->increment(-1);
    }

private:
    // whether current scanner is finished
    bool eos = false;
    Status status = Status::OK();
    QueryThreadContext _query_thread_context;

public:
    std::weak_ptr<ScannerDelegate> scanner;
    std::list<vectorized::BlockUPtr> cached_blocks;
    uint64_t last_submit_time; // nanoseconds

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

public:
    ScannerContext(RuntimeState* state, VScanNode* parent, const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<std::shared_ptr<ScannerDelegate>>& scanners, int64_t limit_,
                   bool ignore_data_distribution, bool is_file_scan_operator,
                   pipeline::ScanLocalStateBase* local_state = nullptr);

    ~ScannerContext() override {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_thread_context.query_mem_tracker);
        _blocks_queue.clear();
        vectorized::BlockUPtr block;
        while (_free_blocks.try_dequeue(block)) {
            // do nothing
        }
        block.reset();
        DorisMetrics::instance()->scanner_ctx_cnt->increment(-1);
    }
    virtual Status init();

    vectorized::BlockUPtr get_free_block(bool force);
    void return_free_block(vectorized::BlockUPtr block);
    inline void inc_block_usage(size_t usage) { _block_memory_usage += usage; }

    // Caller should make sure the pipeline task is still running when calling this function
    void update_peak_running_scanner(int num);
    // Caller should make sure the pipeline task is still running when calling this function
    void update_peak_memory_usage(int64_t usage);

    // Get next block from blocks queue. Called by ScanNode/ScanOperator
    // Set eos to true if there is no more data to read.
    virtual Status get_block_from_queue(RuntimeState* state, vectorized::Block* block, bool* eos,
                                        int id, bool wait = true);

    [[nodiscard]] Status validate_block_schema(Block* block);

    // submit the running scanner to thread pool in `ScannerScheduler`
    // set the next scanned block to `ScanTask::current_block`
    // set the error state to `ScanTask::status`
    // set the `eos` to `ScanTask::eos` if there is no more data in current scanner
    Status submit_scan_task(std::shared_ptr<ScanTask> scan_task);

    // append the running scanner and its cached block to `_blocks_queue`
    virtual void append_block_to_queue(std::shared_ptr<ScanTask> scan_task);

    void set_status_on_error(const Status& status);

    // Return true if this ScannerContext need no more process
    bool done() const { return _is_finished || _should_stop; }
    bool is_finished() { return _is_finished.load(); }
    bool should_stop() { return _should_stop.load(); }

    virtual std::string debug_string();

    RuntimeState* state() { return _state; }
    void incr_ctx_scheduling_time(int64_t num) { _scanner_ctx_sched_time->update(num); }
    std::string parent_name();

    virtual bool empty_in_queue(int id);

    SimplifiedScanScheduler* get_scan_scheduler() { return _scanner_scheduler; }

    void stop_scanners(RuntimeState* state);

    int32_t get_max_thread_num() const { return _max_thread_num; }
    void set_max_thread_num(int32_t num) { _max_thread_num = num; }

    int batch_size() const { return _batch_size; }

    // the unique id of this context
    std::string ctx_id;
    TUniqueId _query_id;
    int32_t queue_idx = -1;
    ThreadPoolToken* thread_token = nullptr;

    bool _should_reset_thread_name = true;

protected:
    ScannerContext(RuntimeState* state_, const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<std::shared_ptr<ScannerDelegate>>& scanners_, int64_t limit_,
                   bool ignore_data_distribution, bool is_file_scan_operator,
                   pipeline::ScanLocalStateBase* local_state);

    /// Four criteria to determine whether to increase the parallelism of the scanners
    /// 1. It ran for at least `SCALE_UP_DURATION` ms after last scale up
    /// 2. Half(`WAIT_BLOCK_DURATION_RATIO`) of the duration is waiting to get blocks
    /// 3. `_free_blocks_memory_usage` < `_max_bytes_in_queue`, remains enough memory to scale up
    /// 4. At most scale up `MAX_SCALE_UP_RATIO` times to `_max_thread_num`
    virtual void _set_scanner_done() {};
    Status _try_to_scale_up();

    RuntimeState* _state = nullptr;
    VScanNode* _parent = nullptr;
    pipeline::ScanLocalStateBase* _local_state = nullptr;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const RowDescriptor* _output_row_descriptor = nullptr;

    std::mutex _transfer_lock;
    std::condition_variable _blocks_queue_added_cv;
    std::list<std::shared_ptr<ScanTask>> _blocks_queue;

    Status _process_status = Status::OK();
    std::atomic_bool _should_stop = false;
    std::atomic_bool _is_finished = false;

    // Lazy-allocated blocks for all scanners to share, for memory reuse.
    moodycamel::ConcurrentQueue<vectorized::BlockUPtr> _free_blocks;

    int _batch_size;
    // The limit from SQL's limit clause
    int64_t limit;

    int32_t _max_thread_num = 0;
    int64_t _max_bytes_in_queue = 0;
    doris::vectorized::ScannerScheduler* _scanner_scheduler_global = nullptr;
    SimplifiedScanScheduler* _scanner_scheduler = nullptr;
    moodycamel::ConcurrentQueue<std::weak_ptr<ScannerDelegate>> _scanners;
    int32_t _num_scheduled_scanners = 0;
    int32_t _num_finished_scanners = 0;
    int32_t _num_running_scanners = 0;
    // weak pointer for _scanners, used in stop function
    std::vector<std::weak_ptr<ScannerDelegate>> _all_scanners;
    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeProfile::Counter* _scanner_sched_counter = nullptr;
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_time = nullptr;
    RuntimeProfile::Counter* _scale_up_scanners_counter = nullptr;
    QueryThreadContext _query_thread_context;
    bool _ignore_data_distribution = false;
    bool _is_file_scan_operator;

    // for scaling up the running scanners
    size_t _estimated_block_size = 0;
    std::atomic_long _block_memory_usage = 0;
    int64_t _last_scale_up_time = 0;
    int64_t _last_fetch_time = 0;
    int64_t _total_wait_block_time = 0;
    double _last_wait_duration_ratio = 0;
    const int64_t SCALE_UP_DURATION = 5000; // 5000ms
    const float WAIT_BLOCK_DURATION_RATIO = 0.5;
    const float SCALE_UP_RATIO = 0.5;
    float MAX_SCALE_UP_RATIO;
};
} // namespace vectorized
} // namespace doris
