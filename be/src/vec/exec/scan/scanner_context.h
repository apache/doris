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

namespace pipeline {
class ScanLocalStateBase;
class ScanDependency;
class Dependency;
} // namespace pipeline

namespace taskgroup {
class TaskGroup;
} // namespace taskgroup

namespace vectorized {

class VScanner;
class VScanNode;
class ScannerScheduler;
class SimplifiedScanScheduler;

// ScannerContext is responsible for recording the execution status
// of a group of Scanners corresponding to a ScanNode.
// Including how many scanners are being scheduled, and maintaining
// a producer-consumer blocks queue between scanners and scan nodes.
//
// ScannerContext is also the scheduling unit of ScannerScheduler.
// ScannerScheduler schedules a ScannerContext at a time,
// and submits the Scanners to the scanner thread pool for data scanning.
class ScannerContext : public std::enable_shared_from_this<ScannerContext> {
    ENABLE_FACTORY_CREATOR(ScannerContext);

public:
    ScannerContext(RuntimeState* state, VScanNode* parent, const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<VScannerSPtr>& scanners, int64_t limit_,
                   int64_t max_bytes_in_blocks_queue, const int num_parallel_instances = 1,
                   pipeline::ScanLocalStateBase* local_state = nullptr);

    virtual ~ScannerContext() = default;
    virtual Status init();

    vectorized::BlockUPtr get_free_block();
    void return_free_block(std::unique_ptr<vectorized::Block> block);

    // Append blocks from scanners to the blocks queue.
    virtual void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks);
    // Get next block from blocks queue. Called by ScanNode
    // Set eos to true if there is no more data to read.
    // And if eos is true, the block returned must be nullptr.
    virtual Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block,
                                        bool* eos, int id, bool wait = true);

    [[nodiscard]] Status validate_block_schema(Block* block);

    // When a scanner complete a scan, this method will be called
    // to return the scanner to the list for next scheduling.
    void push_back_scanner_and_reschedule(VScannerSPtr scanner);

    bool set_status_on_error(const Status& status, bool need_lock = true);

    Status status() {
        if (_process_status.is<ErrorCode::END_OF_FILE>()) {
            return Status::OK();
        }
        return _process_status;
    }

    // Called by ScanNode.
    // Used to notify the scheduler that this ScannerContext can stop working.
    void set_should_stop();

    // Return true if this ScannerContext need no more process
    virtual bool done() { return _is_finished || _should_stop; }
    bool is_finished() { return _is_finished.load(); }
    bool should_stop() { return _should_stop.load(); }
    bool status_error() { return _status_error.load(); }

    void inc_num_running_scanners(int32_t scanner_inc);

    void set_ready_to_finish();

    int get_num_running_scanners() const { return _num_running_scanners; }

    int get_num_unfinished_scanners() const { return _num_unfinished_scanners; }

    void dec_num_scheduling_ctx();

    int get_num_scheduling_ctx() const { return _num_scheduling_ctx; }

    void get_next_batch_of_scanners(std::list<VScannerSPtr>* current_run);

    template <typename Parent>
    void clear_and_join(Parent* parent, RuntimeState* state);

    bool no_schedule();

    virtual std::string debug_string();

    RuntimeState* state() { return _state; }

    void incr_num_ctx_scheduling(int64_t num) { _scanner_ctx_sched_counter->update(num); }
    void incr_ctx_scheduling_time(int64_t num) { _scanner_ctx_sched_time->update(num); }
    void incr_num_scanner_scheduling(int64_t num) { _scanner_sched_counter->update(num); }

    std::string parent_name();

    virtual bool empty_in_queue(int id);

    // todo(wb) rethinking how to calculate ```_max_bytes_in_queue``` when executing shared scan
    inline bool should_be_scheduled() const {
        return (_cur_bytes_in_queue < _max_bytes_in_queue / 2) &&
               (_serving_blocks_num < allowed_blocks_num());
    }

    int get_available_thread_slot_num() {
        int thread_slot_num = 0;
        thread_slot_num = (allowed_blocks_num() + _block_per_scanner - 1) / _block_per_scanner;
        thread_slot_num = std::min(thread_slot_num, _max_thread_num - _num_running_scanners);
        if (thread_slot_num <= 0) {
            thread_slot_num = 1;
        }
        return thread_slot_num;
    }

    int32_t allowed_blocks_num() const {
        int32_t blocks_num = std::min(_free_blocks_capacity,
                                      int32_t((_max_bytes_in_queue + _estimated_block_bytes - 1) /
                                              _estimated_block_bytes));
        return blocks_num;
    }

    SimplifiedScanScheduler* get_simple_scan_scheduler() { return _simple_scan_scheduler; }

    virtual void reschedule_scanner_ctx();

    // the unique id of this context
    std::string ctx_id;
    TUniqueId _query_id;
    int32_t queue_idx = -1;
    ThreadPoolToken* thread_token = nullptr;

    bool _should_reset_thread_name = true;

    std::weak_ptr<TaskExecutionContext> get_task_execution_context() { return _task_exec_ctx; }

private:
    template <typename Parent>
    Status _close_and_clear_scanners(Parent* parent, RuntimeState* state);

protected:
    ScannerContext(RuntimeState* state_, const TupleDescriptor* output_tuple_desc,
                   const RowDescriptor* output_row_descriptor,
                   const std::list<VScannerSPtr>& scanners_, int64_t limit_,
                   int64_t max_bytes_in_blocks_queue_, const int num_parallel_instances,
                   pipeline::ScanLocalStateBase* local_state,
                   std::shared_ptr<pipeline::ScanDependency> dependency,
                   std::shared_ptr<pipeline::Dependency> finish_dependency);
    virtual void _dispose_coloate_blocks_not_in_queue() {}

    void _set_scanner_done();

    RuntimeState* _state = nullptr;
    std::weak_ptr<TaskExecutionContext> _task_exec_ctx;
    VScanNode* _parent = nullptr;
    pipeline::ScanLocalStateBase* _local_state = nullptr;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc = nullptr;
    const RowDescriptor* _output_row_descriptor = nullptr;

    // _transfer_lock is used to protect the critical section
    // where the ScanNode and ScannerScheduler interact.
    // Including access to variables such as blocks_queue, _process_status, _is_finished, etc.
    std::mutex _transfer_lock;
    // The blocks got from scanners will be added to the "blocks_queue".
    // And the upper scan node will be as a consumer to fetch blocks from this queue.
    // Should be protected by "_transfer_lock"
    std::list<vectorized::BlockUPtr> _blocks_queue;
    // Wait in get_block_from_queue(), by ScanNode.
    std::condition_variable _blocks_queue_added_cv;
    // Wait in clear_and_join(), by ScanNode.
    std::condition_variable _ctx_finish_cv;

    // The following 3 variables control the process of the scanner scheduling.
    // Use _transfer_lock to protect them.
    // 1. _process_status
    //      indicates the global status of this scanner context.
    //      Set to non-ok if encounter errors.
    //      And if it is non-ok, the scanner process should stop.
    //      Set be set by either ScanNode or ScannerScheduler.
    // 2. _should_stop
    //      Always be set by ScanNode.
    //      True means no more data need to be read(reach limit or closed)
    // 3. _is_finished
    //      Always be set by ScannerScheduler.
    //      True means all scanners are finished to scan.
    Status _process_status;
    std::atomic_bool _status_error = false;
    std::atomic_bool _should_stop = false;
    std::atomic_bool _is_finished = false;

    // Lazy-allocated blocks for all scanners to share, for memory reuse.
    moodycamel::ConcurrentQueue<vectorized::BlockUPtr> _free_blocks;
    std::atomic<int32_t> _serving_blocks_num = 0;
    // The current number of free blocks available to the scanners.
    // Used to limit the memory usage of the scanner.
    // NOTE: this is NOT the size of `_free_blocks`.
    int32_t _free_blocks_capacity = 0;
    int64_t _estimated_block_bytes = 0;

    int _batch_size;
    // The limit from SQL's limit clause
    int64_t limit;

    // Current number of running scanners.
    std::atomic_int32_t _num_running_scanners = 0;
    // Current number of ctx being scheduled.
    // After each Scanner finishes a task, it will put the corresponding ctx
    // back into the scheduling queue.
    // Therefore, there will be multiple pointer of same ctx in the scheduling queue.
    // Here we record the number of ctx in the scheduling  queue to clean up at the end.
    std::atomic_int32_t _num_scheduling_ctx = 0;
    // Num of unfinished scanners. Should be set in init()
    std::atomic_int32_t _num_unfinished_scanners = 0;
    // Max number of scan thread for this scanner context.
    int32_t _max_thread_num = 0;
    // How many blocks a scanner can use in one task.
    int32_t _block_per_scanner = 0;

    // The current bytes of blocks in blocks queue
    int64_t _cur_bytes_in_queue = 0;
    // The max limit bytes of blocks in blocks queue
    const int64_t _max_bytes_in_queue;

    doris::vectorized::ScannerScheduler* _scanner_scheduler;
    SimplifiedScanScheduler* _simple_scan_scheduler = nullptr; // used for cpu hard limit
    // List "scanners" saves all "unfinished" scanners.
    // The scanner scheduler will pop scanners from this list, run scanner,
    // and then if the scanner is not finished, will be pushed back to this list.
    // Not need to protect by lock, because only one scheduler thread will access to it.
    std::mutex _scanners_lock;
    std::list<VScannerSPtr> _scanners;
    // weak pointer for _scanners, used in stop function
    std::vector<VScannerWPtr> _scanners_ref;
    std::vector<int64_t> _finished_scanner_runtime;
    std::vector<int64_t> _finished_scanner_rows_read;
    std::vector<int64_t> _finished_scanner_wait_worker_time;

    const int _num_parallel_instances;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeProfile::Counter* _scanner_sched_counter = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_counter = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_time = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _free_blocks_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _queued_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;

    std::shared_ptr<pipeline::ScanDependency> _dependency = nullptr;
    std::shared_ptr<pipeline::Dependency> _finish_dependency = nullptr;
};
} // namespace vectorized
} // namespace doris
