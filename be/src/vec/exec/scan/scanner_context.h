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
#include "util/lock.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {

class ThreadPoolToken;
class RuntimeState;
class TupleDescriptor;

namespace pipeline {
class ScanLocalState;
} // namespace pipeline

namespace taskgroup {
class TaskGroup;
} // namespace taskgroup

namespace vectorized {

class VScanner;
class VScanNode;
class ScannerScheduler;

// ScannerContext is responsible for recording the execution status
// of a group of Scanners corresponding to a ScanNode.
// Including how many scanners are being scheduled, and maintaining
// a producer-consumer blocks queue between scanners and scan nodes.
//
// ScannerContext is also the scheduling unit of ScannerScheduler.
// ScannerScheduler schedules a ScannerContext at a time,
// and submits the Scanners to the scanner thread pool for data scanning.
class ScannerContext {
    ENABLE_FACTORY_CREATOR(ScannerContext);

public:
    ScannerContext(RuntimeState* state_, VScanNode* parent,
                   const TupleDescriptor* output_tuple_desc,
                   const std::list<VScannerSPtr>& scanners_, int64_t limit_,
                   int64_t max_bytes_in_blocks_queue_, const int num_parallel_instances = 0,
                   pipeline::ScanLocalState* local_state = nullptr);

    virtual ~ScannerContext() = default;
    virtual Status init();

    vectorized::BlockUPtr get_free_block(bool* has_free_block, bool get_not_empty_block = false);
    void return_free_block(std::unique_ptr<vectorized::Block> block);

    // Append blocks from scanners to the blocks queue.
    virtual void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks);
    // Get next block from blocks queue. Called by ScanNode
    // Set eos to true if there is no more data to read.
    // And if eos is true, the block returned must be nullptr.
    virtual Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block,
                                        bool* eos, int id, bool wait = true);

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
    void set_should_stop() {
        std::lock_guard l(_transfer_lock);
        _should_stop = true;
        _blocks_queue_added_cv.notify_one();
    }

    // Return true if this ScannerContext need no more process
    virtual bool done() { return _is_finished || _should_stop; }

    // Update the running num of scanners and contexts
    void update_num_running(int32_t scanner_inc, int32_t sched_inc) {
        std::lock_guard l(_transfer_lock);
        _num_running_scanners += scanner_inc;
        _num_scheduling_ctx += sched_inc;
        _blocks_queue_added_cv.notify_one();
        _ctx_finish_cv.notify_one();
    }

    int get_num_running_scanners() const { return _num_running_scanners; }

    int get_num_scheduling_ctx() const { return _num_scheduling_ctx; }

    void get_next_batch_of_scanners(std::list<VScannerSPtr>* current_run);

    template <typename Parent>
    void clear_and_join(Parent* parent, RuntimeState* state);

    bool no_schedule();

    std::string debug_string();

    RuntimeState* state() { return _state; }

    void incr_num_ctx_scheduling(int64_t num) { _scanner_ctx_sched_counter->update(num); }
    void incr_ctx_scheduling_time(int64_t num) { _scanner_ctx_sched_time->update(num); }
    void incr_num_scanner_scheduling(int64_t num) { _scanner_sched_counter->update(num); }

    VScanNode* parent() { return _parent; }

    virtual bool empty_in_queue(int id);

    // todo(wb) rethinking how to calculate ```_max_bytes_in_queue``` when executing shared scan
    virtual inline bool has_enough_space_in_blocks_queue() const {
        return _cur_bytes_in_queue < _max_bytes_in_queue / 2;
    }

    int cal_thread_slot_num_by_free_block_num() {
        int thread_slot_num = 0;
        thread_slot_num = (_free_blocks_capacity + _block_per_scanner - 1) / _block_per_scanner;
        thread_slot_num = std::min(thread_slot_num, _max_thread_num - _num_running_scanners);
        if (thread_slot_num <= 0) {
            thread_slot_num = 1;
        }
        return thread_slot_num;
    }
    taskgroup::TaskGroup* get_task_group() const;

    void reschedule_scanner_ctx();

    // the unique id of this context
    std::string ctx_id;
    int32_t queue_idx = -1;
    ThreadPoolToken* thread_token;
    std::vector<bthread_t> _btids;

private:
    template <typename Parent>
    Status _close_and_clear_scanners(Parent* parent, RuntimeState* state);

protected:
    virtual void _dispose_coloate_blocks_not_in_queue() {}

    RuntimeState* _state;
    VScanNode* _parent;
    pipeline::ScanLocalState* _local_state;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _output_tuple_desc;

    // _transfer_lock is used to protect the critical section
    // where the ScanNode and ScannerScheduler interact.
    // Including access to variables such as blocks_queue, _process_status, _is_finished, etc.
    doris::Mutex _transfer_lock;
    // The blocks got from scanners will be added to the "blocks_queue".
    // And the upper scan node will be as a consumer to fetch blocks from this queue.
    // Should be protected by "_transfer_lock"
    std::list<vectorized::BlockUPtr> _blocks_queue;
    // Wait in get_block_from_queue(), by ScanNode.
    doris::ConditionVariable _blocks_queue_added_cv;
    // Wait in clear_and_join(), by ScanNode.
    doris::ConditionVariable _ctx_finish_cv;

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
    // The current number of free blocks available to the scanners.
    // Used to limit the memory usage of the scanner.
    // NOTE: this is NOT the size of `_free_blocks`.
    std::atomic_int32_t _free_blocks_capacity = 0;

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
    // List "scanners" saves all "unfinished" scanners.
    // The scanner scheduler will pop scanners from this list, run scanner,
    // and then if the scanner is not finished, will be pushed back to this list.
    // Not need to protect by lock, because only one scheduler thread will access to it.
    doris::Mutex _scanners_lock;
    std::list<VScannerSPtr> _scanners;
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
};
} // namespace vectorized
} // namespace doris
