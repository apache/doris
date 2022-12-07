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

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "util/uid_util.h"
#include "vec/core/block.h"

namespace doris {

class PriorityThreadPool;
class ThreadPool;
class ThreadPoolToken;
class ScannerScheduler;

namespace vectorized {

class VScanner;
class VScanNode;

// ScannerContext is responsible for recording the execution status
// of a group of Scanners corresponding to a ScanNode.
// Including how many scanners are being scheduled, and maintaining
// a producer-consumer blocks queue between scanners and scan nodes.
//
// ScannerContext is also the scheduling unit of ScannerScheduler.
// ScannerScheduler schedules a ScannerContext at a time,
// and submits the Scanners to the scanner thread pool for data scanning.
class ScannerContext {
public:
    ScannerContext(RuntimeState* state_, VScanNode* parent, const TupleDescriptor* input_tuple_desc,
                   const TupleDescriptor* output_tuple_desc, const std::list<VScanner*>& scanners_,
                   int64_t limit_, int64_t max_bytes_in_blocks_queue_)
            : _state(state_),
              _parent(parent),
              _input_tuple_desc(input_tuple_desc),
              _output_tuple_desc(output_tuple_desc),
              _process_status(Status::OK()),
              limit(limit_),
              _max_bytes_in_queue(max_bytes_in_blocks_queue_),
              _scanners(scanners_) {
        ctx_id = UniqueId::gen_uid().to_string();
        if (_scanners.empty()) {
            _is_finished = true;
        }
    }

    virtual ~ScannerContext() = default;

    Status init();

    vectorized::Block* get_free_block(bool* get_free_block);
    void return_free_block(vectorized::Block* block);

    // Append blocks from scanners to the blocks queue.
    void append_blocks_to_queue(const std::vector<vectorized::Block*>& blocks);

    // Get next block from blocks queue. Called by ScanNode
    // Set eos to true if there is no more data to read.
    // And if eos is true, the block returned must be nullptr.
    Status get_block_from_queue(vectorized::Block** block, bool* eos, bool wait = true);

    // When a scanner complete a scan, this method will be called
    // to return the scanner to the list for next scheduling.
    void push_back_scanner_and_reschedule(VScanner* scanner);

    bool set_status_on_error(const Status& status);

    Status status() {
        std::lock_guard<std::mutex> l(_transfer_lock);
        return _process_status;
    }

    // Called by ScanNode.
    // Used to notify the scheduler that this ScannerContext can stop working.
    void set_should_stop() {
        std::lock_guard<std::mutex> l(_transfer_lock);
        _should_stop = true;
        _blocks_queue_added_cv.notify_one();
    }

    // Return true if this ScannerContext need no more process
    virtual bool done() {
        std::unique_lock<std::mutex> l(_transfer_lock);
        return _is_finished || _should_stop || !_process_status.ok();
    }

    // Update the running num of scanners and contexts
    void update_num_running(int32_t scanner_inc, int32_t sched_inc) {
        std::lock_guard<std::mutex> l(_transfer_lock);
        _num_running_scanners += scanner_inc;
        _num_scheduling_ctx += sched_inc;
        _blocks_queue_added_cv.notify_one();
        _ctx_finish_cv.notify_one();
    }

    void get_next_batch_of_scanners(std::list<VScanner*>* current_run);

    void clear_and_join();

    virtual bool can_finish();

    std::string debug_string();

    RuntimeState* state() { return _state; }

    void incr_num_ctx_scheduling(int64_t num) { _num_ctx_scheduling += num; }
    void incr_num_scanner_scheduling(int64_t num) { _num_scanner_scheduling += num; }

    VScanNode* parent() { return _parent; }

    virtual bool empty_in_queue();

    // the unique id of this context
    std::string ctx_id;
    int32_t queue_idx = -1;
    ThreadPoolToken* thread_token;

private:
    Status _close_and_clear_scanners();

    inline bool _has_enough_space_in_blocks_queue() const {
        return _cur_bytes_in_queue < _max_bytes_in_queue / 2;
    }

    // do nothing here, we only do update on pip_scanner_context
    virtual void _update_block_queue_empty() {}

protected:
    RuntimeState* _state;
    VScanNode* _parent;

    // the comment of same fields in VScanNode
    const TupleDescriptor* _input_tuple_desc;
    const TupleDescriptor* _output_tuple_desc;
    // If _input_tuple_desc is not null, _real_tuple_desc point to _input_tuple_desc,
    // otherwise, _real_tuple_desc point to _output_tuple_desc
    const TupleDescriptor* _real_tuple_desc;

    // _transfer_lock is used to protect the critical section
    // where the ScanNode and ScannerScheduler interact.
    // Including access to variables such as blocks_queue, _process_status, _is_finished, etc.
    std::mutex _transfer_lock;
    // The blocks got from scanners will be added to the "blocks_queue".
    // And the upper scan node will be as a consumer to fetch blocks from this queue.
    // Should be protected by "_transfer_lock"
    std::list<vectorized::Block*> _blocks_queue;
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

    // Pre-allocated blocks for all scanners to share, for memory reuse.
    std::mutex _free_blocks_lock;
    std::vector<vectorized::Block*> _free_blocks;

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
    int32_t _num_unfinished_scanners = 0;
    // Max number of scan thread for this scanner context.
    int32_t _max_thread_num = 0;
    // How many blocks a scanner can use in one task.
    int32_t _block_per_scanner = 0;

    // The current bytes of blocks in blocks queue
    int64_t _cur_bytes_in_queue = 0;
    // The max limit bytes of blocks in blocks queue
    int64_t _max_bytes_in_queue;

    // List "scanners" saves all "unfinished" scanners.
    // The scanner scheduler will pop scanners from this list, run scanner,
    // and then if the scanner is not finished, will be pushed back to this list.
    // Not need to protect by lock, because only one scheduler thread will access to it.
    std::mutex _scanners_lock;
    std::list<VScanner*> _scanners;

    int64_t _num_ctx_scheduling = 0;
    int64_t _num_scanner_scheduling = 0;
};
} // namespace vectorized
} // namespace doris
