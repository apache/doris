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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/disk-io-mgr-internal.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_INTERNAL_H
#define DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_INTERNAL_H

#include <unistd.h>

#include <queue>

#include "common/logging.h"
#include "common/status.h"
#include "disk_io_mgr.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/filesystem_util.h"

// This file contains internal structures to the IoMgr. Users of the IoMgr do
// not need to include this file.
namespace doris {

// Per disk state
struct DiskIoMgr::DiskQueue {
    // Disk id (0-based)
    int disk_id;

    // Lock that protects access to 'request_contexts' and 'work_available'
    std::mutex lock;

    // Condition variable to signal the disk threads that there is work to do or the
    // thread should shut down.  A disk thread will be woken up when there is a reader
    // added to the queue. A reader is only on the queue when it has at least one
    // scan range that is not blocked on available buffers.
    std::condition_variable work_available;

    // list of all request contexts that have work queued on this disk
    std::list<RequestContext*> request_contexts;

    // Enqueue the request context to the disk queue.  The DiskQueue lock must not be taken.
    void enqueue_context(RequestContext* worker) {
        {
            std::unique_lock<std::mutex> disk_lock(lock);
            // Check that the reader is not already on the queue
            DCHECK(find(request_contexts.begin(), request_contexts.end(), worker) ==
                   request_contexts.end());
            request_contexts.push_back(worker);
        }
        work_available.notify_all();
    }

    DiskQueue(int id) : disk_id(id) {}
};

// Internal per request-context state. This object maintains a lot of state that is
// carefully synchronized. The context maintains state across all disks as well as
// per disk state.
// The unit for an IO request is a RequestRange, which may be a ScanRange or a
// WriteRange.
// A scan range for the reader is on one of five states:
// 1) PerDiskState's unstarted_ranges: This range has only been queued
//    and nothing has been read from it.
// 2) RequestContext's _ready_to_start_ranges: This range is about to be started.
//    As soon as the reader picks it up, it will move to the in_flight_ranges
//    queue.
// 3) PerDiskState's in_flight_ranges: This range is being processed and will
//    be read from the next time a disk thread picks it up in get_next_request_range()
// 4) ScanRange's outgoing ready buffers is full. We can't read for this range
//    anymore. We need the caller to pull a buffer off which will put this in
//    the in_flight_ranges queue. These ranges are in the RequestContext's
//    _blocked_ranges queue.
// 5) ScanRange is cached and in the _cached_ranges queue.
//
// If the scan range is read and does not get blocked on the outgoing queue, the
// transitions are: 1 -> 2 -> 3.
// If the scan range does get blocked, the transitions are
// 1 -> 2 -> 3 -> (4 -> 3)*
//
// In the case of a cached scan range, the range is immediately put in _cached_ranges.
// When the caller asks for the next range to process, we first pull ranges from
// the _cache_ranges queue. If the range was cached, the range is removed and
// done (ranges are either entirely cached or not at all). If the cached read attempt
// fails, we put the range in state 1.
//
// A write range for a context may be in one of two lists:
// 1) _unstarted_write_ranges : Ranges that have been queued but not processed.
// 2) _in_flight_ranges: The write range is ready to be processed by the next disk thread
//    that picks it up in get_next_request_range().
//
// AddWriteRange() adds WriteRanges for a disk.
// It is the responsibility of the client to pin the data to be written via a WriteRange
// in memory. After a WriteRange has been written, a callback is invoked to inform the
// client that the write has completed.
//
// An important assumption is that write does not exceed the maximum read size and that
// the entire range is written when the write request is handled. (In other words, writes
// are not broken up.)
//
// When a RequestContext is processed by a disk thread in get_next_request_range(), a write
// range is always removed from the list of unstarted write ranges and appended to the
// _in_flight_ranges queue. This is done to alternate reads and writes - a read that is
// scheduled (by calling GetNextRange()) is always followed by a write (if one exists).
// And since at most one WriteRange can be present in _in_flight_ranges at any time
// (once a write range is returned from GetNetxRequestRange() it is completed and not
// re-enqueued), a scan range scheduled via a call to GetNextRange() can be queued up
// behind at most one write range.
class DiskIoMgr::RequestContext {
public:
    enum State {
        // Reader is initialized and maps to a client
        Active,

        // Reader is in the process of being cancelled.  Cancellation is coordinated between
        // different threads and when they are all complete, the reader context is moved to
        // the inactive state.
        Cancelled,

        // Reader context does not map to a client.  Accessing memory in this context
        // is invalid (i.e. it is equivalent to a dangling pointer).
        Inactive,
    };

    RequestContext(DiskIoMgr* parent, int num_disks);

    // Resets this object.
    void reset(std::shared_ptr<MemTracker> tracker);

    // Decrements the number of active disks for this reader.  If the disk count
    // goes to 0, the disk complete condition variable is signaled.
    // Reader lock must be taken before this call.
    void decrement_disk_ref_count() {
        // boost doesn't let us dcheck that the reader lock is taken
        DCHECK_GT(_num_disks_with_ranges, 0);
        if (--_num_disks_with_ranges == 0) {
            _disks_complete_cond_var.notify_one();
        }
        DCHECK(validate()) << std::endl << debug_string();
    }

    // Reader & Disk Scheduling: Readers that currently can't do work are not on
    // the disk's queue. These readers are ones that don't have any ranges in the
    // in_flight_queue AND have not prepared a range by setting next_range_to_start.
    // The rule to make sure readers are scheduled correctly is to ensure anytime a
    // range is put on the in_flight_queue or anytime next_range_to_start is set to
    // nullptr, the reader is scheduled.

    // Adds range to in_flight_ranges, scheduling this reader on the disk threads
    // if necessary.
    // Reader lock must be taken before this.
    void schedule_scan_range(DiskIoMgr::ScanRange* range) {
        DCHECK_EQ(_state, Active);
        DCHECK(range != nullptr);
        RequestContext::PerDiskState& state = _disk_states[range->disk_id()];
        state.in_flight_ranges()->enqueue(range);
        state.schedule_context(this, range->disk_id());
    }

    // Cancels the context with status code 'status'.
    void cancel(const Status& status);

    // Adds request range to disk queue for this request context. Currently,
    // schedule_immediately must be false is RequestRange is a write range.
    void add_request_range(DiskIoMgr::RequestRange* range, bool schedule_immediately);

    // Returns the default queue capacity for scan ranges. This is updated
    // as the reader processes ranges.
    int initial_scan_range_queue_capacity() const { return _initial_queue_capacity; }

    // Validates invariants of reader.  Reader lock must be taken beforehand.
    bool validate() const;

    // Dumps out reader information.  Lock should be taken by caller
    std::string debug_string() const;

private:
    friend class DiskIoMgr;
    class PerDiskState;

    // Parent object
    DiskIoMgr* _parent;

    // Memory used for this reader.  This is unowned by this object.
    std::shared_ptr<MemTracker> _mem_tracker;

    // Total bytes read for this reader
    RuntimeProfile::Counter* _bytes_read_counter;

    // Total time spent in hdfs reading
    RuntimeProfile::Counter* _read_timer;

    // Number of active read threads
    RuntimeProfile::Counter* _active_read_thread_counter;

    // Disk access bitmap. The counter's bit[i] is set if disk id i has been accessed.
    // TODO: we can only support up to 64 disks with this bitmap but it lets us use a
    // builtin atomic instruction. Probably good enough for now.
    RuntimeProfile::Counter* _disks_accessed_bitmap;

    // Total number of bytes read locally, updated at end of each range scan
    std::atomic<int64_t> _bytes_read_local {0};

    // Total number of bytes read via short circuit read, updated at end of each range scan
    std::atomic<int64_t> _bytes_read_short_circuit {0};

    // Total number of bytes read from date node cache, updated at end of each range scan
    std::atomic<int64_t> _bytes_read_dn_cache {0};

    // Total number of bytes from remote reads that were expected to be local.
    std::atomic<int64_t> _unexpected_remote_bytes {0};

    // The number of buffers that have been returned to the reader (via get_next) that the
    // reader has not returned. Only included for debugging and diagnostics.
    std::atomic<int> _num_buffers_in_reader {0};

    // The number of scan ranges that have been completed for this reader.
    std::atomic<int> _num_finished_ranges {0};

    // The number of scan ranges that required a remote read, updated at the end of each
    // range scan. Only used for diagnostics.
    std::atomic<int> _num_remote_ranges {0};

    // The total number of scan ranges that have not been started. Only used for
    // diagnostics. This is the sum of all unstarted_scan_ranges across all disks.
    std::atomic<int> _num_unstarted_scan_ranges {0};

    // The number of buffers that are being used for this reader. This is the sum
    // of all buffers in ScanRange queues and buffers currently being read into (i.e. about
    // to be queued).
    std::atomic<int> _num_used_buffers {0};

    // The total number of ready buffers across all ranges.  Ready buffers are buffers
    // that have been read from disk but not retrieved by the caller.
    // This is the sum of all queued buffers in all ranges for this reader context.
    std::atomic<int> _num_ready_buffers {0};

    // The total (sum) of queue capacities for finished scan ranges. This value
    // divided by _num_finished_ranges is the average for finished ranges and
    // used to seed the starting queue capacity for future ranges. The assumption
    // is that if previous ranges were fast, new ones will be fast too. The scan
    // range adjusts the queue capacity dynamically so a rough approximation will do.
    std::atomic<int> _total_range_queue_capacity {0};

    // The initial queue size for new scan ranges. This is always
    // _total_range_queue_capacity / _num_finished_ranges but stored as a separate
    // variable to allow reading this value without taking a lock. Doing the division
    // at read time (with no lock) could lead to a race where only
    // _total_range_queue_capacity or _num_finished_ranges was updated.
    int _initial_queue_capacity;

    // All fields below are accessed by multiple threads and the lock needs to be
    // taken before accessing them.
    std::mutex _lock;

    // Current state of the reader
    State _state;

    // Status of this reader.  Set to non-ok if cancelled.
    Status _status;

    // The number of disks with scan ranges remaining (always equal to the sum of
    // disks with ranges).
    int _num_disks_with_ranges;

    // This is the list of ranges that are expected to be cached on the DN.
    // When the reader asks for a new range (GetNextScanRange()), we first
    // return ranges from this list.
    InternalQueue<ScanRange> _cached_ranges;

    // A list of ranges that should be returned in subsequent calls to
    // GetNextRange.
    // There is a trade-off with when to populate this list.  Populating it on
    // demand means consumers need to wait (happens in DiskIoMgr::GetNextRange()).
    // Populating it preemptively means we make worse scheduling decisions.
    // We currently populate one range per disk.
    // TODO: think about this some more.
    InternalQueue<ScanRange> _ready_to_start_ranges;
    std::condition_variable _ready_to_start_ranges_cv; // used with _lock

    // Ranges that are blocked due to back pressure on outgoing buffers.
    InternalQueue<ScanRange> _blocked_ranges;

    // Condition variable for UnregisterContext() to wait for all disks to complete
    std::condition_variable _disks_complete_cond_var;

    // Struct containing state per disk. See comments in the disk read loop on how
    // they are used.
    class PerDiskState {
    public:
        bool done() const { return _done; }
        void set_done(bool b) { _done = b; }

        int num_remaining_ranges() const { return _num_remaining_ranges; }
        int& num_remaining_ranges() { return _num_remaining_ranges; }

        ScanRange* next_scan_range_to_start() { return _next_scan_range_to_start; }
        void set_next_scan_range_to_start(ScanRange* range) { _next_scan_range_to_start = range; }

        // We need to have a memory barrier to prevent this load from being reordered
        // with num_threads_in_op(), since these variables are set without the reader
        // lock taken
        bool is_on_queue() const {
            bool b = _is_on_queue;
            __sync_synchronize();
            return b;
        }

        int num_threads_in_op() const {
            int v = _num_threads_in_op;
            __sync_synchronize();
            return v;
        }

        const InternalQueue<ScanRange>* unstarted_scan_ranges() const {
            return &_unstarted_scan_ranges;
        }
        const InternalQueue<WriteRange>* unstarted_write_ranges() const {
            return &_unstarted_write_ranges;
        }
        const InternalQueue<RequestRange>* in_flight_ranges() const { return &_in_flight_ranges; }

        InternalQueue<ScanRange>* unstarted_scan_ranges() { return &_unstarted_scan_ranges; }
        InternalQueue<WriteRange>* unstarted_write_ranges() { return &_unstarted_write_ranges; }
        InternalQueue<RequestRange>* in_flight_ranges() { return &_in_flight_ranges; }

        PerDiskState() { reset(); }

        // Schedules the request context on this disk if it's not already on the queue.
        // Context lock must be taken before this.
        void schedule_context(RequestContext* context, int disk_id) {
            if (!_is_on_queue && !_done) {
                _is_on_queue = true;
                context->_parent->_disk_queues[disk_id]->enqueue_context(context);
            }
        }

        // Increment the ref count on reader.  We need to track the number of threads per
        // reader per disk that are in the unlocked hdfs read code section. This is updated
        // by multiple threads without a lock so we need to use an atomic int.
        void increment_request_thread_and_dequeue() {
            ++_num_threads_in_op;
            _is_on_queue = false;
        }

        void decrement_request_thread() { --_num_threads_in_op; }

        // Decrement request thread count and do final cleanup if this is the last
        // thread. RequestContext lock must be taken before this.
        void decrement_request_thread_and_check_done(RequestContext* context) {
            --_num_threads_in_op;
            // We don't need to worry about reordered loads here because updating
            // _num_threads_in_request uses an atomic, which is a barrier.
            if (!_is_on_queue && _num_threads_in_op == 0 && !_done) {
                // This thread is the last one for this reader on this disk, do final cleanup
                context->decrement_disk_ref_count();
                _done = true;
            }
        }

        void reset() {
            DCHECK(_in_flight_ranges.empty());
            DCHECK(_unstarted_scan_ranges.empty());
            DCHECK(_unstarted_write_ranges.empty());

            _done = true;
            _num_remaining_ranges = 0;
            _is_on_queue = false;
            _num_threads_in_op = 0;
            _next_scan_range_to_start = nullptr;
        }

    private:
        // If true, this disk is all done for this request context, including any cleanup.
        // If done is true, it means that this request must not be on this disk's queue
        // *AND* there are no threads currently working on this context. To satisfy
        // this, only the last thread (per disk) can set this to true.
        bool _done;

        // For each disk, keeps track if the context is on this disk's queue, indicating
        // the disk must do some work for this context. The disk needs to do work in 4 cases:
        //  1) in_flight_ranges is not empty, the disk needs to read for this reader.
        //  2) next_range_to_start is nullptr, the disk needs to prepare a scan range to be
        //     read next.
        //  3) the reader has been cancelled and this disk needs to participate in the
        //     cleanup.
        //  4) A write range is added to queue.
        // In general, we only want to put a context on the disk queue if there is something
        // useful that can be done. If there's nothing useful, the disk queue will wake up
        // and then remove the reader from the queue. Doing this causes thrashing of the
        // threads.
        bool _is_on_queue;

        // For each disks, the number of request ranges that have not been fully read.
        // In the non-cancellation path, this will hit 0, and done will be set to true
        // by the disk thread. This is undefined in the cancellation path (the various
        // threads notice by looking at the RequestContext's _state).
        int _num_remaining_ranges;

        // Queue of ranges that have not started being read.  This list is exclusive
        // with in_flight_ranges.
        InternalQueue<ScanRange> _unstarted_scan_ranges;

        // Queue of pending IO requests for this disk in the order that they will be
        // processed. A ScanRange is added to this queue when it is returned in
        // GetNextRange(), or when it is added with schedule_immediately = true.
        // A WriteRange is added to this queue from _unstarted_write_ranges for each
        // invocation of get_next_request_range() in WorkLoop().
        // The size of this queue is always less than or equal to num_remaining_ranges.
        InternalQueue<RequestRange> _in_flight_ranges;

        // The next range to start for this reader on this disk. Each disk (for each reader)
        // picks the next range to start. The range is set here and also added to the
        // _ready_to_start_ranges queue. The reader pulls from the queue in FIFO order,
        // so the ranges from different disks are round-robined. When the range is pulled
        // off the _ready_to_start_ranges queue, it sets this variable to nullptr, so the disk
        // knows to populate it again and add it to _ready_to_start_ranges i.e. it is used
        // as a flag by DiskIoMgr::GetNextScanRange to determine if it needs to add another
        // range to _ready_to_start_ranges.
        ScanRange* _next_scan_range_to_start;

        // For each disk, the number of threads issuing the underlying read/write on behalf
        // of this context. There are a few places where we release the context lock, do some
        // work, and then grab the lock again.  Because we don't hold the lock for the
        // entire operation, we need this ref count to keep track of which thread should do
        // final resource cleanup during cancellation.
        // Only the thread that sees the count at 0 should do the final cleanup.
        std::atomic<int> _num_threads_in_op {0};

        // Queue of write ranges to process for this disk. A write range is always added
        // to _in_flight_ranges in get_next_request_range(). There is a separate
        // _unstarted_read_ranges and _unstarted_write_ranges to alternate between reads
        // and writes. (Otherwise, since next_scan_range_to_start is set
        // in get_next_request_range() whenever it is null, repeated calls to
        // get_next_request_range() and GetNextRange() may result in only reads being processed)
        InternalQueue<WriteRange> _unstarted_write_ranges;
    };

    // Per disk states to synchronize multiple disk threads accessing the same request
    // context.
    std::vector<PerDiskState> _disk_states;
};

} // namespace doris

#endif // DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_INTERNAL_H
