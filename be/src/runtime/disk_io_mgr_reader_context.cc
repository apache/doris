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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/disk-io-mgr-reader-context.cc
// and modified by Doris

#include "runtime/disk_io_mgr_internal.h"

namespace doris {

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::endl;

using std::lock_guard;
using std::unique_lock;
using std::mutex;

void DiskIoMgr::RequestContext::cancel(const Status& status) {
    DCHECK(!status.ok());

    // Callbacks are collected in this vector and invoked while no lock is held.
    vector<WriteRange::WriteDoneCallback> write_callbacks;
    {
        lock_guard<mutex> lock(_lock);
        DCHECK(validate()) << endl << debug_string();

        // Already being cancelled
        if (_state == RequestContext::Cancelled) {
            return;
        }

        DCHECK(_status.ok());
        _status = status;

        // The reader will be put into a cancelled state until call cleanup is complete.
        _state = RequestContext::Cancelled;

        // Cancel all scan ranges for this reader. Each range could be one one of
        // four queues.
        for (int i = 0; i < _disk_states.size(); ++i) {
            RequestContext::PerDiskState& state = _disk_states[i];
            RequestRange* range = nullptr;
            while ((range = state.in_flight_ranges()->dequeue()) != nullptr) {
                if (range->request_type() == RequestType::READ) {
                    static_cast<ScanRange*>(range)->cancel(status);
                } else {
                    DCHECK(range->request_type() == RequestType::WRITE);
                    write_callbacks.push_back(static_cast<WriteRange*>(range)->_callback);
                }
            }

            ScanRange* scan_range = nullptr;
            while ((scan_range = state.unstarted_scan_ranges()->dequeue()) != nullptr) {
                scan_range->cancel(status);
            }
            WriteRange* write_range = nullptr;
            while ((write_range = state.unstarted_write_ranges()->dequeue()) != nullptr) {
                write_callbacks.push_back(write_range->_callback);
            }
        }

        ScanRange* range = nullptr;
        while ((range = _ready_to_start_ranges.dequeue()) != nullptr) {
            range->cancel(status);
        }
        while ((range = _blocked_ranges.dequeue()) != nullptr) {
            range->cancel(status);
        }
        while ((range = _cached_ranges.dequeue()) != nullptr) {
            range->cancel(status);
        }

        // Schedule reader on all disks. The disks will notice it is cancelled and do any
        // required cleanup
        for (int i = 0; i < _disk_states.size(); ++i) {
            RequestContext::PerDiskState& state = _disk_states[i];
            state.schedule_context(this, i);
        }
    }

    for (const WriteRange::WriteDoneCallback& write_callback : write_callbacks) {
        write_callback(_status);
    }

    // Signal reader and unblock the get_next/Read thread.  That read will fail with
    // a cancelled status.
    _ready_to_start_ranges_cv.notify_all();
}

void DiskIoMgr::RequestContext::add_request_range(DiskIoMgr::RequestRange* range,
                                                  bool schedule_immediately) {
    // DCHECK(_lock.is_locked()); // TODO: boost should have this API
    RequestContext::PerDiskState& state = _disk_states[range->disk_id()];
    if (state.done()) {
        DCHECK_EQ(state.num_remaining_ranges(), 0);
        state.set_done(false);
        ++_num_disks_with_ranges;
    }

    bool schedule_context = false;
    if (range->request_type() == RequestType::READ) {
        DiskIoMgr::ScanRange* scan_range = static_cast<DiskIoMgr::ScanRange*>(range);
        if (schedule_immediately) {
            schedule_scan_range(scan_range);
        } else {
            state.unstarted_scan_ranges()->enqueue(scan_range);
            ++_num_unstarted_scan_ranges;
        }
        // If next_scan_range_to_start is nullptr, schedule this RequestContext so that it will
        // be set. If it's not nullptr, this context will be scheduled when GetNextRange() is
        // invoked.
        schedule_context = state.next_scan_range_to_start() == nullptr;
    } else {
        DCHECK(range->request_type() == RequestType::WRITE);
        DCHECK(!schedule_immediately);
        DiskIoMgr::WriteRange* write_range = static_cast<DiskIoMgr::WriteRange*>(range);
        state.unstarted_write_ranges()->enqueue(write_range);

        // schedule_context() has no effect if the context is already scheduled,
        // so this is safe.
        schedule_context = true;
    }

    if (schedule_context) {
        state.schedule_context(this, range->disk_id());
    }
    ++state.num_remaining_ranges();
}

DiskIoMgr::RequestContext::RequestContext(DiskIoMgr* parent, int num_disks)
        : _parent(parent),
          _bytes_read_counter(nullptr),
          _read_timer(nullptr),
          _active_read_thread_counter(nullptr),
          _disks_accessed_bitmap(nullptr),
          _state(Inactive),
          _disk_states(num_disks) {}

// Resets this object.
void DiskIoMgr::RequestContext::reset(std::shared_ptr<MemTracker> tracker) {
    DCHECK_EQ(_state, Inactive);
    _status = Status::OK();

    _bytes_read_counter = nullptr;
    _read_timer = nullptr;
    _active_read_thread_counter = nullptr;
    _disks_accessed_bitmap = nullptr;

    _state = Active;
    _mem_tracker = std::move(tracker);

    _num_unstarted_scan_ranges = 0;
    _num_disks_with_ranges = 0;
    _num_used_buffers = 0;
    _num_buffers_in_reader = 0;
    _num_ready_buffers = 0;
    _total_range_queue_capacity = 0;
    _num_finished_ranges = 0;
    _num_remote_ranges = 0;
    _bytes_read_local = 0;
    _bytes_read_short_circuit = 0;
    _bytes_read_dn_cache = 0;
    _unexpected_remote_bytes = 0;
    _initial_queue_capacity = DiskIoMgr::DEFAULT_QUEUE_CAPACITY;

    DCHECK(_ready_to_start_ranges.empty());
    DCHECK(_blocked_ranges.empty());
    DCHECK(_cached_ranges.empty());

    for (int i = 0; i < _disk_states.size(); ++i) {
        _disk_states[i].reset();
    }
}

// Dumps out request context information. Lock should be taken by caller
string DiskIoMgr::RequestContext::debug_string() const {
    stringstream ss;
    ss << endl << "  RequestContext: " << (void*)this << " (state=";
    if (_state == RequestContext::Inactive) {
        ss << "Inactive";
    }
    if (_state == RequestContext::Cancelled) ss << "Cancelled";
    if (_state == RequestContext::Active) ss << "Active";
    if (_state != RequestContext::Inactive) {
        ss << " _status=" << (_status.ok() ? "OK" : _status.get_error_msg())
           << " #ready_buffers=" << _num_ready_buffers << " #used_buffers=" << _num_used_buffers
           << " #num_buffers_in_reader=" << _num_buffers_in_reader
           << " #finished_scan_ranges=" << _num_finished_ranges
           << " #disk_with_ranges=" << _num_disks_with_ranges
           << " #disks=" << _num_disks_with_ranges;
        for (int i = 0; i < _disk_states.size(); ++i) {
            ss << endl
               << "   " << i << ": "
               << "is_on_queue=" << _disk_states[i].is_on_queue()
               << " done=" << _disk_states[i].done()
               << " #num_remaining_scan_ranges=" << _disk_states[i].num_remaining_ranges()
               << " #in_flight_ranges=" << _disk_states[i].in_flight_ranges()->size()
               << " #unstarted_scan_ranges=" << _disk_states[i].unstarted_scan_ranges()->size()
               << " #unstarted_write_ranges=" << _disk_states[i].unstarted_write_ranges()->size()
               << " #reading_threads=" << _disk_states[i].num_threads_in_op();
        }
    }
    ss << ")";
    return ss.str();
}

bool DiskIoMgr::RequestContext::validate() const {
    if (_state == RequestContext::Inactive) {
        LOG(WARNING) << "_state == RequestContext::Inactive";
        return false;
    }

    if (_num_used_buffers < 0) {
        LOG(WARNING) << "_num_used_buffers < 0: #used=" << _num_used_buffers;
        return false;
    }

    if (_num_ready_buffers < 0) {
        LOG(WARNING) << "_num_ready_buffers < 0: #used=" << _num_ready_buffers;
        return false;
    }

    int total_unstarted_ranges = 0;
    for (int i = 0; i < _disk_states.size(); ++i) {
        const PerDiskState& state = _disk_states[i];
        bool on_queue = state.is_on_queue();
        int num_reading_threads = state.num_threads_in_op();

        total_unstarted_ranges += state.unstarted_scan_ranges()->size();

        if (num_reading_threads < 0) {
            LOG(WARNING) << "disk_id=" << i
                         << "state.num_threads_in_read < 0: #threads=" << num_reading_threads;
            return false;
        }

        if (_state != RequestContext::Cancelled) {
            if (state.unstarted_scan_ranges()->size() + state.in_flight_ranges()->size() >
                state.num_remaining_ranges()) {
                LOG(WARNING) << "disk_id=" << i
                             << " state.unstarted_ranges.size() + state.in_flight_ranges.size()"
                             << " > state.num_remaining_ranges:"
                             << " #unscheduled=" << state.unstarted_scan_ranges()->size()
                             << " #in_flight=" << state.in_flight_ranges()->size()
                             << " #remaining=" << state.num_remaining_ranges();
                return false;
            }

            // If we have an in_flight range, the reader must be on the queue or have a
            // thread actively reading for it.
            if (!state.in_flight_ranges()->empty() && !on_queue && num_reading_threads == 0) {
                LOG(WARNING) << "disk_id=" << i
                             << " reader has inflight ranges but is not on the disk queue."
                             << " #in_flight_ranges=" << state.in_flight_ranges()->size()
                             << " #reading_threads=" << num_reading_threads
                             << " on_queue=" << on_queue;
                return false;
            }

            if (state.done() && num_reading_threads > 0) {
                LOG(WARNING) << "disk_id=" << i
                             << " state set to done but there are still threads working."
                             << " #reading_threads=" << num_reading_threads;
                return false;
            }
        } else {
            // Is Cancelled
            if (!state.in_flight_ranges()->empty()) {
                LOG(WARNING) << "disk_id=" << i << "Reader cancelled but has in flight ranges.";
                return false;
            }
            if (!state.unstarted_scan_ranges()->empty()) {
                LOG(WARNING) << "disk_id=" << i << "Reader cancelled but has unstarted ranges.";
                return false;
            }
        }

        if (state.done() && on_queue) {
            LOG(WARNING) << "disk_id=" << i
                         << " state set to done but the reader is still on the disk queue."
                         << " state.done=true and state.is_on_queue=true";
            return false;
        }
    }

    if (_state != RequestContext::Cancelled) {
        if (total_unstarted_ranges != _num_unstarted_scan_ranges) {
            LOG(WARNING) << "total_unstarted_ranges=" << total_unstarted_ranges
                         << " sum_in_states=" << _num_unstarted_scan_ranges;
            return false;
        }
    } else {
        if (!_ready_to_start_ranges.empty()) {
            LOG(WARNING) << "Reader cancelled but has ready to start ranges.";
            return false;
        }
        if (!_blocked_ranges.empty()) {
            LOG(WARNING) << "Reader cancelled but has blocked ranges.";
            return false;
        }
    }

    return true;
}

} // namespace doris
