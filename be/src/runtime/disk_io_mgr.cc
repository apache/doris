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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/disk-io-mgr.cc
// and modified by Doris

#include "runtime/disk_io_mgr.h"

#include <boost/algorithm/string.hpp>

#include "runtime/disk_io_mgr_internal.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::endl;

using std::lock_guard;
using std::unique_lock;
using std::mutex;
using std::thread;

// Returns the ceil of value/divisor
static int64_t bit_ceil(int64_t value, int64_t divisor) {
    return value / divisor + (value % divisor != 0);
}

// Returns ceil(log2(x)).
// TODO: this could be faster if we use __builtin_clz.  Fix this if this ever shows up
// in a hot path.
static int bit_log2(uint64_t x) {
    DCHECK_GT(x, 0);
    if (x == 1) {
        return 0;
    }
    // Compute result = ceil(log2(x))
    //                = floor(log2(x - 1)) + 1, for x > 1
    // by finding the position of the most significant bit (1-indexed) of x - 1
    // (floor(log2(n)) = MSB(n) (0-indexed))
    --x;
    int result = 1;
    while (x >>= 1) {
        ++result;
    }
    return result;
}

namespace doris {

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_FLASH_DISK = 8;

// The IoMgr is able to run with a wide range of memory usage. If a query has memory
// remaining less than this value, the IoMgr will stop all buffering regardless of the
// current queue size.
static const int LOW_MEMORY = 64 * 1024 * 1024;

const int DiskIoMgr::DEFAULT_QUEUE_CAPACITY = 2;

// namespace detail {
// Indicates if file handle caching should be used
// static inline bool is_file_handle_caching_enabled() {
//     return config::max_cached_file_handles > 0;
// }
// }

// This method is used to clean up resources upon eviction of a cache file handle.
// void DiskIoMgr::HdfsCachedFileHandle::release(DiskIoMgr::HdfsCachedFileHandle** h) {
//   VLOG_FILE << "Cached file handle evicted, hdfsCloseFile() fid=" << (*h)->_hdfs_file;
//   delete (*h);
// }

// DiskIoMgr::HdfsCachedFileHandle::HdfsCachedFileHandle(const hdfsFS& fs, const char* fname,
//     int64_t mtime)
//     : _fs(fs), _hdfs_file(hdfsOpenFile(fs, fname, O_RDONLY, 0, 0, 0)), _mtime(mtime) {
//   VLOG_FILE << "hdfsOpenFile() file=" << fname << " fid=" << _hdfs_file;
// }

// DiskIoMgr::HdfsCachedFileHandle::~HdfsCachedFileHandle() {
//   if (_hdfs_file != nullptr && _fs != nullptr) {
//     VLOG_FILE << "hdfsCloseFile() fid=" << _hdfs_file;
//     hdfsCloseFile(_fs, _hdfs_file);
//   }
//   _fs = nullptr;
//   _hdfs_file = nullptr;
// }

// This class provides a cache of RequestContext objects.  RequestContexts are recycled.
// This is good for locality as well as lock contention.  The cache has the property that
// regardless of how many clients get added/removed, the memory locations for
// existing clients do not change (not the case with std::vector) minimizing the locks we
// have to take across all readers.
// All functions on this object are thread safe
class DiskIoMgr::RequestContextCache {
public:
    RequestContextCache(DiskIoMgr* io_mgr) : _io_mgr(io_mgr) {}

    // Returns a context to the cache.  This object can now be reused.
    void return_context(RequestContext* reader) {
        DCHECK(reader->_state != RequestContext::Inactive);
        reader->_state = RequestContext::Inactive;
        lock_guard<mutex> l(_lock);
        _inactive_contexts.push_back(reader);
    }

    // Returns a new RequestContext object.  Allocates a new object if necessary.
    RequestContext* get_new_context() {
        lock_guard<mutex> l(_lock);
        if (!_inactive_contexts.empty()) {
            RequestContext* reader = _inactive_contexts.front();
            _inactive_contexts.pop_front();
            return reader;
        } else {
            RequestContext* reader = new RequestContext(_io_mgr, _io_mgr->num_total_disks());
            _all_contexts.push_back(reader);
            return reader;
        }
    }

    // This object has the same lifetime as the disk IoMgr.
    ~RequestContextCache() {
        for (list<RequestContext*>::iterator it = _all_contexts.begin(); it != _all_contexts.end();
             ++it) {
            delete *it;
        }
    }

    // Validates that all readers are cleaned up and in the inactive state.
    bool validate_all_inactive() {
        lock_guard<mutex> l(_lock);
        for (list<RequestContext*>::iterator it = _all_contexts.begin(); it != _all_contexts.end();
             ++it) {
            if ((*it)->_state != RequestContext::Inactive) {
                return false;
            }
        }
        DCHECK_EQ(_all_contexts.size(), _inactive_contexts.size());
        return _all_contexts.size() == _inactive_contexts.size();
    }

    string debug_string();

private:
    DiskIoMgr* _io_mgr;

    // lock to protect all members below
    mutex _lock;

    // List of all request contexts created.  Used for debugging
    list<RequestContext*> _all_contexts;

    // List of inactive readers.  These objects can be used for a new reader.
    list<RequestContext*> _inactive_contexts;
};

string DiskIoMgr::RequestContextCache::debug_string() {
    lock_guard<mutex> l(_lock);
    stringstream ss;
    for (list<RequestContext*>::iterator it = _all_contexts.begin(); it != _all_contexts.end();
         ++it) {
        unique_lock<mutex> lock((*it)->_lock);
        ss << (*it)->debug_string() << endl;
    }
    return ss.str();
}

string DiskIoMgr::debug_string() {
    stringstream ss;
    ss << "RequestContexts: " << endl << _request_context_cache->debug_string() << endl;

    ss << "Disks: " << endl;
    for (int i = 0; i < _disk_queues.size(); ++i) {
        unique_lock<mutex> lock(_disk_queues[i]->lock);
        ss << "  " << (void*)_disk_queues[i] << ":";
        if (!_disk_queues[i]->request_contexts.empty()) {
            ss << " Readers: ";
            for (RequestContext* req_context : _disk_queues[i]->request_contexts) {
                ss << (void*)req_context;
            }
        }
        ss << endl;
    }
    return ss.str();
}

DiskIoMgr::BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr)
        : _io_mgr(io_mgr), _reader(nullptr), _buffer(nullptr) {}

void DiskIoMgr::BufferDescriptor::reset(RequestContext* reader, ScanRange* range, char* buffer,
                                        int64_t buffer_len) {
    DCHECK(_io_mgr != nullptr);
    DCHECK(_buffer == nullptr);
    DCHECK(range != nullptr);
    DCHECK(buffer != nullptr);
    DCHECK_GE(buffer_len, 0);
    _reader = reader;
    _scan_range = range;
    _buffer = buffer;
    _buffer_len = buffer_len;
    _len = 0;
    _eosr = false;
    _status = Status::OK();
}

void DiskIoMgr::BufferDescriptor::return_buffer() {
    DCHECK(_io_mgr != nullptr);
    _io_mgr->return_buffer(this);
}

DiskIoMgr::WriteRange::WriteRange(const string& file, int64_t file_offset, int disk_id,
                                  WriteDoneCallback callback) {
    _file = file;
    _offset = file_offset;
    _disk_id = disk_id;
    _callback = callback;
    _request_type = RequestType::WRITE;
}

void DiskIoMgr::WriteRange::set_data(const uint8_t* buffer, int64_t len) {
    _data = buffer;
    _len = len;
}

static void check_sse_support() {
    if (!CpuInfo::is_supported(CpuInfo::SSE4_2)) {
        LOG(WARNING) << "This machine does not support sse4_2.  The default IO system "
                        "configurations are suboptimal for this hardware.  Consider "
                        "increasing the number of threads per disk by restarting doris "
                        "using the --num_threads_per_disk flag with a higher value";
    }
}

DiskIoMgr::DiskIoMgr()
        : _num_threads_per_disk(config::num_threads_per_disk),
          _max_buffer_size(config::read_size),
          _min_buffer_size(config::min_buffer_size),
          _cached_read_options(nullptr),
          _shut_down(false),
          _total_bytes_read_counter(TUnit::BYTES),
          _read_timer(TUnit::TIME_NS)
// _read_timer(TUnit::TIME_NS),
// _file_handle_cache(
//         std::min((uint64_t)config::max_cached_file_handles, FileSystemUtil::max_num_file_handles()),
//         &HdfsCachedFileHandle::release) {
{
    int64_t max_buffer_size_scaled = bit_ceil(_max_buffer_size, _min_buffer_size);
    _free_buffers.resize(bit_log2(max_buffer_size_scaled) + 1);
    int num_local_disks = (config::num_disks == 0 ? DiskInfo::num_disks() : config::num_disks);
    _disk_queues.resize(num_local_disks + REMOTE_NUM_DISKS);
    check_sse_support();
}

DiskIoMgr::DiskIoMgr(int num_local_disks, int threads_per_disk, int min_buffer_size,
                     int max_buffer_size)
        : _num_threads_per_disk(threads_per_disk),
          _max_buffer_size(max_buffer_size),
          _min_buffer_size(min_buffer_size),
          _cached_read_options(nullptr),
          _shut_down(false),
          _total_bytes_read_counter(TUnit::BYTES),
          _read_timer(TUnit::TIME_NS)
// _read_timer(TUnit::TIME_NS),
// _file_handle_cache(::min(config::max_cached_file_handles,
//             FileSystemUtil::max_num_file_handles()), &HdfsCachedFileHandle::release) {
{
    int64_t max_buffer_size_scaled = bit_ceil(_max_buffer_size, _min_buffer_size);
    _free_buffers.resize(bit_log2(max_buffer_size_scaled) + 1);
    if (num_local_disks == 0) {
        num_local_disks = DiskInfo::num_disks();
    }
    _disk_queues.resize(num_local_disks + REMOTE_NUM_DISKS);
    check_sse_support();
}

DiskIoMgr::~DiskIoMgr() {
    _shut_down = true;
    // Notify all worker threads and shut them down.
    for (int i = 0; i < _disk_queues.size(); ++i) {
        if (_disk_queues[i] == nullptr) {
            continue;
        }
        {
            // This lock is necessary to properly use the condition var to notify
            // the disk worker threads.  The readers also grab this lock so updates
            // to _shut_down are protected.
            unique_lock<mutex> disk_lock(_disk_queues[i]->lock);
        }
        _disk_queues[i]->work_available.notify_all();
    }
    _disk_thread_group.join_all();

    for (int i = 0; i < _disk_queues.size(); ++i) {
        if (_disk_queues[i] == nullptr) {
            continue;
        }
        int disk_id = _disk_queues[i]->disk_id;
        for (list<RequestContext*>::iterator it = _disk_queues[i]->request_contexts.begin();
             it != _disk_queues[i]->request_contexts.end(); ++it) {
            DCHECK_EQ((*it)->_disk_states[disk_id].num_threads_in_op(), 0);
            DCHECK((*it)->_disk_states[disk_id].done());
            (*it)->decrement_disk_ref_count();
        }
    }

    DCHECK(_request_context_cache.get() == nullptr ||
           _request_context_cache->validate_all_inactive())
            << endl
            << debug_string();
    DCHECK_EQ(_num_buffers_in_readers, 0);

    // Delete all allocated buffers
    int num_free_buffers = 0;
    for (int idx = 0; idx < _free_buffers.size(); ++idx) {
        num_free_buffers += _free_buffers[idx].size();
    }
    DCHECK_EQ(_num_allocated_buffers, num_free_buffers);
    gc_io_buffers();

    for (int i = 0; i < _disk_queues.size(); ++i) {
        delete _disk_queues[i];
    }

    /*
     * if (_cached_read_options != nullptr) {
     *     hadoopRzOptionsFree(_cached_read_options);
     * }
     */
}

Status DiskIoMgr::init(const int64_t mem_limit) {
    _mem_tracker = std::make_unique<MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL, "DiskIO",
                                                       mem_limit);

    for (int i = 0; i < _disk_queues.size(); ++i) {
        _disk_queues[i] = new DiskQueue(i);
        int num_threads_per_disk = 0;
        if (i >= num_local_disks()) {
            // remote disks, do nothing
            continue;
        } else if (_num_threads_per_disk != 0) {
            num_threads_per_disk = _num_threads_per_disk;
        } else if (DiskInfo::is_rotational(i)) {
            num_threads_per_disk = THREADS_PER_ROTATIONAL_DISK;
        } else {
            num_threads_per_disk = THREADS_PER_FLASH_DISK;
        }
        for (int j = 0; j < num_threads_per_disk; ++j) {
            stringstream ss;
            ss << "work-loop(Disk: " << i << ", Thread: " << j << ")";
            // _disk_thread_group.AddThread(new Thread("disk-io-mgr", ss.str(),
            //             &DiskIoMgr::work_loop, this, _disk_queues[i]));
            _disk_thread_group.add_thread(
                    new std::thread(std::bind(&DiskIoMgr::work_loop, this, _disk_queues[i])));
        }
    }
    _request_context_cache.reset(new RequestContextCache(this));

    // _cached_read_options = hadoopRzOptionsAlloc();
    // DCHECK(_cached_read_options != nullptr);
    // Disable checksum for cached reads.
    // int ret = hadoopRzOptionsSetSkipChecksum(_cached_read_options, true);
    // DCHECK_EQ(ret, 0);
    // Disable automatic fallback for cached reads.
    // ret = hadoopRzOptionsSetByteBufferPool(_cached_read_options, nullptr);
    // DCHECK_EQ(ret, 0);

    return Status::OK();
}

Status DiskIoMgr::register_context(RequestContext** request_context) {
    DCHECK(_request_context_cache) << "Must call init() first.";
    *request_context = _request_context_cache->get_new_context();
    (*request_context)->reset();
    return Status::OK();
}

void DiskIoMgr::unregister_context(RequestContext* reader) {
    // Blocking cancel (waiting for disks completion).
    cancel_context(reader, true);

    // All the disks are done with clean, validate nothing is leaking.
    unique_lock<mutex> reader_lock(reader->_lock);
    DCHECK_EQ(reader->_num_buffers_in_reader, 0) << endl << reader->debug_string();
    DCHECK_EQ(reader->_num_used_buffers, 0) << endl << reader->debug_string();

    DCHECK(reader->validate()) << endl << reader->debug_string();
    _request_context_cache->return_context(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the request context must notice the cancel and remove it from its
// tracking structures.  The last thread to touch the context should deallocate (aka
// recycle) the request context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.
//  2. Caller threads that are waiting in get_next.
//
// The steps are:
// 1. Cancel will immediately set the context in the Cancelled state.  This prevents any
// other thread from adding more ready buffers to the context (they all take a lock and
// check the state before doing so), or any write ranges to the context.
// 2. Cancel will call cancel on each ScanRange that is not yet complete, unblocking
// any threads in get_next(). The reader will see the cancelled Status returned. Cancel
// also invokes the callback for the WriteRanges with the cancelled state.
// 3. Disk threads notice the context is cancelled either when picking the next context
// to process or when they try to enqueue a ready buffer.  Upon noticing the cancelled
// state, removes the context from the disk queue.  The last thread per disk with an
// outstanding reference to the context decrements the number of disk queues the context
// is on.
// If wait_for_disks_completion is true, wait for the number of active disks to become 0.
void DiskIoMgr::cancel_context(RequestContext* context, bool wait_for_disks_completion) {
    context->cancel(Status::Cancelled("Cancelled"));

    if (wait_for_disks_completion) {
        unique_lock<mutex> lock(context->_lock);
        DCHECK(context->validate()) << endl << context->debug_string();
        while (context->_num_disks_with_ranges > 0) {
            context->_disks_complete_cond_var.wait(lock);
        }
    }
}

void DiskIoMgr::set_read_timer(RequestContext* r, RuntimeProfile::Counter* c) {
    r->_read_timer = c;
}

void DiskIoMgr::set_bytes_read_counter(RequestContext* r, RuntimeProfile::Counter* c) {
    r->_bytes_read_counter = c;
}

void DiskIoMgr::set_active_read_thread_counter(RequestContext* r, RuntimeProfile::Counter* c) {
    r->_active_read_thread_counter = c;
}

void DiskIoMgr::set_disks_access_bitmap(RequestContext* r, RuntimeProfile::Counter* c) {
    r->_disks_accessed_bitmap = c;
}

int64_t DiskIoMgr::queue_size(RequestContext* reader) const {
    return reader->_num_ready_buffers;
}

Status DiskIoMgr::context_status(RequestContext* context) const {
    unique_lock<mutex> lock(context->_lock);
    return context->_status;
}

int DiskIoMgr::num_unstarted_ranges(RequestContext* reader) const {
    return reader->_num_unstarted_scan_ranges;
}

int64_t DiskIoMgr::bytes_read_local(RequestContext* reader) const {
    return reader->_bytes_read_local;
}

int64_t DiskIoMgr::bytes_read_short_circuit(RequestContext* reader) const {
    return reader->_bytes_read_short_circuit;
}

int64_t DiskIoMgr::bytes_read_dn_cache(RequestContext* reader) const {
    return reader->_bytes_read_dn_cache;
}

int DiskIoMgr::num_remote_ranges(RequestContext* reader) const {
    return reader->_num_remote_ranges;
}

int64_t DiskIoMgr::unexpected_remote_bytes(RequestContext* reader) const {
    return reader->_unexpected_remote_bytes;
}

int64_t DiskIoMgr::get_read_throughput() {
    return RuntimeProfile::units_per_second(&_total_bytes_read_counter, &_read_timer);
}

Status DiskIoMgr::validate_scan_range(ScanRange* range) {
    int disk_id = range->_disk_id;
    if (disk_id < 0 || disk_id >= _disk_queues.size()) {
        stringstream ss;
        ss << "Invalid scan range.  Bad disk id: " << disk_id;
        DCHECK(false) << ss.str();
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status DiskIoMgr::add_scan_ranges(RequestContext* reader, const vector<ScanRange*>& ranges,
                                  bool schedule_immediately) {
    if (ranges.empty()) {
        return Status::OK();
    }

    // Validate and initialize all ranges
    for (int i = 0; i < ranges.size(); ++i) {
        RETURN_IF_ERROR(validate_scan_range(ranges[i]));
        ranges[i]->init_internal(this, reader);
    }

    // disks that this reader needs to be scheduled on.
    unique_lock<mutex> reader_lock(reader->_lock);
    DCHECK(reader->validate()) << endl << reader->debug_string();

    if (reader->_state == RequestContext::Cancelled) {
        DCHECK(!reader->_status.ok());
        return reader->_status;
    }

    // Add each range to the queue of the disk the range is on
    for (int i = 0; i < ranges.size(); ++i) {
        // Don't add empty ranges.
        DCHECK_NE(ranges[i]->len(), 0);
        ScanRange* range = ranges[i];

        /*
         * if (range->_try_cache) {
         *     if (schedule_immediately) {
         *         bool cached_read_succeeded;
         *         RETURN_IF_ERROR(range->read_from_cache(&cached_read_succeeded));
         *         if (cached_read_succeeded) continue;
         *         // Cached read failed, fall back to add_request_range() below.
         *     } else {
         *         reader->_cached_ranges.enqueue(range);
         *         continue;
         *     }
         * }
         */
        reader->add_request_range(range, schedule_immediately);
    }
    DCHECK(reader->validate()) << endl << reader->debug_string();

    return Status::OK();
}

// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::get_next_range(RequestContext* reader, ScanRange** range) {
    DCHECK(reader != nullptr);
    DCHECK(range != nullptr);
    *range = nullptr;
    Status status = Status::OK();

    unique_lock<mutex> reader_lock(reader->_lock);
    DCHECK(reader->validate()) << endl << reader->debug_string();

    while (true) {
        if (reader->_state == RequestContext::Cancelled) {
            DCHECK(!reader->_status.ok());
            status = reader->_status;
            break;
        }

        if (reader->_num_unstarted_scan_ranges == 0 && reader->_ready_to_start_ranges.empty() &&
            reader->_cached_ranges.empty()) {
            // All ranges are done, just return.
            break;
        }

        // if (!reader->_cached_ranges.empty()) {
        //     // We have a cached range.
        //     *range = reader->_cached_ranges.dequeue();
        //     DCHECK((*range)->_try_cache);
        //     // bool cached_read_succeeded;
        //     // RETURN_IF_ERROR((*range)->read_from_cache(&cached_read_succeeded));
        //     // if (cached_read_succeeded) return Status::OK();

        //     // This range ended up not being cached. Loop again and pick up a new range.
        //     reader->add_request_range(*range, false);
        //     DCHECK(reader->validate()) << endl << reader->debug_string();
        //     *range = nullptr;
        //     continue;
        // }

        if (reader->_ready_to_start_ranges.empty()) {
            reader->_ready_to_start_ranges_cv.wait(reader_lock);
        } else {
            *range = reader->_ready_to_start_ranges.dequeue();
            DCHECK(*range != nullptr);
            int disk_id = (*range)->disk_id();
            DCHECK_EQ(*range, reader->_disk_states[disk_id].next_scan_range_to_start());
            // Set this to nullptr, the next time this disk runs for this reader, it will
            // get another range ready.
            reader->_disk_states[disk_id].set_next_scan_range_to_start(nullptr);
            reader->schedule_scan_range(*range);
            break;
        }
    }
    return status;
}

Status DiskIoMgr::read(RequestContext* reader, ScanRange* range, BufferDescriptor** buffer) {
    DCHECK(range != nullptr);
    DCHECK(buffer != nullptr);
    *buffer = nullptr;

    if (range->len() > _max_buffer_size) {
        return Status::InternalError("Cannot perform sync read larger than {}. Request was {}",
                                     _max_buffer_size, range->len());
    }

    vector<DiskIoMgr::ScanRange*> ranges;
    ranges.push_back(range);
    RETURN_IF_ERROR(add_scan_ranges(reader, ranges, true));
    RETURN_IF_ERROR(range->get_next(buffer));
    DCHECK((*buffer) != nullptr);
    DCHECK((*buffer)->eosr());
    return Status::OK();
}

void DiskIoMgr::return_buffer(BufferDescriptor* buffer_desc) {
    DCHECK(buffer_desc != nullptr);
    if (!buffer_desc->_status.ok()) {
        DCHECK(buffer_desc->_buffer == nullptr);
    }

    RequestContext* reader = buffer_desc->_reader;
    if (buffer_desc->_buffer != nullptr) {
        if (buffer_desc->_scan_range->_cached_buffer == nullptr) {
            // Not a cached buffer. Return the io buffer and update mem tracking.
            return_free_buffer(buffer_desc);
        }
        buffer_desc->_buffer = nullptr;
        --_num_buffers_in_readers;
        --reader->_num_buffers_in_reader;
    } else {
        // A nullptr buffer means there was an error in which case there is no buffer
        // to return.
    }

    if (buffer_desc->_eosr || buffer_desc->_scan_range->_is_cancelled) {
        // Need to close the scan range if returning the last buffer or the scan range
        // has been cancelled (and the caller might never get the last buffer).
        // close() is idempotent so multiple cancelled buffers is okay.
        buffer_desc->_scan_range->close();
    }
    return_buffer_desc(buffer_desc);
}

void DiskIoMgr::return_buffer_desc(BufferDescriptor* desc) {
    DCHECK(desc != nullptr);
    unique_lock<mutex> lock(_free_buffers_lock);
    DCHECK(find(_free_buffer_descs.begin(), _free_buffer_descs.end(), desc) ==
           _free_buffer_descs.end());
    _free_buffer_descs.push_back(desc);
}

DiskIoMgr::BufferDescriptor* DiskIoMgr::get_buffer_desc(RequestContext* reader, ScanRange* range,
                                                        char* buffer, int64_t buffer_size) {
    BufferDescriptor* buffer_desc = nullptr;
    {
        unique_lock<mutex> lock(_free_buffers_lock);
        if (_free_buffer_descs.empty()) {
            buffer_desc = _pool.add(new BufferDescriptor(this));
        } else {
            buffer_desc = _free_buffer_descs.front();
            _free_buffer_descs.pop_front();
        }
    }
    buffer_desc->reset(reader, range, buffer, buffer_size);
    return buffer_desc;
}

char* DiskIoMgr::get_free_buffer(int64_t* buffer_size) {
    DCHECK_LE(*buffer_size, _max_buffer_size);
    DCHECK_GT(*buffer_size, 0);
    *buffer_size = std::min(static_cast<int64_t>(_max_buffer_size), *buffer_size);
    int idx = free_buffers_idx(*buffer_size);
    // Quantize buffer size to nearest power of 2 greater than the specified buffer size and
    // convert to bytes
    *buffer_size = (1 << idx) * _min_buffer_size;

    unique_lock<mutex> lock(_free_buffers_lock);
    char* buffer = nullptr;
    if (_free_buffers[idx].empty()) {
        ++_num_allocated_buffers;
        buffer = new char[*buffer_size];
    } else {
        // This means the buffer's memory ownership is transferred from DiskIoMgr to tls tracker.
        THREAD_MEM_TRACKER_TRANSFER_FROM(*buffer_size, _mem_tracker.get());
        buffer = _free_buffers[idx].front();
        _free_buffers[idx].pop_front();
    }
    DCHECK(buffer != nullptr);
    return buffer;
}

void DiskIoMgr::gc_io_buffers(int64_t bytes_to_free) {
    unique_lock<mutex> lock(_free_buffers_lock);
    int bytes_freed = 0;
    for (int idx = 0; idx < _free_buffers.size(); ++idx) {
        for (list<char*>::iterator iter = _free_buffers[idx].begin();
             iter != _free_buffers[idx].end(); ++iter) {
            int64_t buffer_size = (1 << idx) * _min_buffer_size;
            --_num_allocated_buffers;
            delete[] * iter;

            bytes_freed += buffer_size;
        }
        _free_buffers[idx].clear();
        if (bytes_freed >= bytes_to_free) {
            break;
        }
    }
    // The deleted buffer is released in the tls mem tracker, the deleted buffer belongs to DiskIoMgr,
    // so the freed memory should be recorded in the DiskIoMgr mem tracker. So if the tls mem tracker
    // and the DiskIoMgr tracker are different, transfer memory ownership.
    THREAD_MEM_TRACKER_TRANSFER_FROM(bytes_freed, _mem_tracker.get());
}

void DiskIoMgr::return_free_buffer(BufferDescriptor* desc) {
    return_free_buffer(desc->_buffer, desc->_buffer_len);
}

void DiskIoMgr::return_free_buffer(char* buffer, int64_t buffer_size) {
    DCHECK(buffer != nullptr);
    int idx = free_buffers_idx(buffer_size);
    DCHECK_EQ(bit_ceil(buffer_size, _min_buffer_size) & ~(1 << idx), 0)
            << "_buffer_size / _min_buffer_size should be power of 2, got buffer_size = "
            << buffer_size << ", _min_buffer_size = " << _min_buffer_size;
    unique_lock<mutex> lock(_free_buffers_lock);
    if (!config::disable_mem_pools && _free_buffers[idx].size() < config::max_free_io_buffers) {
        // The buffer's memory ownership is transferred from desc->buffer_mem_tracker to DiskIoMgr tracker.
        THREAD_MEM_TRACKER_TRANSFER_TO(buffer_size, _mem_tracker.get());
        _free_buffers[idx].push_back(buffer);
    } else {
        --_num_allocated_buffers;
        delete[] buffer;
    }
}

// This function gets the next RequestRange to work on for this disk. It checks for
// cancellation and
// a) Updates ready_to_start_ranges if there are no scan ranges queued for this disk.
// b) Adds an unstarted write range to _in_flight_ranges. The write range is processed
//    immediately if there are no preceding scan ranges in _in_flight_ranges
// It blocks until work is available or the thread is shut down.
// Work is available if there is a RequestContext with
//  - A ScanRange with a buffer available, or
//  - A WriteRange in _unstarted_write_ranges.
bool DiskIoMgr::get_next_request_range(DiskQueue* disk_queue, RequestRange** range,
                                       RequestContext** request_context) {
    int disk_id = disk_queue->disk_id;
    *range = nullptr;

    // This loops returns either with work to do or when the disk IoMgr shuts down.
    while (!_shut_down) {
        *request_context = nullptr;
        RequestContext::PerDiskState* request_disk_state = nullptr;
        {
            unique_lock<mutex> disk_lock(disk_queue->lock);

            while (!_shut_down && disk_queue->request_contexts.empty()) {
                // wait if there are no readers on the queue
                disk_queue->work_available.wait(disk_lock);
            }
            if (_shut_down) {
                break;
            }
            DCHECK(!disk_queue->request_contexts.empty());

            // Get the next reader and remove the reader so that another disk thread
            // can't pick it up.  It will be enqueued before issuing the read to HDFS
            // so this is not a big deal (i.e. multiple disk threads can read for the
            // same reader).
            // TODO: revisit.
            *request_context = disk_queue->request_contexts.front();
            disk_queue->request_contexts.pop_front();
            DCHECK(*request_context != nullptr);
            request_disk_state = &((*request_context)->_disk_states[disk_id]);
            request_disk_state->increment_request_thread_and_dequeue();
        }

        // NOTE: no locks were taken in between.  We need to be careful about what state
        // could have changed to the reader and disk in between.
        // There are some invariants here.  Only one disk thread can have the
        // same reader here (the reader is removed from the queue).  There can be
        // other disk threads operating on this reader in other functions though.

        unique_lock<mutex> request_lock((*request_context)->_lock);
        VLOG_FILE << "Disk (id=" << disk_id << ") reading for "
                  << (*request_context)->debug_string();

        // Check if reader has been cancelled
        if ((*request_context)->_state == RequestContext::Cancelled) {
            request_disk_state->decrement_request_thread_and_check_done(*request_context);
            continue;
        }

        DCHECK_EQ((*request_context)->_state, RequestContext::Active)
                << (*request_context)->debug_string();

        if (request_disk_state->next_scan_range_to_start() == nullptr &&
            !request_disk_state->unstarted_scan_ranges()->empty()) {
            // We don't have a range queued for this disk for what the caller should
            // read next. Populate that.  We want to have one range waiting to minimize
            // wait time in get_next_range.
            ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->dequeue();
            --(*request_context)->_num_unstarted_scan_ranges;
            (*request_context)->_ready_to_start_ranges.enqueue(new_range);
            request_disk_state->set_next_scan_range_to_start(new_range);

            if ((*request_context)->_num_unstarted_scan_ranges == 0) {
                // All the ranges have been started, notify everyone blocked on get_next_range.
                // Only one of them will get work so make sure to return nullptr to the other
                // caller threads.
                (*request_context)->_ready_to_start_ranges_cv.notify_all();
            } else {
                (*request_context)->_ready_to_start_ranges_cv.notify_one();
            }
        }

        // Always enqueue a WriteRange to be processed into _in_flight_ranges.
        // This is done so _in_flight_ranges does not exclusively contain ScanRanges.
        // For now, enqueuing a WriteRange on each invocation of get_next_request_range()
        // does not flood in_flight_ranges() with WriteRanges because the entire
        // WriteRange is processed and removed from the queue after get_next_request_range()
        // returns. (A DCHECK is used to ensure that writes do not exceed 8MB).
        if (!request_disk_state->unstarted_write_ranges()->empty()) {
            WriteRange* write_range = request_disk_state->unstarted_write_ranges()->dequeue();
            request_disk_state->in_flight_ranges()->enqueue(write_range);
        }

        // Get the next scan range to work on from the reader. Only in_flight_ranges
        // are eligible since the disk threads do not start new ranges on their own.

        // There are no inflight ranges, nothing to do.
        if (request_disk_state->in_flight_ranges()->empty()) {
            request_disk_state->decrement_request_thread();
            continue;
        }
        DCHECK_GT(request_disk_state->num_remaining_ranges(), 0);
        *range = request_disk_state->in_flight_ranges()->dequeue();
        DCHECK(*range != nullptr);

        // Now that we've picked a request range, put the context back on the queue so
        // another thread can pick up another request range for this context.
        request_disk_state->schedule_context(*request_context, disk_id);
        DCHECK((*request_context)->validate()) << endl << (*request_context)->debug_string();
        return true;
    }

    DCHECK(_shut_down);
    return false;
}

void DiskIoMgr::handle_write_finished(RequestContext* writer, WriteRange* write_range,
                                      const Status& write_status) {
    // Execute the callback before decrementing the thread count. Otherwise cancel_context()
    // that waits for the disk ref count to be 0 will return, creating a race, e.g.
    // between BufferedBlockMgr::WriteComplete() and BufferedBlockMgr::~BufferedBlockMgr().
    // See IMPALA-1890.
    // The status of the write does not affect the status of the writer context.
    write_range->_callback(write_status);
    {
        unique_lock<mutex> writer_lock(writer->_lock);
        DCHECK(writer->validate()) << endl << writer->debug_string();
        RequestContext::PerDiskState& state = writer->_disk_states[write_range->_disk_id];
        if (writer->_state == RequestContext::Cancelled) {
            state.decrement_request_thread_and_check_done(writer);
        } else {
            state.decrement_request_thread();
        }
        --state.num_remaining_ranges();
    }
}

void DiskIoMgr::handle_read_finished(DiskQueue* disk_queue, RequestContext* reader,
                                     BufferDescriptor* buffer) {
    unique_lock<mutex> reader_lock(reader->_lock);

    RequestContext::PerDiskState& state = reader->_disk_states[disk_queue->disk_id];
    DCHECK(reader->validate()) << endl << reader->debug_string();
    DCHECK_GT(state.num_threads_in_op(), 0);
    DCHECK(buffer->_buffer != nullptr);

    if (reader->_state == RequestContext::Cancelled) {
        state.decrement_request_thread_and_check_done(reader);
        DCHECK(reader->validate()) << endl << reader->debug_string();
        return_free_buffer(buffer);
        buffer->_buffer = nullptr;
        buffer->_scan_range->cancel(reader->_status);
        // Enqueue the buffer to use the scan range's buffer cleanup path.
        buffer->_scan_range->enqueue_buffer(buffer);
        return;
    }

    DCHECK_EQ(reader->_state, RequestContext::Active);
    DCHECK(buffer->_buffer != nullptr);

    // Update the reader's scan ranges.  There are a three cases here:
    //  1. Read error
    //  2. End of scan range
    //  3. Middle of scan range
    if (!buffer->_status.ok()) {
        // Error case
        return_free_buffer(buffer);
        buffer->_eosr = true;
        --state.num_remaining_ranges();
        buffer->_scan_range->cancel(buffer->_status);
    } else if (buffer->_eosr) {
        --state.num_remaining_ranges();
    }

    // After calling enqueue_buffer(), it is no longer valid to read from buffer.
    // Store the state we need before calling enqueue_buffer().
    bool eosr = buffer->_eosr;
    ScanRange* scan_range = buffer->_scan_range;
    bool queue_full = buffer->_scan_range->enqueue_buffer(buffer);
    if (eosr) {
        // For cached buffers, we can't close the range until the cached buffer is returned.
        // close() is called from DiskIoMgr::return_buffer().
        /*
         * if (scan_range->_cached_buffer == nullptr) {
         *     scan_range->close();
         * }
         */
    } else {
        if (queue_full) {
            reader->_blocked_ranges.enqueue(scan_range);
        } else {
            reader->schedule_scan_range(scan_range);
        }
    }
    state.decrement_request_thread();
}

void DiskIoMgr::work_loop(DiskQueue* disk_queue) {
    // The thread waits until there is work or the entire system is being shut down.
    // If there is work, performs the read or write requested and re-enqueues the
    // requesting context.
    // Locks are not taken when reading from or writing to disk.
    // The main loop has three parts:
    //   1. GetNextRequestContext(): get the next request context (read or write) to
    //      process and dequeue it.
    //   2. For the dequeued request, gets the next scan- or write-range to process and
    //      re-enqueues the request.
    //   3. Perform the read or write as specified.
    // Cancellation checking needs to happen in both steps 1 and 3.

    while (!_shut_down) {
        RequestContext* worker_context = nullptr;
        ;
        RequestRange* range = nullptr;

        if (!get_next_request_range(disk_queue, &range, &worker_context)) {
            DCHECK(_shut_down);
            break;
        }

        if (range->request_type() == RequestType::READ) {
            read_range(disk_queue, worker_context, static_cast<ScanRange*>(range));
        } else {
            DCHECK(range->request_type() == RequestType::WRITE);
            write(worker_context, static_cast<WriteRange*>(range));
        }
    }

    DCHECK(_shut_down);
}

// This function reads the specified scan range associated with the
// specified reader context and disk queue.
void DiskIoMgr::read_range(DiskQueue* disk_queue, RequestContext* reader, ScanRange* range) {
    char* buffer = nullptr;
    int64_t bytes_remaining = range->_len - range->_bytes_read;
    DCHECK_GT(bytes_remaining, 0);
    int64_t buffer_size = std::min(bytes_remaining, static_cast<int64_t>(_max_buffer_size));
    bool enough_memory = _mem_tracker->spare_capacity() > LOW_MEMORY;
    if (!enough_memory) {
        // Low memory, GC and try again.
        gc_io_buffers();
        enough_memory = _mem_tracker->spare_capacity() > LOW_MEMORY;
    }

    if (!enough_memory) {
        RequestContext::PerDiskState& state = reader->_disk_states[disk_queue->disk_id];
        unique_lock<mutex> reader_lock(reader->_lock);

        // Just grabbed the reader lock, check for cancellation.
        if (reader->_state == RequestContext::Cancelled) {
            DCHECK(reader->validate()) << endl << reader->debug_string();
            state.decrement_request_thread_and_check_done(reader);
            range->cancel(reader->_status);
            DCHECK(reader->validate()) << endl << reader->debug_string();
            return;
        }

        if (!range->_ready_buffers.empty()) {
            // We have memory pressure and this range doesn't need another buffer
            // (it already has one queued). Skip this range and pick it up later.
            range->_blocked_on_queue = true;
            reader->_blocked_ranges.enqueue(range);
            state.decrement_request_thread();
            return;
        } else {
            // We need to get a buffer anyway since there are none queued. The query
            // is likely to fail due to mem limits but there's nothing we can do about that
            // now.
        }
    }

    buffer = get_free_buffer(&buffer_size);
    ++reader->_num_used_buffers;

    // Validate more invariants.
    DCHECK_GT(reader->_num_used_buffers, 0);
    DCHECK(range != nullptr);
    DCHECK(reader != nullptr);
    DCHECK(buffer != nullptr);

    BufferDescriptor* buffer_desc = get_buffer_desc(reader, range, buffer, buffer_size);
    DCHECK(buffer_desc != nullptr);

    // No locks in this section.  Only working on local vars.  We don't want to hold a
    // lock across the read call.
    buffer_desc->_status = range->open();
    if (buffer_desc->_status.ok()) {
        // Update counters.
        if (reader->_active_read_thread_counter) {
            reader->_active_read_thread_counter->update(1L);
        }
        if (reader->_disks_accessed_bitmap) {
            int64_t disk_bit = 1 << disk_queue->disk_id;
            reader->_disks_accessed_bitmap->bit_or(disk_bit);
        }
        SCOPED_TIMER(&_read_timer);
        SCOPED_TIMER(reader->_read_timer);

        buffer_desc->_status = range->read(buffer, &buffer_desc->_len, &buffer_desc->_eosr);
        buffer_desc->_scan_range_offset = range->_bytes_read - buffer_desc->_len;

        if (reader->_bytes_read_counter != nullptr) {
            COUNTER_UPDATE(reader->_bytes_read_counter, buffer_desc->_len);
        }

        COUNTER_UPDATE(&_total_bytes_read_counter, buffer_desc->_len);
        if (reader->_active_read_thread_counter) {
            reader->_active_read_thread_counter->update(-1L);
        }
    }

    // Finished read, update reader/disk based on the results
    handle_read_finished(disk_queue, reader, buffer_desc);
}

void DiskIoMgr::write(RequestContext* writer_context, WriteRange* write_range) {
    FILE* file_handle = fopen(write_range->file(), "rb+");
    Status ret_status;
    if (file_handle == nullptr) {
        stringstream error_msg;
        error_msg << "fopen(" << write_range->_file << ", \"rb+\") failed with errno=" << errno
                  << " description=" << get_str_err_msg();
        ret_status = Status::InternalError(error_msg.str());
    } else {
        ret_status = write_range_helper(file_handle, write_range);

        int success = fclose(file_handle);
        if (ret_status.ok() && success != 0) {
            ret_status = Status::InternalError("fclose({}) failed", write_range->_file);
        }
    }

    handle_write_finished(writer_context, write_range, ret_status);
}

Status DiskIoMgr::write_range_helper(FILE* file_handle, WriteRange* write_range) {
    // Seek to the correct offset and perform the write.
    int success = fseek(file_handle, write_range->offset(), SEEK_SET);
    if (success != 0) {
        return Status::InternalError("fseek({}, {} SEEK_SET) failed with errno={} description={}",
                                     write_range->_file, write_range->offset(), errno,
                                     get_str_err_msg());
    }

    int64_t bytes_written = fwrite(write_range->_data, 1, write_range->_len, file_handle);
    if (bytes_written < write_range->_len) {
        return Status::InternalError(
                "fwrite(buffer, 1, {}, {}) failed with errno={} description={}", write_range->_len,
                write_range->_file, errno, get_str_err_msg());
    }

    return Status::OK();
}

int DiskIoMgr::free_buffers_idx(int64_t buffer_size) {
    int64_t buffer_size_scaled = bit_ceil(buffer_size, _min_buffer_size);
    int idx = bit_log2(buffer_size_scaled);
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, _free_buffers.size());
    return idx;
}

Status DiskIoMgr::add_write_range(RequestContext* writer, WriteRange* write_range) {
    DCHECK_LE(write_range->len(), _max_buffer_size);
    unique_lock<mutex> writer_lock(writer->_lock);

    if (writer->_state == RequestContext::Cancelled) {
        DCHECK(!writer->_status.ok());
        return writer->_status;
    }

    writer->add_request_range(write_range, false);
    return Status::OK();
}

/*
 * int DiskIoMgr::AssignQueue(const char* file, int disk_id, bool expected_local) {
 *   // If it's a remote range, check for an appropriate remote disk queue.
 *   if (!expected_local) {
 *     if (IsDfsPath(file) && FLAGS_num_remote_hdfs_io_threads > 0) return RemoteDfsDiskId();
 *     if (IsS3APath(file)) return RemoteS3DiskId();
 *   }
 *   // Assign to a local disk queue.
 *   DCHECK(!IsS3APath(file)); // S3 is always remote.
 *   if (disk_id == -1) {
 *     // disk id is unknown, assign it a random one.
 *     static int next_disk_id = 0;
 *     disk_id = next_disk_id++;
 *   }
 *   // TODO: we need to parse the config for the number of dirs configured for this
 *   // data node.
 *   return disk_id % num_local_disks();
 * }
 */

/*
 * DiskIoMgr::HdfsCachedFileHandle* DiskIoMgr::OpenHdfsFile(const hdfsFS& fs,
 *     const char* fname, int64_t mtime) {
 *   HdfsCachedFileHandle* fh = nullptr;
 *
 *   // Check if a cached file handle exists and validate the mtime, if the mtime of the
 *   // cached handle is not matching the mtime of the requested file, reopen.
 *   if (detail::is_file_handle_caching_enabled() && _file_handle_cache.Pop(fname, &fh)) {
 *     if (fh->mtime() == mtime) {
 *       return fh;
 *     }
 *     VLOG_FILE << "mtime mismatch, closing cached file handle. Closing file=" << fname;
 *     delete fh;
 *   }
 *
 *   fh = new HdfsCachedFileHandle(fs, fname, mtime);
 *
 *   // Check if the file handle was opened correctly
 *   if (!fh->ok())  {
 *     VLOG_FILE << "Opening the file " << fname << " failed.";
 *     delete fh;
 *     return nullptr;
 *   }
 *
 *   return fh;
 * }
 */

/*
 * void DiskIoMgr::cache_or_close_file_handle(const char* fname,
 *     DiskIoMgr::HdfsCachedFileHandle* fid, bool close) {
 *   // Try to unbuffer the handle, on filesystems that do not support this call a non-zero
 *   // return code indicates that the operation was not successful and thus the file is
 *   // closed.
 *   if (detail::is_file_handle_caching_enabled() &&
 *       !close && hdfsUnbufferFile(fid->file()) == 0) {
 *     // Clear read statistics before returning
 *     hdfsFileClearReadStatistics(fid->file());
 *     _file_handle_cache.Put(fname, fid);
 *   } else {
 *     if (close) {
 *       VLOG_FILE << "Closing file=" << fname;
 *     } else {
 *       VLOG_FILE << "FS does not support file handle unbuffering, closing file="
 *                 << fname;
 *     }
 *     delete fid;
 *   }
 * }
 */

} // namespace doris
