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

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_H
#define DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_H

#include <condition_variable>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/atomic.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/mem_tracker.h"
#include "util/error_util.h"
#include "util/internal_queue.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "util/thread_group.h"

namespace doris {

class MemTracker;

// Manager object that schedules IO for all queries on all disks and remote filesystems
// (such as S3). Each query maps to one or more RequestContext objects, each of which
// has its own queue of scan ranges and/or write ranges.
//
// The API splits up requesting scan/write ranges (non-blocking) and reading the data
// (blocking). The DiskIoMgr has worker threads that will read from and write to
// disk/hdfs/remote-filesystems, allowing interleaving of IO and CPU. This allows us to
// keep all disks and all cores as busy as possible.
//
// All public APIs are thread-safe. It is not valid to call any of the APIs after
// unregister_context() returns.
//
// For Readers:
// We can model this problem as a multiple producer (threads for each disk), multiple
// consumer (scan ranges) problem. There are multiple queues that need to be
// synchronized. Conceptually, there are two queues:
//   1. The per disk queue: this contains a queue of readers that need reads.
//   2. The per scan range ready-buffer queue: this contains buffers that have been
//      read and are ready for the caller.
// The disk queue contains a queue of readers and is scheduled in a round robin fashion.
// Readers map to scan nodes. The reader then contains a queue of scan ranges. The caller
// asks the IoMgr for the next range to process. The IoMgr then selects the best range
// to read based on disk activity and begins reading and queuing buffers for that range.
// TODO: We should map readers to queries. A reader is the unit of scheduling and queries
// that have multiple scan nodes shouldn't have more 'turns'.
//
// For Writers:
// Data is written via add_write_range(). This is non-blocking and adds a WriteRange to a
// per-disk queue. After the write is complete, a callback in WriteRange is invoked.
// No memory is allocated within IoMgr for writes and no copies are made. It is the
// responsibility of the client to ensure that the data to be written is valid and that
// the file to be written to exists until the callback is invoked.
//
// The IoMgr provides three key APIs.
//  1. add_scan_ranges: this is non-blocking and tells the IoMgr all the ranges that
//     will eventually need to be read.
//  2. get_next_range: returns to the caller the next scan range it should process.
//     This is based on disk load. This also begins reading the data in this scan
//     range. This is blocking.
//  3. ScanRange::get_next: returns the next buffer for this range.  This is blocking.
//
// The disk threads do not synchronize with each other. The readers and writers don't
// synchronize with each other. There is a lock and condition variable for each request
// context queue and each disk queue.
// IMPORTANT: whenever both locks are needed, the lock order is to grab the context lock
// before the disk lock.
//
// Scheduling: If there are multiple request contexts with work for a single disk, the
// request contexts are scheduled in round-robin order. Multiple disk threads can
// operate on the same request context. Exactly one request range is processed by a
// disk thread at a time. If there are multiple scan ranges scheduled via
// get_next_range() for a single context, these are processed in round-robin order.
// If there are multiple scan and write ranges for a disk, a read is always followed
// by a write, and a write is followed by a read, i.e. reads and writes alternate.
// If multiple write ranges are enqueued for a single disk, they will be processed
// by the disk threads in order, but may complete in any order. No guarantees are made
// on ordering of writes across disks.
//
// Resource Management: effective resource management in the IoMgr is key to good
// performance. The IoMgr helps coordinate two resources: CPU and disk. For CPU,
// spinning up too many threads causes thrashing.
// Memory usage in the IoMgr comes from queued read buffers.  If we queue the minimum
// (i.e. 1), then the disks are idle while we are processing the buffer. If we don't
// limit the queue, then it possible we end up queueing the entire data set (i.e. CPU
// is slower than disks) and run out of memory.
// For both CPU and memory, we want to model the machine as having a fixed amount of
// resources.  If a single query is running, it should saturate either CPU or Disk
// as well as using as little memory as possible. With multiple queries, each query
// should get less CPU. In that case each query will need fewer queued buffers and
// therefore have less memory usage.
//
// The IoMgr defers CPU management to the caller. The IoMgr provides a get_next_range
// API which will return the next scan range the caller should process. The caller
// can call this from the desired number of reading threads. Once a scan range
// has been returned via get_next_range, the IoMgr will start to buffer reads for
// that range and it is expected the caller will pull those buffers promptly. For
// example, if the caller would like to have 1 scanner thread, the read loop
// would look like:
//   while (more_ranges)
//     range = get_next_range()
//     while (!range.eosr)
//       buffer = range.get_next()
// To have multiple reading threads, the caller would simply spin up the threads
// and each would process the loops above.
//
// To control the number of IO buffers, each scan range has a soft max capacity for
// the number of queued buffers. If the number of buffers is at capacity, the IoMgr
// will no longer read for that scan range until the caller has processed a buffer.
// This capacity does not need to be fixed, and the caller can dynamically adjust
// it if necessary.
//
// As an example: If we allowed 5 buffers per range on a 24 core, 72 thread
// (we default to allowing 3x threads) machine, we should see at most
// 72 * 5 * 8MB = 2.8GB in io buffers memory usage. This should remain roughly constant
// regardless of how many concurrent readers are running.
//
// Buffer Management:
// Buffers are allocated by the IoMgr as necessary to service reads. These buffers
// are directly returned to the caller. The caller must call Return() on the buffer
// when it is done, at which point the buffer will be recycled for another read. In error
// cases, the IoMgr will recycle the buffers more promptly but regardless, the caller
// must always call Return()
//
// Caching support:
// Scan ranges contain metadata on whether or not it is cached on the DN. In that
// case, we use the HDFS APIs to read the cached data without doing any copies. For these
// ranges, the reads happen on the caller thread (as opposed to the disk threads).
// It is possible for the cached read APIs to fail, in which case the ranges are then
// queued on the disk threads and behave identically to the case where the range
// is not cached.
// Resources for these ranges are also not accounted against the reader because none
// are consumed.
// While a cached block is being processed, the block is mlocked. We want to minimize
// the time the mlock is held.
//   - HDFS will time us out if we hold onto the mlock for too long
//   - Holding the lock prevents uncaching this file due to a caching policy change.
// Therefore, we only issue the cached read when the caller is ready to process the
// range (get_next_range()) instead of when the ranges are issued. This guarantees that
// there will be a CPU available to process the buffer and any throttling we do with
// the number of scanner threads properly controls the amount of files we mlock.
// With cached scan ranges, we cannot close the scan range until the cached buffer
// is returned (HDFS does not allow this). We therefore need to defer the close until
// the cached buffer is returned (BufferDescriptor::Return()).
//
// Remote filesystem support (e.g. S3):
// Remote filesystems are modeled as "remote disks". That is, there is a seperate disk
// queue for each supported remote filesystem type. In order to maximize throughput,
// multiple connections are opened in parallel by having multiple threads running per
// queue. Also note that reading from a remote filesystem service can be more CPU
// intensive than local disk/hdfs because of non-direct I/O and SSL processing, and can
// be CPU bottlenecked especially if not enough I/O threads for these queues are
// started.
//
// TODO: IoMgr should be able to request additional scan ranges from the coordinator
// to help deal with stragglers.
// TODO: look into using a lock free queue
// TODO: simplify the common path (less locking, memory allocations).
// TODO: Break this up the .h/.cc into multiple files under an /io subdirectory.
//
// Structure of the Implementation:
//  - All client APIs are defined in this file
//  - Internal classes are defined in disk-io-mgr-internal.h
//  - ScanRange APIs are implemented in disk-io-mgr-scan-range.cc
//    This contains the ready buffer queue logic
//  - RequestContext APIs are implemented in disk-io-mgr-reader-context.cc
//    This contains the logic for picking scan ranges for a reader.
//  - Disk Thread and general APIs are implemented in disk-io-mgr.cc.

typedef void* hdfsFS;
typedef void* hdfsFile;

class DiskIoMgr {
public:
    class RequestContext;
    class ScanRange;

    // This class is a small wrapper around the hdfsFile handle and the file system
    // instance which is needed to close the file handle in case of eviction. It
    // additionally encapsulates the last modified time of the associated file when it was
    // last opened.
    class HdfsCachedFileHandle {
    public:
        // Constructor will open the file
        HdfsCachedFileHandle(const hdfsFS& fs, const char* fname, int64_t mtime);

        // Destructor will close the file handle
        ~HdfsCachedFileHandle();

        hdfsFile file() const { return _hdfs_file; }

        int64_t mtime() const { return _mtime; }

        // This method is called to release acquired resources by the cached handle when it
        // is evicted.
        static void release(HdfsCachedFileHandle** h);

        bool ok() const { return _hdfs_file != nullptr; }

    private:
        hdfsFS _fs;
        hdfsFile _hdfs_file;
        int64_t _mtime;
    };

    // Buffer struct that is used by the caller and IoMgr to pass read buffers.
    // It is is expected that only one thread has ownership of this object at a
    // time.
    class BufferDescriptor {
    public:
        // a null dtor to pass codestyle check
        ~BufferDescriptor() {}

        ScanRange* scan_range() { return _scan_range; }
        char* buffer() { return _buffer; }
        int64_t buffer_len() { return _buffer_len; }
        int64_t len() { return _len; }
        bool eosr() { return _eosr; }

        // Returns the offset within the scan range that this buffer starts at
        int64_t scan_range_offset() const { return _scan_range_offset; }

        // Updates this buffer to be owned by the new tracker. Consumption is
        // release from the current tracker and added to the new one.
        void set_mem_tracker(std::shared_ptr<MemTracker> tracker);

        // Returns the buffer to the IoMgr. This must be called for every buffer
        // returned by get_next()/read() that did not return an error. This is non-blocking.
        // After calling this, the buffer descriptor is invalid and cannot be accessed.
        void return_buffer();

    private:
        friend class DiskIoMgr;
        BufferDescriptor(DiskIoMgr* io_mgr);

        // Resets the buffer descriptor state for a new reader, range and data buffer.
        void reset(RequestContext* reader, ScanRange* range, char* buffer, int64_t buffer_len);

        DiskIoMgr* _io_mgr;

        // Reader that this buffer is for
        RequestContext* _reader;

        // The current tracker this buffer is associated with.
        std::shared_ptr<MemTracker> _mem_tracker;

        // Scan range that this buffer is for.
        ScanRange* _scan_range;

        // buffer with the read contents
        char* _buffer;

        // length of _buffer. For buffers from cached reads, the length is 0.
        int64_t _buffer_len;

        // length of read contents
        int64_t _len;

        // true if the current scan range is complete
        bool _eosr;

        // Status of the read to this buffer. if status is not ok, 'buffer' is nullptr
        Status _status;

        int64_t _scan_range_offset;
    };

    // The request type, read or write associated with a request range.
    struct RequestType {
        enum type {
            READ,
            WRITE,
        };
    };

    // Represents a contiguous sequence of bytes in a single file.
    // This is the common base class for read and write IO requests - ScanRange and
    // WriteRange. Each disk thread processes exactly one RequestRange at a time.
    class RequestRange : public InternalQueue<RequestRange>::Node {
    public:
        // hdfsFS fs() const { return _fs; }
        const char* file() const { return _file.c_str(); }
        int64_t offset() const { return _offset; }
        int64_t len() const { return _len; }
        int disk_id() const { return _disk_id; }
        RequestType::type request_type() const { return _request_type; }

    protected:
        // Hadoop filesystem that contains _file, or set to nullptr for local filesystem.
        hdfsFS _fs;

        // Path to file being read or written.
        std::string _file;

        // Offset within _file being read or written.
        int64_t _offset;

        // Length of data read or written.
        int64_t _len;

        // Id of disk containing byte range.
        int _disk_id;

        // The type of IO request, READ or WRITE.
        RequestType::type _request_type;
    };

    // ScanRange description. The caller must call Reset() to initialize the fields
    // before calling add_scan_ranges(). The private fields are used internally by
    // the IoMgr.
    class ScanRange : public RequestRange {
    public:
        // If the mtime is set to NEVER_CACHE, the file handle should never be cached.
        const static int64_t NEVER_CACHE = -1;

        // The initial queue capacity for this.  Specify -1 to use IoMgr default.
        ScanRange() : ScanRange(-1) {}
        ScanRange(int initial_capacity);

        virtual ~ScanRange();

        // Resets this scan range object with the scan range description.  The scan range
        // must fall within the file bounds (offset >= 0 and offset + len <= file_length).
        // Resets this scan range object with the scan range description.
        void reset(hdfsFS fs, const char* file, int64_t len, int64_t offset, int disk_id,
                   bool try_cache, bool expected_local, int64_t mtime, void* metadata = nullptr);

        void* meta_data() const { return _meta_data; }
        // bool try_cache() const { return _try_cache; }
        bool expected_local() const { return _expected_local; }
        int ready_buffers_capacity() const { return _ready_buffers_capacity; }

        // Returns the next buffer for this scan range. buffer is an output parameter.
        // This function blocks until a buffer is ready or an error occurred. If this is
        // called when all buffers have been returned, *buffer is set to nullptr and Status::OK()
        // is returned.
        // Only one thread can be in get_next() at any time.
        Status get_next(BufferDescriptor** buffer);

        // Cancel this scan range. This cleans up all queued buffers and
        // wakes up any threads blocked on get_next().
        // Status is the reason the range was cancelled. Must not be ok().
        // Status is returned to the user in get_next().
        void cancel(const Status& status);

        // return a descriptive string for debug.
        std::string debug_string() const;

        int64_t mtime() const { return _mtime; }

    private:
        friend class DiskIoMgr;

        // Initialize internal fields
        void init_internal(DiskIoMgr* io_mgr, RequestContext* reader);

        // Enqueues a buffer for this range. This does not block.
        // Returns true if this scan range has hit the queue capacity, false otherwise.
        // The caller passes ownership of buffer to the scan range and it is not
        // valid to access buffer after this call.
        bool enqueue_buffer(BufferDescriptor* buffer);

        // Cleanup any queued buffers (i.e. due to cancellation). This cannot
        // be called with any locks taken.
        void cleanup_queued_buffers();

        // Validates the internal state of this range. _lock must be taken
        // before calling this.
        bool validate();

        // Maximum length in bytes for hdfsRead() calls.
        int64_t max_read_chunk_size() const;

        // Opens the file for this range. This function only modifies state in this range.
        Status open();

        // Closes the file for this range. This function only modifies state in this range.
        void close();

        // Reads from this range into 'buffer'. Buffer is preallocated. Returns the number
        // of bytes read. Updates range to keep track of where in the file we are.
        Status read(char* buffer, int64_t* bytes_read, bool* eosr);

        // Reads from the DN cache. On success, sets _cached_buffer to the DN buffer
        // and *read_succeeded to true.
        // If the data is not cached, returns ok() and *read_succeeded is set to false.
        // Returns a non-ok status if it ran into a non-continuable error.
        Status read_from_cache(bool* read_succeeded);

        // Pointer to caller specified metadata. This is untouched by the io manager
        // and the caller can put whatever auxiliary data in here.
        void* _meta_data;

        // If true, this scan range is expected to be cached. Note that this might be wrong
        // since the block could have been uncached. In that case, the cached path
        // will fail and we'll just put the scan range on the normal read path.
        bool _try_cache;

        // If true, we expect this scan range to be a local read. Note that if this is false,
        // it does not necessarily mean we expect the read to be remote, and that we never
        // create scan ranges where some of the range is expected to be remote and some of it
        // local.
        // TODO: we can do more with this
        bool _expected_local;

        DiskIoMgr* _io_mgr;

        // Reader/owner of the scan range
        RequestContext* _reader;

        // File handle either to hdfs or local fs (FILE*)
        //
        // TODO: The pointer to HdfsCachedFileHandle is manually managed and should be
        // replaced by unique_ptr in C++11
        union {
            FILE* _local_file;
            HdfsCachedFileHandle* _hdfs_file;
        };

        // If non-null, this is DN cached buffer. This means the cached read succeeded
        // and all the bytes for the range are in this buffer.
        struct hadoopRzBuffer* _cached_buffer;

        // Lock protecting fields below.
        // This lock should not be taken during Open/Read/Close.
        std::mutex _lock;

        // Number of bytes read so far for this scan range
        int _bytes_read;

        // Status for this range. This is non-ok if _is_cancelled is true.
        // Note: an individual range can fail without the RequestContext being
        // cancelled. This allows us to skip individual ranges.
        Status _status;

        // If true, the last buffer for this scan range has been queued.
        bool _eosr_queued;

        // If true, the last buffer for this scan range has been returned.
        bool _eosr_returned;

        // If true, this scan range has been removed from the reader's in_flight_ranges
        // queue because the _ready_buffers queue is full.
        bool _blocked_on_queue;

        // IO buffers that are queued for this scan range.
        // Condition variable for get_next
        std::condition_variable _buffer_ready_cv;
        std::list<BufferDescriptor*> _ready_buffers;

        // The soft capacity limit for _ready_buffers. _ready_buffers can exceed
        // the limit temporarily as the capacity is adjusted dynamically.
        // In that case, the capacity is only realized when the caller removes buffers
        // from _ready_buffers.
        int _ready_buffers_capacity;

        // Lock that should be taken during hdfs calls. Only one thread (the disk reading
        // thread) calls into hdfs at a time so this lock does not have performance impact.
        // This lock only serves to coordinate cleanup. Specifically it serves to ensure
        // that the disk threads are finished with HDFS calls before _is_cancelled is set
        // to true and cleanup starts.
        // If this lock and _lock need to be taken, _lock must be taken first.
        std::mutex _hdfs_lock;

        // If true, this scan range has been cancelled.
        bool _is_cancelled;

        // Last modified time of the file associated with the scan range
        int64_t _mtime;
    };

    // Used to specify data to be written to a file and offset.
    // It is the responsibility of the client to ensure that the data to be written is
    // valid and that the file to be written to exists until the callback is invoked.
    // A callback is invoked to inform the client when the write is done.
    class WriteRange : public RequestRange {
    public:
        // a null dtor to pass codestyle check
        ~WriteRange() {}

        // This callback is invoked on each WriteRange after the write is complete or the
        // context is cancelled. The status returned by the callback parameter indicates
        // if the write was successful (i.e. Status::OK()), if there was an error
        // TStatusCode::RUNTIME_ERROR) or if the context was cancelled
        // (TStatusCode::CANCELLED). The callback is only invoked if this WriteRange was
        // successfully added (i.e. add_write_range() succeeded). No locks are held while
        // the callback is invoked.
        typedef std::function<void(const Status&)> WriteDoneCallback;
        WriteRange(const std::string& file, int64_t file_offset, int disk_id,
                   WriteDoneCallback callback);

        // Set the data and number of bytes to be written for this WriteRange.
        // File data can be over-written by calling set_data() and add_write_range().
        void set_data(const uint8_t* buffer, int64_t len);

    private:
        friend class DiskIoMgr;

        // Data to be written. RequestRange::_len contains the length of data
        // to be written.
        const uint8_t* _data;

        // Callback to invoke after the write is complete.
        WriteDoneCallback _callback;
    };

    // Create a DiskIoMgr object.
    //  - num_disks: The number of disks the IoMgr should use. This is used for testing.
    //    Specify 0, to have the disk IoMgr query the os for the number of disks.
    //  - threads_per_disk: number of read threads to create per disk. This is also
    //    the max queue depth.
    //  - min_buffer_size: minimum io buffer size (in bytes)
    //  - max_buffer_size: maximum io buffer size (in bytes). Also the max read size.
    DiskIoMgr(int num_disks, int threads_per_disk, int min_buffer_size, int max_buffer_size);

    // Create DiskIoMgr with default configs.
    DiskIoMgr();

    // Clean up all threads and resources. This is mostly useful for testing since
    // for impalad, this object is never destroyed.
    ~DiskIoMgr();

    // Initialize the IoMgr. Must be called once before any of the other APIs.
    Status init(const std::shared_ptr<MemTracker>& process_mem_tracker);

    // Allocates tracking structure for a request context.
    // Register a new request context which is returned in *request_context.
    // The IoMgr owns the allocated RequestContext object. The caller must call
    // unregister_context() for each context.
    // reader_mem_tracker: Is non-null only for readers. IO buffers
    //    used for this reader will be tracked by this. If the limit is exceeded
    //    the reader will be cancelled and MEM_LIMIT_EXCEEDED will be returned via
    //    get_next().
    Status register_context(
            RequestContext** request_context,
            std::shared_ptr<MemTracker> reader_mem_tracker = std::shared_ptr<MemTracker>());

    // Unregisters context from the disk IoMgr. This must be called for every
    // register_context() regardless of cancellation and must be called in the
    // same thread as get_next()
    // The 'context' cannot be used after this call.
    // This call blocks until all the disk threads have finished cleaning up.
    // unregister_context also cancels the reader/writer from the disk IoMgr.
    void unregister_context(RequestContext* context);

    // This function cancels the context asynchronously. All outstanding requests
    // are aborted and tracking structures cleaned up. This does not need to be
    // called if the context finishes normally.
    // This will also fail any outstanding get_next()/Read requests.
    // If wait_for_disks_completion is true, wait for the number of active disks for this
    // context to reach 0. After calling with wait_for_disks_completion = true, the only
    // valid API is returning IO buffers that have already been returned.
    // Takes context->_lock if wait_for_disks_completion is true.
    void cancel_context(RequestContext* context, bool wait_for_disks_completion = false);

    // Adds the scan ranges to the queues. This call is non-blocking. The caller must
    // not deallocate the scan range pointers before unregister_context().
    // If schedule_immediately, the ranges are immediately put on the read queue
    // (i.e. the caller should not/cannot call get_next_range for these ranges).
    // This can be used to do synchronous reads as well as schedule dependent ranges,
    // as in the case for columnar formats.
    Status add_scan_ranges(RequestContext* reader, const std::vector<ScanRange*>& ranges,
                           bool schedule_immediately = false);

    // Add a WriteRange for the writer. This is non-blocking and schedules the context
    // on the IoMgr disk queue. Does not create any files.
    Status add_write_range(RequestContext* writer, WriteRange* write_range);

    // Returns the next unstarted scan range for this reader. When the range is returned,
    // the disk threads in the IoMgr will already have started reading from it. The
    // caller is expected to call ScanRange::get_next on the returned range.
    // If there are no more unstarted ranges, nullptr is returned.
    // This call is blocking.
    Status get_next_range(RequestContext* reader, ScanRange** range);

    // Reads the range and returns the result in buffer.
    // This behaves like the typical synchronous read() api, blocking until the data
    // is read. This can be called while there are outstanding ScanRanges and is
    // thread safe. Multiple threads can be calling read() per reader at a time.
    // range *cannot* have already been added via add_scan_ranges.
    Status read(RequestContext* reader, ScanRange* range, BufferDescriptor** buffer);

    // Determine which disk queue this file should be assigned to.  Returns an index into
    // _disk_queues.  The disk_id is the volume ID for the local disk that holds the
    // files, or -1 if unknown.  Flag expected_local is true iff this impalad is
    // co-located with the datanode for this file.
    /*
     * int AssignQueue(const char* file, int disk_id, bool expected_local);
     */

    // TODO: The functions below can be moved to RequestContext.
    // Returns the current status of the context.
    Status context_status(RequestContext* context) const;

    // Returns the number of unstarted scan ranges for this reader.
    int num_unstarted_ranges(RequestContext* reader) const;

    void set_bytes_read_counter(RequestContext*, RuntimeProfile::Counter*);
    void set_read_timer(RequestContext*, RuntimeProfile::Counter*);
    void set_active_read_thread_counter(RequestContext*, RuntimeProfile::Counter*);
    void set_disks_access_bitmap(RequestContext*, RuntimeProfile::Counter*);

    int64_t queue_size(RequestContext* reader) const;
    int64_t bytes_read_local(RequestContext* reader) const;
    int64_t bytes_read_short_circuit(RequestContext* reader) const;
    int64_t bytes_read_dn_cache(RequestContext* reader) const;
    int num_remote_ranges(RequestContext* reader) const;
    int64_t unexpected_remote_bytes(RequestContext* reader) const;

    // Returns the read throughput across all readers.
    // TODO: should this be a sliding window?  This should report metrics for the
    // last minute, hour and since the beginning.
    int64_t get_read_throughput();

    // Returns the maximum read buffer size
    int max_read_buffer_size() const { return _max_buffer_size; }

    // Returns the total number of disk queues (both local and remote).
    int num_total_disks() const { return _disk_queues.size(); }

    // Returns the total number of remote "disk" queues.
    int num_remote_disks() const { return REMOTE_NUM_DISKS; }

    // Returns the number of local disks attached to the system.
    int num_local_disks() const { return num_total_disks() - num_remote_disks(); }

    // The disk ID (and therefore _disk_queues index) used for DFS accesses.
    // int RemoteDfsDiskId() const { return num_local_disks() + REMOTE_DFS_DISK_OFFSET; }

    // The disk ID (and therefore _disk_queues index) used for S3 accesses.
    // int RemoteS3DiskId() const { return num_local_disks() + REMOTE_S3_DISK_OFFSET; }

    // Returns the number of allocated buffers.
    int num_allocated_buffers() const { return _num_allocated_buffers; }

    // Returns the number of buffers currently owned by all readers.
    int num_buffers_in_readers() const { return _num_buffers_in_readers; }

    // Dumps the disk IoMgr queues (for readers and disks)
    std::string debug_string();

    // Validates the internal state is consistent. This is intended to only be used
    // for debugging.
    bool validate() const;

    // Given a FS handle, name and last modified time of the file, tries to open that file
    // and return an instance of HdfsCachedFileHandle. In case of an error returns nullptr.
    // HdfsCachedFileHandle* OpenHdfsFile(const hdfsFS& fs, const char* fname, int64_t mtime);

    // When the file handle is no longer in use by the scan range, return it and try to
    // unbuffer the handle. If unbuffering, closing sockets and dropping buffers in the
    // libhdfs client, is not supported, close the file handle. If the unbuffer operation
    // is supported, put the file handle together with the mtime in the LRU cache for
    // later reuse.
    // void cache_or_close_file_handle(const char* fname, HdfsCachedFileHandle* fid, bool close);

    // Default ready buffer queue capacity. This constant doesn't matter too much
    // since the system dynamically adjusts.
    static const int DEFAULT_QUEUE_CAPACITY;

    // "Disk" queue offsets for remote accesses.  Offset 0 corresponds to
    // disk ID (i.e. _disk_queue index) of num_local_disks().
    enum { REMOTE_DFS_DISK_OFFSET = 0, REMOTE_S3_DISK_OFFSET, REMOTE_NUM_DISKS };

private:
    friend class BufferDescriptor;
    struct DiskQueue;
    class RequestContextCache;

    // Pool to allocate BufferDescriptors.
    ObjectPool _pool;

    // Process memory tracker; needed to account for io buffers.
    std::shared_ptr<MemTracker> _process_mem_tracker;

    // Number of worker(read) threads per disk. Also the max depth of queued
    // work to the disk.
    const int _num_threads_per_disk;

    // Maximum read size. This is also the maximum size of each allocated buffer.
    const int _max_buffer_size;

    // The minimum size of each read buffer.
    const int _min_buffer_size;

    // Thread group containing all the worker threads.
    // ThreadGroup _disk_thread_group;
    ThreadGroup _disk_thread_group;

    // Options object for cached hdfs reads. Set on startup and never modified.
    struct hadoopRzOptions* _cached_read_options;

    // True if the IoMgr should be torn down. Worker threads watch for this to
    // know to terminate. This variable is read/written to by different threads.
    std::atomic<bool> _shut_down;

    // Total bytes read by the IoMgr.
    RuntimeProfile::Counter _total_bytes_read_counter;

    // Total time spent in hdfs reading
    RuntimeProfile::Counter _read_timer;

    // Contains all contexts that the IoMgr is tracking. This includes contexts that are
    // active as well as those in the process of being cancelled. This is a cache
    // of context objects that get recycled to minimize object allocations and lock
    // contention.
    std::unique_ptr<RequestContextCache> _request_context_cache;

    // Protects _free_buffers and _free_buffer_descs
    std::mutex _free_buffers_lock;

    // Free buffers that can be handed out to clients. There is one list for each buffer
    // size, indexed by the Log2 of the buffer size in units of _min_buffer_size. The
    // maximum buffer size is _max_buffer_size, so the maximum index is
    // Log2(_max_buffer_size / _min_buffer_size).
    //
    // E.g. if _min_buffer_size = 1024 bytes:
    //  _free_buffers[0]  => list of free buffers with size 1024 B
    //  _free_buffers[1]  => list of free buffers with size 2048 B
    //  _free_buffers[10] => list of free buffers with size 1 MB
    //  _free_buffers[13] => list of free buffers with size 8 MB
    //  _free_buffers[n]  => list of free buffers with size 2^n * 1024 B
    std::vector<std::list<char*>> _free_buffers;

    // List of free buffer desc objects that can be handed out to clients
    std::list<BufferDescriptor*> _free_buffer_descs;

    // Total number of allocated buffers, used for debugging.
    AtomicInt<int> _num_allocated_buffers;

    // Total number of buffers in readers
    AtomicInt<int> _num_buffers_in_readers;

    // Per disk queues. This is static and created once at init() time.  One queue is
    // allocated for each local disk on the system and for each remote filesystem type.
    // It is indexed by disk id.
    std::vector<DiskQueue*> _disk_queues;

    // Caching structure that maps file names to cached file handles. The cache has an upper
    // limit of entries defined by FLAGS_max_cached_file_handles. Evicted cached file
    // handles are closed.
    // FifoMultimap<std::string, HdfsCachedFileHandle*> _file_handle_cache;
    std::multimap<std::string, HdfsCachedFileHandle*> _file_handle_cache;

    // Returns the index into _free_buffers for a given buffer size
    int free_buffers_idx(int64_t buffer_size);

    // Gets a buffer description object, initialized for this reader, allocating one as
    // necessary. buffer_size / _min_buffer_size should be a power of 2, and buffer_size
    // should be <= _max_buffer_size. These constraints will be met if buffer was acquired
    // via get_free_buffer() (which it should have been).
    BufferDescriptor* get_buffer_desc(RequestContext* reader, ScanRange* range, char* buffer,
                                      int64_t buffer_size);

    // Returns a buffer desc object which can now be used for another reader.
    void return_buffer_desc(BufferDescriptor* desc);

    // Returns the buffer desc and underlying buffer to the disk IoMgr. This also updates
    // the reader and disk queue state.
    void return_buffer(BufferDescriptor* buffer);

    // Returns a buffer to read into with size between *buffer_size and _max_buffer_size,
    // and *buffer_size is set to the size of the buffer. If there is an
    // appropriately-sized free buffer in the '_free_buffers', that is returned, otherwise
    // a new one is allocated. *buffer_size must be between 0 and _max_buffer_size.
    char* get_free_buffer(int64_t* buffer_size);

    // Garbage collect all unused io buffers. This is currently only triggered when the
    // process wide limit is hit. This is not good enough. While it is sufficient for
    // the IoMgr, other components do not trigger this GC.
    // TODO: make this run periodically?
    void gc_io_buffers();

    // Returns a buffer to the free list. buffer_size / _min_buffer_size should be a power
    // of 2, and buffer_size should be <= _max_buffer_size. These constraints will be met
    // if buffer was acquired via get_free_buffer() (which it should have been).
    void return_free_buffer(char* buffer, int64_t buffer_size);

    // Returns the buffer in desc (cannot be nullptr), sets buffer to nullptr and clears the
    // mem tracker.
    void return_free_buffer(BufferDescriptor* desc);

    // Disk worker thread loop. This function retrieves the next range to process on
    // the disk queue and invokes read_range() or Write() depending on the type of Range().
    // There can be multiple threads per disk running this loop.
    void work_loop(DiskQueue* queue);

    // This is called from the disk thread to get the next range to process. It will
    // wait until a scan range and buffer are available, or a write range is available.
    // This functions returns the range to process.
    // Only returns false if the disk thread should be shut down.
    // No locks should be taken before this function call and none are left taken after.
    bool get_next_request_range(DiskQueue* disk_queue, RequestRange** range,
                                RequestContext** request_context);

    // Updates disk queue and reader state after a read is complete. The read result
    // is captured in the buffer descriptor.
    void handle_read_finished(DiskQueue*, RequestContext*, BufferDescriptor*);

    // Invokes write_range->_callback  after the range has been written and
    // updates per-disk state and handle state. The status of the write OK/RUNTIME_ERROR
    // etc. is passed via write_status and to the callback.
    // The write_status does not affect the writer->_status. That is, an write error does
    // not cancel the writer context - that decision is left to the callback handler.
    // TODO: On the read path, consider not canceling the reader context on error.
    void handle_write_finished(RequestContext* writer, WriteRange* write_range,
                               const Status& write_status);

    // Validates that range is correctly initialized
    Status validate_scan_range(ScanRange* range);

    // Write the specified range to disk and calls handle_write_finished when done.
    // Responsible for opening and closing the file that is written.
    void write(RequestContext* writer_context, WriteRange* write_range);

    // Helper method to write a range using the specified FILE handle. Returns Status:OK
    // if the write succeeded, or a RUNTIME_ERROR with an appropriate message otherwise.
    // Does not open or close the file that is written.
    Status write_range_helper(FILE* file_handle, WriteRange* write_range);

    // Reads the specified scan range and calls handle_read_finished when done.
    void read_range(DiskQueue* disk_queue, RequestContext* reader, ScanRange* range);
};

} // end namespace doris

#endif // DORIS_BE_SRC_QUERY_RUNTIME_DISK_IO_MGR_H
