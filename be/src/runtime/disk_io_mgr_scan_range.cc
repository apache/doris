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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/disk-io-mgr-scan-range.cc
// and modified by Doris

#include "runtime/disk_io_mgr.h"
#include "runtime/disk_io_mgr_internal.h"
#include "util/error_util.h"

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::endl;

using std::lock_guard;
using std::unique_lock;
using std::mutex;

namespace doris {

// A very large max value to prevent things from going out of control. Not
// expected to ever hit this value (1GB of buffered data per range).
const int MAX_QUEUE_CAPACITY = 128;
const int MIN_QUEUE_CAPACITY = 2;

// Implementation of the ScanRange functionality. Each ScanRange contains a queue
// of ready buffers. For each ScanRange, there is only a single producer and
// consumer thread, i.e. only one disk thread will push to a scan range at
// any time and only one thread will remove from the queue. This is to guarantee
// that buffers are queued and read in file order.

// This must be called with the reader lock taken.
bool DiskIoMgr::ScanRange::enqueue_buffer(BufferDescriptor* buffer) {
    {
        unique_lock<mutex> scan_range_lock(_lock);
        DCHECK(validate()) << debug_string();
        DCHECK(!_eosr_returned);
        DCHECK(!_eosr_queued);
        if (_is_cancelled) {
            // Return the buffer, this range has been cancelled
            if (buffer->_buffer != nullptr) {
                ++_io_mgr->_num_buffers_in_readers;
                ++_reader->_num_buffers_in_reader;
            }
            --_reader->_num_used_buffers;
            buffer->return_buffer();
            return false;
        }
        ++_reader->_num_ready_buffers;
        _ready_buffers.push_back(buffer);
        _eosr_queued = buffer->eosr();

        _blocked_on_queue = _ready_buffers.size() >= _ready_buffers_capacity;
        if (_blocked_on_queue && _ready_buffers_capacity > MIN_QUEUE_CAPACITY) {
            // We have filled the queue, indicating we need back pressure on
            // the producer side (i.e. we are pushing buffers faster than they
            // are pulled off, throttle this range more).
            --_ready_buffers_capacity;
        }
    }

    _buffer_ready_cv.notify_one();

    return _blocked_on_queue;
}

Status DiskIoMgr::ScanRange::get_next(BufferDescriptor** buffer) {
    *buffer = nullptr;

    {
        unique_lock<mutex> scan_range_lock(_lock);
        if (_eosr_returned) {
            return Status::OK();
        }
        DCHECK(validate()) << debug_string();

        if (_ready_buffers.empty()) {
            // The queue is empty indicating this thread could use more
            // IO. Increase the capacity to allow for more queueing.
            ++_ready_buffers_capacity;
            _ready_buffers_capacity = std::min(_ready_buffers_capacity, MAX_QUEUE_CAPACITY);
        }

        while (_ready_buffers.empty() && !_is_cancelled) {
            _buffer_ready_cv.wait(scan_range_lock);
        }

        if (_is_cancelled) {
            DCHECK(!_status.ok());
            return _status;
        }

        // Remove the first ready buffer from the queue and return it
        DCHECK(!_ready_buffers.empty());
        *buffer = _ready_buffers.front();
        _ready_buffers.pop_front();
        _eosr_returned = (*buffer)->eosr();
    }

    // Update tracking counters. The buffer has now moved from the IoMgr to the
    // caller.
    ++_io_mgr->_num_buffers_in_readers;
    ++_reader->_num_buffers_in_reader;
    --_reader->_num_ready_buffers;
    --_reader->_num_used_buffers;

    Status status = (*buffer)->_status;
    if (!status.ok()) {
        (*buffer)->return_buffer();
        *buffer = nullptr;
        return status;
    }

    unique_lock<mutex> reader_lock(_reader->_lock);
    if (_eosr_returned) {
        _reader->_total_range_queue_capacity += _ready_buffers_capacity;
        ++_reader->_num_finished_ranges;
        _reader->_initial_queue_capacity =
                _reader->_total_range_queue_capacity / _reader->_num_finished_ranges;
    }

    DCHECK(_reader->validate()) << endl << _reader->debug_string();
    if (_reader->_state == RequestContext::Cancelled) {
        _reader->_blocked_ranges.remove(this);
        cancel(_reader->_status);
        (*buffer)->return_buffer();
        *buffer = nullptr;
        return _status;
    }

    bool was_blocked = _blocked_on_queue;
    _blocked_on_queue = _ready_buffers.size() >= _ready_buffers_capacity;
    if (was_blocked && !_blocked_on_queue && !_eosr_queued) {
        // This scan range was blocked and is no longer, add it to the reader
        // queue again.
        _reader->_blocked_ranges.remove(this);
        _reader->schedule_scan_range(this);
    }
    return Status::OK();
}

void DiskIoMgr::ScanRange::cancel(const Status& status) {
    // Cancelling a range that was never started, ignore.
    if (_io_mgr == nullptr) {
        return;
    }

    DCHECK(!status.ok());
    {
        // Grab both locks to make sure that all working threads see _is_cancelled.
        unique_lock<mutex> scan_range_lock(_lock);
        unique_lock<mutex> hdfs_lock(_hdfs_lock);
        DCHECK(validate()) << debug_string();
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
        _status = status;
    }
    _buffer_ready_cv.notify_all();
    cleanup_queued_buffers();

    // For cached buffers, we can't close the range until the cached buffer is returned.
    // close() is called from DiskIoMgr::return_buffer().
    if (_cached_buffer == nullptr) {
        close();
    }
}

void DiskIoMgr::ScanRange::cleanup_queued_buffers() {
    DCHECK(_is_cancelled);
    _io_mgr->_num_buffers_in_readers += _ready_buffers.size();
    _reader->_num_buffers_in_reader += _ready_buffers.size();
    _reader->_num_used_buffers -= _ready_buffers.size();
    _reader->_num_ready_buffers -= _ready_buffers.size();

    while (!_ready_buffers.empty()) {
        BufferDescriptor* buffer = _ready_buffers.front();
        buffer->return_buffer();
        _ready_buffers.pop_front();
    }
}

string DiskIoMgr::ScanRange::debug_string() const {
    stringstream ss;
    ss << "file=" << _file << " disk_id=" << _disk_id << " offset=" << _offset << " len=" << _len
       << " bytes_read=" << _bytes_read << " buffer_queue=" << _ready_buffers.size()
       << " capacity=" << _ready_buffers_capacity << " hdfs_file=" << _hdfs_file;
    return ss.str();
}

bool DiskIoMgr::ScanRange::validate() {
    if (_bytes_read > _len) {
        LOG(WARNING) << "Bytes read tracking is wrong. Shouldn't read past the scan range."
                     << " _bytes_read=" << _bytes_read << " _len=" << _len;
        return false;
    }
    if (_eosr_returned && !_eosr_queued) {
        LOG(WARNING) << "Returned eosr to reader before finishing reading the scan range"
                     << " _eosr_returned=" << _eosr_returned << " _eosr_queued=" << _eosr_queued;
        return false;
    }
    return true;
}

DiskIoMgr::ScanRange::ScanRange(int capacity) : _ready_buffers_capacity(capacity) {
    _request_type = RequestType::READ;
    reset(nullptr, "", -1, -1, -1, false, false, NEVER_CACHE);
}

DiskIoMgr::ScanRange::~ScanRange() {
    DCHECK(_hdfs_file == nullptr) << "File was not closed.";
    DCHECK(_cached_buffer == nullptr) << "Cached buffer was not released.";
}

void DiskIoMgr::ScanRange::reset(hdfsFS fs, const char* file, int64_t len, int64_t offset,
                                 int disk_id, bool try_cache, bool expected_local, int64_t mtime,
                                 void* meta_data) {
    DCHECK(_ready_buffers.empty());
    _fs = fs;
    _file = file;
    _len = len;
    _offset = offset;
    _disk_id = disk_id;
    _try_cache = try_cache;
    _expected_local = expected_local;
    _meta_data = meta_data;
    _cached_buffer = nullptr;
    _io_mgr = nullptr;
    _reader = nullptr;
    _hdfs_file = nullptr;
    _mtime = mtime;
}

void DiskIoMgr::ScanRange::init_internal(DiskIoMgr* io_mgr, RequestContext* reader) {
    DCHECK(_hdfs_file == nullptr);
    _io_mgr = io_mgr;
    _reader = reader;
    _local_file = nullptr;
    _hdfs_file = nullptr;
    _bytes_read = 0;
    _is_cancelled = false;
    _eosr_queued = false;
    _eosr_returned = false;
    _blocked_on_queue = false;
    if (_ready_buffers_capacity <= 0) {
        _ready_buffers_capacity = reader->initial_scan_range_queue_capacity();
        DCHECK_GE(_ready_buffers_capacity, MIN_QUEUE_CAPACITY);
    }
    DCHECK(validate()) << debug_string();
}

Status DiskIoMgr::ScanRange::open() {
    unique_lock<mutex> hdfs_lock(_hdfs_lock);
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    // if (_fs != nullptr) {
    //     if (_hdfs_file != nullptr) {
    //         return Status::OK();
    //     }
    //     _hdfs_file = _io_mgr->OpenHdfsFile(_fs, file(), mtime());
    //     if (_hdfs_file == nullptr) {
    //         return Status::InternalError("GetHdfsErrorMsg("Failed to open HDFS file ", _file));
    //     }

    //     if (hdfsSeek(_fs, _hdfs_file->file(), _offset) != 0) {
    //         _io_mgr->cache_or_close_file_handle(file(), _hdfs_file, false);
    //         _hdfs_file = nullptr;
    //         string error_msg = GetHdfsErrorMsg("");
    //         stringstream ss;
    //         ss << "Error seeking to " << _offset << " in file: " << _file << " " << error_msg;
    //         return Status::InternalError(ss.str());
    //     }
    // } else {
    if (_local_file != nullptr) {
        return Status::OK();
    }

    _local_file = fopen(file(), "r");
    if (_local_file == nullptr) {
        string error_msg = get_str_err_msg();
        stringstream ss;
        ss << "Could not open file: " << _file << ": " << error_msg;
        return Status::InternalError(ss.str());
    }
    if (fseek(_local_file, _offset, SEEK_SET) == -1) {
        fclose(_local_file);
        _local_file = nullptr;
        string error_msg = get_str_err_msg();
        stringstream ss;
        ss << "Could not seek to " << _offset << " for file: " << _file << ": " << error_msg;
        return Status::InternalError(ss.str());
    }
    // }
    return Status::OK();
}

void DiskIoMgr::ScanRange::close() {
    unique_lock<mutex> hdfs_lock(_hdfs_lock);
    /*
 *   if (_fs != nullptr) {
 *     if (_hdfs_file == nullptr) return;
 *
 *     struct hdfsReadStatistics* stats;
 *     if (IsDfsPath(file())) {
 *       int success = hdfsFileGetReadStatistics(_hdfs_file->file(), &stats);
 *       if (success == 0) {
 *         _reader->_bytes_read_local += stats->totalLocalBytesRead;
 *         _reader->_bytes_read_short_circuit += stats->totalShortCircuitBytesRead;
 *         _reader->_bytes_read_dn_cache += stats->totalZeroCopyBytesRead;
 *         if (stats->totalLocalBytesRead != stats->totalBytesRead) {
 *           ++_reader->_num_remote_ranges;
 *           if (_expected_local) {
 *             int remote_bytes = stats->totalBytesRead - stats->totalLocalBytesRead;
 *             _reader->_unexpected_remote_bytes += remote_bytes;
 *             VLOG_FILE << "Unexpected remote HDFS read of "
 *                       << PrettyPrinter::Print(remote_bytes, TUnit::BYTES)
 *                       << " for file '" << _file << "'";
 *           }
 *         }
 *         hdfsFileFreeReadStatistics(stats);
 *       }
 *     }
 *     if (_cached_buffer != nullptr) {
 *       hadoopRzBufferFree(_hdfs_file->file(), _cached_buffer);
 *       _cached_buffer = nullptr;
 *     }
 *     _io_mgr->cache_or_close_file_handle(file(), _hdfs_file, false);
 *     VLOG_FILE << "Cache HDFS file handle file=" << file();
 *     _hdfs_file = nullptr;
 *   } else {
 */
    {
        if (_local_file == nullptr) {
            return;
        }
        fclose(_local_file);
        _local_file = nullptr;
    }
}

/*
 * int64_t DiskIoMgr::ScanRange::max_read_chunk_size() const {
 *     // S3 InputStreams don't support DIRECT_READ (i.e. java.nio.ByteBuffer read()
 *     // interface).  So, hdfsRead() needs to allocate a Java byte[] and copy the data out.
 *     // Profiles show that both the JNI array allocation and the memcpy adds much more
 *     // overhead for larger buffers, so limit the size of each read request.  128K was
 *     // chosen empirically by trying values between 4K and 8M and optimizing for lower CPU
 *     // utilization and higher S3 throughput.
 *     if (_disk_id == _io_mgr->RemoteS3DiskId()) {
 *         DCHECK(IsS3APath(file()));
 *         return 128 * 1024;
 *     }
 *     return numeric_limits<int64_t>::max();
 * }
 */

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status DiskIoMgr::ScanRange::read(char* buffer, int64_t* bytes_read, bool* eosr) {
    unique_lock<mutex> hdfs_lock(_hdfs_lock);
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    *eosr = false;
    *bytes_read = 0;
    // hdfsRead() length argument is an int.  Since _max_buffer_size type is no bigger
    // than an int, this min() will ensure that we don't overflow the length argument.
    DCHECK_LE(sizeof(_io_mgr->_max_buffer_size), sizeof(int));
    int bytes_to_read =
            std::min(static_cast<int64_t>(_io_mgr->_max_buffer_size), _len - _bytes_read);
    DCHECK_GE(bytes_to_read, 0);

    /*
     * if (_fs != nullptr) {
     *     DCHECK(_hdfs_file != nullptr);
     *     int64_t max_chunk_size = max_read_chunk_size();
     *     while (*bytes_read < bytes_to_read) {
     *         int chunk_size = min(bytes_to_read - *bytes_read, max_chunk_size);
     *         int last_read = hdfsRead(_fs, _hdfs_file->file(), buffer + *bytes_read, chunk_size);
     *         if (last_read == -1) {
     *             return Status::InternalError("GetHdfsErrorMsg("Error reading from HDFS file: ", _file));
     *         } else if (last_read == 0) {
     *             // No more bytes in the file. The scan range went past the end.
     *             *eosr = true;
     *             break;
     *         }
     *         *bytes_read += last_read;
     *     }
     * } else {
     */
    DCHECK(_local_file != nullptr);
    *bytes_read = fread(buffer, 1, bytes_to_read, _local_file);
    DCHECK_GE(*bytes_read, 0);
    DCHECK_LE(*bytes_read, bytes_to_read);
    if (*bytes_read < bytes_to_read) {
        if (ferror(_local_file) != 0) {
            string error_msg = get_str_err_msg();
            stringstream ss;
            ss << "Error reading from " << _file << " at byte offset: " << (_offset + _bytes_read)
               << ": " << error_msg;
            return Status::InternalError(ss.str());
        } else {
            // On Linux, we should only get partial reads from block devices on error or eof.
            DCHECK(feof(_local_file) != 0);
            *eosr = true;
        }
    }
    // }
    _bytes_read += *bytes_read;
    DCHECK_LE(_bytes_read, _len);
    if (_bytes_read == _len) {
        *eosr = true;
    }
    return Status::OK();
}

/*
 * Status DiskIoMgr::ScanRange::read_from_cache(bool* read_succeeded) {
 *   DCHECK(_try_cache);
 *   DCHECK_EQ(_bytes_read, 0);
 *   *read_succeeded = false;
 *   Status status = open();
 *   if (!status.ok()) return status;
 *
 *   // Cached reads not supported on local filesystem.
 *   if (_fs == nullptr) return Status::OK();
 *
 *   {
 *     unique_lock<mutex> hdfs_lock(_hdfs_lock);
 *     if (_is_cancelled) return Status::Cancelled("Cancelled");
 *
 *     DCHECK(_hdfs_file != nullptr);
 *     DCHECK(_cached_buffer == nullptr);
 *     _cached_buffer = hadoopReadZero(_hdfs_file->file(),
 *         _io_mgr->_cached_read_options, len());
 *
 *     // Data was not cached, caller will fall back to normal read path.
 *     if (_cached_buffer == nullptr) return Status::OK();
 *   }
 *
 *   // Cached read succeeded.
 *   void* buffer = const_cast<void*>(hadoopRzBufferGet(_cached_buffer));
 *   int32_t bytes_read = hadoopRzBufferLength(_cached_buffer);
 *   // For now, entire the entire block is cached or none of it.
 *   // TODO: if HDFS ever changes this, we'll have to handle the case where half
 *   // the block is cached.
 *   DCHECK_EQ(bytes_read, len());
 *
 *   // Create a single buffer desc for the entire scan range and enqueue that.
 *   BufferDescriptor* desc = _io_mgr->get_buffer_desc(
 *       _reader, this, reinterpret_cast<char*>(buffer), 0);
 *   desc->_len = bytes_read;
 *   desc->_scan_range_offset = 0;
 *   desc->_eosr = true;
 *   _bytes_read = bytes_read;
 *   enqueue_buffer(desc);
 *   if (_reader->_bytes_read_counter != nullptr) {
 *     COUNTER_ADD(_reader->_bytes_read_counter, bytes_read);
 *   }
 *   *read_succeeded = true;
 *   ++_reader->_num_used_buffers;
 *   return Status::OK();
 * }
 */
} // namespace doris
