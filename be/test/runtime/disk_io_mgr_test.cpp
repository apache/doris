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

#include "runtime/disk_io_mgr.h"

#include <gtest/gtest.h>
#include <sched.h>
#include <sys/stat.h>

#include <functional>
#include <thread>

#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/thread_group.h"

using std::string;
using std::stringstream;
using std::vector;
using std::list;

using std::lock_guard;
using std::unique_lock;
using std::mutex;
using std::mem_fn;
using std::condition_variable;
using std::unique_ptr;
using std::thread;

namespace doris {

const int MIN_BUFFER_SIZE = 512;
const int MAX_BUFFER_SIZE = 1024;
const int LARGE_MEM_LIMIT = 1024 * 1024 * 1024;

class DiskIoMgrTest : public testing::Test {
public:
    void write_validate_callback(int num_writes, DiskIoMgr::WriteRange** written_range,
                                 DiskIoMgr* io_mgr, DiskIoMgr::RequestContext* reader,
                                 int32_t* data, Status expected_status, const Status& status) {
        if (expected_status.code() == TStatusCode::CANCELLED) {
            EXPECT_TRUE(status.ok() || status.is_cancelled());
        } else {
            EXPECT_TRUE(status.code() == expected_status.code());
        }
        if (status.ok()) {
            DiskIoMgr::ScanRange* scan_range = _pool->add(new DiskIoMgr::ScanRange());
            scan_range->reset(nullptr, (*written_range)->file(), (*written_range)->len(),
                              (*written_range)->offset(), 0, false, false,
                              DiskIoMgr::ScanRange::NEVER_CACHE);
            validate_sync_read(io_mgr, reader, scan_range, reinterpret_cast<const char*>(data),
                               sizeof(int32_t));
        }

        {
            lock_guard<mutex> l(_written_mutex);
            ++_num_ranges_written;
            if (_num_ranges_written == num_writes) {
                _writes_done.notify_one();
            }
        }
    }

    void write_complete_callback(int num_writes, const Status& status) {
        EXPECT_TRUE(status.ok());
        {
            lock_guard<mutex> l(_written_mutex);
            ++_num_ranges_written;
            if (_num_ranges_written == num_writes) {
                _writes_done.notify_all();
            }
        }
    }

protected:
    void CreateTempFile(const char* filename, const char* data) {
        FILE* file = fopen(filename, "w");
        EXPECT_TRUE(file != nullptr);
        fwrite(data, 1, strlen(data), file);
        fclose(file);
    }

    int CreateTempFile(const char* filename, int file_size) {
        FILE* file = fopen(filename, "w");
        EXPECT_TRUE(file != nullptr);
        int success = fclose(file);
        if (success != 0) {
            LOG(ERROR) << "Error closing file " << filename;
            return success;
        }
        return truncate(filename, file_size);
    }

    // Validates that buffer[i] is \0 or expected[i]
    static void validate_empty_or_correct(const char* expected, const char* buffer, int len) {
        for (int i = 0; i < len; ++i) {
            if (buffer[i] != '\0') {
                EXPECT_EQ(expected[i], buffer[i]) << (int)expected[i] << " != " << (int)buffer[i];
            }
        }
    }

    static void validate_sync_read(DiskIoMgr* io_mgr, DiskIoMgr::RequestContext* reader,
                                   DiskIoMgr::ScanRange* range, const char* expected,
                                   int expected_len = -1) {
        DiskIoMgr::BufferDescriptor* buffer;
        Status status = io_mgr->read(reader, range, &buffer);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(buffer != nullptr);
        EXPECT_EQ(buffer->len(), range->len());
        if (expected_len < 0) {
            expected_len = strlen(expected);
        }
        int cmp = memcmp(buffer->buffer(), expected, expected_len);
        EXPECT_TRUE(cmp == 0);
        buffer->return_buffer();
    }

    static void validate_scan_range(DiskIoMgr::ScanRange* range, const char* expected,
                                    int expected_len, const Status& expected_status) {
        char result[expected_len + 1];
        memset(result, 0, expected_len + 1);

        while (true) {
            DiskIoMgr::BufferDescriptor* buffer = nullptr;
            Status status = range->get_next(&buffer);
            ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
            if (buffer == nullptr || !status.ok()) {
                if (buffer != nullptr) buffer->return_buffer();
                break;
            }
            ASSERT_LE(buffer->len(), expected_len);
            memcpy(result + range->offset() + buffer->scan_range_offset(), buffer->buffer(),
                   buffer->len());
            buffer->return_buffer();
        }
        validate_empty_or_correct(expected, result, expected_len);
    }

    // Continues pulling scan ranges from the io mgr until they are all done.
    // Updates num_ranges_processed with the number of ranges seen by this thread.
    static void scan_range_thread(DiskIoMgr* io_mgr, DiskIoMgr::RequestContext* reader,
                                  const char* expected_result, int expected_len,
                                  const Status& expected_status, int max_ranges,
                                  AtomicInt<int>* num_ranges_processed) {
        int num_ranges = 0;
        while (max_ranges == 0 || num_ranges < max_ranges) {
            DiskIoMgr::ScanRange* range;
            Status status = io_mgr->get_next_range(reader, &range);
            ASSERT_TRUE(status.ok() || status.code() == expected_status.code());
            if (range == nullptr) break;
            validate_scan_range(range, expected_result, expected_len, expected_status);
            ++(*num_ranges_processed);
            ++num_ranges;
        }
    }

    DiskIoMgr::ScanRange* init_range(int num_buffers, const char* file_path, int offset, int len,
                                     int disk_id, int64_t mtime, void* meta_data = nullptr,
                                     bool is_cached = false) {
        DiskIoMgr::ScanRange* range = _pool->add(new DiskIoMgr::ScanRange(num_buffers));
        range->reset(nullptr, file_path, len, offset, disk_id, is_cached, true, mtime, meta_data);
        EXPECT_EQ(mtime, range->mtime());
        return range;
    }

    std::unique_ptr<ObjectPool> _pool;

    mutex _written_mutex;
    condition_variable _writes_done;
    int _num_ranges_written;
};

// Test a single writer with multiple disks and threads per disk. Each WriteRange
// writes random 4-byte integers, and upon completion, the written data is validated
// by reading the data back via a separate IoMgr instance. All writes are expected to
// complete successfully.
TEST_F(DiskIoMgrTest, SingleWriter) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    _num_ranges_written = 0;
    string tmp_file = "/tmp/disk_io_mgr_test.txt";
    int num_ranges = 100;
    int64_t file_size = 1024 * 1024;
    int64_t cur_offset = 0;
    int success = CreateTempFile(tmp_file.c_str(), file_size);
    if (success != 0) {
        LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " << file_size;
        EXPECT_TRUE(false);
    }

    std::unique_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 10));
    std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    Status status = read_io_mgr->init(reader_mem_tracker);
    ASSERT_TRUE(status.ok());
    DiskIoMgr::RequestContext* reader;
    status = read_io_mgr->register_context(&reader, reader_mem_tracker);
    ASSERT_TRUE(status.ok());
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            _pool.reset(new ObjectPool);
            DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1, 10);
            status = io_mgr.init(mem_tracker);
            ASSERT_TRUE(status.ok());
            DiskIoMgr::RequestContext* writer;
            io_mgr.register_context(&writer, mem_tracker);
            for (int i = 0; i < num_ranges; ++i) {
                int32_t* data = _pool->add(new int32_t);
                *data = rand();
                DiskIoMgr::WriteRange** new_range = _pool->add(new DiskIoMgr::WriteRange*);
                DiskIoMgr::WriteRange::WriteDoneCallback callback =
                        bind(mem_fn(&DiskIoMgrTest::write_validate_callback), this, num_ranges,
                             new_range, read_io_mgr.get(), reader, data, Status::OK(), _1);
                *new_range = _pool->add(new DiskIoMgr::WriteRange(
                        tmp_file, cur_offset, num_ranges % num_disks, callback));
                (*new_range)->set_data(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
                Status add_status = io_mgr.add_write_range(writer, *new_range);
                EXPECT_TRUE(add_status.ok());
                cur_offset += sizeof(int32_t);
            }

            {
                unique_lock<mutex> lock(_written_mutex);
                while (_num_ranges_written < num_ranges) {
                    _writes_done.wait(lock);
                }
            }
            _num_ranges_written = 0;
            io_mgr.unregister_context(writer);
        }
    }

    read_io_mgr->unregister_context(reader);
    read_io_mgr.reset();
}

// Perform invalid writes (e.g. non-existent file, negative offset) and validate
// that an error status is returned via the write callback.
TEST_F(DiskIoMgrTest, InvalidWrite) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    _num_ranges_written = 0;
    string tmp_file = "/tmp/non-existent.txt";
    DiskIoMgr io_mgr(1, 1, 1, 10);
    Status status = io_mgr.init(mem_tracker);
    ASSERT_TRUE(status.ok());
    DiskIoMgr::RequestContext* writer;
    status = io_mgr.register_context(&writer);
    _pool.reset(new ObjectPool);
    int32_t* data = _pool->add(new int32_t);
    *data = rand();

    // Write to a non-existent file.
    DiskIoMgr::WriteRange** new_range = _pool->add(new DiskIoMgr::WriteRange*);
    DiskIoMgr::WriteRange::WriteDoneCallback callback =
            bind(mem_fn(&DiskIoMgrTest::write_validate_callback), this, 2, new_range,
                 (DiskIoMgr*)nullptr, (DiskIoMgr::RequestContext*)nullptr, data,
                 Status::InternalError("Test Failure"), _1);
    *new_range = _pool->add(new DiskIoMgr::WriteRange(tmp_file, rand(), 0, callback));

    (*new_range)->set_data(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    status = io_mgr.add_write_range(writer, *new_range);
    EXPECT_TRUE(status.ok());

    // Write to a bad location in a file that exists.
    tmp_file = "/tmp/disk_io_mgr_test.txt";
    int success = CreateTempFile(tmp_file.c_str(), 100);
    if (success != 0) {
        LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size 100";
        EXPECT_TRUE(false);
    }

    new_range = _pool->add(new DiskIoMgr::WriteRange*);
    callback = bind(mem_fn(&DiskIoMgrTest::write_validate_callback), this, 2, new_range,
                    (DiskIoMgr*)nullptr, (DiskIoMgr::RequestContext*)nullptr, data,
                    Status::InternalError("Test Failure"), _1);

    *new_range = _pool->add(new DiskIoMgr::WriteRange(tmp_file, -1, 0, callback));
    (*new_range)->set_data(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
    status = io_mgr.add_write_range(writer, *new_range);
    EXPECT_TRUE(status.ok());

    {
        unique_lock<mutex> lock(_written_mutex);
        while (_num_ranges_written < 2) {
            _writes_done.wait(lock);
        }
    }
    _num_ranges_written = 0;
    io_mgr.unregister_context(writer);
}

// Issue a number of writes, cancel the writer context and issue more writes.
// add_write_range() is expected to succeed before the cancel and fail after it.
// The writes themselves may finish with status cancelled or ok.
TEST_F(DiskIoMgrTest, SingleWriterCancel) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    _num_ranges_written = 0;
    string tmp_file = "/tmp/disk_io_mgr_test.txt";
    int num_ranges = 100;
    int num_ranges_before_cancel = 25;
    int64_t file_size = 1024 * 1024;
    int64_t cur_offset = 0;
    int success = CreateTempFile(tmp_file.c_str(), file_size);
    if (success != 0) {
        LOG(ERROR) << "Error creating temp file " << tmp_file.c_str() << " of size " << file_size;
        EXPECT_TRUE(false);
    }

    std::unique_ptr<DiskIoMgr> read_io_mgr(new DiskIoMgr(1, 1, 1, 10));
    std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    Status status = read_io_mgr->init(reader_mem_tracker);
    ASSERT_TRUE(status.ok());
    DiskIoMgr::RequestContext* reader;
    status = read_io_mgr->register_context(&reader, reader_mem_tracker);
    ASSERT_TRUE(status.ok());
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            _pool.reset(new ObjectPool);
            DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1, 10);
            status = io_mgr.init(mem_tracker);
            DiskIoMgr::RequestContext* writer;
            io_mgr.register_context(&writer, mem_tracker);
            Status validate_status = Status::OK();
            for (int i = 0; i < num_ranges; ++i) {
                if (i == num_ranges_before_cancel) {
                    io_mgr.cancel_context(writer);
                    validate_status = Status::Cancelled("");
                }
                int32_t* data = _pool->add(new int32_t);
                *data = rand();
                DiskIoMgr::WriteRange** new_range = _pool->add(new DiskIoMgr::WriteRange*);
                DiskIoMgr::WriteRange::WriteDoneCallback callback =
                        bind(mem_fn(&DiskIoMgrTest::write_validate_callback), this,
                             num_ranges_before_cancel, new_range, read_io_mgr.get(), reader, data,
                             Status::Cancelled(""), _1);
                *new_range = _pool->add(new DiskIoMgr::WriteRange(
                        tmp_file, cur_offset, num_ranges % num_disks, callback));
                (*new_range)->set_data(reinterpret_cast<uint8_t*>(data), sizeof(int32_t));
                cur_offset += sizeof(int32_t);
                Status add_status = io_mgr.add_write_range(writer, *new_range);
                EXPECT_TRUE(add_status.code() == validate_status.code());
            }

            {
                unique_lock<mutex> lock(_written_mutex);
                while (_num_ranges_written < num_ranges_before_cancel) {
                    _writes_done.wait(lock);
                }
            }
            _num_ranges_written = 0;
            io_mgr.unregister_context(writer);
        }
    }

    read_io_mgr->unregister_context(reader);
    read_io_mgr.reset();
}

// Basic test with a single reader, testing multiple threads, disks and a different
// number of buffers.
TEST_F(DiskIoMgrTest, SingleReader) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    int64_t iters = 0;
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
                for (int num_read_threads = 1; num_read_threads <= 5; ++num_read_threads) {
                    _pool.reset(new ObjectPool);
                    LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                              << " num_disk=" << num_disks << " num_buffers=" << num_buffers
                              << " num_read_threads=" << num_read_threads;

                    if (++iters % 5000 == 0) {
                        LOG(ERROR) << "Starting iteration " << iters;
                    }
                    DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1, 1);

                    Status status = io_mgr.init(mem_tracker);
                    ASSERT_TRUE(status.ok());
                    std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
                    DiskIoMgr::RequestContext* reader;
                    status = io_mgr.register_context(&reader, reader_mem_tracker);
                    ASSERT_TRUE(status.ok());

                    std::vector<DiskIoMgr::ScanRange*> ranges;
                    for (int i = 0; i < len; ++i) {
                        int disk_id = i % num_disks;
                        ranges.push_back(init_range(num_buffers, tmp_file, 0, len, disk_id,
                                                    stat_val.st_mtime));
                    }
                    status = io_mgr.add_scan_ranges(reader, ranges);
                    ASSERT_TRUE(status.ok());

                    AtomicInt<int> num_ranges_processed;
                    ThreadGroup threads;
                    for (int i = 0; i < num_read_threads; ++i) {
                        threads.add_thread(new thread(scan_range_thread, &io_mgr, reader, data, len,
                                                      Status::OK(), 0, &num_ranges_processed));
                    }
                    threads.join_all();

                    EXPECT_EQ(num_ranges_processed, ranges.size());
                    io_mgr.unregister_context(reader);
                    EXPECT_EQ(reader_mem_tracker->consumption(), 0);
                }
            }
        }
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

// This test issues adding additional scan ranges while there are some still in flight.
TEST_F(DiskIoMgrTest, AddScanRangeTest) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    int64_t iters = 0;
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
                _pool.reset(new ObjectPool);
                LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                          << " num_disk=" << num_disks << " num_buffers=" << num_buffers;

                if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
                DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1, 1);

                Status status = io_mgr.init(mem_tracker);
                ASSERT_TRUE(status.ok());
                std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
                DiskIoMgr::RequestContext* reader;
                status = io_mgr.register_context(&reader, reader_mem_tracker);
                ASSERT_TRUE(status.ok());

                std::vector<DiskIoMgr::ScanRange*> ranges_first_half;
                std::vector<DiskIoMgr::ScanRange*> ranges_second_half;
                for (int i = 0; i < len; ++i) {
                    int disk_id = i % num_disks;
                    if (i > len / 2) {
                        ranges_second_half.push_back(init_range(num_buffers, tmp_file, i, 1,
                                                                disk_id, stat_val.st_mtime));
                    } else {
                        ranges_first_half.push_back(init_range(num_buffers, tmp_file, i, 1, disk_id,
                                                               stat_val.st_mtime));
                    }
                }
                AtomicInt<int> num_ranges_processed;

                // Issue first half the scan ranges.
                status = io_mgr.add_scan_ranges(reader, ranges_first_half);
                ASSERT_TRUE(status.ok());

                // Read a couple of them
                scan_range_thread(&io_mgr, reader, data, strlen(data), Status::OK(), 2,
                                  &num_ranges_processed);

                // Issue second half
                status = io_mgr.add_scan_ranges(reader, ranges_second_half);
                ASSERT_TRUE(status.ok());

                // Start up some threads and then cancel
                ThreadGroup threads;
                for (int i = 0; i < 3; ++i) {
                    threads.add_thread(new thread(scan_range_thread, &io_mgr, reader, data,
                                                  strlen(data), Status::Cancelled(""), 0,
                                                  &num_ranges_processed));
                }

                threads.join_all();
                EXPECT_EQ(num_ranges_processed, len);
                io_mgr.unregister_context(reader);
                EXPECT_EQ(reader_mem_tracker->consumption(), 0);
            }
        }
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

// Test to make sure that sync reads and async reads work together
// Note: this test is constructed so the number of buffers is greater than the
// number of scan ranges.
TEST_F(DiskIoMgrTest, SyncReadTest) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    int64_t iters = 0;
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
                _pool.reset(new ObjectPool);
                LOG(INFO) << "Starting SyncReadTest test with num_threads_per_disk="
                          << num_threads_per_disk << " num_disk=" << num_disks
                          << " num_buffers=" << num_buffers;

                if (++iters % 5000 == 0) {
                    LOG(ERROR) << "Starting iteration " << iters;
                }
                DiskIoMgr io_mgr(num_disks, num_threads_per_disk, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

                Status status = io_mgr.init(mem_tracker);
                ASSERT_TRUE(status.ok());
                std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
                DiskIoMgr::RequestContext* reader;
                status = io_mgr.register_context(&reader, reader_mem_tracker);
                ASSERT_TRUE(status.ok());

                DiskIoMgr::ScanRange* complete_range =
                        init_range(1, tmp_file, 0, strlen(data), 0, stat_val.st_mtime);

                // Issue some reads before the async ones are issued
                validate_sync_read(&io_mgr, reader, complete_range, data);
                validate_sync_read(&io_mgr, reader, complete_range, data);

                std::vector<DiskIoMgr::ScanRange*> ranges;
                for (int i = 0; i < len; ++i) {
                    int disk_id = i % num_disks;
                    ranges.push_back(
                            init_range(num_buffers, tmp_file, 0, len, disk_id, stat_val.st_mtime));
                }
                status = io_mgr.add_scan_ranges(reader, ranges);
                ASSERT_TRUE(status.ok());

                AtomicInt<int> num_ranges_processed;
                ThreadGroup threads;
                for (int i = 0; i < 5; ++i) {
                    threads.add_thread(new thread(scan_range_thread, &io_mgr, reader, data,
                                                  strlen(data), Status::OK(), 0,
                                                  &num_ranges_processed));
                }

                // Issue some more sync ranges
                for (int i = 0; i < 5; ++i) {
                    sched_yield();
                    validate_sync_read(&io_mgr, reader, complete_range, data);
                }

                threads.join_all();

                validate_sync_read(&io_mgr, reader, complete_range, data);
                validate_sync_read(&io_mgr, reader, complete_range, data);

                EXPECT_EQ(num_ranges_processed, ranges.size());
                io_mgr.unregister_context(reader);
                EXPECT_EQ(reader_mem_tracker->consumption(), 0);
            }
        }
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

// Tests a single reader cancelling half way through scan ranges.
TEST_F(DiskIoMgrTest, SingleReaderCancel) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    int64_t iters = 0;
    for (int num_threads_per_disk = 1; num_threads_per_disk <= 5; ++num_threads_per_disk) {
        for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
            for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
                _pool.reset(new ObjectPool);
                LOG(INFO) << "Starting test with num_threads_per_disk=" << num_threads_per_disk
                          << " num_disk=" << num_disks << " num_buffers=" << num_buffers;

                if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
                DiskIoMgr io_mgr(num_disks, num_threads_per_disk, 1, 1);

                Status status = io_mgr.init(mem_tracker);
                ASSERT_TRUE(status.ok());
                std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
                DiskIoMgr::RequestContext* reader;
                status = io_mgr.register_context(&reader, reader_mem_tracker);
                ASSERT_TRUE(status.ok());

                std::vector<DiskIoMgr::ScanRange*> ranges;
                for (int i = 0; i < len; ++i) {
                    int disk_id = i % num_disks;
                    ranges.push_back(
                            init_range(num_buffers, tmp_file, 0, len, disk_id, stat_val.st_mtime));
                }
                status = io_mgr.add_scan_ranges(reader, ranges);
                ASSERT_TRUE(status.ok());

                AtomicInt<int> num_ranges_processed;
                int num_succesful_ranges = ranges.size() / 2;
                // Read half the ranges
                for (int i = 0; i < num_succesful_ranges; ++i) {
                    scan_range_thread(&io_mgr, reader, data, strlen(data), Status::OK(), 1,
                                      &num_ranges_processed);
                }
                EXPECT_EQ(num_ranges_processed, num_succesful_ranges);

                // Start up some threads and then cancel
                ThreadGroup threads;
                for (int i = 0; i < 3; ++i) {
                    threads.add_thread(new thread(scan_range_thread, &io_mgr, reader, data,
                                                  strlen(data), Status::Cancelled(""), 0,
                                                  &num_ranges_processed));
                }

                io_mgr.cancel_context(reader);
                sched_yield();

                threads.join_all();
                EXPECT_TRUE(io_mgr.context_status(reader).is_cancelled());
                io_mgr.unregister_context(reader);
                EXPECT_EQ(reader_mem_tracker->consumption(), 0);
            }
        }
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

// Test when the reader goes over the mem limit
TEST_F(DiskIoMgrTest, MemTrackers) {
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    const int num_buffers = 25;
    // Give the reader more buffers than the limit
    const int mem_limit_num_buffers = 2;

    int64_t iters = 0;
    {
        _pool.reset(new ObjectPool);
        if (++iters % 1000 == 0) {
            LOG(ERROR) << "Starting iteration " << iters;
        }

        std::shared_ptr<MemTracker> mem_tracker(
                new MemTracker(mem_limit_num_buffers * MAX_BUFFER_SIZE));
        DiskIoMgr io_mgr(1, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

        Status status = io_mgr.init(mem_tracker);
        ASSERT_TRUE(status.ok());
        std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
        DiskIoMgr::RequestContext* reader;
        status = io_mgr.register_context(&reader, reader_mem_tracker);
        ASSERT_TRUE(status.ok());

        std::vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < num_buffers; ++i) {
            ranges.push_back(init_range(num_buffers, tmp_file, 0, len, 0, stat_val.st_mtime));
        }
        status = io_mgr.add_scan_ranges(reader, ranges);
        ASSERT_TRUE(status.ok());

        // Don't return buffers to force memory pressure
        std::vector<DiskIoMgr::BufferDescriptor*> buffers;

        AtomicInt<int> num_ranges_processed;
        scan_range_thread(&io_mgr, reader, data, strlen(data), Status::MemoryLimitExceeded("Mem"),
                          1, &num_ranges_processed);

        char result[strlen(data) + 1];
        // Keep reading new ranges without returning buffers. This forces us
        // to go over the limit eventually.
        while (true) {
            memset(result, 0, strlen(data) + 1);
            DiskIoMgr::ScanRange* range = nullptr;
            status = io_mgr.get_next_range(reader, &range);
            ASSERT_TRUE(status.ok() || status.is_mem_limit_exceeded());
            if (range == nullptr) break;

            while (true) {
                DiskIoMgr::BufferDescriptor* buffer = nullptr;
                Status status = range->get_next(&buffer);
                ASSERT_TRUE(status.ok() || status.is_mem_limit_exceeded());
                if (buffer == nullptr) break;
                memcpy(result + range->offset() + buffer->scan_range_offset(), buffer->buffer(),
                       buffer->len());
                buffers.push_back(buffer);
            }
            validate_empty_or_correct(data, result, strlen(data));
        }

        for (int i = 0; i < buffers.size(); ++i) {
            buffers[i]->return_buffer();
        }

        EXPECT_TRUE(io_mgr.context_status(reader).is_mem_limit_exceeded());
        io_mgr.unregister_context(reader);
        EXPECT_EQ(reader_mem_tracker->consumption(), 0);
    }
}
#if 0
// Test when some scan ranges are marked as being cached.
// Since these files are not in HDFS, the cached path always fails so this
// only tests the fallback mechanism.
// TODO: we can fake the cached read path without HDFS
TEST_F(DiskIoMgrTest, CachedReads) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "abcdefghijklm";
    int len = strlen(data);
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    const int num_disks = 2;
    const int num_buffers = 3;

    int64_t iters = 0;
    {
        _pool.reset(new ObjectPool);
        if (++iters % 5000 == 0) LOG(ERROR) << "Starting iteration " << iters;
        DiskIoMgr io_mgr(num_disks, 1, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

        Status status = io_mgr.init(mem_tracker);
        ASSERT_TRUE(status.ok());
        std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker());
        DiskIoMgr::RequestContext* reader;
        status = io_mgr.register_context(&reader, reader_mem_tracker);
        ASSERT_TRUE(status.ok());

        DiskIoMgr::ScanRange* complete_range =
            init_range(1, tmp_file, 0, strlen(data), 0, stat_val.st_mtime, nullptr, true);

        // Issue some reads before the async ones are issued
        validate_sync_read(&io_mgr, reader, complete_range, data);
        validate_sync_read(&io_mgr, reader, complete_range, data);

        std::vector<DiskIoMgr::ScanRange*> ranges;
        for (int i = 0; i < len; ++i) {
            int disk_id = i % num_disks;
            ranges.push_back(init_range(num_buffers, tmp_file, 0, len, disk_id,
                        stat_val.st_mtime, nullptr, true));
        }
        status = io_mgr.add_scan_ranges(reader, ranges);
        ASSERT_TRUE(status.ok());

        AtomicInt<int> num_ranges_processed;
        ThreadGroup threads;
        for (int i = 0; i < 5; ++i) {
            threads.add_thread(new thread(scan_range_thread, &io_mgr, reader, data,
                        strlen(data), Status::OK(), 0, &num_ranges_processed));
        }

        // Issue some more sync ranges
        for (int i = 0; i < 5; ++i) {
            sched_yield();
            validate_sync_read(&io_mgr, reader, complete_range, data);
        }

        threads.join_all();

        validate_sync_read(&io_mgr, reader, complete_range, data);
        validate_sync_read(&io_mgr, reader, complete_range, data);

        EXPECT_EQ(num_ranges_processed, ranges.size());
        io_mgr.unregister_context(reader);
        EXPECT_EQ(reader_mem_tracker->consumption(), 0);
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}
#endif // end #if 0

TEST_F(DiskIoMgrTest, MultipleReaderWriter) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const int ITERATIONS = 1;
    const char* data = "abcdefghijklmnopqrstuvwxyz";
    const int num_contexts = 5;
    const int file_size = 4 * 1024;
    const int num_writes_queued = 5;
    const int num_reads_queued = 5;

    string file_name = "/tmp/disk_io_mgr_test.txt";
    int success = CreateTempFile(file_name.c_str(), file_size);
    if (success != 0) {
        LOG(ERROR) << "Error creating temp file " << file_name.c_str() << " of size " << file_size;
        ASSERT_TRUE(false);
    }

    // Get mtime for file
    struct stat stat_val;
    stat(file_name.c_str(), &stat_val);

    int64_t iters = 0;
    std::vector<DiskIoMgr::RequestContext*> contexts(num_contexts);
    Status status;
    for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
        for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
            for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
                DiskIoMgr io_mgr(num_disks, threads_per_disk, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
                io_mgr.init(mem_tracker);
                for (int file_index = 0; file_index < num_contexts; ++file_index) {
                    status = io_mgr.register_context(&contexts[file_index]);
                    ASSERT_TRUE(status.ok());
                }
                _pool.reset(new ObjectPool);
                int read_offset = 0;
                int write_offset = 0;
                while (read_offset < file_size) {
                    for (int context_index = 0; context_index < num_contexts; ++context_index) {
                        if (++iters % 5000 == 0) {
                            LOG(ERROR) << "Starting iteration " << iters;
                        }
                        AtomicInt<int> num_ranges_processed;
                        ThreadGroup threads;
                        std::vector<DiskIoMgr::ScanRange*> ranges;
                        int num_scan_ranges =
                                std::min<int>(num_reads_queued, write_offset - read_offset);
                        for (int i = 0; i < num_scan_ranges; ++i) {
                            ranges.push_back(init_range(1, file_name.c_str(), read_offset, 1,
                                                        i % num_disks, stat_val.st_mtime));
                            threads.add_thread(new thread(
                                    scan_range_thread, &io_mgr, contexts[context_index],
                                    reinterpret_cast<const char*>(data +
                                                                  (read_offset % strlen(data))),
                                    1, Status::OK(), num_scan_ranges, &num_ranges_processed));
                            ++read_offset;
                        }

                        _num_ranges_written = 0;
                        int num_write_ranges =
                                std::min<int>(num_writes_queued, file_size - write_offset);
                        for (int i = 0; i < num_write_ranges; ++i) {
                            DiskIoMgr::WriteRange::WriteDoneCallback callback =
                                    bind(mem_fn(&DiskIoMgrTest::write_complete_callback), this,
                                         num_write_ranges, _1);
                            DiskIoMgr::WriteRange* new_range = _pool->add(new DiskIoMgr::WriteRange(
                                    file_name, write_offset, i % num_disks, callback));
                            new_range->set_data(reinterpret_cast<const uint8_t*>(
                                                        data + (write_offset % strlen(data))),
                                                1);
                            status = io_mgr.add_write_range(contexts[context_index], new_range);
                            ++write_offset;
                        }

                        {
                            unique_lock<mutex> lock(_written_mutex);
                            while (_num_ranges_written < num_write_ranges) {
                                _writes_done.wait(lock);
                            }
                        }

                        threads.join_all();
                    } // for (int context_index
                }     // while (read_offset < file_size)

                for (int file_index = 0; file_index < num_contexts; ++file_index) {
                    io_mgr.unregister_context(contexts[file_index]);
                }
            } // for (int num_disks
        }     // for (int threads_per_disk
    }         // for (int iteration
}

// This test will test multiple concurrent reads each reading a different file.
TEST_F(DiskIoMgrTest, MultipleReader) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const int NUM_READERS = 5;
    const int DATA_LEN = 50;
    const int ITERATIONS = 25;
    const int NUM_THREADS_PER_READER = 3;

    std::vector<string> file_names;
    std::vector<int64_t> mtimes;
    std::vector<string> data;
    std::vector<DiskIoMgr::RequestContext*> readers;
    std::vector<char*> results;

    file_names.resize(NUM_READERS);
    readers.resize(NUM_READERS);
    mtimes.resize(NUM_READERS);
    data.resize(NUM_READERS);
    results.resize(NUM_READERS);

    // Initialize data for each reader.  The data will be
    // 'abcd...' for reader one, 'bcde...' for reader two (wrapping around at 'z')
    for (int i = 0; i < NUM_READERS; ++i) {
        char buf[DATA_LEN];
        for (int j = 0; j < DATA_LEN; ++j) {
            int c = (j + i) % 26;
            buf[j] = 'a' + c;
        }
        data[i] = string(buf, DATA_LEN);

        std::stringstream ss;
        ss << "/tmp/disk_io_mgr_test" << i << ".txt";
        file_names[i] = ss.str();
        CreateTempFile(ss.str().c_str(), data[i].c_str());

        // Get mtime for file
        struct stat stat_val;
        stat(file_names[i].c_str(), &stat_val);
        mtimes[i] = stat_val.st_mtime;

        results[i] = new char[DATA_LEN + 1];
        memset(results[i], 0, DATA_LEN + 1);
    }

    // This exercises concurrency, run the test multiple times
    int64_t iters = 0;
    for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
        for (int threads_per_disk = 1; threads_per_disk <= 5; ++threads_per_disk) {
            for (int num_disks = 1; num_disks <= 5; num_disks += 2) {
                for (int num_buffers = 1; num_buffers <= 5; ++num_buffers) {
                    _pool.reset(new ObjectPool);
                    LOG(INFO) << "Starting test with num_threads_per_disk=" << threads_per_disk
                              << " num_disk=" << num_disks << " num_buffers=" << num_buffers;
                    if (++iters % 2500 == 0) LOG(ERROR) << "Starting iteration " << iters;

                    DiskIoMgr io_mgr(num_disks, threads_per_disk, MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);
                    Status status = io_mgr.init(mem_tracker);
                    ASSERT_TRUE(status.ok());

                    for (int i = 0; i < NUM_READERS; ++i) {
                        status = io_mgr.register_context(&readers[i], nullptr);
                        ASSERT_TRUE(status.ok());

                        std::vector<DiskIoMgr::ScanRange*> ranges;
                        for (int j = 0; j < DATA_LEN; ++j) {
                            int disk_id = j % num_disks;
                            ranges.push_back(init_range(num_buffers, file_names[i].c_str(), j, 1,
                                                        disk_id, mtimes[i]));
                        }
                        status = io_mgr.add_scan_ranges(readers[i], ranges);
                        ASSERT_TRUE(status.ok());
                    }

                    AtomicInt<int> num_ranges_processed;
                    ThreadGroup threads;
                    for (int i = 0; i < NUM_READERS; ++i) {
                        for (int j = 0; j < NUM_THREADS_PER_READER; ++j) {
                            threads.add_thread(new thread(scan_range_thread, &io_mgr, readers[i],
                                                          data[i].c_str(), data[i].size(),
                                                          Status::OK(), 0, &num_ranges_processed));
                        }
                    }
                    threads.join_all();
                    EXPECT_EQ(num_ranges_processed, DATA_LEN * NUM_READERS);
                    for (int i = 0; i < NUM_READERS; ++i) {
                        io_mgr.unregister_context(readers[i]);
                    }
                }
            }
        }
    }
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

#if 0
// Stress test for multiple clients with cancellation
// TODO: the stress app should be expanded to include sync reads and adding scan
// ranges in the middle.
TEST_F(DiskIoMgrTest, StressTest) {
  // Run the test with 5 disks, 5 threads per disk, 10 clients and with cancellation
  DiskIoMgrStress test(5, 5, 10, true);
  test.Run(2); // In seconds
}
#endif

TEST_F(DiskIoMgrTest, Buffers) {
    // Test default min/max buffer size
    int min_buffer_size = 1024;
    int max_buffer_size = 8 * 1024 * 1024; // 8 MB
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(max_buffer_size * 2));

    DiskIoMgr io_mgr(1, 1, min_buffer_size, max_buffer_size);
    Status status = io_mgr.init(mem_tracker);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(mem_tracker->consumption(), 0);

    // buffer length should be rounded up to min buffer size
    int64_t buffer_len = 1;
    char* buf = io_mgr.get_free_buffer(&buffer_len);
    EXPECT_EQ(buffer_len, min_buffer_size);
    EXPECT_EQ(io_mgr._num_allocated_buffers, 1);
    io_mgr.return_free_buffer(buf, buffer_len);
    EXPECT_EQ(mem_tracker->consumption(), min_buffer_size);

    // reuse buffer
    buffer_len = min_buffer_size;
    buf = io_mgr.get_free_buffer(&buffer_len);
    EXPECT_EQ(buffer_len, min_buffer_size);
    EXPECT_EQ(io_mgr._num_allocated_buffers, 1);
    io_mgr.return_free_buffer(buf, buffer_len);
    EXPECT_EQ(mem_tracker->consumption(), min_buffer_size);

    // bump up to next buffer size
    buffer_len = min_buffer_size + 1;
    buf = io_mgr.get_free_buffer(&buffer_len);
    EXPECT_EQ(buffer_len, min_buffer_size * 2);
    EXPECT_EQ(io_mgr._num_allocated_buffers, 2);
    EXPECT_EQ(mem_tracker->consumption(), min_buffer_size * 3);

    // gc unused buffer
    io_mgr.gc_io_buffers();
    EXPECT_EQ(io_mgr._num_allocated_buffers, 1);
    EXPECT_EQ(mem_tracker->consumption(), min_buffer_size * 2);

    io_mgr.return_free_buffer(buf, buffer_len);

    // max buffer size
    buffer_len = max_buffer_size;
    buf = io_mgr.get_free_buffer(&buffer_len);
    EXPECT_EQ(buffer_len, max_buffer_size);
    EXPECT_EQ(io_mgr._num_allocated_buffers, 2);
    io_mgr.return_free_buffer(buf, buffer_len);
    EXPECT_EQ(mem_tracker->consumption(), min_buffer_size * 2 + max_buffer_size);

    // gc buffers
    io_mgr.gc_io_buffers();
    EXPECT_EQ(io_mgr._num_allocated_buffers, 0);
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

// IMPALA-2366: handle partial read where range goes past end of file.
TEST_F(DiskIoMgrTest, PartialRead) {
    std::shared_ptr<MemTracker> mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    const char* tmp_file = "/tmp/disk_io_mgr_test.txt";
    const char* data = "the quick brown fox jumped over the lazy dog";
    int len = strlen(data);
    int read_len = len + 1000; // Read past end of file.
    CreateTempFile(tmp_file, data);

    // Get mtime for file
    struct stat stat_val;
    stat(tmp_file, &stat_val);

    _pool.reset(new ObjectPool);
    std::unique_ptr<DiskIoMgr> io_mgr(new DiskIoMgr(1, 1, read_len, read_len));

    Status status = io_mgr->init(mem_tracker);
    ASSERT_TRUE(status.ok());
    std::shared_ptr<MemTracker> reader_mem_tracker(new MemTracker(LARGE_MEM_LIMIT));
    DiskIoMgr::RequestContext* reader;
    status = io_mgr->register_context(&reader, reader_mem_tracker);
    ASSERT_TRUE(status.ok());

    // We should not read past the end of file.
    DiskIoMgr::ScanRange* range = init_range(1, tmp_file, 0, read_len, 0, stat_val.st_mtime);
    DiskIoMgr::BufferDescriptor* buffer;
    status = io_mgr->read(reader, range, &buffer);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(buffer->eosr());
    ASSERT_EQ(len, buffer->len());
    ASSERT_TRUE(memcmp(buffer->buffer(), data, len) == 0);
    buffer->return_buffer();

    io_mgr->unregister_context(reader);
    _pool.reset();
    io_mgr.reset();
    EXPECT_EQ(reader_mem_tracker->consumption(), 0);
    EXPECT_EQ(mem_tracker->consumption(), 0);
}

} // end namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. \n");
    //     return -1;
    // }
    doris::config::query_scratch_dirs = "/tmp";
    doris::config::max_free_io_buffers = 128;
    doris::config::disable_mem_pools = false;
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();

    return RUN_ALL_TESTS();
}
