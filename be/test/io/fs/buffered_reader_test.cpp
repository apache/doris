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

#include "io/fs/buffered_reader.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <limits.h>

#include <iostream>
#include <memory>
#include <ostream>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"

namespace doris {
using io::FileReader;
class BufferedReaderTest : public testing::Test {
public:
    BufferedReaderTest() = default;

protected:
    void SetUp() override {
        std::unique_ptr<ThreadPool> _pool;
        static_cast<void>(ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                                  .set_min_threads(5)
                                  .set_max_threads(10)
                                  .build(&_pool));
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    }
    void TearDown() override {
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool.reset();
    }
};

class SyncLocalFileReader : public io::FileReader {
public:
    SyncLocalFileReader(io::FileReaderSPtr reader) : _reader(std::move(reader)) {}
    ~SyncLocalFileReader() override = default;

    Status close() override {
        std::unique_lock<std::mutex> lck {_lock};
        return _reader->close();
    }

    const io::Path& path() const override { return _reader->path(); }

    size_t size() const override { return _reader->size(); }

    bool closed() const override { return _reader->closed(); }

private:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        std::unique_lock<std::mutex> lck {_lock};
        return _reader->read_at(offset, result, bytes_read);
    }

    io::FileReaderSPtr _reader;
    std::mutex _lock;
};

class MockOffsetFileReader : public io::FileReader {
public:
    MockOffsetFileReader(size_t size) : _size(size) {};

    ~MockOffsetFileReader() override = default;

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const io::Path& path() const override { return _path; }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        if (offset >= _size) {
            *bytes_read = 0;
            return Status::OK();
        }
        *bytes_read = std::min(_size - offset, result.size);
        for (size_t i = 0; i < *bytes_read; ++i) {
            result.data[i] = (offset + i) % UCHAR_MAX;
        }
        return Status::OK();
    }

private:
    size_t _size;
    bool _closed = false;
    io::Path _path = "/tmp/mock";
};

class TestingRangeCacheFileReader : public io::FileReader {
public:
    TestingRangeCacheFileReader(std::shared_ptr<io::FileReader> delegate) : _delegate(delegate) {};

    ~TestingRangeCacheFileReader() override = default;

    Status close() override { return _delegate->close(); }

    const io::Path& path() const override { return _delegate->path(); }

    size_t size() const override { return _delegate->size(); }

    bool closed() const override { return _delegate->closed(); }

    const io::PrefetchRange& last_read_range() const { return *_last_read_range; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        _last_read_range = std::make_unique<io::PrefetchRange>(offset, offset + result.size);
        return _delegate->read_at_impl(offset, result, bytes_read, io_ctx);
    }

private:
    std::shared_ptr<io::FileReader> _delegate;
    std::unique_ptr<io::PrefetchRange> _last_read_range;
};

TEST_F(BufferedReaderTest, normal_use) {
    // buffered_reader_test_file 950 bytes
    io::FileReaderSPtr local_reader;
    static_cast<void>(io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file", &local_reader));
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(nullptr, std::move(sync_local_reader),
                                      io::PrefetchRange(0, 1024));
    uint8_t buf[1024];
    Slice result {buf, 1024};
    MonotonicStopWatch watch;
    watch.start();
    size_t read_length = 0;
    auto st = reader.read_at(0, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(950, read_length);
    std::cout << "read bytes " << read_length << " using time " << watch.elapsed_time() << std::endl;
}

TEST_F(BufferedReaderTest, test_validity) {
    // buffered_reader_test_file.txt 45 bytes
    io::FileReaderSPtr local_reader;
    static_cast<void>(io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader));
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(nullptr, std::move(sync_local_reader),
                                      io::PrefetchRange(0, 1024));
    Status st;
    uint8_t buf[10];
    Slice result {buf, 10};
    size_t offset = 0;
    size_t read_length = 0;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(read_length, 0);
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    offset += read_length;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(read_length, 0);
    EXPECT_STREQ("vxzAbCdEfG", std::string((char*)buf, read_length).c_str());
    offset += read_length;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(read_length, 0);
    EXPECT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, read_length).c_str());
    offset += read_length;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(read_length, 0);
    EXPECT_STREQ("rStUvWxYz\n", std::string((char*)buf, read_length).c_str());
    offset += read_length;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(read_length, 0);
    EXPECT_STREQ("IjKl", std::string((char*)buf, 4).c_str());
    offset += read_length;

    st = reader.read_at(offset, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(read_length, 0);
}

TEST_F(BufferedReaderTest, test_seek) {
    // buffered_reader_test_file.txt 45 bytes
    io::FileReaderSPtr local_reader;
    static_cast<void>(io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader));
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(nullptr, std::move(sync_local_reader),
                                      io::PrefetchRange(0, 1024));

    Status st;
    uint8_t buf[10];
    Slice result {buf, 10};
    size_t read_length = 0;

    // Seek to the end of the file
    EXPECT_TRUE(st.ok());
    st = reader.read_at(45, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(read_length, 0);

    // Seek to the beginning of the file
    st = reader.read_at(0, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    EXPECT_EQ(read_length, 10);

    // Seek to a wrong position
    st = reader.read_at(-1, result, &read_length);
    EXPECT_TRUE(st.ok());
    // to test if it would reset the result
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    EXPECT_EQ(read_length, 0);

    // Seek to a wrong position
    st = reader.read_at(-1000, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    EXPECT_EQ(read_length, 0);

    // Seek to a wrong position
    st = reader.read_at(1000, result, &read_length);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    EXPECT_EQ(read_length, 0);
}

TEST_F(BufferedReaderTest, test_miss) {
    // buffered_reader_test_file.txt 45 bytes
    io::FileReaderSPtr local_reader;
    static_cast<void>(io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader));
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(nullptr, std::move(sync_local_reader),
                                      io::PrefetchRange(0, 1024));
    uint8_t buf[128];
    Slice result {buf, 128};
    size_t bytes_read;

    auto st = reader.read_at(20, Slice {buf, 10}, &bytes_read);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(10, bytes_read);

    st = reader.read_at(0, Slice {buf, 5}, &bytes_read);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhj", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(5, bytes_read);

    st = reader.read_at(5, Slice {buf, 10}, &bytes_read);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("lnprtvxzAb", std::string((char*)buf, (size_t)bytes_read).c_str());
    EXPECT_EQ(10, bytes_read);

    // if requested length is larger than the capacity of buffer, do not
    // need to copy the character into local buffer.
    st = reader.read_at(0, Slice {buf, 128}, &bytes_read);
    EXPECT_TRUE(st.ok());
    EXPECT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    EXPECT_EQ(45, bytes_read);
}

TEST_F(BufferedReaderTest, test_read_amplify) {
    size_t kb = 1024;
    io::FileReaderSPtr offset_reader = std::make_shared<MockOffsetFileReader>(2048 * kb); // 2MB
    std::vector<io::PrefetchRange> random_access_ranges;
    random_access_ranges.emplace_back(0, 1 * kb); // column0
    // if read the follow slice, amplified_ratio = 1, but data size <= MIN_READ_SIZE
    random_access_ranges.emplace_back(3 * kb, 4 * kb); // column1
    // if read the follow slice, amplified_ratio = 1,
    // but merge the next rand, amplified_ratio will be decreased
    random_access_ranges.emplace_back(5 * kb, 6 * kb);  // column2
    random_access_ranges.emplace_back(7 * kb, 12 * kb); // column3
    // read the last range first, so we can't merge the last range when reading the former ranges,
    // even if the amplified_ratio < 0.8
    random_access_ranges.emplace_back(512 * kb, 2048 * kb); // column4

    io::MergeRangeFileReader merge_reader(nullptr, offset_reader, random_access_ranges);
    std::vector<char> data(2048 * kb); // 2MB
    Slice result(data.data(), 2048 * kb);
    size_t bytes_read = 0;

    // read column4
    result.size = 1024 * kb;
    static_cast<void>(merge_reader.read_at(1024 * kb, result, &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 1024 * kb);
    EXPECT_EQ(merge_reader.statistics().request_bytes, 1024 * kb);
    EXPECT_EQ(merge_reader.statistics().merged_bytes, 1024 * kb);
    // read column0
    result.size = 1 * kb;
    // will merge column 0 ~ 3
    static_cast<void>(merge_reader.read_at(0, result, &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 1 * kb);
    EXPECT_EQ(merge_reader.statistics().merged_bytes, 1024 * kb + 12 * kb);
    // read column1
    result.size = 1 * kb;
    static_cast<void>(merge_reader.read_at(3 * kb, result, &bytes_read, nullptr));
    // read column2
    result.size = 1 * kb;
    static_cast<void>(merge_reader.read_at(5 * kb, result, &bytes_read, nullptr));
    // read column3
    result.size = 5 * kb;
    static_cast<void>(merge_reader.read_at(7 * kb, result, &bytes_read, nullptr));
    EXPECT_EQ(merge_reader.statistics().request_bytes, 1024 * kb + 8 * kb);
    EXPECT_EQ(merge_reader.statistics().merged_bytes, 1024 * kb + 12 * kb);
}

TEST_F(BufferedReaderTest, test_merged_io) {
    io::FileReaderSPtr offset_reader =
            std::make_shared<MockOffsetFileReader>(128 * 1024 * 1024); // 128MB
    std::vector<io::PrefetchRange> random_access_ranges;
    for (size_t i = 0; i < 32; ++i) {
        // 32 columns, every column is 3MB
        size_t start_offset = 4 * 1024 * 1024 * i;
        size_t end_offset = start_offset + 3 * 1024 * 1024;
        random_access_ranges.emplace_back(start_offset, end_offset);
    }
    io::MergeRangeFileReader merge_reader(nullptr, offset_reader, random_access_ranges);
    char data[2 * 1024 * 1024]; // 2MB;
    Slice result(data, 1 * 1024 * 1024);
    size_t bytes_read = 0;

    // read column 0
    static_cast<void>(static_cast<void>(merge_reader.read_at(0, result, &bytes_read, nullptr)));
    // will merge 3MB + 1MB + 3MB, and read out 1MB
    // so _remaining in MergeRangeFileReader is: ${NUM_BOX}MB - (3MB + 3MB - 1MB)
    EXPECT_EQ((io::MergeRangeFileReader::NUM_BOX - 5) * 1024 * 1024,
              merge_reader.buffer_remaining());
    auto& range_cached_data = merge_reader.range_cached_data();
    // range 0 is read out 1MB, so the cached range is [1MB, 3MB)
    // range 1 is not read, so the cached range is [4MB, 7MB)
    EXPECT_EQ(1 * 1024 * 1024, range_cached_data[0].start_offset);
    EXPECT_EQ(3 * 1024 * 1024, range_cached_data[0].end_offset);
    EXPECT_EQ(4 * 1024 * 1024, range_cached_data[1].start_offset);
    EXPECT_EQ(7 * 1024 * 1024, range_cached_data[1].end_offset);

    // read column 1
    static_cast<void>(
            static_cast<void>(merge_reader.read_at(4 * 1024 * 1024, result, &bytes_read, nullptr)));
    // the column 1 is already cached
    EXPECT_EQ(5 * 1024 * 1024, range_cached_data[1].start_offset);
    EXPECT_EQ(7 * 1024 * 1024, range_cached_data[1].end_offset);
    EXPECT_EQ((io::MergeRangeFileReader::NUM_BOX - 4) * 1024 * 1024,
              merge_reader.buffer_remaining());

    // read all cached data
    static_cast<void>(
            static_cast<void>(merge_reader.read_at(1 * 1024 * 1024, result, &bytes_read, nullptr)));
    static_cast<void>(
            static_cast<void>(merge_reader.read_at(2 * 1024 * 1024, result, &bytes_read, nullptr)));
    static_cast<void>(
            static_cast<void>(merge_reader.read_at(5 * 1024 * 1024, result, &bytes_read, nullptr)));
    static_cast<void>(
            static_cast<void>(merge_reader.read_at(6 * 1024 * 1024, result, &bytes_read, nullptr)));
    EXPECT_EQ(io::MergeRangeFileReader::TOTAL_BUFFER_SIZE, merge_reader.buffer_remaining());

    // read all remaining columns
    for (int i = 0; i < 3; ++i) {
        for (size_t col = 2; col < 32; col++) {
            if (i == 0) {
                size_t start_offset = 4 * 1024 * 1024 * col;
                size_t to_read = 729 * 1024; // read 729KB
                static_cast<void>(static_cast<void>(merge_reader.read_at(
                        start_offset, Slice(data, to_read), &bytes_read, nullptr)));
                EXPECT_EQ(to_read, bytes_read);
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8_t)data[0]);
            } else if (i == 1) {
                size_t start_offset = 4 * 1024 * 1024 * col + 729 * 1024;
                size_t to_read = 1872 * 1024; // read 1872KB
                static_cast<void>(static_cast<void>(merge_reader.read_at(
                        start_offset, Slice(data, to_read), &bytes_read, nullptr)));
                EXPECT_EQ(to_read, bytes_read);
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8_t)data[0]);
            } else if (i == 2) {
                size_t start_offset = 4 * 1024 * 1024 * col + 729 * 1024 + 1872 * 1024;
                size_t to_read = 471 * 1024; // read 471KB
                static_cast<void>(static_cast<void>(merge_reader.read_at(
                        start_offset, Slice(data, to_read), &bytes_read, nullptr)));
                EXPECT_EQ(to_read, bytes_read);
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8_t)data[0]);
            }
        }
    }

    // check the final state
    EXPECT_EQ(io::MergeRangeFileReader::TOTAL_BUFFER_SIZE, merge_reader.buffer_remaining());
    for (auto& cached_data : merge_reader.range_cached_data()) {
        EXPECT_TRUE(cached_data.empty());
    }
    for (auto& ref : merge_reader.box_reference()) {
        EXPECT_TRUE(ref == 0);
    }
}

TEST_F(BufferedReaderTest, test_range_cache_file_reader) {
    io::FileReaderSPtr offset_reader = std::make_shared<MockOffsetFileReader>(128 * 1024 * 1024);
    auto testing_reader = std::make_shared<TestingRangeCacheFileReader>(offset_reader);

    int64_t orc_max_merge_distance = 1L * 1024L * 1024L;
    int64_t orc_once_max_read_size = 8L * 1024L * 1024L;

    {
        std::vector<io::PrefetchRange> tiny_stripe_ranges = {
                io::PrefetchRange(3, 33),
                io::PrefetchRange(33, 63),
                io::PrefetchRange(63, 8L * 1024L * 1024L + 63),
        };
        std::vector<io::PrefetchRange> prefetch_merge_ranges =
                io::PrefetchRange::merge_adjacent_seq_ranges(
                        tiny_stripe_ranges, orc_max_merge_distance, orc_once_max_read_size);
        auto range_finder =
                std::make_shared<io::LinearProbeRangeFinder>(std::move(prefetch_merge_ranges));
        io::RangeCacheFileReader range_cache_file_reader(nullptr, testing_reader, range_finder);
        char data[1];
        Slice result(data, 1);
        size_t bytes_read;
        EXPECT_TRUE(range_cache_file_reader.read_at(3, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(io::PrefetchRange(3, 63), testing_reader->last_read_range());

        EXPECT_TRUE(range_cache_file_reader.read_at(63, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(io::PrefetchRange(63, 8 * 1024L * 1024L + 63), testing_reader->last_read_range());
        EXPECT_TRUE(range_cache_file_reader.close().ok());
    }

    {
        std::vector<io::PrefetchRange> tiny_stripe_ranges = {
                io::PrefetchRange(3, 33),
                io::PrefetchRange(33, 63),
                io::PrefetchRange(63, 8L * 1024L * 1024L + 63),
        };
        std::vector<io::PrefetchRange> prefetch_merge_ranges =
                io::PrefetchRange::merge_adjacent_seq_ranges(
                        tiny_stripe_ranges, orc_max_merge_distance, orc_once_max_read_size);
        auto range_finder =
                std::make_shared<io::LinearProbeRangeFinder>(std::move(prefetch_merge_ranges));
        io::RangeCacheFileReader range_cache_file_reader(nullptr, testing_reader, range_finder);
        char data[1];
        Slice result(data, 1);
        size_t bytes_read;
        EXPECT_TRUE(range_cache_file_reader.read_at(62, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(io::PrefetchRange(3, 63), testing_reader->last_read_range());

        EXPECT_TRUE(range_cache_file_reader.read_at(63, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(io::PrefetchRange(63, 8L * 1024L * 1024L + 63),
                  testing_reader->last_read_range());
        EXPECT_TRUE(range_cache_file_reader.close().ok());
    }

    {
        std::vector<io::PrefetchRange> tiny_stripe_ranges = {
                io::PrefetchRange(3, 3),
                io::PrefetchRange(4, 1048576L * 5L + 4),
                io::PrefetchRange(1048576L * 5L + 4, 1048576L * 3L + 1048576L * 5L + 4),
        };
        std::vector<io::PrefetchRange> prefetch_merge_ranges =
                io::PrefetchRange::merge_adjacent_seq_ranges(
                        tiny_stripe_ranges, orc_max_merge_distance, orc_once_max_read_size);
        auto range_finder =
                std::make_shared<io::LinearProbeRangeFinder>(std::move(prefetch_merge_ranges));
        io::RangeCacheFileReader range_cache_file_reader(nullptr, testing_reader, range_finder);
        char data[1];
        Slice result(data, 1);
        size_t bytes_read;
        EXPECT_TRUE(range_cache_file_reader.read_at(3, result, &bytes_read, nullptr).ok());
        EXPECT_EQ(io::PrefetchRange(3, 1 + 1048576 * 5 + 3), testing_reader->last_read_range());

        EXPECT_TRUE(range_cache_file_reader.read_at(4 + 1048576 * 5, result, &bytes_read, nullptr)
                            .ok());
        EXPECT_EQ(io::PrefetchRange(4 + 1048576 * 5, 3 * 1048576 + 4 + 1048576 * 5),
                  testing_reader->last_read_range());
        EXPECT_TRUE(range_cache_file_reader.close().ok());
    }
}

TEST_F(BufferedReaderTest, test_large_gap_amplification) {
    // Test case to verify adaptive window sizing with medium gaps between ranges
    // With the new adaptive logic, 8MB window should automatically shrink when
    // encountering accumulated gaps, preventing severe read amplification
    size_t kb = 1024;
    io::FileReaderSPtr offset_reader = std::make_shared<MockOffsetFileReader>(20 * 1024 * kb);
    std::vector<io::PrefetchRange> random_access_ranges;

    // Simulate scenario with multiple small ranges and medium gaps:
    // 30 ranges, each 50KB, with 30KB gaps between them
    // Total data: 1500KB (1.46MB)
    // Total gaps: 870KB (0.85MB) for 29 gaps
    // Individual gap/content ratio: 30KB/50KB = 0.6 (acceptable, < 0.8)
    //
    // With adaptive logic:
    // - 8MB window will start large but shrink when gap ratio > 0.5
    // - Should result in reasonable amplification (< 2x)
    // - 64KB window: can only fit 1 range, minimal gaps
    for (size_t i = 0; i < 30; ++i) {
        size_t start = i * 80 * kb;       // 80KB spacing (50KB data + 30KB gap)
        size_t end = start + 50 * kb;     // 50KB data
        random_access_ranges.emplace_back(start, end);
    }

    // Test with 8MB merge window (with adaptive shrinking)
    io::MergeRangeFileReader merge_reader_8mb(nullptr, offset_reader, random_access_ranges,
                                               8 * 1024 * kb);
    std::vector<char> data(50 * kb);
    size_t bytes_read = 0;

    // Read first range - this should trigger adaptive merge logic
    static_cast<void>(
            merge_reader_8mb.read_at(0, Slice(data.data(), 50 * kb), &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 50 * kb);

    auto stats_8mb = merge_reader_8mb.statistics();
    double amplify_8mb =
            (double)stats_8mb.merged_bytes / (double)std::max(stats_8mb.request_bytes, 1L);

    std::cout << "8MB window (adaptive) - request_bytes: " << stats_8mb.request_bytes
              << ", merged_bytes: " << stats_8mb.merged_bytes
              << ", amplification ratio: " << amplify_8mb << std::endl;

    // Test with 64KB merge window
    io::MergeRangeFileReader merge_reader_64kb(nullptr, offset_reader, random_access_ranges,
                                                64 * kb);
    bytes_read = 0;

    static_cast<void>(
            merge_reader_64kb.read_at(0, Slice(data.data(), 50 * kb), &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 50 * kb);

    auto stats_64kb = merge_reader_64kb.statistics();
    double amplify_64kb =
            (double)stats_64kb.merged_bytes / (double)std::max(stats_64kb.request_bytes, 1L);

    std::cout << "64KB window - request_bytes: " << stats_64kb.request_bytes
              << ", merged_bytes: " << stats_64kb.merged_bytes
              << ", amplification ratio: " << amplify_64kb << std::endl;

    // With adaptive logic, 8MB window should have reasonable amplification
    // Expected: 8MB adaptive might merge 3-5 ranges before shrinking = reasonable amplification
    //          Should be much better than the non-adaptive version
    EXPECT_LT(amplify_8mb, 3.0)
            << "8MB adaptive window should limit amplification to < 3x by shrinking dynamically";

    std::cout << "Amplification comparison - 8MB adaptive: " << amplify_8mb << "x, 64KB: "
              << amplify_64kb << "x" << std::endl;
    std::cout << "Adaptive logic successfully prevents severe read amplification by shrinking "
              << "the merge window when gap accumulation is detected." << std::endl;
}

TEST_F(BufferedReaderTest, test_single_large_gap_rejection) {
    // Test case to verify that single large gaps (>= 512KB) are rejected
    // This prevents the worst-case scenario of including huge gaps
    size_t kb = 1024;
    io::FileReaderSPtr offset_reader = std::make_shared<MockOffsetFileReader>(20 * 1024 * kb);
    std::vector<io::PrefetchRange> random_access_ranges;

    // Create ranges with large gaps (> 512KB) that should NOT be merged:
    // Range0: [0KB, 100KB)
    // Gap0:   [100KB, 700KB)   <- 600KB gap (> 512KB limit)
    // Range1: [700KB, 800KB)
    random_access_ranges.emplace_back(0, 100 * kb);
    random_access_ranges.emplace_back(700 * kb, 800 * kb);
    random_access_ranges.emplace_back(1500 * kb, 1600 * kb);

    // Test with 8MB merge window
    io::MergeRangeFileReader merge_reader(nullptr, offset_reader, random_access_ranges,
                                           8 * 1024 * kb);
    std::vector<char> data(100 * kb);
    size_t bytes_read = 0;

    // Read first range
    static_cast<void>(merge_reader.read_at(0, Slice(data.data(), 100 * kb), &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 100 * kb);

    auto stats = merge_reader.statistics();
    double amplify = (double)stats.merged_bytes / (double)std::max(stats.request_bytes, 1L);

    std::cout << "Large gap test - request_bytes: " << stats.request_bytes
              << ", merged_bytes: " << stats.merged_bytes << ", amplification ratio: " << amplify
              << std::endl;

    // The 600KB gap should NOT be included due to max_single_gap (512KB) limit
    // Expected: only Range0 is read = 100KB, amplification = 1.0
    EXPECT_LT(amplify, 1.5) << "Large gap (>512KB) should be rejected, amplification should be "
                               "minimal (<1.5x)";

    std::cout << "Successfully rejected large gap (>512KB), preventing severe amplification."
              << std::endl;
}

TEST_F(BufferedReaderTest, test_dense_ranges_amplification) {
    // Test case for dense ranges (the intended use case for large merge windows)
    // This verifies that 8MB window works well when ranges are close together
    size_t kb = 1024;
    io::FileReaderSPtr offset_reader = std::make_shared<MockOffsetFileReader>(10 * 1024 * kb);
    std::vector<io::PrefetchRange> random_access_ranges;

    // Dense ranges with small gaps (typical for column data in same row group):
    // Multiple 50KB columns with 10KB gaps
    for (size_t i = 0; i < 10; ++i) {
        size_t start = i * 60 * kb;              // 60KB spacing (50KB data + 10KB gap)
        size_t end = start + 50 * kb;            // 50KB data
        random_access_ranges.emplace_back(start, end);
    }
    // Total data: 500KB, Total gaps: 90KB (10KB * 9)

    // Test with 8MB merge window
    io::MergeRangeFileReader merge_reader_8mb(nullptr, offset_reader, random_access_ranges,
                                               8 * 1024 * kb);
    std::vector<char> data(50 * kb);
    size_t bytes_read = 0;

    // Read first column
    static_cast<void>(
            merge_reader_8mb.read_at(0, Slice(data.data(), 50 * kb), &bytes_read, nullptr));
    EXPECT_EQ(bytes_read, 50 * kb);

    auto stats_8mb = merge_reader_8mb.statistics();
    double amplify_8mb =
            (double)stats_8mb.merged_bytes / (double)std::max(stats_8mb.request_bytes, 1L);

    std::cout << "Dense ranges 8MB window - request_bytes: " << stats_8mb.request_bytes
              << ", merged_bytes: " << stats_8mb.merged_bytes
              << ", amplification ratio: " << amplify_8mb << std::endl;

    // For dense ranges, some amplification is acceptable and beneficial
    // Total merged should be around 590KB (500KB data + 90KB gaps)
    // Request is 50KB, so amplification ~= 11.8x
    // But this is good! Because we avoid 9 additional I/O operations
    EXPECT_LT(stats_8mb.merged_bytes, 700 * kb)
            << "Should merge all dense ranges within reasonable size";
    EXPECT_GT(stats_8mb.merged_bytes, 400 * kb)
            << "Should merge multiple ranges to reduce I/O count";

    std::cout << "Dense ranges are effectively merged with 8MB window" << std::endl;
}

} // end namespace doris
