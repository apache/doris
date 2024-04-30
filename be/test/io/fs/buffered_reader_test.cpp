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

#include <memory>
#include <ostream>

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
    BufferedReaderTest() {
        std::unique_ptr<ThreadPool> _pool;
        static_cast<void>(ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                                  .set_min_threads(5)
                                  .set_max_threads(10)
                                  .build(&_pool));
        ExecEnv::GetInstance()->_buffered_reader_prefetch_thread_pool = std::move(_pool);
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
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
    LOG(INFO) << "read bytes " << read_length << " using time " << watch.elapsed_time();
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
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8)data[0]);
            } else if (i == 1) {
                size_t start_offset = 4 * 1024 * 1024 * col + 729 * 1024;
                size_t to_read = 1872 * 1024; // read 1872KB
                static_cast<void>(static_cast<void>(merge_reader.read_at(
                        start_offset, Slice(data, to_read), &bytes_read, nullptr)));
                EXPECT_EQ(to_read, bytes_read);
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8)data[0]);
            } else if (i == 2) {
                size_t start_offset = 4 * 1024 * 1024 * col + 729 * 1024 + 1872 * 1024;
                size_t to_read = 471 * 1024; // read 471KB
                static_cast<void>(static_cast<void>(merge_reader.read_at(
                        start_offset, Slice(data, to_read), &bytes_read, nullptr)));
                EXPECT_EQ(to_read, bytes_read);
                EXPECT_EQ(start_offset % UCHAR_MAX, (uint8)data[0]);
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

} // end namespace doris
