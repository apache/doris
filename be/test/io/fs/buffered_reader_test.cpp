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

#include <gtest/gtest.h>

#include <memory>

#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/stopwatch.hpp"

namespace doris {
using io::FileReader;
class BufferedReaderTest : public testing::Test {
public:
    BufferedReaderTest() {
        std::unique_ptr<ThreadPool> _pool;
        ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                .set_min_threads(5)
                .set_max_threads(10)
                .build(&_pool);
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

    std::shared_ptr<io::FileSystem> fs() const override { return _reader->fs(); }

private:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        std::unique_lock<std::mutex> lck {_lock};
        return _reader->read_at(offset, result, bytes_read);
    }

    io::FileReaderSPtr _reader;
    std::mutex _lock;
};

TEST_F(BufferedReaderTest, normal_use) {
    // buffered_reader_test_file 950 bytes
    io::FileReaderSPtr local_reader;
    io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file", &local_reader);
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(std::move(sync_local_reader), 0, 1024);
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
    io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader);
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(std::move(sync_local_reader), 0, 1024);
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
    io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader);
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(std::move(sync_local_reader), 0, 1024);

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
    io::global_local_filesystem()->open_file(
            "./be/test/io/fs/test_data/buffered_reader/buffered_reader_test_file.txt",
            &local_reader);
    auto sync_local_reader = std::make_shared<SyncLocalFileReader>(std::move(local_reader));
    io::PrefetchBufferedReader reader(std::move(sync_local_reader), 0, 1024);
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

} // end namespace doris
