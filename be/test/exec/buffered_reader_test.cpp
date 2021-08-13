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

#include "exec/buffered_reader.h"

#include <gtest/gtest.h>

#include "exec/local_file_reader.h"
#include "util/stopwatch.hpp"

namespace doris {
class BufferedReaderTest : public testing::Test {
public:
    BufferedReaderTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(BufferedReaderTest, normal_use) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file 950 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file", 0);
    BufferedReader reader(&profile, file_reader, 1024);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[1024];
    MonotonicStopWatch watch;
    watch.start();
    int64_t read_length = 0;
    st = reader.readat(0, 1024, &read_length, buf);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(950, read_length);
    LOG(INFO) << "read bytes " << read_length << " using time " << watch.elapsed_time();
}

TEST_F(BufferedReaderTest, test_validity) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    BufferedReader reader(&profile, file_reader, 64);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[10];
    bool eof = false;
    int64_t buf_len = 10;
    int64_t read_length = 0;

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("vxzAbCdEfG", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("rStUvWxYz\n", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("IjKl", std::string((char*)buf, 4).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
}

TEST_F(BufferedReaderTest, test_seek) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    BufferedReader reader(&profile, file_reader, 64);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[10];
    bool eof = false;
    size_t buf_len = 10;
    int64_t read_length = 0;

    // Seek to the end of the file
    st = reader.seek(45);
    ASSERT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);

    // Seek to the beginning of the file
    st = reader.seek(0);
    ASSERT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(-1);
    ASSERT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(-1000);
    ASSERT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, read_length).c_str());
    ASSERT_FALSE(eof);

    // Seek to a wrong position
    st = reader.seek(1000);
    ASSERT_TRUE(st.ok());
    st = reader.read(buf, buf_len, &read_length, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
}

TEST_F(BufferedReaderTest, test_miss) {
    RuntimeProfile profile("test");
    // buffered_reader_test_file.txt 45 bytes
    auto file_reader = new LocalFileReader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    BufferedReader reader(&profile, file_reader, 64);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[128];
    int64_t bytes_read;

    st = reader.readat(20, 10, &bytes_read, buf);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, (size_t)bytes_read).c_str());
    ASSERT_EQ(10, bytes_read);

    st = reader.readat(0, 5, &bytes_read, buf);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhj", std::string((char*)buf, (size_t)bytes_read).c_str());
    ASSERT_EQ(5, bytes_read);

    st = reader.readat(5, 10, &bytes_read, buf);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("lnprtvxzAb", std::string((char*)buf, (size_t)bytes_read).c_str());
    ASSERT_EQ(10, bytes_read);

    // if requested length is larger than the capacity of buffer, do not
    // need to copy the character into local buffer.
    st = reader.readat(0, 128, &bytes_read, buf);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, 10).c_str());
    ASSERT_EQ(45, bytes_read);
}

} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
