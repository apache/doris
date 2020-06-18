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

#include <gtest/gtest.h>

#include "exec/local_file_reader.h"
#include "exec/buffered_reader.h"
#include "util/stopwatch.hpp"

namespace doris {
class BufferedReaderTest : public testing::Test {
public:
    BufferedReaderTest() {}

protected:
    virtual void SetUp() {
    }
    virtual void TearDown() {
    }
};

TEST_F(BufferedReaderTest, normal_use) {
    LocalFileReader file_reader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file", 0);
    BufferedReader reader(&file_reader);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[32 * 1024];
    MonotonicStopWatch watch;
    watch.start();
    bool eof = false;
    size_t read_length = 0;
    while (!eof) {
        size_t buf_len = 32 * 1024;
        st = reader.read(buf, &buf_len, &eof);
        ASSERT_TRUE(st.ok());
        read_length += buf_len;
    }

    LOG(INFO) << "read bytes " << read_length << " using time " << watch.elapsed_time();
}

TEST_F(BufferedReaderTest, test_validity) {
    LocalFileReader file_reader(
            "./be/test/exec/test_data/buffered_reader/buffered_reader_test_file.txt", 0);
    BufferedReader reader(&file_reader, 128 * 1024);
    auto st = reader.open();
    ASSERT_TRUE(st.ok());
    uint8_t buf[10];
    bool eof = false;
    size_t buf_len = 10;

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("bdfhjlnprt", std::string((char*)buf, buf_len).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("vxzAbCdEfG", std::string((char*)buf, buf_len).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("hIj\n\nMnOpQ", std::string((char*)buf, buf_len).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("rStUvWxYz\n", std::string((char*)buf, buf_len).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_STREQ("IjKl", std::string((char*)buf, 4).c_str());
    ASSERT_FALSE(eof);

    st = reader.read(buf, &buf_len, &eof);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(eof);
}

} // end namespace doris

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
