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

#include "exec/decompressor.h"
#include "exec/plain_text_line_reader.h"
#include "io/local_file_reader.h"
#include "util/runtime_profile.h"

namespace doris {

class PlainTextLineReaderTest : public testing::Test {
public:
    PlainTextLineReaderTest() : _profile("TestProfile") {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    RuntimeProfile _profile;
};

TEST_F(PlainTextLineReaderTest, bzip2_normal_use) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/test_file.csv.bz2",
                                0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, -1, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;

    // 1,2
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(3, size);
    EXPECT_FALSE(eof);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(0, size);
    EXPECT_FALSE(eof);

    // 1,2,3,4
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(7, size);
    EXPECT_FALSE(eof);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
    delete decompressor;
}

TEST_F(PlainTextLineReaderTest, bzip2_test_limit) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/limit.csv.bz2", 0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, 8, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, size);
    EXPECT_FALSE(eof);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(0, size);

    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, size);
    EXPECT_FALSE(eof);
    LOG(INFO) << std::string((const char*)ptr, size);

    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    delete decompressor;
}

TEST_F(PlainTextLineReaderTest, bzip2_test_limit2) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/limit.csv.bz2", 0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, 6, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, size);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    delete decompressor;
}

TEST_F(PlainTextLineReaderTest, bzip2_test_limit3) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/limit.csv.bz2", 0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, 7, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, size);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(0, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    delete decompressor;
}

TEST_F(PlainTextLineReaderTest, bzip2_test_limit4) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/limit.csv.bz2", 0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, 7, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, size);
    LOG(INFO) << std::string((const char*)ptr, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(0, size);

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    delete decompressor;
}

TEST_F(PlainTextLineReaderTest, bzip2_test_limit5) {
    LocalFileReader file_reader("./be/test/exec/test_data/plain_text_line_reader/limit.csv.bz2", 0);
    auto st = file_reader.open();
    EXPECT_TRUE(st.ok());

    Decompressor* decompressor;
    st = Decompressor::create_decompressor(CompressType::BZIP2, &decompressor);
    EXPECT_TRUE(st.ok());

    PlainTextLineReader line_reader(&_profile, &file_reader, decompressor, 0, "\n", 1);
    const uint8_t* ptr;
    size_t size;
    bool eof;

    // Empty
    st = line_reader.read_line(&ptr, &size, &eof);
    EXPECT_TRUE(st.ok());
    delete decompressor;
}

} // end namespace doris
