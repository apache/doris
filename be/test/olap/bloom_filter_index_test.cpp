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

#include <string>

#include "olap/bloom_filter_reader.h"
#include "olap/bloom_filter_writer.h"
#include "util/logging.h"

using std::string;

namespace doris {

class TestBloomFilterIndex : public testing::Test {
public:
    virtual ~TestBloomFilterIndex() {}

    virtual void SetUp() {}
    virtual void TearDown() {}
};

// Test the normal read and write process
TEST_F(TestBloomFilterIndex, normal_read_and_write) {
    string bytes;
    BloomFilterIndexReader reader;
    BloomFilterIndexWriter writer;

    BloomFilter* bf_0 = new (std::nothrow) BloomFilter();
    bf_0->init(1024);
    bytes = "hello";
    bf_0->add_bytes(nullptr, 0);
    bf_0->add_bytes(bytes.c_str(), bytes.size());
    writer.add_bloom_filter(bf_0);

    BloomFilter* bf_1 = new (std::nothrow) BloomFilter();
    bf_1->init(1024);
    bytes = "doris";
    bf_1->add_bytes(bytes.c_str(), bytes.size());
    writer.add_bloom_filter(bf_1);

    uint64_t expect_size = sizeof(BloomFilterIndexHeader) + bf_0->bit_num() * 2 / 8;
    ASSERT_EQ(expect_size, writer.estimate_buffered_memory());

    char buffer[expect_size];
    memset(buffer, 0, expect_size);
    ASSERT_EQ(OLAP_SUCCESS, writer.write_to_buffer(buffer, expect_size));

    ASSERT_EQ(OLAP_SUCCESS,
              reader.init(buffer, expect_size, true, bf_0->hash_function_num(), bf_0->bit_num()));
    ASSERT_EQ(2, reader.entry_count());

    bytes = "hello";
    const BloomFilter& bf__0 = reader.entry(0);
    ASSERT_TRUE(bf__0.test_bytes(nullptr, 0));
    ASSERT_TRUE(bf__0.test_bytes(bytes.c_str(), bytes.size()));

    bytes = "doris";
    const BloomFilter& bf__1 = reader.entry(1);
    ASSERT_TRUE(bf__1.test_bytes(bytes.c_str(), bytes.size()));
}

// Test abnormal write case
TEST_F(TestBloomFilterIndex, abnormal_write) {
    char buffer[24];
    BloomFilterIndexWriter writer;
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, writer.write_to_buffer(nullptr));
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, writer.write_to_buffer(nullptr, 0));
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, writer.write_to_buffer(buffer, 0));
    ASSERT_EQ(sizeof(BloomFilterIndexHeader), writer.estimate_buffered_memory());
}

// Test abnormal read case
TEST_F(TestBloomFilterIndex, abnormal_read) {
    uint32_t bit_num = 64;
    uint32_t buffer_size = 24;
    uint32_t hash_function_num = 3;
    char buffer[buffer_size];
    BloomFilterIndexHeader* header = reinterpret_cast<BloomFilterIndexHeader*>(buffer);
    BloomFilterIndexReader reader;

    header->block_count = 1;
    ASSERT_EQ(OLAP_SUCCESS, reader.init(buffer, buffer_size, true, hash_function_num, bit_num));

    header->block_count = 3;
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR,
              reader.init(buffer, buffer_size, true, hash_function_num, bit_num));
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
