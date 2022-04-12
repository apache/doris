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

#include "olap/bloom_filter.hpp"

#include <gtest/gtest.h>

#include <string>

#include "common/configbase.h"
#include "util/logging.h"

using std::string;

namespace doris {

class TestBloomFilter : public testing::Test {
public:
    virtual ~TestBloomFilter() {}

    virtual void SetUp() {}
    virtual void TearDown() {}
};

// Init BloomFilter with different item number and fpp,
//     and verify bit_num and hash_function_num calculated by BloomFilter
TEST_F(TestBloomFilter, init_bloom_filter) {
    {
        BloomFilter bf;
        bf.init(1024);
        EXPECT_EQ(6400, bf.bit_num());
        EXPECT_EQ(4, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        bf.init(1024, 0.01);
        EXPECT_EQ(9856, bf.bit_num());
        EXPECT_EQ(7, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        bf.init(10240, 0.1);
        EXPECT_EQ(49088, bf.bit_num());
        EXPECT_EQ(3, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        uint32_t data_len = 100;
        uint32_t hash_function_num = 4;
        uint64_t* data = new uint64_t[data_len];

        bf.init(data, data_len, hash_function_num);
        EXPECT_EQ(6400, bf.bit_num());
        EXPECT_EQ(4, bf.hash_function_num());
        EXPECT_EQ(data, bf.bit_set_data());

        bf.reset();
        EXPECT_EQ(0, bf.bit_num());
        EXPECT_EQ(0, bf.hash_function_num());
        EXPECT_EQ(nullptr, bf.bit_set_data());
        delete[] data;
    }
}

// Add different buffer to BloomFilter and verify existence
TEST_F(TestBloomFilter, add_and_test_bytes) {
    string bytes;
    BloomFilter bf;
    bf.init(1024);

    bf.add_bytes(nullptr, 0);
    EXPECT_TRUE(bf.test_bytes(nullptr, 0));

    bytes = "hello";
    bf.add_bytes(bytes.c_str(), bytes.size());
    EXPECT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));

    bytes = "doris";
    bf.add_bytes(bytes.c_str(), bytes.size());
    EXPECT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));

    BloomFilter new_bf;
    new_bf.init(1024);

    bytes = "world";
    new_bf.add_bytes(bytes.c_str(), bytes.size());
    EXPECT_TRUE(bf.merge(new_bf));
    EXPECT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));
}

// Print bloom filter buffer and points of specified string
TEST_F(TestBloomFilter, bloom_filter_info) {
    string bytes;
    BloomFilter bf;
    bf.init(8, 0.1);

    bytes = "doris";
    bf.add_bytes(bytes.c_str(), bytes.size());
    string buffer_expect =
            "bit_num:64 hash_function_num:6 "
            "bit_set:0000100000000000100000010000000000010000001000000000000000000100";
    string buffer = bf.to_string();
    EXPECT_TRUE(buffer_expect == buffer);

    string points_expect = "4-23-42-61-16-35";
    string points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    EXPECT_TRUE(points_expect == points);

    bytes = "a";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "ab";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "b";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "ba";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "c";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "bc";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "ac";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;

    bytes = "abc";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    LOG(WARNING) << "bytes=" << bytes << " points=" << points;
}

} // namespace doris
