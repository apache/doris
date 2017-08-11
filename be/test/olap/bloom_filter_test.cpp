// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/column_file/bloom_filter.hpp"
#include "util/logging.h"

using std::string;

namespace palo {
namespace column_file {

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
        ASSERT_EQ(6400, bf.bit_num());
        ASSERT_EQ(4, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        bf.init(1024, 0.01);
        ASSERT_EQ(9856, bf.bit_num());
        ASSERT_EQ(7, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        bf.init(10240, 0.1);
        ASSERT_EQ(49088, bf.bit_num());
        ASSERT_EQ(3, bf.hash_function_num());
    }

    {
        BloomFilter bf;
        uint32_t data_len = 100;
        uint32_t hash_function_num = 4;
        uint64_t* data = new uint64_t[data_len];

        bf.init(data, data_len, hash_function_num);
        ASSERT_EQ(6400, bf.bit_num());
        ASSERT_EQ(4, bf.hash_function_num());
        ASSERT_EQ(data, bf.bit_set_data());

        bf.reset();
        ASSERT_EQ(0, bf.bit_num());
        ASSERT_EQ(0, bf.hash_function_num());
        ASSERT_EQ(NULL, bf.bit_set_data());
    }
}

// Add different buffer to BloomFilter and verify existence
TEST_F(TestBloomFilter, add_and_test_bytes) {
    string bytes;
    BloomFilter bf;
    bf.init(1024);

    bf.add_bytes(NULL, 0);
    ASSERT_TRUE(bf.test_bytes(NULL, 0));

    bytes = "hello";
    bf.add_bytes(bytes.c_str(), bytes.size());
    ASSERT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));

    bytes = "palo";
    bf.add_bytes(bytes.c_str(), bytes.size());
    ASSERT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));

    BloomFilter new_bf;
    new_bf.init(1024);

    bytes = "world";
    new_bf.add_bytes(bytes.c_str(), bytes.size());
    ASSERT_TRUE(bf.merge(new_bf));
    ASSERT_TRUE(bf.test_bytes(bytes.c_str(), bytes.size()));
}

// Print bloom filter buffer and points of specified string
TEST_F(TestBloomFilter, bloom_filter_info) {
    string bytes;
    BloomFilter bf;
    bf.init(8, 0.1);

    bytes = "palo";
    bf.add_bytes(bytes.c_str(), bytes.size());
    string buffer_expect = "bit_num:64 hash_function_num:6 "
            "bit_set:0000000000000000101000000000000000000010100000000000000000101000";
    string buffer = bf.to_string();
    ASSERT_TRUE(buffer_expect == buffer);

    string points_expect = "58-16-38-60-18-40";
    string points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    ASSERT_TRUE(points_expect == points);

    bytes = "a";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "ab";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "b";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "ba";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "c";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "bc";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "ac";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());

    bytes = "abc";
    points = bf.get_bytes_points_string(bytes.c_str(), bytes.size());
    OLAP_LOG_WARNING("bytes=%s points=%s", bytes.c_str(), points.c_str());
}

} // namespace column_file
} // namespace palo

int main(int argc, char **argv) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    palo::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
