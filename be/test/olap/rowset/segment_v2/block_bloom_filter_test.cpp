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

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <stdlib.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

class BlockBloomFilterTest : public testing::Test {
public:
    virtual ~BlockBloomFilterTest() {}

private:
    uint64_t _expected_num = 1024;
    double _fpp = 0.05;
};

// Test for int
TEST_F(BlockBloomFilterTest, Normal) {
    // test write
    std::unique_ptr<BloomFilter> bf;
    // now CLASSIC_BLOOM_FILTER is not supported
    auto st = BloomFilter::create(CLASSIC_BLOOM_FILTER, &bf);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(nullptr, bf);
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf);
    st = bf->init(_expected_num, _fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf->size() > 0);
    int num = 1000;
    uint32_t values[1000];
    for (int i = 0; i < num; ++i) {
        values[i] = random();
    }
    for (int i = 0; i < num; ++i) {
        bf->add_bytes((char*)&values[i], sizeof(uint32_t));
    }
    // add nullptr
    bf->add_bytes(nullptr, 1);
    for (int i = 0; i < num; ++i) {
        EXPECT_TRUE(bf->test_bytes((char*)&values[i], sizeof(uint32_t)));
    }
    // test nullptr
    EXPECT_TRUE(bf->test_bytes(nullptr, 1));

    // test read
    std::unique_ptr<BloomFilter> bf2;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf2);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf2);
    st = bf2->init(bf->data(), bf->size(), HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf2->size() > 0);
    for (int i = 0; i < num; ++i) {
        EXPECT_TRUE(bf2->test_bytes((char*)&values[i], sizeof(uint32_t)));
    }
    // test nullptr
    EXPECT_TRUE(bf2->test_bytes(nullptr, 1));

    bf->reset();
    char* data = bf->data();
    // data is reset to 0
    for (int i = 0; i < bf->size(); ++i) {
        EXPECT_EQ(*data, 0);
        data++;
    }
}

// Test for int
TEST_F(BlockBloomFilterTest, SP) {
    // test write
    std::unique_ptr<BloomFilter> bf;
    auto st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf);
    st = bf->init(_expected_num, _fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf->size() > 0);

    std::unique_ptr<BloomFilter> bf2;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf2);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf2);
    st = bf2->init(_expected_num, _fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf2->size() > 0);

    int num = _expected_num;
    std::vector<int32_t> values(num);
    for (int32_t i = 0; i < num; ++i) {
        values[i] = i * 10 + 1;
        bf->add_bytes((char*)&values[i], sizeof(int32_t));
    }

    std::vector<int32_t> values2(num);
    for (int32_t i = 0; i < num; ++i) {
        values2[i] = 15360 + i * 10 + 1;
        bf2->add_bytes((char*)&values2[i], sizeof(int32_t));
    }

    // true test
    for (int i = 0; i < num; ++i) {
        EXPECT_TRUE(bf->test_bytes((char*)&values[i], 4));
        EXPECT_TRUE(bf2->test_bytes((char*)&values2[i], 4));
    }

    // false test
    int false_count1 = 0;
    int false_count2 = 0;
    for (int i = 0; i < num; ++i) {
        int32_t to_check1 = values[i];
        for (int j = 1; j < 10; ++j) {
            ++to_check1;
            false_count1 += bf->test_bytes((char*)&to_check1, 4);
        }

        int32_t to_check2 = values2[i];
        for (int j = 1; j < 10; ++j) {
            ++to_check2;
            false_count2 += bf2->test_bytes((char*)&to_check2, 4);
        }
    }
    EXPECT_LE((double)false_count1 / (num * 9), _fpp);
    EXPECT_LE((double)false_count2 / (num * 9), _fpp);
}

// Test for slice
TEST_F(BlockBloomFilterTest, slice) {
    // test write
    std::unique_ptr<BloomFilter> bf;
    auto st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf);
    st = bf->init(_expected_num, _fpp, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf->size() > 0);

    int num = 1024;
    std::string values[1024];
    for (int32_t i = 0; i < 1024; ++i) {
        values[i] = "prefix_" + std::to_string(10000 + i);
    }
    Slice slices[1024];
    for (int32_t i = 0; i < 1024; ++i) {
        slices[i] = Slice(values[i]);
    }

    for (int i = 0; i < num; ++i) {
        bf->add_bytes(slices[i].data, slices[i].size);
    }

    std::string value_not_exist = "char_value_not_exist";
    Slice s = Slice(value_not_exist);
    EXPECT_FALSE(bf->test_bytes(s.data, s.size));
}

// Test contains
TEST_F(BlockBloomFilterTest, contains) {
    std::unique_ptr<BloomFilter> bf1;
    auto st1 = BloomFilter::create(NGRAM_BLOOM_FILTER, &bf1, 512);
    ASSERT_TRUE(st1.ok());
    ASSERT_NE(nullptr, bf1);
    ASSERT_TRUE(st1.ok());
    ASSERT_TRUE(bf1->size() > 0);

    std::unique_ptr<BloomFilter> bf2;
    auto st2 = BloomFilter::create(NGRAM_BLOOM_FILTER, &bf2, 512);
    ASSERT_TRUE(st2.ok());
    ASSERT_NE(nullptr, bf2);
    ASSERT_TRUE(st2.ok());
    ASSERT_TRUE(bf2->size() > 0);

    std::vector<std::string> str_list = {"abc", "csx", "d2", "csxx", "vaa"};
    for (int i = 0; i < str_list.size(); ++i) {
        auto str = str_list[i];
        bf1->add_bytes(str.data(), str.size());
        if (1 == i % 2) {
            bf2->add_bytes(str.data(), str.size());
        }
    }

    ASSERT_TRUE(bf1->contains(*bf2));
    ASSERT_FALSE(bf2->contains(*bf1));
}

} // namespace segment_v2
} // namespace doris
