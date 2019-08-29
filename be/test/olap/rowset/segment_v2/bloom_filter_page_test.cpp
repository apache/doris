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

#include "olap/rowset/segment_v2/bloom_filter_page.h"

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"
#include "util/debug_util.h"

namespace doris {
namespace segment_v2 {

class BloomFilterPageTest : public testing::Test {
public:
    BloomFilterPageTest() { }
    virtual ~BloomFilterPageTest() {
    }
};

TEST_F(BloomFilterPageTest, normal) {
    TypeInfo* type_info = get_type_info(OLAP_FIELD_TYPE_VARCHAR);
    BloomFilterPageBuilder bf_page_builder(type_info, 2, 0.05);
    std::string bytes;
    // first block
    Slice slice1("hello");
    bf_page_builder.add((const uint8_t*)&slice1, 1);
    Slice slice2("doris");
    bf_page_builder.add((const uint8_t*)&slice2, 1);
    // second block
    Slice slice3("hi");
    bf_page_builder.add((const uint8_t*)&slice3, 1);
    Slice slice4("world");
    bf_page_builder.add((const uint8_t*)&slice4, 1);
    // third block
    // nullptr
    bf_page_builder.add(nullptr, 1);

    Slice bf_data = bf_page_builder.finish();
    BloomFilterPage bf_page(bf_data);
    auto st = bf_page.load();
    ASSERT_TRUE(st.ok()) << "st:" << st.to_string();
    ASSERT_EQ(3, bf_page.block_num());
    ASSERT_EQ(2, bf_page.expected_num());

    std::shared_ptr<BloomFilter> bf_1 = bf_page.get_bloom_filter(0);
    ASSERT_TRUE(bf_1->test_bytes(slice1.data, slice1.size));
    ASSERT_TRUE(bf_1->test_bytes(slice2.data, slice2.size));
    ASSERT_FALSE(bf_1->test_bytes(slice3.data, slice3.size));
    
    std::shared_ptr<BloomFilter> bf_2 = bf_page.get_bloom_filter(1);
    ASSERT_TRUE(bf_2->test_bytes(slice3.data, slice3.size));
    ASSERT_TRUE(bf_2->test_bytes(slice4.data, slice4.size));
    ASSERT_FALSE(bf_2->test_bytes(slice1.data, slice1.size));

    std::shared_ptr<BloomFilter> bf_3 = bf_page.get_bloom_filter(2);
    ASSERT_TRUE(bf_3->test_bytes(nullptr, 1));
}

TEST_F(BloomFilterPageTest, corrupt) {
    std::string str;
    str.resize(4);

    encode_fixed32_le((uint8_t*)str.data(), 1);

    Slice slice(str);
    BloomFilterPage bf_page(slice);
    auto st = bf_page.load();
    ASSERT_FALSE(st.ok());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

