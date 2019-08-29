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
#include <vector>

#include "common/logging.h"
#include "olap/rowset/segment_v2/bloom_filter.h"

namespace doris {
namespace segment_v2 {

class BloomFilterTest : public testing::Test {
public:
    BloomFilterTest() { }
    virtual ~BloomFilterTest() {
    }
};

TEST_F(BloomFilterTest, normal) {
    // for string
    BloomFilterBuilder bf_builder(1024, 0.05);
    std::vector<std::string> values;
    for (int i = 0; i < 1024; ++i) {
        std::string tmp = "string_" + std::to_string(rand());
        BloomKeyProbe key(Slice(tmp.data(), tmp.size()));
        bf_builder.add_key(key);
    }
    // check nullptr
    BloomKeyProbe null_key(Slice((const char*)nullptr, 0));
    bf_builder.add_key(null_key);
    uint32_t expected_size = bf_builder.get_bf_size();
    char* bf_data = new char[expected_size];
    bf_builder.write(bf_data);
    Slice data(bf_data, expected_size);
    BloomFilter bf(data, bf_builder.hash_function_num(), bf_builder.bit_size());
    auto st = bf.load();
    ASSERT_TRUE(st.ok());
    for (auto& value : values) {
        BloomKeyProbe key(Slice(value.data(), value.size()));
        ASSERT_TRUE(bf.check_key(key));
    }

    ASSERT_TRUE(bf.check_key(null_key));
    
    delete [] bf_data;
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

