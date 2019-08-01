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

#include "olap/short_key_index.h"

#include <gtest/gtest.h>

#include "olap/lru_cache.h"
#include "olap/file_helper.h"

namespace doris {

class ShortKeyIndexTest : public testing::Test {
public:
    ShortKeyIndexTest() { }
    virtual ~ShortKeyIndexTest() {
    }
};

TEST_F(ShortKeyIndexTest, buider) {
    ShortKeyIndexBuilder builder(0, 1024);

    for (int i = 1000; i < 10000; i += 2) {
        builder.add_item(std::to_string(i));
    }
    std::vector<Slice> slices;
    auto st = builder.finalize(10000, 9000 * 1024, &slices);
    ASSERT_TRUE(st.ok());
    
    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    ShortKeyIndexDecoder decoder(buf);
    st = decoder.parse();
    ASSERT_TRUE(st.ok());

    // find 1499
    {
        auto iter = decoder.lower_bound("1499");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 lower bound
    {
        auto iter = decoder.lower_bound("1500");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 upper bound
    {
        auto iter = decoder.upper_bound("1500");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("1502", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.lower_bound("87");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("8700", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.upper_bound("87");
        ASSERT_TRUE(iter.valid());
        ASSERT_STREQ("8700", (*iter).to_string().c_str());
    }

    // find prefix "9999"
    {
        auto iter = decoder.upper_bound("9999");
        ASSERT_FALSE(iter.valid());
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

