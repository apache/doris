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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class ShortKeyIndexTest : public testing::Test {
public:
    ShortKeyIndexTest() {}
    virtual ~ShortKeyIndexTest() {}
};

TEST_F(ShortKeyIndexTest, builder) {
    ShortKeyIndexBuilder builder(0, 1024);

    int num_items = 0;
    for (int i = 1000; i < 10000; i += 2) {
        static_cast<void>(builder.add_item(std::to_string(i)));
        num_items++;
    }
    std::vector<Slice> slices;
    segment_v2::PageFooterPB footer;
    auto st = builder.finalize(9000 * 1024, &slices, &footer);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(segment_v2::SHORT_KEY_PAGE, footer.type());
    EXPECT_EQ(num_items, footer.short_key_page_footer().num_items());

    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    ShortKeyIndexDecoder decoder;
    st = decoder.parse(buf, footer.short_key_page_footer());
    EXPECT_TRUE(st.ok());

    // find 1499
    {
        auto iter = decoder.lower_bound("1499");
        EXPECT_TRUE(iter.valid());
        EXPECT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 lower bound
    {
        auto iter = decoder.lower_bound("1500");
        EXPECT_TRUE(iter.valid());
        EXPECT_STREQ("1500", (*iter).to_string().c_str());
    }
    // find 1500 upper bound
    {
        auto iter = decoder.upper_bound("1500");
        EXPECT_TRUE(iter.valid());
        EXPECT_STREQ("1502", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.lower_bound("87");
        EXPECT_TRUE(iter.valid());
        EXPECT_STREQ("8700", (*iter).to_string().c_str());
    }
    // find prefix "87"
    {
        auto iter = decoder.upper_bound("87");
        EXPECT_TRUE(iter.valid());
        EXPECT_STREQ("8700", (*iter).to_string().c_str());
    }

    // find prefix "9999"
    {
        auto iter = decoder.upper_bound("9999");
        EXPECT_FALSE(iter.valid());
    }
}

} // namespace doris
