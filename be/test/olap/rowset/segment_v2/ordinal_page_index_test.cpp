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

#include "olap/rowset/segment_v2/ordinal_page_index.h"

#include <gtest/gtest.h>
#include <iostream>

#include "common/logging.h"

namespace doris {
namespace segment_v2 {

class OrdinalPageIndexTest : public testing::Test {
public:
    OrdinalPageIndexTest() { }
    virtual ~OrdinalPageIndexTest() {
    }
};

TEST_F(OrdinalPageIndexTest, normal) {
    //    rowid, page pointer
    //        1, (0, 4096)
    // 1 + 4096, (1 * 4096, 4096)
    // a page have 16KB, and have 4096 rows
    OrdinalPageIndexBuilder builder;

    // we test a 16KB page
    for (uint64_t i = 0; i < 16 * 1024; ++i) {
        builder.append_entry(1 + 4096 * i, {16 * 1024 * i, 16 * 1024});
    }

    auto slice = builder.finish();
    LOG(INFO) << "index block's size=" << slice.size;

    OrdinalPageIndex index(slice);
    auto st = index.load();
    ASSERT_TRUE(st.ok());

    PagePointer page;
    {
        auto iter = index.seek_at_or_before(1);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(1, iter.rowid());
        ASSERT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4095);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(1, iter.rowid());
        ASSERT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4098);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4097, iter.rowid());
        ASSERT_EQ(PagePointer(1 * 16 * 1024, 16 * 1024), iter.page());

        iter.next();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4097 + 4096, iter.rowid());
        ASSERT_EQ(PagePointer(2 * 16 * 1024, 16 * 1024), iter.page());

    }

    {
        auto iter = index.seek_at_or_before(0);
        ASSERT_FALSE(iter.valid());
    }
}

TEST_F(OrdinalPageIndexTest, corrupt) {
    std::string str;
    str.resize(4);

    encode_fixed32_le((uint8_t*)str.data(), 1);

    Slice slice(str);
    OrdinalPageIndex index(slice);
    auto st = index.load();
    ASSERT_FALSE(st.ok());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

