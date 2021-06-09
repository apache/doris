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

#include "olap/row_cursor.h"
#include "olap/tablet_schema_helper.h"
#include "util/debug_util.h"

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
        builder.add_item(std::to_string(i));
        num_items++;
    }
    std::vector<Slice> slices;
    segment_v2::PageFooterPB footer;
    auto st = builder.finalize(9000 * 1024, &slices, &footer);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(segment_v2::SHORT_KEY_PAGE, footer.type());
    ASSERT_EQ(num_items, footer.short_key_page_footer().num_items());

    std::string buf;
    for (auto& slice : slices) {
        buf.append(slice.data, slice.size);
    }

    ShortKeyIndexDecoder decoder;
    st = decoder.parse(buf, footer.short_key_page_footer());
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

TEST_F(ShortKeyIndexTest, encode) {
    TabletSchema tablet_schema;
    tablet_schema._cols.push_back(create_int_key(0));
    tablet_schema._cols.push_back(create_int_key(1));
    tablet_schema._cols.push_back(create_int_key(2));
    tablet_schema._cols.push_back(create_int_value(3));
    tablet_schema._num_columns = 4;
    tablet_schema._num_key_columns = 3;
    tablet_schema._num_short_key_columns = 3;

    // test encoding with padding
    {
        RowCursor row;
        row.init(tablet_schema, 2);

        {
            // test padding
            {
                auto cell = row.cell(0);
                cell.set_is_null(false);
                *(int*)cell.mutable_cell_ptr() = 12345;
            }
            {
                auto cell = row.cell(1);
                cell.set_is_null(false);
                *(int*)cell.mutable_cell_ptr() = 54321;
            }
            std::string buf;
            encode_key_with_padding(&buf, row, 3, true);
            // should be \x02\x80\x00\x30\x39\x02\x80\x00\xD4\x31\x00
            ASSERT_STREQ("0280003039028000D43100", hexdump(buf.c_str(), buf.size()).c_str());
        }
        // test with null
        {
            {
                auto cell = row.cell(0);
                cell.set_is_null(false);
                *(int*)cell.mutable_cell_ptr() = 54321;
            }
            {
                auto cell = row.cell(1);
                cell.set_is_null(true);
                *(int*)cell.mutable_cell_ptr() = 54321;
            }

            {
                std::string buf;
                encode_key_with_padding(&buf, row, 3, false);
                // should be \x02\x80\x00\xD4\x31\x01\xff
                ASSERT_STREQ("028000D43101FF", hexdump(buf.c_str(), buf.size()).c_str());
            }
            // encode key
            {
                std::string buf;
                encode_key(&buf, row, 2);
                // should be \x02\x80\x00\xD4\x31\x01
                ASSERT_STREQ("028000D43101", hexdump(buf.c_str(), buf.size()).c_str());
            }
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
