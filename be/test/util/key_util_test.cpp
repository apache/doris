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

#include "util/key_util.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/row_cursor.h"
#include "olap/row_cursor_cell.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "util/debug_util.h"

namespace doris {

class KeyUtilTest : public testing::Test {
public:
    KeyUtilTest() {}
    virtual ~KeyUtilTest() {}
};

TEST_F(KeyUtilTest, encode) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->_cols.push_back(create_int_key(0));
    tablet_schema->_cols.push_back(create_int_key(1));
    tablet_schema->_cols.push_back(create_int_key(2));
    tablet_schema->_cols.push_back(create_int_value(3));
    tablet_schema->_num_columns = 4;
    tablet_schema->_num_key_columns = 3;
    tablet_schema->_num_short_key_columns = 3;

    // test encoding with padding
    {
        RowCursor row;
        static_cast<void>(row.init(tablet_schema, 2));

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
            EXPECT_STREQ("0280003039028000D43100", hexdump(buf.c_str(), buf.size()).c_str());
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
                EXPECT_STREQ("028000D43101FF", hexdump(buf.c_str(), buf.size()).c_str());
            }
            // encode key
            {
                std::string buf;
                encode_key(&buf, row, 2);
                // should be \x02\x80\x00\xD4\x31\x01
                EXPECT_STREQ("028000D43101", hexdump(buf.c_str(), buf.size()).c_str());
            }
        }
    }
}

} // namespace doris
