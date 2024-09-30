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

#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class ColumnTest : public ::testing::Test {
protected:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);
    }

    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
};

TEST_F(ColumnTest, CutColumnString) {
    auto cut_col = col_str->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(cut_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(cut_col->get_data_at(1), StringRef("bb"));
    EXPECT_THROW({ col_str->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, CutColumnInt64) {
    auto cut_col = col_int->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnInt64*>(cut_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(cut_col.get())->get_element(1), 2);
    EXPECT_THROW({ col_int->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, CutColumnDecimal64) {
    auto cut_col = col_dcm->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(cut_col.get())->get_element(0), Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(cut_col.get())->get_element(1), Decimal64(4.56));
    EXPECT_THROW({ col_dcm->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, ShrinkColumnString) {
    auto shrunk_col = col_str->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(shrunk_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(shrunk_col->get_data_at(1), StringRef("bb"));

    shrunk_col = shrunk_col->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(shrunk_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(shrunk_col->get_data_at(1), StringRef("bb"));
    // column string scale in will not clear
}

TEST_F(ColumnTest, ShrinkColumnInt64) {
    auto shrunk_col = col_int->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(1), 2);

    shrunk_col = col_int->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(1), 2);
    // column vector scale out will not empty init
}

TEST_F(ColumnTest, ShrinkColumnDecimal64) {
    auto shrunk_col = col_dcm->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(0),
              Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(1),
              Decimal64(4.56));

    shrunk_col = col_dcm->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(0),
              Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(1),
              Decimal64(4.56));
    // column decimal scale out will not empty init
}
} // namespace doris::vectorized
