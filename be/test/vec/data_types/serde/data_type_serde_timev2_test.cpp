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

#include <arrow/array/builder_base.h>
#include <gtest/gtest.h>

#include <string>

#include "runtime/primitive_type.h"
#include "util/slice.h"
#include "vec/columns/column_complex.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/serde/data_type_time_serde.h"

namespace doris::vectorized {

TEST(TimeV2SerdeTest, writeColumnToMysql) {
    auto timev2_serde = std::make_shared<vectorized::DataTypeTimeV2SerDe>(1);
    auto column_timev2 = ColumnTimeV2::create();
    column_timev2->insert_default();
    ASSERT_EQ(column_timev2->size(), 1);
    MysqlRowBuffer<false> mysql_rb;
    DataTypeSerDe::FormatOptions options;
    options.nested_string_wrapper = "\"";
    options.wrapper_len = 1;
    options.map_key_delim = ':';
    options.null_format = "null";
    options.null_len = 4;
    auto st = timev2_serde->write_column_to_mysql(*column_timev2, mysql_rb, 0, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 11);

    std::string time_str = "11:00:00";
    column_timev2->insert_data(time_str.c_str(), time_str.size());
    st = timev2_serde->write_column_to_mysql(*column_timev2, mysql_rb, 1, false, options);
    EXPECT_TRUE(st.ok());
    ASSERT_EQ(mysql_rb.length(), 22);
    std::cout << "test write_column_to_mysql success" << std::endl;
}

} // namespace doris::vectorized
