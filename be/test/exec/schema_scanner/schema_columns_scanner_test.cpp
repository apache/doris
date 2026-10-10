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

#include "information_schema/schema_columns_scanner.h"

#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/define_primitive_type.h"

namespace doris {

TEST(SchemaColumnsScannerTest, time_metadata_in_block) {
    struct TestCase {
        TPrimitiveType::type type;
        int scale;
        const char* data_type;
        const char* column_type;
    };
    const std::vector<TestCase> cases = {
            {TPrimitiveType::TIMEV2, -1, "time", "time"},
            {TPrimitiveType::TIMEV2, 0, "time", "time"},
            {TPrimitiveType::TIMEV2, 3, "time", "time(3)"},
            {TPrimitiveType::TIMEV2, 6, "time", "time(6)"},
            {TPrimitiveType::DATETIMEV2, 6, "datetime", "datetime(6)"},
            {TPrimitiveType::DECIMAL64, 3, "decimal", "decimalv3(18, 3)"},
    };

    SchemaColumnsScanner scanner;
    scanner._db_result.__set_dbs({"test_time_metadata"});
    scanner._db_index = 1;
    scanner._table_result.__set_tables({"time_view"});
    scanner._table_index = 1;
    // Feed the same type/precision/scale fields as FE describe_tables, without an RPC.
    for (size_t i = 0; i < cases.size(); ++i) {
        TColumnDesc desc;
        desc.__set_columnName("c" + std::to_string(i));
        desc.__set_columnType(cases[i].type);
        desc.__set_columnPrecision(18);
        if (cases[i].scale >= 0) {
            desc.__set_columnScale(cases[i].scale);
        }
        TColumnDef column;
        column.__set_columnDesc(desc);
        scanner._desc_result.columns.push_back(column);
    }
    scanner._desc_result.__set_tables_offset({static_cast<int>(cases.size())});

    auto block = Block::create_unique();
    scanner._init_block(block.get());
    ASSERT_TRUE(scanner._fill_block_impl(block.get()).ok());
    ASSERT_EQ(cases.size(), block->rows());
    const auto value_at = [&](const char* name, size_t row) {
        return (*block->get_by_position(block->get_position_by_name(name)).column)[row];
    };
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(i);
        EXPECT_EQ(cases[i].data_type, value_at("DATA_TYPE", i).get<TYPE_STRING>());
        EXPECT_EQ(cases[i].column_type, value_at("COLUMN_TYPE", i).get<TYPE_STRING>());
        const auto datetime_precision = value_at("DATETIME_PRECISION", i);
        if (cases[i].type == TPrimitiveType::DECIMAL64 || cases[i].scale < 0) {
            EXPECT_TRUE(datetime_precision.is_null());
        } else {
            ASSERT_FALSE(datetime_precision.is_null());
            EXPECT_EQ(cases[i].scale, datetime_precision.get<TYPE_BIGINT>());
        }
        for (const auto* name : {"NUMERIC_PRECISION", "NUMERIC_SCALE", "DECIMAL_DIGITS"}) {
            const auto value = value_at(name, i);
            if (cases[i].type == TPrimitiveType::DECIMAL64) {
                ASSERT_FALSE(value.is_null());
                EXPECT_EQ(std::string(name) == "NUMERIC_PRECISION" ? 18 : 3,
                          value.get<TYPE_BIGINT>());
            } else {
                EXPECT_TRUE(value.is_null());
            }
        }
    }
}

} // namespace doris
