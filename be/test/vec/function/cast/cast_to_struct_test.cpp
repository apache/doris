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

#include <cstdint>
#include <memory>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "cast_test.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_map.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {
using namespace ut_type;

template <typename... DataType>
struct ColumnStructBuilder {
    void add(const typename ColumnBuilder<DataType>::Ty... ty) {
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 add
                    (col.add(ty), ...);
                },
                columns);
        null_map.push_back(0);
    }

    void add_null() {
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 add_null
                    (col.add(NullTag {}), ...);
                },
                columns);
        null_map.push_back(1);
    }

    ColumnWithTypeAndName build(Strings names) {
        std::vector<ColumnWithTypeAndName> nested_column;
        std::apply(
                [&](auto&... col) {
                    // 展开调用每个 ColumnBuilder 的 build
                    (nested_column.push_back(col.build()), ...);
                },
                columns);

        DataTypes data_types;
        Columns data_columns;
        for (const auto& col : nested_column) {
            data_types.push_back(col.type);
        }
        for (const auto& col : nested_column) {
            data_columns.push_back(col.column);
        }

        auto col_struct = ColumnStruct::create(std::move(data_columns));
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_struct), col_null_map);

        auto data_type = std::make_shared<DataTypeNullable>(
                std::make_shared<DataTypeStruct>(std::move(data_types), std::move(names)));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column_struct");
    }

    std::tuple<ColumnBuilder<DataType>...> columns;
    std::vector<DataTypeUInt8::FieldType> null_map;
};

TEST_F(FunctionCastTest, test_from_string_to_struct1) {
    ColumnStructBuilder<DataTypeInt32, DataTypeString, DataTypeInt32> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({123,"abc", 456})");
    builder.add(123, "abc", 456);

    from_str.push_back(R"({789,"def", 101112})");
    builder.add(789, "def", 101112);

    from_str.push_back(R"(  {}   )");
    builder.add_null();

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build({"a", "b", "c"}), false);
}

TEST_F(FunctionCastTest, test_from_string_to_struct2) {
    ColumnStructBuilder<DataTypeInt32, DataTypeBool, DataTypeInt32, DataTypeBool> builder;
    std::vector<std::string> from_str;
    from_str.push_back(R"({123,true, 456, false})");
    builder.add(123, true, 456, false);
    from_str.push_back(R"({789,false, 101112, true})");
    builder.add(789, false, 101112, true);
    from_str.push_back(R"(  {}   )");
    builder.add_null();

    from_str.push_back(R"({789,abc, 101112, true})");
    builder.add(789, NullTag {}, 101112, true);

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build({"a", "b", "c", "d"}), false);
}
} // namespace doris::vectorized
