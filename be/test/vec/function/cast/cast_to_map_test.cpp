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

template <typename KeyDataType, typename ValueDataType>
struct ColumnMapBuilder {
    using Ty = std::pair<std::variant<NullTag, typename KeyDataType::FieldType>,
                         std::variant<NullTag, typename ValueDataType::FieldType>>;

    void add(const std::vector<Ty>& values) {
        for (const auto& kv : values) {
            auto key = kv.first;
            auto value = kv.second;
            if (std::holds_alternative<typename KeyDataType::FieldType>(key)) {
                key_data.push_back(std::get<typename KeyDataType::FieldType>(key));
                key_null_map.push_back(0);
            } else {
                key_data.push_back(typename KeyDataType::FieldType {});
                key_null_map.push_back(1);
            }

            if (std::holds_alternative<typename ValueDataType::FieldType>(value)) {
                value_data.push_back(std::get<typename ValueDataType::FieldType>(value));
                value_null_map.push_back(0);
            } else {
                value_data.push_back(typename ValueDataType::FieldType {});
                value_null_map.push_back(1);
            }
            size++;
        }
        offsets.push_back(size);
        null_map.push_back(0);
    }

    void add_null() {
        null_map.push_back(1);
        offsets.push_back(size);
    }

    ColumnWithTypeAndName build() {
        auto key_column = ColumnHelper::create_nullable_column<KeyDataType>(key_data, key_null_map);
        auto value_column =
                ColumnHelper::create_nullable_column<ValueDataType>(value_data, value_null_map);
        auto offsets_column = ColumnHelper::create_column_offsets<TYPE_UINT64>(offsets);

        auto col_map = ColumnMap::create(std::move(key_column), std::move(value_column),
                                         std::move(offsets_column));
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_map), col_null_map);
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeNullable>(std::make_shared<KeyDataType>()),
                std::make_shared<DataTypeNullable>(std::make_shared<ValueDataType>())));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column");
    }

    std::vector<typename KeyDataType::FieldType> key_data;
    std::vector<typename ValueDataType::FieldType> value_data;
    std::vector<uint8_t> key_null_map;
    std::vector<uint8_t> value_null_map;
    std::vector<uint8_t> null_map;
    std::vector<uint64_t> offsets;
    int size = 0;
};

TEST_F(FunctionCastTest, test_from_string_to_map_int_string) {
    ColumnMapBuilder<DataTypeInt32, DataTypeString> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({123:456})");
    builder.add({{123, "456"}});

    from_str.push_back(R"({789:101112})");
    builder.add({{789, "101112"}});

    from_str.push_back(R"({1:2, 3:4})");
    builder.add({{1, "2"}, {3, "4"}});

    from_str.push_back(R"({abc:101112})");
    builder.add({{NullTag {}, "101112"}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_map_int_bool) {
    ColumnMapBuilder<DataTypeInt32, DataTypeBool> builder;
    std::vector<std::string> from_str;
    from_str.push_back(R"({123:true})");
    builder.add({{123, true}});

    from_str.push_back(R"({789:false})");
    builder.add({{789, false}});

    from_str.push_back(R"({1:true, 3:false})");
    builder.add({{1, true}, {3, false}});

    from_str.push_back(R"({abc:true})");
    builder.add({{NullTag {}, true}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_map_string_string) {
    ColumnMapBuilder<DataTypeString, DataTypeString> builder;
    std::vector<std::string> from_str;

    from_str.push_back(R"({"abc":"def"})");
    builder.add({{"abc", "def"}});

    from_str.push_back(R"({"ghi":"jkl"})");
    builder.add({{"ghi", "jkl"}});

    from_str.push_back(R"({"mno":"pqr", "stu":"vwx"})");
    builder.add({{"mno", "pqr"}, {"stu", "vwx"}});

    from_str.push_back(R"({"xyz":"123"})");
    builder.add({{"xyz", "123"}});

    from_str.push_back(R"(  {}  )");
    builder.add_null();

    from_str.push_back(R"({})");
    builder.add({});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

} // namespace doris::vectorized
