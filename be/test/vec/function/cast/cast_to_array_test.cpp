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

#include <cstdint>
#include <memory>
#include <variant>
#include <vector>

#include "cast_test.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/column_helper.h"
#include "vec/columns/column_array.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/cast/cast_base.h"

namespace doris::vectorized {
using namespace ut_type;

template <typename DataType>
struct ColumnArrayBuilder {
    using FieldType = typename DataType::FieldType;
    using Ty = std::variant<FieldType, NullTag>;

    void add(const std::vector<Ty>& values) {
        for (const auto& value : values) {
            if (std::holds_alternative<FieldType>(value)) {
                nested_values.push_back(std::get<FieldType>(value));
                nested_null_map.push_back(false);
            } else {
                nested_values.push_back(FieldType {});
                nested_null_map.push_back(true);
            }
        }
        size += values.size();
        null_map.push_back(0);
        offsets.push_back(size);
    }

    ColumnWithTypeAndName build() {
        auto nested_column =
                ColumnHelper::create_nullable_column<DataType>(nested_values, nested_null_map);
        auto offsets_column = ColumnHelper::create_column_offsets<TYPE_UINT64>(offsets);
        auto col_array = ColumnArray::create(std::move(nested_column), offsets_column);
        auto col_null_map = ColumnHelper::create_column<DataTypeUInt8>(null_map);
        auto col_nullable = ColumnNullable::create(std::move(col_array), std::move(col_null_map));
        auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeNullable>(std::make_shared<DataType>())));

        return ColumnWithTypeAndName(std::move(col_nullable), data_type, "column");
    }

    void add_null() {
        null_map.push_back(1);
        offsets.push_back(size);
    }

    int size = 0;
    std::vector<FieldType> nested_values;
    std::vector<DataTypeUInt8::FieldType> nested_null_map;
    std::vector<uint64_t> offsets;
    std::vector<DataTypeUInt8::FieldType> null_map;
};

TEST_F(FunctionCastTest, test_from_string_to_array_int) {
    ColumnArrayBuilder<DataTypeInt32> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123,456])");
    builder.add({123, 456});

    from_str.push_back(R"([789, 101112])");
    builder.add({789, 101112});

    from_str.push_back(R"([1, 2, 3])");
    builder.add({1, 2, 3});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, 101112});

    from_str.push_back(R"([1, 2, 3, 4])");
    builder.add({1, 2, 3, 4});

    from_str.push_back(R"([)");
    builder.add_null();

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_double) {
    ColumnArrayBuilder<DataTypeFloat64> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123.456, 789.101])");
    builder.add({123.456, 789.101});

    from_str.push_back(R"([1.2, 3.4, 5.6])");
    builder.add({1.2, 3.4, 5.6});

    from_str.push_back(R"(["1.2", '3.4', "5.6"])");
    builder.add({1.2, 3.4, 5.6});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, 101112.0});

    from_str.push_back(R"([1.2, 3.4, 5.6, 7.8])");
    builder.add({1.2, 3.4, 5.6, 7.8});

    from_str.push_back(R"([)");
    builder.add_null();

    from_str.push_back(R"([])");
    builder.add({});

    from_str.push_back(R"([      ])");
    builder.add({NullTag {}});

    from_str.push_back(R"([123;123])");
    builder.add({NullTag {}});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_string) {
    ColumnArrayBuilder<DataTypeString> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([123,456])");
    builder.add({"123", "456"});

    from_str.push_back(R"([789, 101112])");
    builder.add({"789", "101112"});

    from_str.push_back(R"([1, 2, 3])");
    builder.add({"1", "2", "3"});

    from_str.push_back(R"([abc, 101112])");
    builder.add({"abc", "101112"});

    from_str.push_back(R"([1, 2, 3, 4])");
    builder.add({"1", "2", "3", "4"});

    from_str.push_back(R"([)");
    builder.add_null();

    from_str.push_back(R"([])");
    builder.add({});

    from_str.push_back(R"([      ])");
    builder.add({""});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

TEST_F(FunctionCastTest, test_from_string_to_array_bool) {
    ColumnArrayBuilder<DataTypeBool> builder;

    std::vector<std::string> from_str;
    from_str.push_back(R"([true, false])");
    builder.add({true, false});

    from_str.push_back(R"([1, 0])");
    builder.add({true, false});

    from_str.push_back(R"(['true', "0", "false"])");
    builder.add({true, false, false});

    from_str.push_back(R"([abc, 101112])");
    builder.add({NullTag {}, NullTag {}});

    check_cast(ColumnWithTypeAndName(ColumnHelper::create_column<DataTypeString>(from_str),
                                     std::make_shared<DataTypeString>(), "from"),
               builder.build(), false);
}

} // namespace doris::vectorized
