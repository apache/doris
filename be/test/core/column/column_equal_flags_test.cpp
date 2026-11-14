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

// build_equal_flags_for_column() compares a column of a key type by one loop for its type. Its
// flags must be the ones IColumn::compare_at() gives row by row, the comparison the merge orders
// the rows by: for every type a key can have, nullable or not, for several key columns of one
// Block, and for the columns it compares by compare_at() itself.

#include "core/column/column_equal_flags.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "core/value/vdatetime_value.h"

namespace doris {

namespace {

// A row value of a test column: one of a few distinct values, or NULL.
constexpr int64_t NULL_VALUE = -1;

// Append `value` to `column` of `type`: the bytes of `value` for a fixed-width type, "key<value>"
// for a string.
void insert_value(IColumn& column, PrimitiveType type, int64_t value) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (value == NULL_VALUE) {
            nullable->insert_default();
            return;
        }
        insert_value(nullable->get_nested_column(), type, value);
        nullable->get_null_map_data().push_back(0);
        return;
    }
    DORIS_CHECK_NE(value, NULL_VALUE);
    const bool inserted = dispatch_switch_all(type, [&](auto dispatch) {
        using ColumnType =
                typename PrimitiveTypeTraits<std::decay_t<decltype(dispatch)>::PType>::ColumnType;
        auto& typed = assert_cast<ColumnType&>(column);
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const std::string data = "key" + std::to_string(value);
            typed.insert_data(data.data(), data.size());
        } else if constexpr (std::is_same_v<typename ColumnType::value_type, VecDateTimeValue>) {
            // These compare their packed date and time fields: 2020-01-<value + 1>.
            VecDateTimeValue data;
            DORIS_CHECK(data.from_olap_date((2020 << 9) | (1 << 5) | (value + 1)));
            typed.get_data().push_back(data);
        } else {
            typename ColumnType::value_type data {};
            std::memcpy(&data, &value, std::min(sizeof(data), sizeof(value)));
            typed.get_data().push_back(data);
        }
        return true;
    });
    DORIS_CHECK(inserted);
}

// One column per type of `types`, one row per entry of `rows`.
Block make_block(const std::vector<DataTypePtr>& types,
                 const std::vector<std::vector<int64_t>>& rows) {
    Block block;
    for (size_t c = 0; c < types.size(); ++c) {
        auto column = types[c]->create_column();
        for (const auto& row : rows) {
            insert_value(*column, remove_nullable(types[c])->get_primitive_type(), row[c]);
        }
        block.insert(ColumnWithTypeAndName(std::move(column), types[c], "c" + std::to_string(c)));
    }
    return block;
}

// The flags compare_at() gives, row by row: 1 on the rows that equal the row before on every
// column, and on the first row, which build_equal_flags_for_column() leaves as it is.
EqualFlags compare_at_flags(const Block& block) {
    EqualFlags flags(block.rows(), 1);
    for (size_t row = 1; row < block.rows(); ++row) {
        for (size_t c = 0; c < block.columns(); ++c) {
            const IColumn& column = *block.get_by_position(c).column;
            if (column.compare_at(row, row - 1, column, -1) != 0) {
                flags[row] = 0;
            }
        }
    }
    return flags;
}

// The flags of build_equal_flags_for_column() for every column of `block`.
EqualFlags equal_flags(const Block& block) {
    EqualFlags flags(block.rows(), 1);
    for (size_t c = 0; c < block.columns(); ++c) {
        const ColumnWithTypeAndName& column = block.get_by_position(c);
        build_equal_flags_for_column(*column.column,
                                     remove_nullable(column.type)->get_primitive_type(), &flags);
    }
    return flags;
}

// One column with runs of equal values; NULL_VALUE marks NULLs.
Block one_column_block(const DataTypePtr& type) {
    const int64_t null_or_four = type->is_nullable() ? NULL_VALUE : 4;
    return make_block({type}, {{0},
                               {0},
                               {1},
                               {1},
                               {2},
                               {2},
                               {2},
                               {null_or_four},
                               {null_or_four},
                               {2},
                               {null_or_four},
                               {3},
                               {3}});
}

} // namespace

TEST(ColumnEqualFlagsTest, FlagsTheRowsOfEveryKeyTypeAsCompareAtDoes) {
    struct TypeSpec {
        PrimitiveType type;
        int precision = 0;
        int scale = 0;
    };
    const std::vector<TypeSpec> types = {
            {.type = TYPE_BOOLEAN},
            {.type = TYPE_TINYINT},
            {.type = TYPE_SMALLINT},
            {.type = TYPE_INT},
            {.type = TYPE_BIGINT},
            {.type = TYPE_LARGEINT},
            {.type = TYPE_DECIMALV2, .precision = 27, .scale = 9},
            {.type = TYPE_DECIMAL32, .precision = 9, .scale = 2},
            {.type = TYPE_DECIMAL64, .precision = 18, .scale = 2},
            {.type = TYPE_DECIMAL128I, .precision = 38, .scale = 2},
            {.type = TYPE_DECIMAL256, .precision = 76, .scale = 2},
            {.type = TYPE_DATE},
            {.type = TYPE_DATETIME},
            {.type = TYPE_DATEV2},
            {.type = TYPE_DATETIMEV2, .scale = 3},
            {.type = TYPE_IPV4},
            {.type = TYPE_IPV6},
            {.type = TYPE_CHAR},
            {.type = TYPE_VARCHAR},
            {.type = TYPE_STRING},
    };
    for (const TypeSpec& spec : types) {
        for (bool nullable : {false, true}) {
            auto type = DataTypeFactory::instance().create_data_type(spec.type, nullable,
                                                                     spec.precision, spec.scale);
            SCOPED_TRACE(type->get_name());
            const Block block = one_column_block(type);
            const EqualFlags flags = equal_flags(block);
            EXPECT_EQ(flags, compare_at_flags(block));
            EXPECT_EQ(flags, (EqualFlags {1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 0, 0, 1}));
        }
    }
}

// Over several key columns a row keeps its flag only when every column equals the row before;
// the flag of the first row is left as it is.
TEST(ColumnEqualFlagsTest, KeepsAFlagWhenEveryColumnRepeats) {
    const Block block =
            make_block({DataTypeFactory::instance().create_data_type(TYPE_INT, false),
                        DataTypeFactory::instance().create_data_type(TYPE_STRING, true)},
                       {{1, 1},
                        {1, 1},
                        {1, 2},
                        {2, 2},
                        {2, 2},
                        {2, NULL_VALUE},
                        {2, NULL_VALUE},
                        {3, NULL_VALUE}});
    const EqualFlags flags = equal_flags(block);
    EXPECT_EQ(flags, compare_at_flags(block));
    EXPECT_EQ(flags, (EqualFlags {1, 1, 0, 0, 1, 0, 1, 0}));

    EqualFlags first_row_cleared(block.rows(), 1);
    first_row_cleared[0] = 0;
    build_equal_flags_for_column(*block.get_by_position(0).column, TYPE_INT, &first_row_cleared);
    EXPECT_EQ(first_row_cleared[0], 0);
}

// Any null-map byte other than 0 is a NULL, as ColumnNullable::is_null_at() reads it: NULLs are
// equal whatever their bytes and the values under them.
TEST(ColumnEqualFlagsTest, TakesEveryNonZeroNullByteAsNull) {
    auto nested = ColumnInt32::create();
    auto null_map = ColumnUInt8::create();
    for (auto [value, null_byte] : std::vector<std::pair<int32_t, uint8_t>> {
                 {0, 1}, {0, 255}, {0, 2}, {7, 2}, {5, 254}, {5, 0}, {5, 0}}) {
        nested->insert_value(value);
        null_map->insert_value(null_byte);
    }
    Block block;
    block.insert(ColumnWithTypeAndName(
            ColumnNullable::create(std::move(nested), std::move(null_map)),
            DataTypeFactory::instance().create_data_type(TYPE_INT, true), "k"));
    const EqualFlags flags = equal_flags(block);
    EXPECT_EQ(flags, compare_at_flags(block));
    EXPECT_EQ(flags, (EqualFlags {1, 1, 1, 1, 1, 0, 1}));
}

// A column that is not of a key type, or not of the column class of its type, is compared by
// compare_at() row by row.
TEST(ColumnEqualFlagsTest, ComparesOtherColumnsByCompareAt) {
    const Block doubles =
            one_column_block(DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false));
    EXPECT_EQ(equal_flags(doubles), compare_at_flags(doubles));

    auto column = ColumnString64::create();
    for (const char* value : {"a", "a", "b", "b", "c"}) {
        column->insert_data(value, std::strlen(value));
    }
    Block strings;
    strings.insert(
            ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), "s"));
    const EqualFlags flags = equal_flags(strings);
    EXPECT_EQ(flags, compare_at_flags(strings));
    EXPECT_EQ(flags, (EqualFlags {1, 1, 0, 1, 0}));
}

} // namespace doris
