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

// SameKeyFlags compares a key column of a key type by one loop for its type. Its flags must be
// the ones IColumn::compare_at() gives row by row, the comparison the merge ordered the rows by:
// for every type a key can have, nullable or not, across Blocks, for several key columns, and for
// the columns it compares by compare_at() itself.

#include "storage/iterator/same_key_flags.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
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

// The flags compare_at() gives, row by row.
std::vector<std::vector<uint8_t>> compare_at_flags(const std::vector<Block>& blocks) {
    std::vector<std::vector<uint8_t>> flags;
    const Block* previous = nullptr;
    for (const Block& block : blocks) {
        auto& block_flags = flags.emplace_back(block.rows(), 1);
        for (size_t row = 0; row < block.rows(); ++row) {
            // The row before: the last row of the previous Block for the first row of a Block.
            const Block* other = &block;
            size_t other_row = row - 1;
            if (row == 0) {
                if (previous == nullptr) {
                    block_flags[row] = 0;
                    continue;
                }
                other = previous;
                other_row = previous->rows() - 1;
            }
            for (size_t c = 0; c < block.columns(); ++c) {
                if (block.get_by_position(c).column->compare_at(
                            row, other_row, *other->get_by_position(c).column, -1) != 0) {
                    block_flags[row] = 0;
                }
            }
        }
        previous = &block;
    }
    return flags;
}

std::vector<std::vector<uint8_t>> same_key_flags(const std::vector<Block>& blocks) {
    std::vector<uint32_t> key_ordinals;
    for (uint32_t c = 0; c < blocks.front().columns(); ++c) {
        key_ordinals.push_back(c);
    }
    SameKeyFlags same_key(key_ordinals);
    std::vector<std::vector<uint8_t>> flags;
    for (const Block& block : blocks) {
        same_key.compute(block, &flags.emplace_back());
    }
    return flags;
}

// Three Blocks of one column cut through runs of equal values; NULL_VALUE marks NULLs.
std::vector<Block> one_column_blocks(const DataTypePtr& type) {
    const int64_t null_or_four = type->is_nullable() ? NULL_VALUE : 4;
    std::vector<Block> blocks;
    blocks.push_back(make_block({type}, {{0}, {0}, {1}}));
    blocks.push_back(make_block({type}, {{1}, {2}, {2}, {null_or_four}}));
    blocks.push_back(make_block({type}, {{null_or_four}, {2}, {null_or_four}, {3}, {3}}));
    return blocks;
}

} // namespace

TEST(SameKeyFlagsTest, FlagsTheRowsOfEveryKeyTypeAsCompareAtDoes) {
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
            const auto blocks = one_column_blocks(type);
            const auto flags = same_key_flags(blocks);
            EXPECT_EQ(flags, compare_at_flags(blocks));
            EXPECT_EQ(flags, (std::vector<std::vector<uint8_t>> {
                                     {0, 1, 0}, {1, 0, 1, 0}, {1, 0, 0, 0, 1}}));
        }
    }
}

// A row is same only when every key column equals the row before it.
TEST(SameKeyFlagsTest, FlagsARowWhenEveryKeyColumnRepeats) {
    const std::vector<DataTypePtr> types = {
            DataTypeFactory::instance().create_data_type(TYPE_INT, false),
            DataTypeFactory::instance().create_data_type(TYPE_STRING, true)};
    std::vector<Block> blocks;
    blocks.push_back(make_block(types, {{1, 1}, {1, 1}, {1, 2}, {2, 2}}));
    blocks.push_back(
            make_block(types, {{2, 2}, {2, NULL_VALUE}, {2, NULL_VALUE}, {3, NULL_VALUE}}));
    const auto flags = same_key_flags(blocks);
    EXPECT_EQ(flags, compare_at_flags(blocks));
    EXPECT_EQ(flags, (std::vector<std::vector<uint8_t>> {{0, 1, 0, 0}, {1, 0, 1, 0}}));
}

// A column that is not of a key type, or not of the column class of its type, is compared by
// compare_at() row by row.
TEST(SameKeyFlagsTest, ComparesOtherColumnsByCompareAt) {
    auto type = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);
    const auto doubles = one_column_blocks(type);
    EXPECT_EQ(same_key_flags(doubles), compare_at_flags(doubles));

    auto string_type = std::make_shared<DataTypeString>();
    std::vector<Block> strings;
    for (const std::vector<std::string>& values :
         {std::vector<std::string> {"a", "a", "b"}, std::vector<std::string> {"b", "c"}}) {
        auto column = ColumnString64::create();
        for (const auto& value : values) {
            column->insert_data(value.data(), value.size());
        }
        Block block;
        block.insert(ColumnWithTypeAndName(std::move(column), string_type, "s"));
        strings.push_back(std::move(block));
    }
    const auto flags = same_key_flags(strings);
    EXPECT_EQ(flags, compare_at_flags(strings));
    EXPECT_EQ(flags, (std::vector<std::vector<uint8_t>> {{0, 1, 0}, {1, 0}}));
}

} // namespace doris
