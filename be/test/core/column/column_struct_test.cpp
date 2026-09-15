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

#include "core/column/column_struct.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "core/types.h"

namespace doris {

class ColumnStructTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}
};

namespace {

// Nullable(Struct<x: Nullable(Int32)>) with 4 rows. Rows 1 and 3 are NULL at the outer level
// but keep different hidden payloads in the field column, which is what IF/CASE produce when
// only one branch is a NULL literal. Rows 0 and 3 share the same payload but differ in nullness;
// NULL is hashed as the field's default value, so only the NULL/NULL equality is asserted.
struct NullableStructFixture {
    ColumnPtr nullable;
    const ColumnStruct* plain_struct = nullptr;
    std::vector<uint8_t> null_map = {0, 1, 0, 1};

    NullableStructFixture() {
        auto field = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        for (int32_t v : {0, std::numeric_limits<int32_t>::min(), 1, 0}) {
            field->insert(Field::create_field<TYPE_INT>(v));
        }
        Columns fields;
        fields.push_back(std::move(field));
        ColumnPtr struct_column = ColumnStruct::create(std::move(fields));
        plain_struct = assert_cast<const ColumnStruct*>(struct_column.get());

        auto null_map_column = ColumnUInt8::create();
        for (auto v : null_map) {
            null_map_column->insert_value(v);
        }
        nullable = ColumnNullable::create(struct_column, std::move(null_map_column));
    }
};

} // namespace

TEST_F(ColumnStructTest, Crc32cBatchRespectsOuterNullMap) {
    NullableStructFixture fixture;
    std::vector<uint32_t> hashes(4, 0);
    fixture.nullable->update_crc32c_batch(hashes.data(), nullptr);
    std::vector<uint32_t> plain_hashes(4, 0);
    fixture.plain_struct->update_crc32c_batch(plain_hashes.data(), nullptr);

    // Two logical NULLs must hash identically regardless of hidden payload.
    EXPECT_EQ(hashes[1], hashes[3]);
    // Non-NULL rows hash exactly like the plain struct column.
    EXPECT_EQ(hashes[0], plain_hashes[0]);
    EXPECT_EQ(hashes[2], plain_hashes[2]);

    // Calling the struct directly with a null map skips the NULL rows.
    std::vector<uint32_t> masked_hashes(4, 0);
    fixture.plain_struct->update_crc32c_batch(masked_hashes.data(), fixture.null_map.data());
    EXPECT_EQ(masked_hashes[0], plain_hashes[0]);
    EXPECT_EQ(masked_hashes[1], 0);
    EXPECT_EQ(masked_hashes[2], plain_hashes[2]);
    EXPECT_EQ(masked_hashes[3], 0);
}

TEST_F(ColumnStructTest, Crc32cSingleRespectsOuterNullMap) {
    NullableStructFixture fixture;
    auto hash_row = [&](const IColumn& column, size_t row) {
        uint32_t hash = 0;
        column.update_crc32c_single(row, row + 1, hash, nullptr);
        return hash;
    };
    EXPECT_EQ(hash_row(*fixture.nullable, 1), hash_row(*fixture.nullable, 3));
    EXPECT_EQ(hash_row(*fixture.nullable, 0), hash_row(*fixture.plain_struct, 0));
    EXPECT_EQ(hash_row(*fixture.nullable, 2), hash_row(*fixture.plain_struct, 2));

    // A multi-row range with a null map only hashes the non-NULL rows.
    uint32_t masked = 0;
    fixture.plain_struct->update_crc32c_single(0, 4, masked, fixture.null_map.data());
    uint32_t expected = 0;
    fixture.plain_struct->update_crc32c_single(0, 1, expected, nullptr);
    fixture.plain_struct->update_crc32c_single(2, 3, expected, nullptr);
    EXPECT_EQ(masked, expected);
}

TEST_F(ColumnStructTest, XxHashRespectsOuterNullMap) {
    NullableStructFixture fixture;
    std::vector<uint64_t> hashes(4, 0);
    fixture.nullable->update_hashes_with_value(hashes.data(), nullptr);
    std::vector<uint64_t> plain_hashes(4, 0);
    fixture.plain_struct->update_hashes_with_value(plain_hashes.data(), nullptr);
    EXPECT_EQ(hashes[1], hashes[3]);
    EXPECT_EQ(hashes[0], plain_hashes[0]);
    EXPECT_EQ(hashes[2], plain_hashes[2]);

    auto hash_row = [&](const IColumn& column, size_t row) {
        uint64_t hash = 0;
        column.update_xxHash_with_value(row, row + 1, hash, nullptr);
        return hash;
    };
    EXPECT_EQ(hash_row(*fixture.nullable, 1), hash_row(*fixture.nullable, 3));
    EXPECT_EQ(hash_row(*fixture.nullable, 0), hash_row(*fixture.plain_struct, 0));
    EXPECT_EQ(hash_row(*fixture.nullable, 2), hash_row(*fixture.plain_struct, 2));
}

TEST_F(ColumnStructTest, CrcRangeRespectsOuterNullMap) {
    NullableStructFixture fixture;
    auto hash_row = [&](const IColumn& column, size_t row) {
        uint32_t hash = 0;
        column.update_crc_with_value(row, row + 1, hash, nullptr);
        return hash;
    };
    EXPECT_EQ(hash_row(*fixture.nullable, 1), hash_row(*fixture.nullable, 3));
    EXPECT_EQ(hash_row(*fixture.nullable, 0), hash_row(*fixture.plain_struct, 0));
    EXPECT_EQ(hash_row(*fixture.nullable, 2), hash_row(*fixture.plain_struct, 2));
}

TEST_F(ColumnStructTest, StructTypeTesterase) {
    DataTypePtr key_type = (std::make_shared<DataTypeString>());
    DataTypePtr value_type = (std::make_shared<DataTypeInt32>());
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(DataTypes {key_type, value_type});
    auto column = struct_type->create_column();
    auto* column_struct = assert_cast<ColumnStruct*>(column.get());
    auto column_res = column_struct->clone_empty();
    auto& column_string = assert_cast<ColumnString&>(column_struct->get_column(0));
    auto& column_int = assert_cast<ColumnInt32&>(column_struct->get_column(1));

    std::vector<String> data_string = {"asd", "1234567", "3", "4", "5"};
    std::vector<int32_t> data_int = {1, 2, 3, 4, 5};

    for (auto d : data_string) {
        column_string.insert_data(d.data(), d.size());
    }
    for (auto d : data_int) {
        column_int.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    // Block tmp;
    // tmp.insert({std::move(column), struct_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    column_struct->erase(0, 2);
    EXPECT_EQ(column_struct->size(), 3);
    for (int i = 0; i < column_struct->size(); ++i) {
        EXPECT_EQ(column_string.get_data_at(i).to_string(), data_string[i + 2]);
        EXPECT_EQ(column_int.get_element(i), data_int[i + 2]);
    }
}

TEST_F(ColumnStructTest, StructTypeTest2erase) {
    DataTypePtr key_type = (std::make_shared<DataTypeString>());
    DataTypePtr value_type = (std::make_shared<DataTypeInt32>());
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(DataTypes {key_type, value_type});
    auto column = struct_type->create_column();
    auto* column_struct = assert_cast<ColumnStruct*>(column.get());
    auto& column_string = assert_cast<ColumnString&>(column_struct->get_column(0));
    auto& column_int = assert_cast<ColumnInt32&>(column_struct->get_column(1));

    std::vector<String> data_string = {"asd", "1234567", "3", "4", "5"};
    std::vector<int32_t> data_int = {1, 2, 3, 4, 5};

    std::vector<String> data_string_res = {"asd", "1234567", "5"};
    std::vector<int32_t> data_int_res = {1, 2, 5};

    for (auto d : data_string) {
        column_string.insert_data(d.data(), d.size());
    }
    for (auto d : data_int) {
        column_int.insert_data(reinterpret_cast<const char*>(&d), sizeof(d));
    }
    // Block tmp;
    // tmp.insert({std::move(column), struct_type, "asd"});
    // std::cout << tmp.dump_data(0, tmp.rows());

    column_struct->erase(2, 2);
    EXPECT_EQ(column_struct->size(), 3);
    for (int i = 0; i < column_struct->size(); ++i) {
        EXPECT_EQ(column_string.get_data_at(i).to_string(), data_string_res[i]);
        EXPECT_EQ(column_int.get_element(i), data_int_res[i]);
    }
}
} // namespace doris
