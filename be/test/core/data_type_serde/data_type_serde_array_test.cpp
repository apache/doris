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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type_serde/data_type_array_serde.h"
#include "core/data_type_serde/data_type_nullable_serde.h"
#include "core/data_type_serde/data_type_number_serde.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/field.h"
#include "storage/olap_common.h"

namespace doris {

// Append one tagged scalar cell (the variant sparse-column binary layout: a 1-byte
// FieldType tag followed by the value) by serializing a single-element column.
static void append_string(ColumnString::Chars& chars, const std::string& v) {
    auto col = ColumnString::create();
    col->insert_data(v.data(), v.size());
    DataTypeStringSerDe(TYPE_STRING).write_one_cell_to_binary(*col, chars, 0);
}
static void append_bigint(ColumnString::Chars& chars, int64_t v) {
    auto col = ColumnInt64::create();
    col->insert_value(v);
    DataTypeNumberSerDe<TYPE_BIGINT>().write_one_cell_to_binary(*col, chars, 0);
}
static void append_int(ColumnString::Chars& chars, int32_t v) {
    auto col = ColumnInt32::create();
    col->insert_value(v);
    DataTypeNumberSerDe<TYPE_INT>().write_one_cell_to_binary(*col, chars, 0);
}
static void append_double(ColumnString::Chars& chars, double v) {
    auto col = ColumnFloat64::create();
    col->insert_value(v);
    DataTypeNumberSerDe<TYPE_DOUBLE>().write_one_cell_to_binary(*col, chars, 0);
}
// Write the array header (ARRAY tag + element count); elements are appended by the caller.
static void append_array_header(ColumnString::Chars& chars, size_t n) {
    const auto tag = static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_ARRAY);
    chars.push_back(tag);
    const size_t old_size = chars.size();
    chars.resize(old_size + sizeof(size_t));
    memcpy(chars.data() + old_size, &n, sizeof(size_t));
}

// Regression for DORIS-26221: DataTypeArraySerDe::deserialize_binary_to_field used to set the
// array element type to the last non-null element's type. For a mixed-type array such as
// ["1", 2, 1.1] that yielded array<double> and dropped the string element, crashing later when
// the field was re-inserted (e.g. AGGREGATE-key merge over a variant sparse column). The element
// type must be the least common (JSONB-aware) supertype instead.
class DataTypeArraySerDeFieldTest : public ::testing::Test {};

TEST_F(DataTypeArraySerDeFieldTest, mixed_type_array_resolves_to_jsonb) {
    // ["1", 2, 1.1]  -> string, bigint, double
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 3);
    append_string(chars, "1");
    append_bigint(chars, 2);
    append_double(chars, 1.1);

    Field field;
    FieldInfo info;
    const uint8_t* end = DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(end, chars.data() + chars.size());

    // Element type is the common JSONB supertype, NOT the last element's DOUBLE.
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_JSONB);
    EXPECT_TRUE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 1);

    // The element values keep their original types in the reconstructed field.
    ASSERT_EQ(field.get_type(), PrimitiveType::TYPE_ARRAY);
    const auto& arr = field.get<TYPE_ARRAY>();
    ASSERT_EQ(arr.size(), 3);
    EXPECT_EQ(arr[0].get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(arr[1].get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(arr[2].get_type(), PrimitiveType::TYPE_DOUBLE);
}

TEST_F(DataTypeArraySerDeFieldTest, mixed_type_array_last_element_string_resolves_to_jsonb) {
    // [1, 2, "3"]: last element is string; last-wins would wrongly pick array<string>.
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 3);
    append_bigint(chars, 1);
    append_bigint(chars, 2);
    append_string(chars, "3");

    Field field;
    FieldInfo info;
    DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_JSONB);
    EXPECT_TRUE(info.need_convert);
}

TEST_F(DataTypeArraySerDeFieldTest, numeric_only_array_promotes_to_double) {
    // [2, 1.1]: int + double unify to the numeric supertype double (not JSONB) -- numeric
    // promotion still works, only string/number mixes fall back to JSONB.
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 2);
    append_int(chars, 2);
    append_double(chars, 1.1);

    Field field;
    FieldInfo info;
    DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_DOUBLE);
    EXPECT_TRUE(info.need_convert);
}

TEST_F(DataTypeArraySerDeFieldTest, homogeneous_array_keeps_element_type) {
    // [1, 2, 3]: single element type, no conversion needed.
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 3);
    append_bigint(chars, 1);
    append_bigint(chars, 2);
    append_bigint(chars, 3);

    Field field;
    FieldInfo info;
    DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_BIGINT);
    EXPECT_FALSE(info.need_convert);
    EXPECT_EQ(info.num_dimensions, 1);
}

TEST_F(DataTypeArraySerDeFieldTest, nested_mixed_array_resolves_to_jsonb) {
    // [[1], ["a"]]: inner arrays have different element types -> outer base type is JSONB.
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 2);
    append_array_header(chars, 1); // [1]
    append_bigint(chars, 1);
    append_array_header(chars, 1); // ["a"]
    append_string(chars, "a");

    Field field;
    FieldInfo info;
    DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_JSONB);
    EXPECT_TRUE(info.need_convert);
    ASSERT_EQ(field.get_type(), PrimitiveType::TYPE_ARRAY);
    EXPECT_EQ(field.get<TYPE_ARRAY>().size(), 2);
}

TEST_F(DataTypeArraySerDeFieldTest, empty_array_is_null_element_type) {
    auto chars_col = ColumnString::create();
    ColumnString::Chars& chars = chars_col->get_chars();
    append_array_header(chars, 0);

    Field field;
    FieldInfo info;
    DataTypeSerDe::deserialize_binary_to_field(chars.data(), field, info);
    EXPECT_EQ(info.scalar_type_id, PrimitiveType::TYPE_NULL);
    EXPECT_FALSE(info.need_convert);
}

TEST_F(DataTypeArraySerDeFieldTest, ReadArrowFixedSizeAndLargeList) {
    auto nested_serde = std::make_shared<DataTypeNullableSerDe>(
            std::make_shared<DataTypeNumberSerDe<TYPE_FLOAT>>());
    DataTypeArraySerDe serde(nested_serde);
    cctz::time_zone tz;

    const auto expect_array = [](const ColumnArray& column,
                                 const std::vector<ColumnArray::Offset64>& expected_offsets,
                                 const std::vector<float>& expected_values) {
        const auto& offsets = column.get_offsets();
        ASSERT_EQ(expected_offsets.size(), offsets.size());
        for (size_t i = 0; i < expected_offsets.size(); ++i) {
            EXPECT_EQ(expected_offsets[i], offsets[i]);
        }
        const auto& nullable_values = assert_cast<const ColumnNullable&>(column.get_data());
        const auto& values =
                assert_cast<const ColumnFloat32&>(nullable_values.get_nested_column()).get_data();
        ASSERT_EQ(expected_values.size(), values.size());
        for (size_t i = 0; i < expected_values.size(); ++i) {
            EXPECT_EQ(0, nullable_values.get_null_map_data()[i]);
            EXPECT_FLOAT_EQ(expected_values[i], values[i]);
        }
    };

    // Lance vectors are Arrow FixedSizeList: every embedding has exactly three floats.
    {
        auto value_builder = std::make_shared<arrow::FloatBuilder>();
        arrow::FixedSizeListBuilder builder(arrow::default_memory_pool(), value_builder, 3);
        for (const std::array<float, 3>& embedding :
             {std::array<float, 3> {0.0F, 0.0F, 0.0F}, std::array<float, 3> {1.0F, 0.0F, 0.0F},
              std::array<float, 3> {-1.5F, 0.25F, 3.75F}}) {
            ASSERT_TRUE(builder.Append().ok());
            for (float value : embedding) {
                ASSERT_TRUE(value_builder->Append(value).ok());
            }
        }
        std::shared_ptr<arrow::FixedSizeListArray> arrow_array;
        ASSERT_TRUE(builder.Finish(&arrow_array).ok());

        auto column = ColumnArray::create(
                ColumnNullable::create(ColumnFloat32::create(), ColumnUInt8::create()),
                ColumnOffset64::create());
        ASSERT_TRUE(serde.read_column_from_arrow(*column, arrow_array.get(), 0,
                                                 arrow_array->length(), tz)
                            .ok());
        expect_array(*column, {3, 6, 9}, {0.0F, 0.0F, 0.0F, 1.0F, 0.0F, 0.0F, -1.5F, 0.25F, 3.75F});
    }

    // LargeList uses 64-bit Arrow offsets but has the same Doris ARRAY representation.
    {
        auto value_builder = std::make_shared<arrow::FloatBuilder>();
        arrow::LargeListBuilder builder(arrow::default_memory_pool(), value_builder);
        ASSERT_TRUE(builder.Append().ok());
        ASSERT_TRUE(value_builder->Append(1.0F).ok());
        ASSERT_TRUE(value_builder->Append(2.0F).ok());
        ASSERT_TRUE(builder.Append().ok());
        ASSERT_TRUE(value_builder->Append(3.0F).ok());
        ASSERT_TRUE(builder.Append().ok());
        std::shared_ptr<arrow::LargeListArray> arrow_array;
        ASSERT_TRUE(builder.Finish(&arrow_array).ok());

        auto column = ColumnArray::create(
                ColumnNullable::create(ColumnFloat32::create(), ColumnUInt8::create()),
                ColumnOffset64::create());
        ASSERT_TRUE(serde.read_column_from_arrow(*column, arrow_array.get(), 0,
                                                 arrow_array->length(), tz)
                            .ok());
        expect_array(*column, {2, 3, 3}, {1.0F, 2.0F, 3.0F});
    }
}

} // namespace doris
