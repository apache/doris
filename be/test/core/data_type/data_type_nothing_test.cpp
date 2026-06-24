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

#include "core/data_type/data_type_nothing.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include "core/column/column_nothing.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "gen_cpp/types.pb.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/slice.h"

namespace doris {

TEST(DataTypeNothingTest, GetFieldWithDataTypeTest) {
    // 1. Create a DataTypeNothing instance and a corresponding column
    DataTypeNothing data_type_nothing;
    auto column = data_type_nothing.create_column();
    column->insert_default(); // A Nothing column can only store empty rows.

    // 2. Call the function under test
    FieldWithDataType fdt = data_type_nothing.get_field_with_data_type(*column, 0);

    // 3. Assert the results
    // The returned FieldWithDataType should contain a default-constructed Field,
    // which is a Null field.
    EXPECT_TRUE(fdt.field.is_null());
    EXPECT_EQ(fdt.field.get_type(), PrimitiveType::TYPE_NULL);

    // Other metadata should have their default values.
    EXPECT_EQ(fdt.base_scalar_type_id, PrimitiveType::INVALID_TYPE);
    EXPECT_EQ(fdt.num_dimensions, 0);
    EXPECT_EQ(fdt.precision, -1);
    EXPECT_EQ(fdt.scale, -1);
}

TEST(DataTypeNothingTest, BasicTypeProperties) {
    DataTypeNothing data_type_nothing;

    EXPECT_EQ(data_type_nothing.get_family_name(), "Nothing");
    EXPECT_EQ(data_type_nothing.get_primitive_type(), PrimitiveType::INVALID_TYPE);
    EXPECT_EQ(data_type_nothing.get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_NONE);
    EXPECT_TRUE(data_type_nothing.have_maximum_size_of_value());
    EXPECT_EQ(data_type_nothing.get_size_of_value_in_memory(), 0);

    auto column = data_type_nothing.create_column();
    EXPECT_TRUE(data_type_nothing.check_column(*column).ok());
    EXPECT_FALSE(data_type_nothing.check_column(*ColumnUInt8::create()).ok());

    EXPECT_TRUE(data_type_nothing.equals(DataTypeNothing()));
    EXPECT_EQ(data_type_nothing.get_uncompressed_serialized_bytes(*column, 0), 0);
    EXPECT_THROW(data_type_nothing.serialize(*column, nullptr, 0), doris::Exception);
    EXPECT_THROW(data_type_nothing.deserialize(nullptr, &column, 0), doris::Exception);

    TExprNode node;
    EXPECT_THROW(data_type_nothing.get_field(node), doris::Exception);
    EXPECT_EQ(data_type_nothing.get_serde()->get_name(), "Nothing");
}

TEST(DataTypeNothingSerdeTest, UnsupportedFormatsReturnErrorsOrThrow) {
    DataTypeNothingSerde serde;
    auto column = ColumnNothing::create(0);
    DataTypeSerDe::FormatOptions options;

    auto string_column = ColumnString::create();
    VectorBufferWriter writer(*string_column);
    Slice slice;
    std::vector<Slice> slices;
    uint64_t num_deserialized = 0;
    PValues values;
    MysqlRowBinaryBuffer mysql_buffer;

    EXPECT_EQ(serde.get_name(), "Nothing");
    EXPECT_FALSE(serde.serialize_one_cell_to_json(*column, 0, writer, options).ok());
    EXPECT_FALSE(serde.serialize_column_to_json(*column, 0, 0, writer, options).ok());
    EXPECT_FALSE(serde.deserialize_one_cell_from_json(*column, slice, options).ok());
    EXPECT_FALSE(
            serde.deserialize_column_from_json_vector(*column, slices, &num_deserialized, options)
                    .ok());
    EXPECT_FALSE(serde.write_column_to_pb(*column, values, 0, 0).ok());
    EXPECT_FALSE(serde.read_column_from_pb(*column, values).ok());
    EXPECT_FALSE(serde.write_column_to_arrow(*column, nullptr, nullptr, 0, 0, cctz::utc_time_zone())
                         .ok());
    EXPECT_FALSE(serde.read_column_from_arrow(*column, nullptr, 0, 0, cctz::utc_time_zone()).ok());
    EXPECT_FALSE(serde.write_column_to_mysql_binary(*column, mysql_buffer, 0, false, options).ok());
    Arena arena;
    EXPECT_FALSE(
            serde.write_column_to_orc("", *column, nullptr, nullptr, 0, 0, arena, options).ok());

    JsonbWriterT<JsonbOutStream> jsonb_writer;
    EXPECT_THROW(serde.write_one_cell_to_jsonb(*column, jsonb_writer, arena, 0, 0, options),
                 doris::Exception);
    EXPECT_THROW(serde.read_one_cell_from_jsonb(*column, nullptr), doris::Exception);
}

} // namespace doris
