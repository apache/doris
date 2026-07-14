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

#include "format/table/partition_column_filler.h"

#include <gtest/gtest.h>

#include <memory>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"

namespace doris {

TEST(PartitionColumnFillerTest, DistinguishLegacyAndExplicitNullMarkers) {
    SlotDescriptor string_slot;
    string_slot._type = make_nullable(std::make_shared<DataTypeString>());
    string_slot._col_name = "part_col";

    auto legacy_null_column = string_slot.get_empty_mutable_column();
    ASSERT_TRUE(fill_partition_column_from_path_value(*legacy_null_column, string_slot, "\\N", 2,
                                                      false, false)
                        .ok());
    const auto& legacy_nullable = assert_cast<const ColumnNullable&>(*legacy_null_column);
    ASSERT_EQ(legacy_nullable.size(), 2);
    EXPECT_TRUE(legacy_nullable.is_null_at(0));
    EXPECT_TRUE(legacy_nullable.is_null_at(1));

    auto literal_null_marker_column = string_slot.get_empty_mutable_column();
    ASSERT_TRUE(fill_partition_column_from_path_value(*literal_null_marker_column, string_slot,
                                                      "\\N", 1, true, false)
                        .ok());
    const auto& literal_nullable = assert_cast<const ColumnNullable&>(*literal_null_marker_column);
    ASSERT_EQ(literal_nullable.size(), 1);
    EXPECT_FALSE(literal_nullable.is_null_at(0));
    EXPECT_EQ(literal_nullable.get_nested_column().get_data_at(0).to_string(), "\\N");

    auto empty_string_column = string_slot.get_empty_mutable_column();
    ASSERT_TRUE(fill_partition_column_from_path_value(*empty_string_column, string_slot, "", 1,
                                                      true, false)
                        .ok());
    const auto& empty_string_nullable = assert_cast<const ColumnNullable&>(*empty_string_column);
    ASSERT_EQ(empty_string_nullable.size(), 1);
    EXPECT_FALSE(empty_string_nullable.is_null_at(0));
    EXPECT_EQ(empty_string_nullable.get_nested_column().get_data_at(0).to_string(), "");

    SlotDescriptor int_slot;
    int_slot._type = make_nullable(std::make_shared<DataTypeInt32>());
    int_slot._col_name = "int_part_col";
    auto explicit_null_column = int_slot.get_empty_mutable_column();
    ASSERT_TRUE(fill_partition_column_from_path_value(*explicit_null_column, int_slot, "", 1, true,
                                                      true)
                        .ok());
    const auto& explicit_nullable = assert_cast<const ColumnNullable&>(*explicit_null_column);
    ASSERT_EQ(explicit_nullable.size(), 1);
    EXPECT_TRUE(explicit_nullable.is_null_at(0));
}

TEST(PartitionColumnFillerTest, RestoreColumnAfterDeserializeFailure) {
    SlotDescriptor int_slot;
    int_slot._type = std::make_shared<DataTypeInt32>();
    int_slot._col_name = "int_part_col";
    auto column = int_slot.get_empty_mutable_column();

    auto status =
            fill_partition_column_from_path_value(*column, int_slot, "not_an_int", 1, false, false);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(column->size(), 0);
}

} // namespace doris
