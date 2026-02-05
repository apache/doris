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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <lz4/lz4.h>
#include <streamvbyte.h>

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "agent/be_exec_version_manager.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

namespace doris::vectorized {
static auto serde_int32 = std::make_shared<DataTypeNumberSerDe<TYPE_INT>>();
static auto serde_str = std::make_shared<DataTypeStringSerDe>();

class DataTypeStructSerDeTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {}
};

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeStructSerDeTest, ArrowMemNotAligned) {
    // 1.Prepare the data.
    std::vector<int32_t> int_data = {1, 2, 3, 4, 5, 6};
    std::vector<std::string> string_data = {"hello", "world", "test", "data", "arrow", "struct"};

    std::vector<int32_t> string_offsets = {0};
    int32_t current_string_offset = 0;
    for (const auto& str : string_data) {
        current_string_offset += static_cast<int32_t>(str.length());
        string_offsets.push_back(current_string_offset);
    }

    std::vector<uint8_t> string_value_data;
    for (const auto& str : string_data) {
        string_value_data.insert(string_value_data.end(), str.begin(), str.end());
    }

    std::vector<int8_t> validity_bitmap = {0x3F};

    const int64_t num_elements = int_data.size();
    const int64_t int_element_size = sizeof(int32_t);
    const int64_t offset_element_size = sizeof(int32_t);

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> int_storage(int_data.size() * int_element_size + 10);
    uint8_t* unaligned_ints = int_storage.data() + 1;

    std::vector<uint8_t> string_offset_storage(string_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_string_offsets = string_offset_storage.data() + 1;

    std::vector<uint8_t> string_value_storage(string_value_data.size() + 10);
    uint8_t* unaligned_string_values = string_value_storage.data() + 1;

    std::vector<uint8_t> validity_storage(validity_bitmap.size() + 10);
    uint8_t* unaligned_validity = validity_storage.data() + 1;

    // 3. Copy data to unaligned memory
    for (size_t i = 0; i < int_data.size(); ++i) {
        memcpy(unaligned_ints + i * int_element_size, &int_data[i], int_element_size);
    }

    for (size_t i = 0; i < string_offsets.size(); ++i) {
        memcpy(unaligned_string_offsets + i * offset_element_size, &string_offsets[i],
               offset_element_size);
    }

    memcpy(unaligned_string_values, string_value_data.data(), string_value_data.size());
    memcpy(unaligned_validity, validity_bitmap.data(), validity_bitmap.size());

    // 4. Create Arrow array with unaligned memory
    auto int_buffer = arrow::Buffer::Wrap(unaligned_ints, int_data.size() * int_element_size);
    auto int_array = std::make_shared<arrow::Int32Array>(num_elements, int_buffer, nullptr, 0);

    auto string_value_buffer =
            arrow::Buffer::Wrap(unaligned_string_values, string_value_data.size());
    auto string_offsets_buffer = arrow::Buffer::Wrap(unaligned_string_offsets,
                                                     string_offsets.size() * offset_element_size);
    auto string_array = std::make_shared<arrow::StringArray>(num_elements, string_offsets_buffer,
                                                             string_value_buffer, nullptr, 0);

    auto validity_buffer = arrow::Buffer::Wrap(unaligned_validity, validity_bitmap.size());

    auto field_int = arrow::field("int_field", arrow::int32());
    auto field_string = arrow::field("string_field", arrow::utf8());

    auto struct_type = arrow::struct_({field_int, field_string});

    arrow::ArrayVector field_arrays = {int_array, string_array};

    auto arr = std::make_shared<arrow::StructArray>(struct_type, num_elements, field_arrays,
                                                    validity_buffer);

    const auto* concrete_array = dynamic_cast<const arrow::StructArray*>(arr.get());

    const auto* int_field_array =
            dynamic_cast<const arrow::Int32Array*>(concrete_array->field(0).get());
    const auto* ints_ptr = int_field_array->raw_values();
    uintptr_t ints_address = reinterpret_cast<uintptr_t>(ints_ptr);
    EXPECT_EQ(ints_address % 4, 1);

    const auto* string_field_array =
            dynamic_cast<const arrow::StringArray*>(concrete_array->field(1).get());
    const auto* string_values_ptr = string_field_array->value_data()->data();
    uintptr_t string_values_address = reinterpret_cast<uintptr_t>(string_values_ptr);
    EXPECT_EQ(string_values_address % 4, 1);

    // 5.Test read_column_from_arrow
    std::vector<ColumnPtr> vector_columns;
    vector_columns.emplace_back(ColumnInt32::create());
    vector_columns.emplace_back(ColumnString::create());
    auto ser_col = ColumnStruct::create(vector_columns);
    cctz::time_zone tz;
    DataTypeSerDeSPtrs elem_serdes = {serde_int32, serde_str};
    Strings field_names = {"int_field", "string_field"};

    auto serde_struct = std::make_shared<DataTypeStructSerDe>(elem_serdes, field_names);

    auto st = serde_struct->read_column_from_arrow(*ser_col, arr.get(), 0, num_elements, tz);
    EXPECT_TRUE(st.ok());
}

} // namespace doris::vectorized
