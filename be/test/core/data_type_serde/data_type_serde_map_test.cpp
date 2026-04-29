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
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_map.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/complex_type_deserialize_util.h"
#include "core/field.h"
#include "core/types.h"
#include "storage/olap_common.h"
#include "testutil/test_util.h"

namespace doris {
static auto serde_str_key = std::make_shared<DataTypeStringSerDe>(TYPE_STRING);
static auto serde_str_value = std::make_shared<DataTypeStringSerDe>(TYPE_STRING);

class DataTypeMapSerDeTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {}
};

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeMapSerDeTest, ArrowMemNotAligned) {
    // 1.Prepare the data.
    std::vector<std::string> key_data = {"key1", "key2", "key3", "key4", "key5", "key6"};
    std::vector<std::string> value_data = {"val1", "val2", "val3", "val4", "val5", "val6"};

    std::vector<int32_t> key_offsets = {0};
    std::vector<int32_t> value_offsets = {0};

    int32_t current_key_offset = 0;
    for (const auto& key : key_data) {
        current_key_offset += static_cast<int32_t>(key.length());
        key_offsets.push_back(current_key_offset);
    }

    int32_t current_value_offset = 0;
    for (const auto& value : value_data) {
        current_value_offset += static_cast<int32_t>(value.length());
        value_offsets.push_back(current_value_offset);
    }

    std::vector<int32_t> map_offsets = {0, 2, 3, 6, 6};
    std::vector<int8_t> validity_bitmap = {0x0B};

    std::vector<uint8_t> key_value_data;
    for (const auto& key : key_data) {
        key_value_data.insert(key_value_data.end(), key.begin(), key.end());
    }

    std::vector<uint8_t> value_value_data;
    for (const auto& value : value_data) {
        value_value_data.insert(value_value_data.end(), value.begin(), value.end());
    }

    const int64_t num_maps = map_offsets.size() - 1;
    const int64_t offset_element_size = sizeof(int32_t);

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> map_offset_storage(map_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_map_offsets = map_offset_storage.data() + 1;

    std::vector<uint8_t> key_offset_storage(key_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_key_offsets = key_offset_storage.data() + 1;

    std::vector<uint8_t> value_offset_storage(value_offsets.size() * offset_element_size + 10);
    uint8_t* unaligned_value_offsets = value_offset_storage.data() + 1;

    std::vector<uint8_t> key_value_storage(key_value_data.size() + 10);
    uint8_t* unaligned_key_values = key_value_storage.data() + 1;

    std::vector<uint8_t> value_value_storage(value_value_data.size() + 10);
    uint8_t* unaligned_value_values = value_value_storage.data() + 1;

    std::vector<uint8_t> validity_storage(validity_bitmap.size() + 10);
    uint8_t* unaligned_validity = validity_storage.data() + 1;

    // 3. Copy data to unaligned memory
    for (size_t i = 0; i < map_offsets.size(); ++i) {
        memcpy(unaligned_map_offsets + i * offset_element_size, &map_offsets[i],
               offset_element_size);
    }

    for (size_t i = 0; i < key_offsets.size(); ++i) {
        memcpy(unaligned_key_offsets + i * offset_element_size, &key_offsets[i],
               offset_element_size);
    }

    for (size_t i = 0; i < value_offsets.size(); ++i) {
        memcpy(unaligned_value_offsets + i * offset_element_size, &value_offsets[i],
               offset_element_size);
    }

    memcpy(unaligned_key_values, key_value_data.data(), key_value_data.size());
    memcpy(unaligned_value_values, value_value_data.data(), value_value_data.size());
    memcpy(unaligned_validity, validity_bitmap.data(), validity_bitmap.size());

    // 4. Create Arrow array with unaligned memory
    auto key_value_buffer = arrow::Buffer::Wrap(unaligned_key_values, key_value_data.size());
    auto key_offsets_buffer =
            arrow::Buffer::Wrap(unaligned_key_offsets, key_offsets.size() * sizeof(int32_t));
    auto key_array = std::make_shared<arrow::StringArray>(key_offsets.size() - 1,
                                                          key_offsets_buffer, key_value_buffer);

    auto value_value_buffer = arrow::Buffer::Wrap(unaligned_value_values, value_value_data.size());
    auto value_offsets_buffer =
            arrow::Buffer::Wrap(unaligned_value_offsets, value_offsets.size() * sizeof(int32_t));
    auto value_array = std::make_shared<arrow::StringArray>(
            value_offsets.size() - 1, value_offsets_buffer, value_value_buffer);

    auto map_offsets_buffer =
            arrow::Buffer::Wrap(unaligned_map_offsets, map_offsets.size() * offset_element_size);
    auto validity_buffer = arrow::Buffer::Wrap(unaligned_validity, validity_bitmap.size());

    auto map_type = arrow::map(arrow::utf8(), arrow::utf8());

    auto arr = std::make_shared<arrow::MapArray>(map_type, num_maps, map_offsets_buffer, key_array,
                                                 value_array, validity_buffer);

    const auto* concrete_array = dynamic_cast<const arrow::MapArray*>(arr.get());
    auto arrow_offsets_array = concrete_array->offsets();
    auto* arrow_offsets = dynamic_cast<arrow::Int32Array*>(arrow_offsets_array.get());

    const auto* offsets_ptr = arrow_offsets->raw_values();
    uintptr_t offsets_address = reinterpret_cast<uintptr_t>(offsets_ptr);
    EXPECT_EQ(offsets_address % 4, 1);

    const auto* keys_ptr = key_array->value_data()->data();
    uintptr_t keys_address = reinterpret_cast<uintptr_t>(keys_ptr);
    EXPECT_EQ(keys_address % 4, 1);

    const auto* values_ptr = value_array->value_data()->data();
    uintptr_t values_address = reinterpret_cast<uintptr_t>(values_ptr);
    EXPECT_EQ(values_address % 4, 1);

    // 5.Test read_column_from_arrow
    auto ser_col = ColumnMap::create(ColumnString::create(), ColumnString::create(),
                                     ColumnOffset64::create());
    cctz::time_zone tz;
    auto serde_map = std::make_shared<DataTypeMapSerDe>(serde_str_key, serde_str_value);
    auto st = serde_map->read_column_from_arrow(*ser_col, arr.get(), 0, 1, tz);
    EXPECT_TRUE(st.ok());
}

// Stream Load JSON stores Map as String via to_json_string, then converts back
// via from_string → split_by_delimiter. The splitter must handle '\' escapes
// so that '\"' inside a value doesn't flip quote state and expose inner ':'/','.
TEST_F(DataTypeMapSerDeTest, SplitByDelimiterHandlesBackslashEscape) {
    DataTypeSerDe::FormatOptions opts;
    opts.map_key_delim = ':';
    opts.collection_delim = ',';

    auto make_map_type = []() {
        auto str = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
        return std::make_shared<DataTypeMap>(str, str);
    };

    // split_by_delimiter: '\"' must not toggle quote state
    // Input (after stripping outer {}): "k":"[{\"a\":\"b\\nc:"
    // Expected: 2 elements — key "k" and value "[{\"a\":\"b\\nc:"
    {
        std::string inner = "\"k\":\"[{\\\"a\\\":\\\"b\\\\nc:\"";
        StringRef str(inner.data(), inner.size());
        auto result = ComplexTypeDeserializeUtil::split_by_delimiter(
                str, [&](char c) { return c == opts.map_key_delim || c == opts.collection_delim; });
        EXPECT_EQ(result.size(), 2u);
    }

    // from_string: value ending with ':' (map_key_delim) must not cause split error
    // Simulates to_json_string output: {"k":"[{\"a\":\"b\\nc:"}
    {
        auto map_type = make_map_type();
        auto col = map_type->create_column();
        std::string map_str = "{\"k\":\"[{\\\"a\\\":\\\"b\\\\nc:\"}";
        StringRef ref(map_str.data(), map_str.size());
        EXPECT_TRUE(map_type->get_serde()->from_string(ref, *col, opts).ok());
        EXPECT_EQ(col->size(), 1u);
    }

    // from_string: value ending with ',' (collection_delim) — same class of bug
    {
        auto map_type = make_map_type();
        auto col = map_type->create_column();
        std::string map_str = "{\"k\":\"[{\\\"a\\\":\\\"b\\\\nc,\"}";
        StringRef ref(map_str.data(), map_str.size());
        EXPECT_TRUE(map_type->get_serde()->from_string(ref, *col, opts).ok());
        EXPECT_EQ(col->size(), 1u);
    }

    // Control: value ending with ')' (not a delimiter) — always worked
    {
        auto map_type = make_map_type();
        auto col = map_type->create_column();
        std::string map_str = "{\"k\":\"[{\\\"a\\\":\\\"b\\\\nc)\"}";
        StringRef ref(map_str.data(), map_str.size());
        EXPECT_TRUE(map_type->get_serde()->from_string(ref, *col, opts).ok());
        EXPECT_EQ(col->size(), 1u);
    }
}

} // namespace doris
