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

#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/function/complex_hash_map_dictionary.h"

namespace doris {

// Helper: build a ColumnArray<Int32> (TYPE_INT) with given nested values.
static ColumnPtr make_array_column_int32(const std::vector<std::vector<Int32>>& rows_data) {
    auto nested = ColumnVector<TYPE_INT>::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    ColumnArray::Offset64 cur_offset = 0;
    for (const auto& row : rows_data) {
        for (auto v : row) {
            nested->insert_value(v);
        }
        cur_offset += row.size();
        offsets->insert_value(cur_offset);
    }
    return ColumnArray::create(std::move(nested), std::move(offsets));
}

// Helper: build a ColumnArray<String> with given nested values.
static ColumnPtr make_array_column_string(const std::vector<std::vector<std::string>>& rows_data) {
    auto nested = ColumnString::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    ColumnArray::Offset64 cur_offset = 0;
    for (const auto& row : rows_data) {
        for (const auto& s : row) {
            nested->insert_data(s.data(), s.size());
        }
        cur_offset += row.size();
        offsets->insert_value(cur_offset);
    }
    return ColumnArray::create(std::move(nested), std::move(offsets));
}

// Helper: build a ColumnMap<Int32, String> with given rows.
// Each row is a list of (int_key, string_val) pairs.
static ColumnPtr make_map_column_int32_string(
        const std::vector<std::vector<std::pair<Int32, std::string>>>& rows_data) {
    auto map_keys = ColumnVector<TYPE_INT>::create();
    auto map_vals = ColumnString::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    ColumnArray::Offset64 cur_offset = 0;
    for (const auto& row : rows_data) {
        for (const auto& [k, v] : row) {
            map_keys->insert_value(k);
            map_vals->insert_data(v.data(), v.size());
        }
        cur_offset += row.size();
        offsets->insert_value(cur_offset);
    }
    return ColumnMap::create(std::move(map_keys), std::move(map_vals), std::move(offsets));
}

// Helper: build a ColumnStruct<Int32, String> with given rows.
static ColumnPtr make_struct_column_int32_string(
        const std::vector<std::pair<Int32, std::string>>& rows_data) {
    auto int_col = ColumnVector<TYPE_INT>::create();
    auto str_col = ColumnString::create();
    for (const auto& [k, v] : rows_data) {
        int_col->insert_value(k);
        str_col->insert_data(v.data(), v.size());
    }
    Columns columns;
    columns.emplace_back(std::move(int_col));
    columns.emplace_back(std::move(str_col));
    return ColumnStruct::create(std::move(columns));
}

// =========================================================
// Test: Array value column (non-nullable) — hash map dict
// =========================================================
TEST(DictionaryArrayTest, ArrayValueNonNullable) {
    // key: int32  value: array<int32>
    // data: {1 -> [10,20], 2 -> [30], 3 -> []}
    auto key_col = ColumnVector<TYPE_INT>::create();
    key_col->insert_value(1);
    key_col->insert_value(2);
    key_col->insert_value(3);

    auto value_col = make_array_column_int32({{10, 20}, {30}, {}});

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    ColumnsWithTypeAndName attribute_data {{value_col, array_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_array_value", {key_col->clone()}, {std::make_shared<DataTypeInt32>()},
            attribute_data);

    // query keys [1, 2, 3, 99]
    auto query_key = ColumnVector<TYPE_INT>::create();
    query_key->insert_value(1);
    query_key->insert_value(2);
    query_key->insert_value(3);
    query_key->insert_value(99); // not found

    auto result = dict->get_column("v0", array_type, query_key->clone(),
                                   std::make_shared<DataTypeInt32>());

    EXPECT_EQ(result->size(), 4);

    // result is Nullable(Array<Int32>)
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0)); // key=1 found
    EXPECT_FALSE(nullable_result->is_null_at(1)); // key=2 found
    EXPECT_FALSE(nullable_result->is_null_at(2)); // key=3 found
    EXPECT_TRUE(nullable_result->is_null_at(3));  // key=99 not found

    const auto* arr_result =
            assert_cast<const ColumnArray*>(&nullable_result->get_nested_column());

    // row 0: [10, 20] -> 2 elements
    EXPECT_EQ(arr_result->get_offsets()[0] - 0, 2);
    // row 1: [30] -> 1 element
    EXPECT_EQ(arr_result->get_offsets()[1] - arr_result->get_offsets()[0], 1);
    // row 2: [] -> 0 elements
    EXPECT_EQ(arr_result->get_offsets()[2] - arr_result->get_offsets()[1], 0);
}

// =========================================================
// Test: Array value column (nullable) — hash map dict
// =========================================================
TEST(DictionaryArrayTest, ArrayValueNullable) {
    // key: int32  value: nullable(array<string>)
    // data: {1 -> ['a','b'], 2 -> null}
    auto key_col = ColumnVector<TYPE_INT>::create();
    key_col->insert_value(1);
    key_col->insert_value(2);

    auto value_inner = make_array_column_string({{"a", "b"}, {}});
    auto null_map_mut = ColumnUInt8::create();
    null_map_mut->insert_value(0); // row 0: not null
    null_map_mut->insert_value(1); // row 1: null
    ColumnPtr null_map_col = std::move(null_map_mut);
    ColumnPtr value_nullable = ColumnNullable::create(value_inner, null_map_col);

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    auto nullable_array_type = std::make_shared<DataTypeNullable>(array_type);

    ColumnsWithTypeAndName attribute_data {{value_nullable, nullable_array_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_array_value_nullable", {key_col->clone()}, {std::make_shared<DataTypeInt32>()},
            attribute_data);

    // query: keys [1, 2, 99]
    auto query_key = ColumnVector<TYPE_INT>::create();
    query_key->insert_value(1);
    query_key->insert_value(2);
    query_key->insert_value(99);

    auto result = dict->get_column("v0", array_type, query_key->clone(),
                                   std::make_shared<DataTypeInt32>());

    EXPECT_EQ(result->size(), 3);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0)); // key=1 found, value not null
    EXPECT_TRUE(nullable_result->is_null_at(1));  // key=2 found but value is null
    EXPECT_TRUE(nullable_result->is_null_at(2));  // key=99 not found
}

// =========================================================
// Test: Array key column — hash map dict
// =========================================================
TEST(DictionaryArrayTest, ArrayKey) {
    // key: array<int32>  value: string
    // data: {[1,2] -> 'a', [3] -> 'b', [] -> 'c'}
    auto key_col = make_array_column_int32({{1, 2}, {3}, {}});
    auto value_col = ColumnString::create();
    value_col->insert_data("a", 1);
    value_col->insert_data("b", 1);
    value_col->insert_data("c", 1);

    auto array_key_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto string_type = std::make_shared<DataTypeString>();

    ColumnsWithTypeAndName attribute_data {{value_col->clone(), string_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_array_key", {key_col}, {array_key_type}, attribute_data);

    // query: [[1,2], [3], [], [99]] (last not found)
    auto query_key = make_array_column_int32({{1, 2}, {3}, {}, {99}});

    auto result = dict->get_column("v0", string_type, query_key, array_key_type);

    EXPECT_EQ(result->size(), 4);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0)); // [1,2] found
    EXPECT_FALSE(nullable_result->is_null_at(1)); // [3] found
    EXPECT_FALSE(nullable_result->is_null_at(2)); // [] found
    EXPECT_TRUE(nullable_result->is_null_at(3));  // [99] not found

    const auto* str_result =
            assert_cast<const ColumnString*>(&nullable_result->get_nested_column());
    EXPECT_EQ(str_result->get_data_at(0).to_string(), "a");
    EXPECT_EQ(str_result->get_data_at(1).to_string(), "b");
    EXPECT_EQ(str_result->get_data_at(2).to_string(), "c");
}

// =========================================================
// Test: Array KEY + Array VALUE — hash map dict
// =========================================================
TEST(DictionaryArrayTest, ArrayKeyAndArrayValue) {
    // key: array<int32>  value: array<string> (non-nullable)
    // data: {[1] -> ['x'], [2,3] -> ['y','z']}
    auto key_col = make_array_column_int32({{1}, {2, 3}});
    auto value_col = make_array_column_string({{"x"}, {"y", "z"}});

    auto array_key_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    auto array_val_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());

    ColumnsWithTypeAndName attribute_data {{value_col, array_val_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_array_kv", {key_col}, {array_key_type}, attribute_data);

    auto query_key = make_array_column_int32({{1}, {2, 3}, {99}});
    auto result = dict->get_column("v0", array_val_type, query_key, array_key_type);

    EXPECT_EQ(result->size(), 3);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0));
    EXPECT_FALSE(nullable_result->is_null_at(1));
    EXPECT_TRUE(nullable_result->is_null_at(2)); // not found

    const auto* arr_result =
            assert_cast<const ColumnArray*>(&nullable_result->get_nested_column());

    // row 0: ['x'] -> 1 element
    EXPECT_EQ(arr_result->get_offsets()[0] - 0, 1);
    // row 1: ['y','z'] -> 2 elements
    EXPECT_EQ(arr_result->get_offsets()[1] - arr_result->get_offsets()[0], 2);
}

// =========================================================
// Test: Map KEY — hash map dict
// =========================================================
TEST(DictionaryArrayTest, MapKey) {
    // key: map<int32, string>  value: string
    // data: {{1->'x'} -> 'a', {2->'y', 3->'z'} -> 'b', {} -> 'c'}
    auto key_col = make_map_column_int32_string({{{1, "x"}}, {{2, "y"}, {3, "z"}}, {}});
    auto value_col = ColumnString::create();
    value_col->insert_data("a", 1);
    value_col->insert_data("b", 1);
    value_col->insert_data("c", 1);

    auto map_key_type =
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>());
    auto string_type = std::make_shared<DataTypeString>();

    ColumnsWithTypeAndName attribute_data {{value_col->clone(), string_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_map_key", {key_col}, {map_key_type}, attribute_data);

    // query: [{1->'x'}, {2->'y',3->'z'}, {}, {99->'miss'}]
    auto query_key = make_map_column_int32_string({{{1, "x"}}, {{2, "y"}, {3, "z"}}, {}, {{99, "miss"}}});

    auto result = dict->get_column("v0", string_type, query_key, map_key_type);

    EXPECT_EQ(result->size(), 4);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0)); // {1->'x'} found
    EXPECT_FALSE(nullable_result->is_null_at(1)); // {2->'y',3->'z'} found
    EXPECT_FALSE(nullable_result->is_null_at(2)); // {} found
    EXPECT_TRUE(nullable_result->is_null_at(3));  // {99->'miss'} not found

    const auto* str_result =
            assert_cast<const ColumnString*>(&nullable_result->get_nested_column());
    EXPECT_EQ(str_result->get_data_at(0).to_string(), "a");
    EXPECT_EQ(str_result->get_data_at(1).to_string(), "b");
    EXPECT_EQ(str_result->get_data_at(2).to_string(), "c");
}

// =========================================================
// Test: Struct KEY — hash map dict
// =========================================================
TEST(DictionaryArrayTest, StructKey) {
    // key: struct<f0:int32, f1:string>  value: string
    // data: {(1,'a') -> 'va', (2,'b') -> 'vb', (3,'c') -> 'vc'}
    auto key_col = make_struct_column_int32_string({{1, "a"}, {2, "b"}, {3, "c"}});
    auto value_col = ColumnString::create();
    value_col->insert_data("va", 2);
    value_col->insert_data("vb", 2);
    value_col->insert_data("vc", 2);

    auto struct_key_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"f0", "f1"});
    auto string_type = std::make_shared<DataTypeString>();

    ColumnsWithTypeAndName attribute_data {{value_col->clone(), string_type, "v0"}};

    auto dict = ComplexHashMapDictionary::create_complex_hash_map_dict(
            "test_struct_key", {key_col}, {struct_key_type}, attribute_data);

    // query: [(1,'a'), (2,'b'), (3,'c'), (99,'x')]
    auto query_key = make_struct_column_int32_string({{1, "a"}, {2, "b"}, {3, "c"}, {99, "x"}});

    auto result = dict->get_column("v0", string_type, query_key, struct_key_type);

    EXPECT_EQ(result->size(), 4);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result.get());
    EXPECT_FALSE(nullable_result->is_null_at(0)); // (1,'a') found
    EXPECT_FALSE(nullable_result->is_null_at(1)); // (2,'b') found
    EXPECT_FALSE(nullable_result->is_null_at(2)); // (3,'c') found
    EXPECT_TRUE(nullable_result->is_null_at(3));  // (99,'x') not found

    const auto* str_result =
            assert_cast<const ColumnString*>(&nullable_result->get_nested_column());
    EXPECT_EQ(str_result->get_data_at(0).to_string(), "va");
    EXPECT_EQ(str_result->get_data_at(1).to_string(), "vb");
    EXPECT_EQ(str_result->get_data_at(2).to_string(), "vc");
}

} // namespace doris
