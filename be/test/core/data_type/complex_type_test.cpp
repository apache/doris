
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

namespace {

std::vector<char> serialize_column(const DataTypePtr& type, const ColumnPtr& column) {
    const int be_exec_version = BeExecVersionManager::get_newest_version();
    std::vector<char> buf(type->get_uncompressed_serialized_bytes(*column, be_exec_version));
    char* end = type->serialize(*column, buf.data(), be_exec_version);
    EXPECT_EQ(buf.data() + buf.size(), end);
    return buf;
}

void deserialize_column(const DataTypePtr& type, const std::vector<char>& buf,
                        MutableColumnPtr* column) {
    const int be_exec_version = BeExecVersionManager::get_newest_version();
    const char* end = type->deserialize(buf.data(), column, be_exec_version);
    EXPECT_EQ(buf.data() + buf.size(), end);
}

} // namespace

TEST(ComplexTypeTest, CreateColumnConstWithDefaultValue) {
    DataTypePtr n1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr n2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    DataTypePtr n3 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt128>());
    DataTypePtr s1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    DataTypes dataTypes;
    dataTypes.push_back(n1);
    dataTypes.push_back(n2);
    dataTypes.push_back(n3);
    dataTypes.push_back(s1);

    // data_type_struct
    DataTypePtr s = std::make_shared<DataTypeStruct>(dataTypes);
    ColumnPtr col_s = s->create_column_const_with_default_value(1);
    Field t;
    EXPECT_EQ(1, col_s->size());
    col_s->get(0, t);
    EXPECT_EQ(PrimitiveType::TYPE_STRUCT, t.get_type());

    // data_type_map
    DataTypePtr m = std::make_shared<DataTypeMap>(n1, s1);
    ColumnPtr col_m = m->create_column_const_with_default_value(1);
    EXPECT_EQ(1, col_m->size());
    Field mf;
    col_m->get(0, mf);
    EXPECT_EQ(PrimitiveType::TYPE_MAP, mf.get_type());

    // data_type_array
    DataTypePtr a = std::make_shared<DataTypeArray>(s1);
    ColumnPtr col_a = a->create_column_const_with_default_value(1);
    EXPECT_EQ(1, col_a->size());
    Field af;
    col_a->get(0, af);
    EXPECT_EQ(PrimitiveType::TYPE_ARRAY, af.get_type());
}

TEST(ComplexTypeTest, DeserializeArrayWritesBackSharedNestedColumn) {
    DataTypePtr nested_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr array_type = std::make_shared<DataTypeArray>(nested_type);

    auto src_column = array_type->create_column();
    src_column->insert(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2)}));
    src_column->insert(Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(3)}));
    auto buf = serialize_column(array_type, src_column->get_ptr());

    ColumnPtr shared_nested_data_column = ColumnInt32::create();
    ColumnPtr shared_nested_null_map_column = ColumnUInt8::create();
    ColumnPtr shared_nested_column =
            ColumnNullable::create(shared_nested_data_column, shared_nested_null_map_column);
    ColumnPtr shared_offsets_column = ColumnArray::ColumnOffsets::create();
    MutableColumnPtr dst_column = ColumnArray::create(shared_nested_column, shared_offsets_column);
    deserialize_column(array_type, buf, &dst_column);

    const auto& array_column = assert_cast<const ColumnArray&>(*dst_column);
    EXPECT_EQ(2, array_column.size());
    EXPECT_EQ(0, shared_nested_data_column->size());
    EXPECT_EQ(0, shared_nested_null_map_column->size());
    EXPECT_EQ(0, shared_offsets_column->size());
    EXPECT_EQ(3, array_column.get_data().size());
    EXPECT_EQ(2, array_column.get_offsets()[0]);
    EXPECT_EQ(3, array_column.get_offsets()[1]);

    const auto& nullable_data = assert_cast<const ColumnNullable&>(array_column.get_data());
    const auto& data =
            assert_cast<const ColumnInt32&>(nullable_data.get_nested_column()).get_data();
    EXPECT_EQ(1, data[0]);
    EXPECT_EQ(2, data[1]);
    EXPECT_EQ(3, data[2]);
    EXPECT_FALSE(nullable_data.has_null());
}

TEST(ComplexTypeTest, DeserializeMapWritesBackSharedKeyAndValueColumns) {
    DataTypePtr key_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    DataTypePtr value_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    DataTypePtr map_type = std::make_shared<DataTypeMap>(key_type, value_type);

    auto src_column = map_type->create_column();
    Map map;
    map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_INT>(10), Field::create_field<TYPE_INT>(20)}));
    map.push_back(Field::create_field<TYPE_ARRAY>(
            Array {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b")}));
    src_column->insert(Field::create_field<TYPE_MAP>(map));
    auto buf = serialize_column(map_type, src_column->get_ptr());

    ColumnPtr shared_keys_data_column = ColumnInt32::create();
    ColumnPtr shared_keys_null_map_column = ColumnUInt8::create();
    ColumnPtr shared_keys_column =
            ColumnNullable::create(shared_keys_data_column, shared_keys_null_map_column);
    ColumnPtr shared_values_data_column = ColumnString::create();
    ColumnPtr shared_values_null_map_column = ColumnUInt8::create();
    ColumnPtr shared_values_column =
            ColumnNullable::create(shared_values_data_column, shared_values_null_map_column);
    ColumnPtr offsets_column = ColumnArray::ColumnOffsets::create();
    MutableColumnPtr dst_column =
            ColumnMap::create(shared_keys_column, shared_values_column, offsets_column);
    deserialize_column(map_type, buf, &dst_column);

    const auto& map_column = assert_cast<const ColumnMap&>(*dst_column);
    EXPECT_EQ(1, map_column.size());
    EXPECT_EQ(0, shared_keys_data_column->size());
    EXPECT_EQ(0, shared_keys_null_map_column->size());
    EXPECT_EQ(0, shared_values_data_column->size());
    EXPECT_EQ(0, shared_values_null_map_column->size());
    EXPECT_EQ(0, offsets_column->size());
    EXPECT_EQ(2, map_column.get_keys().size());
    EXPECT_EQ(2, map_column.get_values().size());

    const auto& nullable_keys = assert_cast<const ColumnNullable&>(map_column.get_keys());
    const auto& keys =
            assert_cast<const ColumnInt32&>(nullable_keys.get_nested_column()).get_data();
    EXPECT_EQ(10, keys[0]);
    EXPECT_EQ(20, keys[1]);
    EXPECT_FALSE(nullable_keys.has_null());
    const auto& nullable_values = assert_cast<const ColumnNullable&>(map_column.get_values());
    EXPECT_EQ("a", nullable_values.get_nested_column().get_data_at(0).to_string());
    EXPECT_EQ("b", nullable_values.get_nested_column().get_data_at(1).to_string());
    EXPECT_FALSE(nullable_values.has_null());
}

TEST(ComplexTypeTest, DataTypeMapCreateColumnSelectsPhysicalMapBySubcolumnNullability) {
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr nullable_string_type =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

    DataTypeMap nullable_map_type(nullable_string_type, nullable_string_type);
    auto nullable_map_column = nullable_map_type.create_column();
    EXPECT_TRUE(check_and_get_column<ColumnMap>(nullable_map_column.get()));

    DataTypeMap not_null_map_type(string_type, string_type);
    auto not_null_map_column = not_null_map_type.create_column();
    EXPECT_TRUE(check_and_get_column<ColumnMapNotNull>(not_null_map_column.get()));

    DataTypeMap mixed_map_type(nullable_string_type, string_type);
    EXPECT_ANY_THROW({ auto mixed_column = mixed_map_type.create_column(); });
}

TEST(ComplexTypeTest, DeserializeStructWritesBackSharedChildren) {
    DataTypes children_types {std::make_shared<DataTypeInt32>(),
                              std::make_shared<DataTypeString>()};
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(children_types);

    auto src_column = struct_type->create_column();
    src_column->insert(Field::create_field<TYPE_STRUCT>(
            Struct {Field::create_field<TYPE_INT>(7), Field::create_field<TYPE_STRING>("seven")}));
    auto buf = serialize_column(struct_type, src_column->get_ptr());

    ColumnPtr shared_int_column = ColumnInt32::create();
    ColumnPtr shared_string_column = ColumnString::create();
    Columns shared_columns {shared_int_column, shared_string_column};
    MutableColumnPtr dst_column = ColumnStruct::create(shared_columns);
    deserialize_column(struct_type, buf, &dst_column);

    const auto& struct_column = assert_cast<const ColumnStruct&>(*dst_column);
    EXPECT_EQ(1, struct_column.size());
    EXPECT_EQ(0, shared_int_column->size());
    EXPECT_EQ(0, shared_string_column->size());

    const auto& ints = assert_cast<const ColumnInt32&>(struct_column.get_column(0)).get_data();
    EXPECT_EQ(7, ints[0]);
    EXPECT_EQ("seven", struct_column.get_column(1).get_data_at(0).to_string());
}
} // namespace doris
