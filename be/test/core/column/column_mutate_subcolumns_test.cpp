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

#include <initializer_list>
#include <memory>

#include "common/logging.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"

namespace doris {
namespace {

ColumnInt64::MutablePtr create_int64_column(int64_t value) {
    auto column = ColumnInt64::create();
    column->insert_value(value);
    return column;
}

ColumnInt64::MutablePtr create_int64_column(std::initializer_list<int64_t> values) {
    auto column = ColumnInt64::create();
    for (const auto value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnUInt8::MutablePtr create_uint8_column(uint8_t value) {
    auto column = ColumnUInt8::create();
    column->insert_value(value);
    return column;
}

ColumnArray::ColumnOffsets::MutablePtr create_single_element_offsets() {
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(1);
    return offsets;
}

ColumnVariant::MutablePtr create_variant_column() {
    auto variant = ColumnVariant::create(0, false);
    variant->create_root(std::make_shared<DataTypeInt64>(), create_int64_column({1, 2}));
    CHECK(variant->add_sub_column(PathInData("v.a"), create_int64_column({10, 20}),
                                  std::make_shared<DataTypeInt64>()));
    variant->finalize();
    return variant;
}

} // namespace

TEST(ColumnMutateSubcolumnsTest, NullableKeepsExclusiveSubcolumns) {
    ColumnPtr nullable = ColumnNullable::create(create_int64_column(10), create_uint8_column(0));
    const auto& nullable_ref = assert_cast<const ColumnNullable&>(*nullable);
    const auto* nested_raw = nullable_ref.get_nested_column_ptr().get();
    const auto* null_map_raw = nullable_ref.get_null_map_column_ptr().get();

    auto mutated = IColumn::mutate(std::move(nullable));
    const auto& mutated_nullable = assert_cast<const ColumnNullable&>(*mutated);

    EXPECT_EQ(mutated_nullable.get_nested_column_ptr().get(), nested_raw);
    EXPECT_EQ(mutated_nullable.get_null_map_column_ptr().get(), null_map_raw);
}

TEST(ColumnMutateSubcolumnsTest, NullableDetachesSharedSubcolumns) {
    ColumnPtr nested = create_int64_column(10);
    ColumnPtr nested_alias = nested;
    ColumnPtr null_map = create_uint8_column(0);
    ColumnPtr null_map_alias = null_map;

    ColumnPtr nullable = ColumnNullable::create(nested, null_map);
    auto mutated = IColumn::mutate(std::move(nullable));
    auto& mutated_nullable = assert_cast<ColumnNullable&>(*mutated);

    EXPECT_NE(mutated_nullable.get_nested_column_ptr().get(), nested_alias.get());
    EXPECT_NE(mutated_nullable.get_null_map_column_ptr().get(), null_map_alias.get());
    EXPECT_EQ(mutated_nullable.get_null_map_data()[0], 0);

    mutated_nullable.get_null_map_data()[0] = 1;
    const auto& original_null_map = assert_cast<const ColumnUInt8&>(*null_map_alias);
    EXPECT_EQ(original_null_map.get_data()[0], 0);
}

TEST(ColumnMutateSubcolumnsTest, ArrayKeepsExclusiveSubcolumns) {
    ColumnPtr array = ColumnArray::create(create_int64_column(10), create_single_element_offsets());
    const auto& array_ref = assert_cast<const ColumnArray&>(*array);
    const auto* data_raw = array_ref.get_data_ptr().get();
    const auto* offsets_raw = array_ref.get_offsets_ptr().get();

    auto mutated = IColumn::mutate(std::move(array));
    const auto& mutated_array = assert_cast<const ColumnArray&>(*mutated);

    EXPECT_EQ(mutated_array.get_data_ptr().get(), data_raw);
    EXPECT_EQ(mutated_array.get_offsets_ptr().get(), offsets_raw);
}

TEST(ColumnMutateSubcolumnsTest, ArrayDetachesSharedSubcolumns) {
    ColumnPtr data = create_int64_column(10);
    ColumnPtr data_alias = data;
    ColumnPtr offsets = create_single_element_offsets();
    ColumnPtr offsets_alias = offsets;

    ColumnPtr array = ColumnArray::create(data, offsets);
    auto mutated = IColumn::mutate(std::move(array));
    auto& mutated_array = assert_cast<ColumnArray&>(*mutated);

    EXPECT_NE(mutated_array.get_data_ptr().get(), data_alias.get());
    EXPECT_NE(mutated_array.get_offsets_ptr().get(), offsets_alias.get());

    assert_cast<ColumnInt64&>(mutated_array.get_data()).get_data_mutable()[0] = 20;
    mutated_array.get_offsets()[0] = 2;
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*data_alias).get_data()[0], 10);
    EXPECT_EQ(assert_cast<const ColumnArray::ColumnOffsets&>(*offsets_alias).get_data()[0], 1);
}

TEST(ColumnMutateSubcolumnsTest, MapKeepsExclusiveSubcolumns) {
    ColumnPtr map = ColumnMap::create(create_int64_column(1), create_int64_column(10),
                                      create_single_element_offsets());
    const auto& map_ref = assert_cast<const ColumnMap&>(*map);
    const auto* keys_raw = map_ref.get_keys_ptr().get();
    const auto* values_raw = map_ref.get_values_ptr().get();
    const auto* offsets_raw = map_ref.get_offsets_ptr().get();

    auto mutated = IColumn::mutate(std::move(map));
    const auto& mutated_map = assert_cast<const ColumnMap&>(*mutated);

    EXPECT_EQ(mutated_map.get_keys_ptr().get(), keys_raw);
    EXPECT_EQ(mutated_map.get_values_ptr().get(), values_raw);
    EXPECT_EQ(mutated_map.get_offsets_ptr().get(), offsets_raw);
}

TEST(ColumnMutateSubcolumnsTest, MapDetachesSharedSubcolumns) {
    ColumnPtr keys = create_int64_column(1);
    ColumnPtr keys_alias = keys;
    ColumnPtr values = create_int64_column(10);
    ColumnPtr values_alias = values;
    ColumnPtr offsets = create_single_element_offsets();
    ColumnPtr offsets_alias = offsets;

    ColumnPtr map = ColumnMap::create(keys, values, offsets);
    auto mutated = IColumn::mutate(std::move(map));
    auto& mutated_map = assert_cast<ColumnMap&>(*mutated);

    EXPECT_NE(mutated_map.get_keys_ptr().get(), keys_alias.get());
    EXPECT_NE(mutated_map.get_values_ptr().get(), values_alias.get());
    EXPECT_NE(mutated_map.get_offsets_ptr().get(), offsets_alias.get());

    assert_cast<ColumnInt64&>(mutated_map.get_keys()).get_data_mutable()[0] = 2;
    assert_cast<ColumnInt64&>(mutated_map.get_values()).get_data_mutable()[0] = 20;
    mutated_map.get_offsets()[0] = 2;
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*keys_alias).get_data()[0], 1);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*values_alias).get_data()[0], 10);
    EXPECT_EQ(assert_cast<const ColumnMap::COffsets&>(*offsets_alias).get_data()[0], 1);
}

TEST(ColumnMutateSubcolumnsTest, ConstKeepsExclusiveSubcolumn) {
    ColumnPtr column_const = ColumnConst::create(create_int64_column(10), 3);
    const auto& const_ref = assert_cast<const ColumnConst&>(*column_const);
    const auto* data_raw = const_ref.get_data_column_ptr().get();

    auto mutated = IColumn::mutate(std::move(column_const));
    const auto& mutated_const = assert_cast<const ColumnConst&>(*mutated);

    EXPECT_EQ(mutated_const.get_data_column_ptr().get(), data_raw);
}

TEST(ColumnMutateSubcolumnsTest, ConstDetachesSharedSubcolumn) {
    ColumnPtr data = create_int64_column(10);
    ColumnPtr data_alias = data;

    ColumnPtr column_const = ColumnConst::create(data, 3);
    auto mutated = IColumn::mutate(std::move(column_const));
    const auto& mutated_const = assert_cast<const ColumnConst&>(*mutated);

    EXPECT_NE(mutated_const.get_data_column_ptr().get(), data_alias.get());
    EXPECT_EQ(mutated_const.get_data_column().get_int(0), 10);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*data_alias).get_data()[0], 10);
}

TEST(ColumnMutateSubcolumnsTest, StructKeepsExclusiveSubcolumns) {
    MutableColumns columns;
    columns.push_back(create_int64_column(10));
    columns.push_back(create_uint8_column(1));

    ColumnPtr column_struct = ColumnStruct::create(std::move(columns));
    const auto& struct_ref = assert_cast<const ColumnStruct&>(*column_struct);
    const auto* first_raw = struct_ref.get_column_ptr(0).get();
    const auto* second_raw = struct_ref.get_column_ptr(1).get();

    auto mutated = IColumn::mutate(std::move(column_struct));
    const auto& mutated_struct = assert_cast<const ColumnStruct&>(*mutated);

    EXPECT_EQ(mutated_struct.get_column_ptr(0).get(), first_raw);
    EXPECT_EQ(mutated_struct.get_column_ptr(1).get(), second_raw);
}

TEST(ColumnMutateSubcolumnsTest, StructDetachesSharedSubcolumns) {
    ColumnPtr first = create_int64_column(10);
    ColumnPtr first_alias = first;
    ColumnPtr second = create_uint8_column(1);
    ColumnPtr second_alias = second;

    ColumnPtr column_struct = ColumnStruct::create(Columns {first, second});
    auto mutated = IColumn::mutate(std::move(column_struct));
    auto& mutated_struct = assert_cast<ColumnStruct&>(*mutated);

    EXPECT_NE(mutated_struct.get_column_ptr(0).get(), first_alias.get());
    EXPECT_NE(mutated_struct.get_column_ptr(1).get(), second_alias.get());

    assert_cast<ColumnInt64&>(mutated_struct.get_column(0)).get_data_mutable()[0] = 20;
    assert_cast<ColumnUInt8&>(mutated_struct.get_column(1)).get_data_mutable()[0] = 2;
    EXPECT_EQ(assert_cast<const ColumnInt64&>(*first_alias).get_data()[0], 10);
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*second_alias).get_data()[0], 1);
}

TEST(ColumnMutateSubcolumnsTest, VariantKeepsExclusiveSubcolumns) {
    auto variant = create_variant_column();
    const auto* root = variant->get_subcolumns().get_root();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ(root->data.data.size(), 1);
    const auto* root_raw = static_cast<const IColumn::Ptr&>(root->data.data[0]).get();
    auto* materialized = variant->get_subcolumn(PathInData("v.a"));
    ASSERT_NE(materialized, nullptr);
    ASSERT_EQ(materialized->data.size(), 1);
    const auto* materialized_raw = static_cast<const IColumn::Ptr&>(materialized->data[0]).get();
    const auto* sparse_raw = variant->get_sparse_column().get();
    const auto* doc_value_raw = variant->get_doc_value_column().get();

    ColumnPtr variant_ptr = std::move(variant);
    auto mutated = IColumn::mutate(std::move(variant_ptr));
    const auto& mutated_variant = assert_cast<const ColumnVariant&>(*mutated);
    const auto* mutated_root = mutated_variant.get_subcolumns().get_root();
    ASSERT_NE(mutated_root, nullptr);
    ASSERT_EQ(mutated_root->data.data.size(), 1);
    const auto* mutated_materialized = mutated_variant.get_subcolumn(PathInData("v.a"));
    ASSERT_NE(mutated_materialized, nullptr);
    ASSERT_EQ(mutated_materialized->data.size(), 1);

    EXPECT_EQ(static_cast<const IColumn::Ptr&>(mutated_root->data.data[0]).get(), root_raw);
    EXPECT_EQ(static_cast<const IColumn::Ptr&>(mutated_materialized->data[0]).get(),
              materialized_raw);
    EXPECT_EQ(mutated_variant.get_sparse_column().get(), sparse_raw);
    EXPECT_EQ(mutated_variant.get_doc_value_column().get(), doc_value_raw);
}

TEST(ColumnMutateSubcolumnsTest, VariantDetachesSharedSubcolumns) {
    auto variant = create_variant_column();
    const auto* root = variant->get_subcolumns().get_root();
    ASSERT_NE(root, nullptr);
    ASSERT_EQ(root->data.data.size(), 1);
    ColumnPtr root_alias = static_cast<const IColumn::Ptr&>(root->data.data[0]);
    auto* materialized = variant->get_subcolumn(PathInData("v.a"));
    ASSERT_NE(materialized, nullptr);
    ASSERT_EQ(materialized->data.size(), 1);
    ColumnPtr materialized_alias = static_cast<const IColumn::Ptr&>(materialized->data[0]);
    ColumnPtr sparse_alias = variant->get_sparse_column();
    ColumnPtr doc_value_alias = variant->get_doc_value_column();

    ColumnPtr variant_ptr = std::move(variant);
    auto mutated = IColumn::mutate(std::move(variant_ptr));
    auto& mutated_variant = assert_cast<ColumnVariant&>(*mutated);
    const auto* mutated_root = mutated_variant.get_subcolumns().get_root();
    ASSERT_NE(mutated_root, nullptr);
    ASSERT_EQ(mutated_root->data.data.size(), 1);
    auto* mutated_materialized = mutated_variant.get_subcolumn(PathInData("v.a"));
    ASSERT_NE(mutated_materialized, nullptr);
    ASSERT_EQ(mutated_materialized->data.size(), 1);

    EXPECT_NE(static_cast<const IColumn::Ptr&>(mutated_root->data.data[0]).get(), root_alias.get());
    EXPECT_NE(static_cast<const IColumn::Ptr&>(mutated_materialized->data[0]).get(),
              materialized_alias.get());
    EXPECT_NE(mutated_variant.get_sparse_column().get(), sparse_alias.get());
    EXPECT_NE(mutated_variant.get_doc_value_column().get(), doc_value_alias.get());

    IColumn::Filter filter {1, 0};
    EXPECT_EQ(mutated_variant.filter(filter), 1);
    EXPECT_EQ(root_alias->size(), 2);
    EXPECT_EQ(materialized_alias->size(), 2);
    EXPECT_EQ(sparse_alias->size(), 2);
    EXPECT_EQ(doc_value_alias->size(), 2);
}

} // namespace doris
