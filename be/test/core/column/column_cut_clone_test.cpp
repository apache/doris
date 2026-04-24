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

#include <cstdint>
#include <vector>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/column/predicate_column.h"

namespace doris {
namespace {

using ColumnTree = std::vector<const IColumn*>;

void collect_column_tree(const IColumn& column, ColumnTree& tree) {
    tree.push_back(&column);
    IColumn::ColumnCallback callback = [&](IColumn::WrappedPtr& subcolumn) {
        collect_column_tree(*subcolumn, tree);
    };
    const_cast<IColumn&>(column).for_each_subcolumn(callback);
}

void assert_fresh_column_tree(const IColumn& source, const IColumn& result) {
    ColumnTree source_tree;
    ColumnTree result_tree;
    collect_column_tree(source, source_tree);
    collect_column_tree(result, result_tree);

    ASSERT_EQ(source_tree.size(), result_tree.size())
            << "source=" << source.dump_structure() << ", result=" << result.dump_structure();

    for (size_t i = 0; i < source_tree.size(); ++i) {
        EXPECT_EQ(source_tree[i]->get_name(), result_tree[i]->get_name()) << "node index=" << i;
        EXPECT_NE(source_tree[i], result_tree[i])
                << "shared node index=" << i << ", node=" << source_tree[i]->get_name();
    }
}

MutableColumnPtr create_int_column() {
    auto column = ColumnInt64::create();
    column->insert_value(1);
    column->insert_value(2);
    column->insert_value(3);
    return column;
}

MutableColumnPtr create_string_column() {
    auto column = ColumnString::create();
    column->insert_data("alpha", 5);
    column->insert_data("beta", 4);
    column->insert_data("gamma", 5);
    return column;
}

MutableColumnPtr create_nullable_column() {
    auto nested = ColumnInt64::create();
    nested->insert_value(10);
    nested->insert_value(20);
    nested->insert_value(30);

    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);
    null_map->insert_value(0);
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

MutableColumnPtr create_const_column() {
    auto nested = ColumnInt64::create();
    nested->insert_value(7);

    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    auto nullable = ColumnNullable::create(std::move(nested), std::move(null_map));
    return ColumnConst::create(nullable->get_ptr(), 3);
}

MutableColumnPtr create_array_column() {
    auto column = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
    Array first = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2)};
    Array second = {Field::create_field<TYPE_BIGINT>(3)};
    column->insert(Field::create_field<TYPE_ARRAY>(first));
    column->insert(Field::create_field<TYPE_ARRAY>(Array()));
    column->insert(Field::create_field<TYPE_ARRAY>(second));
    return column;
}

MutableColumnPtr create_map_column() {
    auto column = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());

    Array keys1 = {Field::create_field<TYPE_STRING>("k1"), Field::create_field<TYPE_STRING>("k2")};
    Array values1 = {Field::create_field<TYPE_BIGINT>(11), Field::create_field<TYPE_BIGINT>(22)};
    Map map1;
    map1.push_back(Field::create_field<TYPE_ARRAY>(keys1));
    map1.push_back(Field::create_field<TYPE_ARRAY>(values1));

    Array keys2 = {Field::create_field<TYPE_STRING>("k3")};
    Array values2 = {Field::create_field<TYPE_BIGINT>(33)};
    Map map2;
    map2.push_back(Field::create_field<TYPE_ARRAY>(keys2));
    map2.push_back(Field::create_field<TYPE_ARRAY>(values2));

    Map empty_map;
    empty_map.push_back(Field::create_field<TYPE_ARRAY>(Array()));
    empty_map.push_back(Field::create_field<TYPE_ARRAY>(Array()));

    column->insert(Field::create_field<TYPE_MAP>(map1));
    column->insert(Field::create_field<TYPE_MAP>(empty_map));
    column->insert(Field::create_field<TYPE_MAP>(map2));
    return column;
}

MutableColumnPtr create_struct_column() {
    MutableColumns columns;

    auto int_column = ColumnInt32::create();
    int_column->insert_value(1);
    int_column->insert_value(2);
    int_column->insert_value(3);
    columns.emplace_back(std::move(int_column));

    auto string_column = ColumnString::create();
    string_column->insert_data("one", 3);
    string_column->insert_data("two", 3);
    string_column->insert_data("three", 5);
    columns.emplace_back(std::move(string_column));

    return ColumnStruct::create(std::move(columns));
}

MutableColumnPtr create_varbinary_column() {
    auto column = ColumnVarbinary::create();
    column->insert_data("ab", 2);
    column->insert_data("cdef", 4);
    column->insert_data("ghi", 3);
    return column;
}

MutableColumnPtr create_fixed_length_object_column() {
    auto column = ColumnFixedLengthObject::create(sizeof(int64_t));
    const int64_t first = 101;
    const int64_t second = 202;
    const int64_t third = 303;
    column->insert_data(reinterpret_cast<const char*>(&first), sizeof(first));
    column->insert_data(reinterpret_cast<const char*>(&second), sizeof(second));
    column->insert_data(reinterpret_cast<const char*>(&third), sizeof(third));
    return column;
}

std::vector<MutableColumnPtr> create_columns_supporting_cut_and_clone() {
    std::vector<MutableColumnPtr> columns;
    columns.emplace_back(create_int_column());
    columns.emplace_back(create_string_column());
    columns.emplace_back(create_nullable_column());
    columns.emplace_back(create_const_column());
    columns.emplace_back(create_array_column());
    columns.emplace_back(create_map_column());
    columns.emplace_back(create_struct_column());
    columns.emplace_back(create_varbinary_column());
    columns.emplace_back(create_fixed_length_object_column());
    return columns;
}

} // namespace

TEST(ColumnCutCloneTest, CloneResizedReturnsFreshColumnTree) {
    auto columns = create_columns_supporting_cut_and_clone();
    for (const auto& column : columns) {
        SCOPED_TRACE(column->dump_structure());
        auto cloned = column->clone_resized(column->size());
        ASSERT_NE(cloned.get(), column.get());
        ASSERT_EQ(cloned->size(), column->size());
        assert_fresh_column_tree(*column, *cloned);
    }

    auto dict_column = ColumnDictI32::create(FieldType::OLAP_FIELD_TYPE_VARCHAR);
    auto cloned_dict = dict_column->clone_resized(0);
    ASSERT_NE(cloned_dict.get(), dict_column.get());
    ASSERT_EQ(cloned_dict->size(), 0);
    assert_fresh_column_tree(*dict_column, *cloned_dict);

    auto predicate_column = PredicateColumnType<TYPE_INT>::create();
    auto cloned_predicate = predicate_column->clone_resized(0);
    ASSERT_NE(cloned_predicate.get(), predicate_column.get());
    ASSERT_EQ(cloned_predicate->size(), 0);
    assert_fresh_column_tree(*predicate_column, *cloned_predicate);
}

TEST(ColumnCutCloneTest, CutReturnsFreshColumnTree) {
    auto columns = create_columns_supporting_cut_and_clone();
    for (const auto& column : columns) {
        SCOPED_TRACE(column->dump_structure());
        ASSERT_GT(column->size(), 1);
        const size_t original_size = column->size();
        auto cutted = column->cut(1, original_size - 1);
        ASSERT_NE(cutted.get(), column.get());
        ASSERT_EQ(cutted->size(), original_size - 1);
        ASSERT_EQ(column->size(), original_size);
        assert_fresh_column_tree(*column, *cutted);
    }
}

} // namespace doris