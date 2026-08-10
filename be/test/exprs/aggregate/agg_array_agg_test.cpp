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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "common/logging.h"
#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_string.h"
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
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/aggregate_function_sort.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {
class IColumn;
} // namespace doris

namespace doris {

struct AggregateFunctionArrayAggTest : public AggregateFunctiontest {};

namespace {

Field array_field(std::initializer_list<Field> values) {
    return Field::create_field<TYPE_ARRAY>(Array(values));
}

Field map_field(std::initializer_list<Field> keys, std::initializer_list<Field> values) {
    return Field::create_field<TYPE_MAP>(Map {array_field(keys), array_field(values)});
}

void add_column_to_state(const IAggregateFunction& function, AggregateDataPtr state,
                         const IColumn& column, Arena& arena) {
    const IColumn* columns[] = {&column};
    for (size_t row = 0; row < column.size(); ++row) {
        function.add(state, columns, row, arena);
    }
}

void check_complex_array_agg_state(const DataTypePtr& data_type,
                                   std::initializer_list<Field> source_values,
                                   std::initializer_list<Field> rhs_values) {
    SCOPED_TRACE(data_type->get_name());
    const auto nullable_type = make_nullable(data_type);
    auto function = AggregateFunctionSimpleFactory::instance().get(
            "array_agg", {nullable_type}, nullptr, false,
            BeExecVersionManager::get_newest_version());
    ASSERT_NE(function, nullptr);
    function->set_version(BeExecVersionManager::get_newest_version());

    auto source_column = nullable_type->create_column();
    for (const auto& value : source_values) {
        source_column->insert(value);
    }
    auto rhs_column = nullable_type->create_column();
    for (const auto& value : rhs_values) {
        rhs_column->insert(value);
    }

    Arena arena;
    AggregateFunctionGuard source(function.get());
    AggregateFunctionGuard restored(function.get());
    AggregateFunctionGuard rhs(function.get());
    add_column_to_state(*function, source.data(), *source_column, arena);
    add_column_to_state(*function, rhs.data(), *rhs_column, arena);

    auto serialized_column = ColumnString::create();
    BufferWritable writer(*serialized_column);
    function->serialize(source.data(), writer);
    writer.commit();
    ASSERT_EQ(serialized_column->size(), 1);

    auto serialized_data = serialized_column->get_data_at(0);
    BufferReadable reader(serialized_data);
    function->deserialize(restored.data(), reader, arena);
    function->merge(restored.data(), rhs.data(), arena);

    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(restored.data(), *result_column);
    auto expected_column = function->get_return_type()->create_column();
    Array expected_values(source_values);
    expected_values.insert(expected_values.end(), rhs_values.begin(), rhs_values.end());
    expected_column->insert(Field::create_field<TYPE_ARRAY>(expected_values));
    EXPECT_TRUE(ColumnHelper::column_equal(std::move(result_column), std::move(expected_column)));
}

} // namespace

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_aint64) {
    create_agg("array_agg", false, {std::make_shared<DataTypeInt64>()},
               std::make_shared<DataTypeInt64>());

    auto data_type = std::make_shared<DataTypeInt64>();
    auto array_data_type = std::make_shared<DataTypeArray>(make_nullable(data_type));

    auto off_column = ColumnOffset64::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 3};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column =
            ColumnArray::create(make_nullable(std::move(data_column)), std::move(off_column));

    execute(Block({ColumnHelper::create_column_with_name<DataTypeInt64>({1, 2, 3})}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_aint64_nullable) {
    auto data_type = make_nullable(std::make_shared<DataTypeInt64>());
    create_agg("array_agg", false, {data_type}, data_type);

    auto array_data_type = std::make_shared<DataTypeArray>(data_type);

    auto off_column = ColumnOffset64::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }
    auto array_column = ColumnArray::create(data_column->clone(), std::move(off_column));

    execute(Block({ColumnWithTypeAndName(data_column->clone(), data_type, "")}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_astr_nullable) {
    auto data_type = make_nullable(std::make_shared<DataTypeString>());
    create_agg("array_agg", false, {data_type}, data_type);

    auto array_data_type = std::make_shared<DataTypeArray>(data_type);

    auto off_column = ColumnOffset64::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), sizeof(v));
    }
    auto array_column = ColumnArray::create(data_column->clone(), std::move(off_column));

    execute(Block({ColumnWithTypeAndName(data_column->clone(), data_type, "")}),
            ColumnWithTypeAndName(std::move(array_column), array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_astr_foreach) {
    auto data_type = make_nullable(std::make_shared<DataTypeString>());
    auto array_data_type = std::make_shared<DataTypeArray>(data_type);
    create_agg("array_agg_foreach", false, {array_data_type}, array_data_type);

    auto off_column = ColumnOffset64::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), sizeof(v));
    }
    auto array_column = ColumnArray::create(data_column->clone(), off_column->clone());

    auto off_column2 = ColumnOffset64::create();
    std::vector<ColumnArray::Offset64> offs2 = {0, 1, 2, 3, 4};
    for (size_t i = 1; i < offs2.size(); ++i) {
        off_column2->insert_data((const char*)(&offs2[i]), 0);
    }

    auto array_array_data_type = std::make_shared<DataTypeArray>(array_data_type);
    auto array_array_off_column = ColumnOffset64::create();
    array_array_off_column->insert_value(4);
    auto nested_array_column = ColumnArray::create(data_column->clone(), off_column2->clone());
    auto nested_array_size = nested_array_column->size();
    auto array_array_column =
            ColumnArray::create(ColumnNullable::create(std::move(nested_array_column),
                                                       ColumnUInt8::create(nested_array_size, 0)),
                                array_array_off_column->clone());
    ASSERT_TRUE(array_array_data_type->check_column(*array_array_column).ok());

    execute(Block({ColumnWithTypeAndName(array_column->clone(), array_data_type, "")}),
            ColumnWithTypeAndName(std::move(array_array_column), array_array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, test_array_agg_aint64_foreach) {
    auto data_type = make_nullable(std::make_shared<DataTypeInt64>());
    auto array_data_type = std::make_shared<DataTypeArray>(data_type);
    create_agg("array_agg_foreach", false, {array_data_type}, array_data_type);

    auto off_column = ColumnOffset64::create();
    auto data_column = data_type->create_column();
    std::vector<ColumnArray::Offset64> offs = {0, 4};
    std::vector<int64_t> vals = {1, 2, 3};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    data_column->insert_default();
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), sizeof(v));
    }
    auto array_column = ColumnArray::create(data_column->clone(), off_column->clone());

    auto off_column2 = ColumnOffset64::create();
    std::vector<ColumnArray::Offset64> offs2 = {0, 1, 2, 3, 4};
    for (size_t i = 1; i < offs2.size(); ++i) {
        off_column2->insert_data((const char*)(&offs2[i]), 0);
    }

    auto array_array_data_type = std::make_shared<DataTypeArray>(array_data_type);
    auto array_array_off_column = ColumnOffset64::create();
    array_array_off_column->insert_value(4);
    auto nested_array_column = ColumnArray::create(data_column->clone(), off_column2->clone());
    auto nested_array_size = nested_array_column->size();
    auto array_array_column =
            ColumnArray::create(ColumnNullable::create(std::move(nested_array_column),
                                                       ColumnUInt8::create(nested_array_size, 0)),
                                array_array_off_column->clone());
    ASSERT_TRUE(array_array_data_type->check_column(*array_array_column).ok());

    execute(Block({ColumnWithTypeAndName(array_column->clone(), array_data_type, "")}),
            ColumnWithTypeAndName(std::move(array_array_column), array_array_data_type, "column"));
}

TEST_F(AggregateFunctionArrayAggTest, complex_type_state_serialize_deserialize_and_merge) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto nullable_string = make_nullable(std::make_shared<DataTypeString>());

    Array streamvbyte_values;
    for (int32_t value = 0; value < 65; ++value) {
        streamvbyte_values.emplace_back(Field::create_field<TYPE_INT>(value));
    }
    check_complex_array_agg_state(std::make_shared<DataTypeArray>(nullable_int),
                                  {Field::create_field<TYPE_ARRAY>(std::move(streamvbyte_values))},
                                  {});

    check_complex_array_agg_state(
            std::make_shared<DataTypeArray>(nullable_int),
            {array_field({Field::create_field<TYPE_INT>(1), Field()}), Field()},
            {array_field({Field::create_field<TYPE_INT>(2), Field::create_field<TYPE_INT>(3)})});

    check_complex_array_agg_state(
            std::make_shared<DataTypeStruct>(DataTypes {nullable_int, nullable_string}),
            {Field::create_field<TYPE_STRUCT>(Struct {Field::create_field<TYPE_INT>(1),
                                                      Field::create_field<TYPE_STRING>("one")}),
             Field()},
            {Field::create_field<TYPE_STRUCT>(
                    Struct {Field(), Field::create_field<TYPE_STRING>("two")})});

    check_complex_array_agg_state(std::make_shared<DataTypeMap>(nullable_string, nullable_int),
                                  {map_field({Field::create_field<TYPE_STRING>("one"),
                                              Field::create_field<TYPE_STRING>("null")},
                                             {Field::create_field<TYPE_INT>(1), Field()}),
                                   Field()},
                                  {map_field({Field::create_field<TYPE_STRING>("two")},
                                             {Field::create_field<TYPE_INT>(2)})});
}

TEST_F(AggregateFunctionArrayAggTest, foreach_complex_type_state_growth_and_round_trip) {
    auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    auto nullable_inner_array = make_nullable(std::make_shared<DataTypeArray>(nullable_int));
    auto input_type = std::make_shared<DataTypeArray>(nullable_inner_array);
    auto function = AggregateFunctionSimpleFactory::instance().get(
            "array_agg_foreach", {input_type}, input_type, false,
            BeExecVersionManager::get_newest_version(), {.is_foreach = true, .column_names = {}});
    ASSERT_NE(function, nullptr);
    function->set_version(BeExecVersionManager::get_newest_version());

    auto input_column = input_type->create_column();
    input_column->insert(array_field({array_field({Field::create_field<TYPE_INT>(1)})}));
    input_column->insert(array_field(
            {array_field({Field::create_field<TYPE_INT>(2)}),
             array_field({Field::create_field<TYPE_INT>(3), Field::create_field<TYPE_INT>(4)}),
             Field()}));

    Arena arena;
    AggregateFunctionGuard source(function.get());
    AggregateFunctionGuard restored(function.get());
    AggregateFunctionGuard merged(function.get());
    add_column_to_state(*function, source.data(), *input_column, arena);

    auto serialized_column = ColumnString::create();
    BufferWritable writer(*serialized_column);
    function->serialize(source.data(), writer);
    writer.commit();

    auto serialized_data = serialized_column->get_data_at(0);
    BufferReadable reader(serialized_data);
    function->deserialize(restored.data(), reader, arena);
    function->merge(merged.data(), restored.data(), arena);

    auto result_column = function->get_return_type()->create_column();
    function->insert_result_into(merged.data(), *result_column);
    auto expected_column = function->get_return_type()->create_column();
    expected_column->insert(
            array_field({array_field({array_field({Field::create_field<TYPE_INT>(1)}),
                                      array_field({Field::create_field<TYPE_INT>(2)})}),
                         array_field({array_field({Field::create_field<TYPE_INT>(3),
                                                   Field::create_field<TYPE_INT>(4)})}),
                         array_field({Field()})}));
    EXPECT_TRUE(ColumnHelper::column_equal(std::move(result_column), std::move(expected_column)));
}

TEST(AggregateFunctionSortDataTest, merge_does_not_share_rhs_block) {
    auto data_type = std::make_shared<DataTypeInt64>();
    Block prototype({ColumnWithTypeAndName(data_type->create_column(), data_type, "value"),
                     ColumnWithTypeAndName(data_type->create_column(), data_type, "sort_key")});
    SortDescription sort_desc {SortColumnDescription(1, 1, 1)};

    AggregateFunctionSortData lhs(sort_desc, prototype);
    AggregateFunctionSortData rhs1(sort_desc, prototype);
    AggregateFunctionSortData rhs2(sort_desc, prototype);

    auto values = ColumnInt64::create();
    values->insert_value(10);
    values->insert_value(20);
    auto sort_keys = ColumnInt64::create();
    sort_keys->insert_value(2);
    sort_keys->insert_value(1);
    const IColumn* row0[] = {values.get(), sort_keys.get()};
    const IColumn* row1[] = {values.get(), sort_keys.get()};

    rhs1.add(row0, 2, 0);
    rhs2.add(row1, 2, 1);

    lhs.merge(rhs1);
    ASSERT_NO_THROW(lhs.merge(rhs2));
    ASSERT_EQ(lhs.block.rows(), 2);
    ASSERT_EQ(rhs1.block.rows(), 1);
}

} // namespace doris
