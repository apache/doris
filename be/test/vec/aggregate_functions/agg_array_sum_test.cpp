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

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_array_sum.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

void register_aggregate_function_array_sum(AggregateFunctionSimpleFactory& factory);

class VAggArraySumTest : public testing::Test {
public:
    void SetUp() {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_array_sum(factory);
    }

    void TearDown() {}

    template <typename DataType>
    void agg_collect_array_add_elements(AggregateFunctionPtr agg_function, AggregateDataPtr place1,
                                        IColumn* column, size_t input_nums) {
        using FieldType = typename DataType::FieldType;

        // fill array: [0],[1,1],[2,2,2],[3,3,3,3]...[input_nums-1,...]
        for (int8_t i = 0; i < input_nums; ++i) {
            Array array;
            if constexpr (IsFloatNumber<FieldType>) {
                Field item = Field::create_field<DataType::PType>(FieldType {i + 0.1F});
                array.resize(i + 1, item);
            } else if constexpr (IsDecimalNumber<FieldType>) {
                Field item = Field::create_field<DataType::PType>(DecimalField<FieldType>(i, 20));
                array.resize(i + 1, item);
            } else {
                Field item = Field::create_field<DataType::PType>(FieldType {i});
                array.resize(i + 1, item);
            }
            column->insert(Field::create_field<TYPE_ARRAY>(array));
        }
        const IColumn* columns[1] = {column};
        for (size_t i = 0; i < input_nums; ++i) {
            agg_function->add(place1, columns, i, _agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_array_sum() {
        size_t input_nums = 10;
        const std::string fn_name {"agg_array_sum"};
        vectorized::DataTypePtr nested_type(make_nullable(std::make_shared<DataType>()));
        vectorized::DataTypePtr array_type(
                std::make_shared<vectorized::DataTypeArray>(nested_type));
        DataTypes data_types = {(DataTypePtr)array_type};
        LOG(INFO) << "test " << fn_name << "(" << data_types[0]->get_name() << ")";
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        // test get agg func
        auto agg_function = factory.get(fn_name, data_types, false, -1);
        EXPECT_NE(agg_function, nullptr);
        EXPECT_EQ(agg_function->get_name(), "agg_array_sum");

        // test return type
        EXPECT_FALSE(agg_function->get_return_type()->is_nullable());
        EXPECT_EQ(agg_function->get_return_type()->get_name(), array_type->get_name());

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place1 = memory.get();
        agg_function->create(place1);

        // test add func
        auto column1 = agg_function->get_return_type()->create_column();
        agg_collect_array_add_elements<DataType>(agg_function, place1, column1.get(), input_nums);

        // test serialize and deserialize
        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place1, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place1, buf_reader, _agg_arena_pool);

        // test insert_result_into func
        auto column_result1 = ColumnArray::create(nested_type->create_column());
        agg_function->insert_result_into(place1, *column_result1);
        EXPECT_EQ(column_result1->size(), 1);
        EXPECT_EQ(column_result1->get_data().size(), input_nums);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        // test merge func
        auto column2 = agg_function->get_return_type()->create_column();
        agg_collect_array_add_elements<DataType>(agg_function, place2, column2.get(), input_nums);
        agg_function->merge(place1, place2, _agg_arena_pool);
        auto column_result2 = ColumnArray::create(nested_type->create_column());
        agg_function->insert_result_into(place2, *column_result2);
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_EQ(column_result2->get_data().size(), input_nums);

        // destroy
        agg_function->destroy(place1);
        agg_function->destroy(place2);
    }

private:
    Arena _agg_arena_pool;
};

TEST_F(VAggArraySumTest, test_array_sum) {
    test_agg_array_sum<DataTypeInt8>();
    test_agg_array_sum<DataTypeInt16>();
    test_agg_array_sum<DataTypeInt32>();
    test_agg_array_sum<DataTypeInt64>();
    test_agg_array_sum<DataTypeInt128>();
    test_agg_array_sum<DataTypeFloat32>();
    test_agg_array_sum<DataTypeFloat64>();
    test_agg_array_sum<DataTypeDecimal32>();
    test_agg_array_sum<DataTypeDecimal64>();
    test_agg_array_sum<DataTypeDecimalV2>();
    test_agg_array_sum<DataTypeDecimal128>();
}

} // namespace doris::vectorized