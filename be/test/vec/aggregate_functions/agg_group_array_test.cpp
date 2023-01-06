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

#include "gtest/gtest.h"
#include "vec/aggregate_functions/aggregate_function_collect.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/arena.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

void register_aggregate_function_group_uniq_array(AggregateFunctionSimpleFactory& factory);

class VAggGroupArrayTest : public testing::Test {
private:
    Arena _agg_arena_pool;

public:
    void SetUp() override {
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        register_aggregate_function_group_uniq_array(factory);
    }

    void TearDown() override {}

    template <typename DataType>
    void agg_group_uniq_array_add_elements(AggregateFunctionPtr agg_function,
                                           AggregateDataPtr place, size_t input_nums) {
        using FieldType = typename DataType::FieldType;
        auto type = std::make_shared<DataType>();
        auto input_col = type->create_column();
        for (size_t i = 0; i < input_nums; ++i) {
            if constexpr (std::is_same_v<DataType, DataTypeString>) {
                auto item = std::string("item") + std::to_string(i);
                input_col->insert_data(item.c_str(), item.size());
            } else {
                auto item = FieldType(static_cast<uint64_t>(i));
                input_col->insert_data(reinterpret_cast<const char*>(&item), 0);
            }
        }
        EXPECT_EQ(input_col->size(), input_nums);

        const IColumn* column[1] = {input_col.get()};
        for (int i = 0; i < input_col->size(); i++) {
            agg_function->add(place, column, i, &_agg_arena_pool);
        }
    }

    template <typename DataType>
    void test_agg_group_uniq_array(size_t input_nums = 0) {
        DataTypes data_types = {(DataTypePtr)std::make_shared<DataType>()};
        LOG(INFO) << "test_agg_group_uniq_array for type"
                  << "(" << data_types[0]->get_name() << ")";

        Array array;
        AggregateFunctionSimpleFactory factory = AggregateFunctionSimpleFactory::instance();
        auto agg_function = factory.get("group_uniq_array", data_types, array);
        EXPECT_NE(agg_function, nullptr);

        std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        agg_function->create(place);

        agg_group_uniq_array_add_elements<DataType>(agg_function, place, input_nums);

        ColumnString buf;
        VectorBufferWriter buf_writer(buf);
        agg_function->serialize(place, buf_writer);
        buf_writer.commit();
        VectorBufferReader buf_reader(buf.get_data_at(0));
        agg_function->deserialize(place, buf_reader, &_agg_arena_pool);

        std::unique_ptr<char[]> memory2(new char[agg_function->size_of_data()]);
        AggregateDataPtr place2 = memory2.get();
        agg_function->create(place2);

        agg_group_uniq_array_add_elements<DataType>(agg_function, place2, input_nums);

        agg_function->merge(place, place2, &_agg_arena_pool);
        auto column_result = ColumnArray::create(data_types[0]->create_column());
        agg_function->insert_result_into(place, *column_result);
        EXPECT_EQ(column_result->size(), 1);
        EXPECT_EQ(column_result->get_offsets()[0], input_nums);

        auto column_result2 = ColumnArray::create(data_types[0]->create_column());
        agg_function->insert_result_into(place2, *column_result2);
        EXPECT_EQ(column_result2->size(), 1);
        EXPECT_EQ(column_result->get_offsets()[0], input_nums);

        LOG(INFO) << column_result->get_offsets()[0];
        LOG(INFO) << column_result2->get_offsets()[0];

        agg_function->destroy(place);
        agg_function->destroy(place2);
    }
};

TEST_F(VAggGroupArrayTest, test_empty) {
    test_agg_group_uniq_array<DataTypeInt8>();
    test_agg_group_uniq_array<DataTypeInt16>();
    test_agg_group_uniq_array<DataTypeInt32>();
    test_agg_group_uniq_array<DataTypeInt64>();
    test_agg_group_uniq_array<DataTypeInt128>();

    test_agg_group_uniq_array<DataTypeDecimal<Decimal128>>();
    test_agg_group_uniq_array<DataTypeDate>();
    test_agg_group_uniq_array<DataTypeDateV2>();
    test_agg_group_uniq_array<DataTypeString>();
}

TEST_F(VAggGroupArrayTest, test_with_data) {
    test_agg_group_uniq_array<DataTypeInt32>(8);
    test_agg_group_uniq_array<DataTypeInt128>(8);

    test_agg_group_uniq_array<DataTypeDecimal<Decimal128>>(12);
    test_agg_group_uniq_array<DataTypeDateTime>(14);
    test_agg_group_uniq_array<DataTypeDateTimeV2>(14);
    test_agg_group_uniq_array<DataTypeString>(10);
}
} // namespace doris::vectorized