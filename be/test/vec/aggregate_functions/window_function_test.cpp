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

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "vec/aggregate_functions/aggregate_function_window.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
// declare function
void register_aggregate_function_window_lead_lag(AggregateFunctionSimpleFactory& factory);

TEST(Window_Function_Test, nth_test) {
    MutableColumns datas(2);
    datas[0] = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    datas[1] = ColumnInt64::create();
    int64_t nth = 3;

    int partition_size = 20;
    for (int i = 0; i < partition_size; i++) {
        datas[0]->insert_data(reinterpret_cast<char*>(&i), sizeof(i));
        datas[1]->insert_data(reinterpret_cast<char*>(&nth), sizeof(nth));
    }

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_window_lead_lag(factory);
    DataTypes data_types = {
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
        std::make_shared<DataTypeInt64>()
    };
    Array array;

    auto nth_function = factory.get("nth_value", data_types, array, true);
    std::unique_ptr<char[]> memory(new char[nth_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    nth_function->create(place);
    IColumn* columns[2] = {datas[0].get(), datas[1].get()};

    {
        DataTypePtr res =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto reslut_col_ptr = res->create_column();
        IColumn* reslut_col = reslut_col_ptr.get();

        nth_function->reset(place);
        for (int i = 0; i < partition_size; i++) {
            nth_function->add_range_single_place(0, partition_size,
                                                 0, partition_size, place,
                const_cast<const IColumn**>(columns), nullptr);
            nth_function->insert_result_into(place, *reslut_col);
            ASSERT_EQ(reslut_col->get64(i), nth - 1);
        }
    }

    {
        DataTypePtr res =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto reslut_col_ptr = res->create_column();
        IColumn* reslut_col = reslut_col_ptr.get();

        nth_function->reset(place);
        for (int i = 0; i < partition_size; i++) {
            nth_function->add_range_single_place(0, partition_size, i, i + 1, place,
                const_cast<const IColumn**>(columns), nullptr);
            nth_function->insert_result_into(place, *reslut_col);
            if (i < nth - 1) {
                ASSERT_TRUE(reslut_col_ptr->is_null_at(i));
            } else {
                ASSERT_FALSE(reslut_col_ptr->is_null_at(i));
                ASSERT_EQ(reslut_col->get64(i), nth - 1);
            }
        }
    }

    {
        DataTypePtr res =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        auto reslut_col_ptr = res->create_column();
        IColumn* reslut_col = reslut_col_ptr.get();

        int64_t start_offset = 2;
        int64_t end_offset = 1;
        for (int i = 0; i < partition_size; i++) {
            nth_function->reset(place);
            nth_function->add_range_single_place(0, partition_size,
                    i - start_offset , i + end_offset + 1, place,
                    const_cast<const IColumn**>(columns), nullptr);
            nth_function->insert_result_into(place, *reslut_col);
            uint64_t pos = (nth - (end_offset + 1));

            if (i < pos) {
                ASSERT_TRUE(reslut_col_ptr->is_null_at(i));
            } else {
                ASSERT_FALSE(reslut_col_ptr->is_null_at(i));
                int32_t expected_pos = std::max(int64(0), i - start_offset) + nth - 1;
                int32_t expected_value = columns[0]->get64(expected_pos);
                ASSERT_EQ(reslut_col->get64(i), expected_value);
            }
        }
    }
}
} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
