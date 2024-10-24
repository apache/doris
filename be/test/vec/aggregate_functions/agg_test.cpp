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
#include <stdint.h>

#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_topn.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

const int agg_test_batch_size = 4096;

namespace doris::vectorized {
// declare function
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_topn(AggregateFunctionSimpleFactory& factory);

TEST(AggTest, basic_test) {
    auto column_vector_int32 = ColumnVector<Int32>::create();
    for (int i = 0; i < agg_test_batch_size; i++) {
        column_vector_int32->insert(cast_to_nearest_field_type(i));
    }
    // test implement interface
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {data_type};
    auto agg_function = factory.get("sum", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, column, i, nullptr);
    }
    int ans = 0;
    for (int i = 0; i < agg_test_batch_size; i++) {
        ans += i;
    }
    EXPECT_EQ(ans, *reinterpret_cast<int32_t*>(place));
    agg_function->destroy(place);
}

TEST(AggTest, topn_test) {
    MutableColumns datas(2);
    datas[0] = ColumnString::create();
    datas[1] = ColumnInt32::create();
    int top = 10;

    for (int i = 0; i < agg_test_batch_size; i++) {
        std::string str = std::to_string(agg_test_batch_size / (i + 1));
        datas[0]->insert_data(str.c_str(), str.length());
        datas[1]->insert_data(reinterpret_cast<char*>(&top), sizeof(top));
    }

    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_topn(factory);
    DataTypes data_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()};

    auto agg_function = factory.get("topn", data_types, false, -1);
    std::unique_ptr<char[]> memory(new char[agg_function->size_of_data()]);
    AggregateDataPtr place = memory.get();
    agg_function->create(place);

    IColumn* columns[2] = {datas[0].get(), datas[1].get()};

    for (int i = 0; i < agg_test_batch_size; i++) {
        agg_function->add(place, const_cast<const IColumn**>(columns), i, nullptr);
    }

    std::string result = reinterpret_cast<AggregateFunctionTopNData<std::string>*>(place)->get();
    std::string expect_result =
            "{\"1\":2048,\"2\":683,\"3\":341,\"4\":205,\"5\":137,\"6\":97,\"7\":73,\"8\":57,\"9\":"
            "46,\"10\":37}";
    EXPECT_EQ(result, expect_result);
    agg_function->destroy(place);
}
} // namespace doris::vectorized
