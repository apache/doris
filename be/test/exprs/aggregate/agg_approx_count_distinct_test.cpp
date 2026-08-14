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
#include <string>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

void register_aggregate_function_approx_count_distinct(AggregateFunctionSimpleFactory& factory);

TEST(AggApproxCountDistinctTest, TimeStampNs) {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_approx_count_distinct(factory);

    auto data_type = std::make_shared<DataTypeTimeStampNs>();
    const DataTypes argument_types = {data_type};
    for (const std::string function_name : {"approx_count_distinct", "ndv"}) {
        auto function = factory.get(function_name, argument_types, nullptr, false, -1);
        ASSERT_NE(function, nullptr);

        auto column = ColumnTimeStampNs::create();
        for (const int64_t epoch_nanos : {-1, 0, 1, 1, 1'000'000'001}) {
            column->insert_value(TimeStampNsValue(epoch_nanos));
        }

        std::unique_ptr<char[]> memory(new char[function->size_of_data()]);
        AggregateDataPtr place = memory.get();
        function->create(place);
        Arena arena;
        const IColumn* columns[] = {column.get()};
        for (size_t row = 0; row < column->size(); ++row) {
            function->add(place, columns, row, arena);
        }

        auto result = ColumnInt64::create();
        function->insert_result_into(place, *result);
        EXPECT_EQ(result->get_element(0), 4);
        function->destroy(place);
    }
}

} // namespace doris
