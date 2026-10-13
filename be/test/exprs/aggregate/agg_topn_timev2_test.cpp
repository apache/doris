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

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_time.h"
#include "core/value/time_value.h"
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

struct AggregateFunctionTopNTimeV2Test : public AggregateFunctiontest {
    void check_topn(const ColumnWithTypeAndName& values, int top_num, const Field& expected_array,
                    bool expanded) {
        SCOPED_TRACE(values.type->get_name());
        SCOPED_TRACE(expanded);
        SCOPED_TRACE(top_num);
        Block block({values});
        auto top_column = ColumnHelper::create_column_with_name<DataTypeInt32>(
                std::vector<Int32>(values.column->size(), top_num));
        block.insert(top_column);
        DataTypes argument_types {values.type, top_column.type};
        if (expanded) {
            auto rate_column = ColumnHelper::create_column_with_name<DataTypeInt32>(
                    std::vector<Int32>(values.column->size(), 100));
            block.insert(rate_column);
            argument_types.push_back(rate_column.type);
        }

        const bool nullable = values.type->is_nullable();
        DataTypePtr result_type = std::make_shared<DataTypeArray>(make_nullable(values.type));
        if (nullable) {
            result_type = make_nullable(result_type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                "topn_array", argument_types, result_type, nullable,
                BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        EXPECT_TRUE(function->get_return_type()->equals(*result_type));

        auto expected_column = result_type->create_column();
        expected_column->insert(expected_array);
        create_agg("topn_array", nullable, argument_types, result_type);
        execute(block, ColumnWithTypeAndName(std::move(expected_column), result_type, "expected"));
    }

    static Field time_array(std::initializer_list<Float64> values) {
        Array array;
        for (auto value : values) {
            array.push_back(Field::create_field<TYPE_TIMEV2>(value));
        }
        return Field::create_field<TYPE_ARRAY>(std::move(array));
    }
};

TEST_F(AggregateFunctionTopNTimeV2Test, PrecisionAndLimits) {
    const Float64 large = TimeValue::make_time(838, 59, 58, 0);
    Float64 step = TimeValue::ONE_SECOND_MICROSECONDS;
    for (UInt32 scale = 0; scale <= DataTypeTimeV2::MAX_SCALE; ++scale, step /= 10) {
        for (bool expanded : {false, true}) {
            ColumnWithTypeAndName values(ColumnHelper::create_column<DataTypeTimeV2>(
                                                 {large, large, large + step, -step}),
                                         std::make_shared<DataTypeTimeV2>(scale), "values");
            check_topn(values, 1, time_array({large}), expanded);
            check_topn(values, 5, time_array({large, large + step, -step}), expanded);
        }
    }
}

TEST_F(AggregateFunctionTopNTimeV2Test, NegativeValuesAndZero) {
    const Float64 minimum = TimeValue::make_time(838, 59, 59, 0, true);
    for (bool expanded : {false, true}) {
        ColumnWithTypeAndName values(
                ColumnHelper::create_column<DataTypeTimeV2>({minimum, -1000000, 0}),
                std::make_shared<DataTypeTimeV2>(6), "values");
        check_topn(values, 3, time_array({0, -1000000, minimum}), expanded);
    }
}

TEST_F(AggregateFunctionTopNTimeV2Test, NullableValues) {
    Float64 step = TimeValue::ONE_SECOND_MICROSECONDS;
    for (UInt32 scale = 0; scale <= DataTypeTimeV2::MAX_SCALE; ++scale, step /= 10) {
        for (bool expanded : {false, true}) {
            auto type = make_nullable(std::make_shared<DataTypeTimeV2>(scale));
            ColumnWithTypeAndName values(ColumnHelper::create_nullable_column<DataTypeTimeV2>(
                                                 {step, step, -step, 0}, {0, 0, 0, 1}),
                                         type, "values");
            check_topn(values, 2, time_array({step, -step}), expanded);

            ColumnWithTypeAndName nulls(
                    ColumnHelper::create_nullable_column<DataTypeTimeV2>({0, -step}, {1, 1}), type,
                    "values");
            check_topn(nulls, 2, Field(), expanded);
        }
    }
}

} // namespace doris
