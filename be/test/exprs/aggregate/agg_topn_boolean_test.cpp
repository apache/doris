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
#include "exprs/aggregate/agg_function_test.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

struct AggregateFunctionTopNBooleanTest : public AggregateFunctiontest {
    void check_topn(ColumnWithTypeAndName values, const std::vector<Int64>& weights, int top_num,
                    const Field& expected_array, bool weighted, bool expanded) {
        const std::string name = weighted ? "topn_weighted" : "topn_array";
        SCOPED_TRACE(name + (expanded ? " with expansion rate" : " without expansion rate"));
        SCOPED_TRACE(top_num);
        Block block({values});
        DataTypes argument_types {values.type};
        if (weighted) {
            auto weight_column = ColumnHelper::create_column_with_name<DataTypeInt64>(weights);
            block.insert(weight_column);
            argument_types.push_back(weight_column.type);
        }
        auto top_column = ColumnHelper::create_column_with_name<DataTypeInt32>(
                std::vector<Int32>(values.column->size(), top_num));
        block.insert(top_column);
        argument_types.push_back(top_column.type);
        if (expanded) {
            auto rate_column = ColumnHelper::create_column_with_name<DataTypeInt32>(
                    std::vector<Int32>(values.column->size(), 50));
            block.insert(rate_column);
            argument_types.push_back(rate_column.type);
        }

        const bool nullable = values.type->is_nullable();
        DataTypePtr result_type =
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));
        if (nullable) {
            result_type = make_nullable(result_type);
        }
        auto function = AggregateFunctionSimpleFactory::instance().get(
                name, argument_types, result_type, nullable,
                BeExecVersionManager::get_newest_version());
        ASSERT_NE(function, nullptr);
        EXPECT_TRUE(function->get_return_type()->equals(*result_type));

        auto expected_column = result_type->create_column();
        expected_column->insert(expected_array);
        create_agg(name, nullable, argument_types, result_type);
        execute(block, ColumnWithTypeAndName(std::move(expected_column), result_type, "expected"));
    }

    static Field boolean_array(std::initializer_list<UInt8> values) {
        Array array;
        for (auto value : values) {
            array.push_back(Field::create_field<TYPE_BOOLEAN>(value));
        }
        return Field::create_field<TYPE_ARRAY>(std::move(array));
    }
};

TEST_F(AggregateFunctionTopNBooleanTest, ValuesAndLimits) {
    for (bool weighted : {false, true}) {
        for (bool expanded : {false, true}) {
            auto values = ColumnHelper::create_column_with_name<DataTypeBool>({false, false, true});
            check_topn(values, {1, 1, 5}, 1, boolean_array({weighted}), weighted, expanded);
            for (int top_num : {2, 3}) {
                check_topn(values, {1, 1, 5}, top_num, boolean_array({weighted, !weighted}),
                           weighted, expanded);
            }
        }
    }
}

TEST_F(AggregateFunctionTopNBooleanTest, NullableValues) {
    for (bool weighted : {false, true}) {
        for (bool expanded : {false, true}) {
            auto values = ColumnHelper::create_nullable_column_with_name<DataTypeBool>(
                    {false, false, true, true}, {0, 0, 0, 1});
            check_topn(values, {1, 1, 5, 100}, 2, boolean_array({weighted, !weighted}), weighted,
                       expanded);

            auto nulls = ColumnHelper::create_nullable_column_with_name<DataTypeBool>({false, true},
                                                                                      {1, 1});
            check_topn(nulls, {1, 5}, 2, Field(), weighted, expanded);
        }
    }
}

TEST_F(AggregateFunctionTopNBooleanTest, Ties) {
    for (bool weighted : {false, true}) {
        for (bool expanded : {false, true}) {
            auto values = ColumnHelper::create_column_with_name<DataTypeBool>({false, true});
            check_topn(values, {3, 3}, 2, boolean_array({true, false}), weighted, expanded);
        }
    }
}

} // namespace doris
