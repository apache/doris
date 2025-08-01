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

#include "mock_agg_fn_evaluator.h"

#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "mock_slot_ref.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr_context.h"
namespace doris::vectorized {

AggFnEvaluator* create_mock_agg_fn_evaluator(ObjectPool& pool, bool is_merge, bool without_key) {
    auto* mock_agg_fn_evaluator = pool.add(new MockAggFnEvaluator(is_merge, without_key));
    mock_agg_fn_evaluator->_function = AggregateFunctionSimpleFactory::instance().get(
            "sum", {std::make_shared<DataTypeInt64>()}, false,
            BeExecVersionManager::get_newest_version(),
            {.enable_decimal256 = false, .column_names = {}});
    EXPECT_TRUE(mock_agg_fn_evaluator->_function != nullptr);
    mock_agg_fn_evaluator->_input_exprs_ctxs =
            MockSlotRef::create_mock_contexts(mock_agg_fn_evaluator->_function->get_return_type());
    return mock_agg_fn_evaluator;
}

AggFnEvaluator* create_mock_agg_fn_evaluator(ObjectPool& pool, VExprContextSPtrs input_exprs_ctxs,
                                             bool is_merge, bool without_key) {
    auto* mock_agg_fn_evaluator = pool.add(new MockAggFnEvaluator(is_merge, without_key));
    mock_agg_fn_evaluator->_function = AggregateFunctionSimpleFactory::instance().get(
            "sum", {std::make_shared<DataTypeInt64>()}, false,
            BeExecVersionManager::get_newest_version(),
            {.enable_decimal256 = false, .column_names = {}});
    EXPECT_TRUE(mock_agg_fn_evaluator->_function != nullptr);
    mock_agg_fn_evaluator->_input_exprs_ctxs = input_exprs_ctxs;
    return mock_agg_fn_evaluator;
}

AggFnEvaluator* create_agg_fn(ObjectPool& pool, const std::string& agg_fn_name,
                              const DataTypes& args_types, bool result_nullable,
                              bool is_window_function) {
    auto* mock_agg_fn_evaluator =
            pool.add(new MockAggFnEvaluator(false, false, is_window_function)); // just falg;
    mock_agg_fn_evaluator->_function = AggregateFunctionSimpleFactory::instance().get(
            agg_fn_name, args_types, result_nullable, BeExecVersionManager::get_newest_version(),
            {.enable_decimal256 = false,
             .is_window_function = is_window_function,
             .column_names = {}});
    EXPECT_TRUE(mock_agg_fn_evaluator->_function != nullptr);
    for (int i = 0; i < args_types.size(); i++) {
        mock_agg_fn_evaluator->_input_exprs_ctxs.push_back(
                MockSlotRef::create_mock_context(i, args_types[i]));
    }
    mock_agg_fn_evaluator->_data_type = mock_agg_fn_evaluator->_function->get_return_type();
    return mock_agg_fn_evaluator;
}

} // namespace doris::vectorized