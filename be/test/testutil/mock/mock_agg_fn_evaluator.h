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

#include "common/object_pool.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::vectorized {

AggFnEvaluator* create_mock_agg_fn_evaluator(ObjectPool& pool, bool is_merge = false,
                                             bool without_key = false);

AggFnEvaluator* create_mock_agg_fn_evaluator(ObjectPool& pool, VExprContextSPtrs input_exprs_ctxs,
                                             bool is_merge = false, bool without_key = false);

AggFnEvaluator* create_agg_fn(ObjectPool& pool, const std::string& agg_fn_name,
                              const DataTypes& args_types, bool result_nullable,
                              bool is_window_function = false);

class MockAggFnEvaluator : public AggFnEvaluator {
public:
    MockAggFnEvaluator(bool is_merge, bool without_key, bool is_window_function = false)
            : AggFnEvaluator(is_merge, without_key, is_window_function) {}
};

} // namespace doris::vectorized