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

#pragma once

#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "udf/udf.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
class VInPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VInPredicate);

public:
    VInPredicate(const TExprNode& node);
    ~VInPredicate() override = default;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;

    std::string debug_string() const override;

    size_t skip_constant_args_size() const;

    const FunctionBasePtr function() { return _function; }

    bool is_not_in() const { return _is_not_in; };
    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override;

private:
    FunctionBasePtr _function;
    std::string _expr_name;

    const bool _is_not_in;
    static const constexpr char* function_name = "in";
    uint32_t _in_list_value_count_threshold = 10;
    bool _is_args_all_constant = false;
};
} // namespace doris::vectorized