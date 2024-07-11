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
#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <string>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "udf/udf.h"
#include "vec/core/column_numbers.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"
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
class VectorizedFnCall : public VExpr {
    ENABLE_FACTORY_CREATOR(VectorizedFnCall);

public:
    VectorizedFnCall(const TExprNode& node);
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    Status execute_runtime_fitler(doris::vectorized::VExprContext* context,
                                  doris::vectorized::Block* block, int* result_column_id,
                                  std::vector<size_t>& args) override;
    Status eval_inverted_index(
            VExprContext* context,
            const std::unordered_map<ColumnId, std::pair<vectorized::IndexFieldNameAndTypePair,
                                                         segment_v2::InvertedIndexIterator*>>&
                    colid_to_inverted_index_iter,
            uint32_t num_rows, roaring::Roaring* bitmap) const override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    std::string debug_string() const override;
    bool is_constant() const override {
        if (!_function->is_use_default_implementation_for_constants() ||
            // udf function with no argument, can't sure it's must return const column
            (_fn.binary_type == TFunctionBinaryType::JAVA_UDF && _children.empty())) {
            return false;
        }
        return VExpr::is_constant();
    }
    static std::string debug_string(const std::vector<VectorizedFnCall*>& exprs);

protected:
    FunctionBasePtr _function;
    bool _can_fast_execute = false;
    std::string _expr_name;
    std::string _function_name;

private:
    Status _do_execute(doris::vectorized::VExprContext* context, doris::vectorized::Block* block,
                       int* result_column_id, std::vector<size_t>& args);
};
} // namespace doris::vectorized
