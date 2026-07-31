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
#include "exprs/function/function.h"
#include "exprs/function_context.h"
#include "exprs/vexpr.h"
#include "storage/index/inverted/inverted_index_parser.h"

namespace lucene {
namespace analysis {
class Analyzer;
}
} // namespace lucene
namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
class Block;
class VExprContext;
} // namespace doris

namespace doris {

class VMatchPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VMatchPredicate);

public:
    VMatchPredicate(const TExprNode& node);
    ~VMatchPredicate() override;
    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override;
    const std::string& expr_name() const override;
    const std::string& function_name() const;
    [[nodiscard]] const std::string& get_analyzer_key() const override;

    std::string debug_string() const override;

    FunctionBasePtr function() { return _function; }

private:
    FunctionBasePtr _function;
    std::string _expr_name;
    std::string _function_name;

    // Lifecycle management: holds ownership of the analyzer
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;

    // Runtime context: holds raw pointer to analyzer and necessary runtime info
    InvertedIndexAnalyzerCtxSPtr _analyzer_ctx;
};
} // namespace doris