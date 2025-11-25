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

#include "gen_cpp/Exprs_types.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

class VSearchExpr : public VExpr {
public:
    VSearchExpr(const TExprNode& node);
    ~VSearchExpr() override = default;
    Status execute_column(VExprContext* context, const Block* block,
                          ColumnPtr& result_column) const override;
    const std::string& expr_name() const override;
    Status evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) override;

    // Override to prevent being treated as constant - search expressions must use inverted index
    bool is_constant() const override { return false; }

    static VExprSPtr create_shared(const TExprNode& node) {
        return std::make_shared<VSearchExpr>(node);
    }

    bool can_push_down_to_index() const override { return true; }

    const TSearchParam& get_search_param() const { return _search_param; }

private:
    TSearchParam _search_param;
    std::string _original_dsl;
};

} // namespace doris::vectorized
