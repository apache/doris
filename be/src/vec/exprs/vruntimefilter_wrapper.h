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

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
class VRuntimeFilterWrapper final : public VExpr {
public:
    VRuntimeFilterWrapper(const TExprNode& node, VExpr* impl);
    VRuntimeFilterWrapper(const VRuntimeFilterWrapper& vexpr);
    ~VRuntimeFilterWrapper() = default;
    doris::Status execute(VExprContext* context, doris::vectorized::Block* block,
                          int* result_column_id) override;
    doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                          VExprContext* context) override;
    doris::Status open(doris::RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) override;
    std::string debug_string() const override { return _impl->debug_string(); };
    bool is_constant() const override;
    void close(doris::RuntimeState* state, VExprContext* context,
               FunctionContext::FunctionStateScope scope) override;
    VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VRuntimeFilterWrapper(*this));
    }
    const std::string& expr_name() const override;

    ColumnPtrWrapper* get_const_col(VExprContext* context) override {
        return _impl->get_const_col(context);
    }

    const VExpr* get_impl() const override { return _impl; }

private:
    VExpr* _impl;

    bool _always_true;
    /// TODO: statistic filter rate in the profile
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    bool _has_calculate_filter = false;
    // loop size must be power of 2
    constexpr static int64_t THRESHOLD_TO_CALCULATE_RATE = 8192;
    // if filter rate less than this, bloom filter will set always true
    constexpr static double EXPECTED_FILTER_RATE = 0.2;

    std::string _expr_name;

    bool _is_closed;
};
} // namespace doris::vectorized