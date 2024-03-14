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

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "udf/udf.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr.h"

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
class VRuntimeFilterWrapper final : public VExpr {
    ENABLE_FACTORY_CREATOR(VRuntimeFilterWrapper);

public:
    VRuntimeFilterWrapper(const TExprNode& node, const VExprSPtr& impl, bool null_aware = false);
    ~VRuntimeFilterWrapper() override = default;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    std::string debug_string() const override { return _impl->debug_string(); }
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    const VExprSPtrs& children() const override { return _impl->children(); }

    const VExprSPtr get_impl() const override { return _impl; }

    void attach_profile_counter(RuntimeProfile::Counter* expr_filtered_rows_counter,
                                RuntimeProfile::Counter* expr_input_rows_counter,
                                RuntimeProfile::Counter* always_true_counter) {
        _expr_filtered_rows_counter = expr_filtered_rows_counter;
        _expr_input_rows_counter = expr_input_rows_counter;
        _always_true_counter = always_true_counter;
    }

    // if filter rate less than this, bloom filter will set always true
    constexpr static double EXPECTED_FILTER_RATE = 0.4;

    static void calculate_filter(double ignore_threshold, int64_t filter_rows, int64_t scan_rows,
                                 bool& has_calculate, bool& always_true) {
        if ((!has_calculate) && (scan_rows > config::rf_predicate_check_row_num)) {
            if (filter_rows / (scan_rows * 1.0) < ignore_threshold) {
                always_true = true;
            }
            has_calculate = true;
        }
    }

private:
    VExprSPtr _impl;

    bool _always_true;
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    RuntimeProfile::Counter* _expr_filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _expr_input_rows_counter = nullptr;
    RuntimeProfile::Counter* _always_true_counter = nullptr;
    bool _has_calculate_filter = false;

    std::string _expr_name;
    bool _null_aware;
};

using VRuntimeFilterPtr = std::shared_ptr<VRuntimeFilterWrapper>;

} // namespace doris::vectorized