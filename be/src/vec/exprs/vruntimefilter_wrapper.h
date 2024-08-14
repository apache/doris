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

double get_in_list_ignore_thredhold(size_t list_size);
double get_comparison_ignore_thredhold();
double get_bloom_filter_ignore_thredhold();

namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class VRuntimeFilterWrapper final : public VExpr {
    ENABLE_FACTORY_CREATOR(VRuntimeFilterWrapper);

public:
    VRuntimeFilterWrapper(const TExprNode& node, const VExprSPtr& impl, double ignore_thredhold,
                          bool null_aware = false);
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

    template <typename T, typename TT>
    static void judge_selectivity(double ignore_threshold, int64_t filter_rows, int64_t input_rows,
                                  T& always_true, TT& judge_counter) {
        always_true = filter_rows / (input_rows * 1.0) < ignore_threshold;
        judge_counter = config::runtime_filter_sampling_frequency;
    }

    bool need_judge_selectivity() override {
        if (_judge_counter <= 0) {
            _always_true = false;
            return true;
        }
        return false;
    }

    void do_judge_selectivity(int64_t filter_rows, int64_t input_rows) override {
        judge_selectivity(_ignore_thredhold, filter_rows, input_rows, _always_true, _judge_counter);
        if (_expr_filtered_rows_counter) {
            COUNTER_UPDATE(_expr_filtered_rows_counter, filter_rows);
        }
        if (_expr_input_rows_counter) {
            COUNTER_UPDATE(_expr_input_rows_counter, input_rows);
        }
    }

private:
    VExprSPtr _impl;
    std::atomic_int _judge_counter = 0;
    std::atomic_int _always_true = false;

    RuntimeProfile::Counter* _expr_filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _expr_input_rows_counter = nullptr;
    RuntimeProfile::Counter* _always_true_counter = nullptr;

    std::string _expr_name;
    double _ignore_thredhold;
    bool _null_aware;
};

using VRuntimeFilterPtr = std::shared_ptr<VRuntimeFilterWrapper>;

} // namespace doris::vectorized