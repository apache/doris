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

#include <gen_cpp/Metrics_types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "vec/exprs/function_context.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;

double get_in_list_ignore_thredhold(size_t list_size);
double get_comparison_ignore_thredhold();
double get_bloom_filter_ignore_thredhold();
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Block;
class VExprContext;

class VRuntimeFilterWrapper final : public VExpr {
    ENABLE_FACTORY_CREATOR(VRuntimeFilterWrapper);

public:
    VRuntimeFilterWrapper(const TExprNode& node, VExprSPtr impl, double ignore_thredhold,
                          bool null_aware, int filter_id);
    ~VRuntimeFilterWrapper() override = default;
    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    std::string debug_string() const override { return _impl->debug_string(); }
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    const VExprSPtrs& children() const override { return _impl->children(); }
    TExprNodeType::type node_type() const override { return _impl->node_type(); }

    double execute_cost() const override { return _impl->execute_cost(); }

    Status execute_filter(VExprContext* context, const Block* block,
                          uint8_t* __restrict result_filter_data, size_t rows, bool accept_null,
                          bool* can_filter_all) const override;

    uint64_t get_digest(uint64_t seed) const override {
        seed = _impl->get_digest(seed);
        if (seed) {
            return HashUtil::crc_hash64(&_null_aware, sizeof(_null_aware), seed);
        }
        return seed;
    }

    VExprSPtr get_impl() const override { return _impl; }

    void attach_profile_counter(std::shared_ptr<RuntimeProfile::Counter> rf_input_rows,
                                std::shared_ptr<RuntimeProfile::Counter> rf_filter_rows,
                                std::shared_ptr<RuntimeProfile::Counter> always_true_filter_rows) {
        DCHECK(rf_input_rows != nullptr);
        DCHECK(rf_filter_rows != nullptr);
        DCHECK(always_true_filter_rows != nullptr);

        if (rf_input_rows != nullptr) {
            _rf_input_rows = rf_input_rows;
        }
        if (rf_filter_rows != nullptr) {
            _rf_filter_rows = rf_filter_rows;
        }
        if (always_true_filter_rows != nullptr) {
            _always_true_filter_rows = always_true_filter_rows;
        }
    }

    bool is_rf_wrapper() const override { return true; }

    int filter_id() const { return _filter_id; }

    std::shared_ptr<RuntimeProfile::Counter> predicate_filtered_rows_counter() const {
        return _rf_filter_rows;
    }
    std::shared_ptr<RuntimeProfile::Counter> predicate_input_rows_counter() const {
        return _rf_input_rows;
    }
    std::shared_ptr<RuntimeProfile::Counter> predicate_always_true_rows_counter() const {
        return _always_true_filter_rows;
    }

private:
    VExprSPtr _impl;

    std::shared_ptr<RuntimeProfile::Counter> _rf_input_rows =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    std::shared_ptr<RuntimeProfile::Counter> _rf_filter_rows =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);
    std::shared_ptr<RuntimeProfile::Counter> _always_true_filter_rows =
            std::make_shared<RuntimeProfile::Counter>(TUnit::UNIT, 0);

    std::string _expr_name;
    double _ignore_thredhold;
    bool _null_aware;
    int _filter_id;
};

using VRuntimeFilterPtr = std::shared_ptr<VRuntimeFilterWrapper>;

#include "common/compile_check_end.h"
} // namespace doris::vectorized