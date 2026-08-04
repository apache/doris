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

#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "core/data_type/data_type_number.h"
#include "exprs/create_predicate_function.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris::parquet_benchmark::file_scanner_expr_detail {

inline TExprNode make_direct_in_node() {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_num_children(1);
    node.__set_is_nullable(false);
    node.in_predicate.__set_is_not_in(false);
    return node;
}

inline void run_direct_in_clone_prepare_open(benchmark::State& state, size_t cardinality,
                                             bool share_pruning_state) {
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, cardinality, false));
    for (size_t index = 0; index < cardinality; ++index) {
        const int32_t value = static_cast<int32_t>(index);
        filter->insert(&value);
    }
    auto root = VDirectInPredicate::create_shared(make_direct_in_node(), std::move(filter), true);
    root->add_child(VSlotRef::create_shared(0, 0, -1, std::make_shared<DataTypeInt32>(),
                                            "runtime_filter_key"));
    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    RowDescriptor row_desc;
    VExprContext original(root);
    auto status = original.prepare(&runtime_state, row_desc);
    if (status.ok()) {
        status = original.open(&runtime_state);
    }
    if (!status.ok()) {
        const auto error = status.to_string();
        state.SkipWithError(error.c_str());
        return;
    }

    for (auto _ : state) {
        VExprSPtr cloned_root;
        if (share_pruning_state) {
            status = root->deep_clone(&cloned_root);
        } else {
            auto rematerialized = VDirectInPredicate::create_shared(make_direct_in_node(),
                                                                    root->get_set_func(), true);
            rematerialized->add_child(VSlotRef::create_shared(
                    0, 0, -1, std::make_shared<DataTypeInt32>(), "runtime_filter_key"));
            cloned_root = std::move(rematerialized);
            status = Status::OK();
        }
        if (status.ok()) {
            VExprContext cloned(cloned_root);
            status = cloned.prepare(&runtime_state, row_desc);
            if (status.ok()) {
                status = cloned.open(&runtime_state);
            }
            benchmark::DoNotOptimize(cloned_root);
        }
        if (!status.ok()) {
            const auto error = status.to_string();
            state.SkipWithError(error.c_str());
            return;
        }
    }
    state.counters["set_values"] = static_cast<double>(cardinality);
}

inline bool register_file_scanner_expr_benchmarks() {
    for (const size_t cardinality : std::array<size_t, 4> {128, 1024, 8192, 65536}) {
        for (const bool share_pruning_state : {false, true}) {
            const std::string name = "FileScannerExpr/direct_in_clone_prepare_open/values_" +
                                     std::to_string(cardinality) +
                                     (share_pruning_state ? "/impl_shared" : "/impl_rematerialize");
            benchmark::RegisterBenchmark(name.c_str(), [cardinality, share_pruning_state](
                                                               benchmark::State& state) {
                run_direct_in_clone_prepare_open(state, cardinality, share_pruning_state);
            })->Unit(benchmark::kNanosecond);
        }
    }
    return true;
}

inline const bool FILE_SCANNER_EXPR_BENCHMARKS_REGISTERED = register_file_scanner_expr_benchmarks();

} // namespace doris::parquet_benchmark::file_scanner_expr_detail
