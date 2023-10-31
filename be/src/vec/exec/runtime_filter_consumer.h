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

#include "exec/exec_node.h"
#include "exprs/runtime_filter.h"
#include "pipeline/pipeline_x/dependency.h"

namespace doris::vectorized {

class RuntimeFilterConsumer {
public:
    RuntimeFilterConsumer(const int32_t filter_id,
                          const std::vector<TRuntimeFilterDesc>& runtime_filters,
                          const RowDescriptor& row_descriptor, VExprContextSPtrs& conjuncts);
    ~RuntimeFilterConsumer() = default;

    Status init(RuntimeState* state);

    // Try to append late arrived runtime filters.
    // Return num of filters which are applied already.
    Status try_append_late_arrival_runtime_filter(int* arrived_rf_num);

    bool runtime_filters_are_ready_or_timeout();

    void init_runtime_filter_dependency(doris::pipeline::RuntimeFilterDependency*);

protected:
    // Register and get all runtime filters at Init phase.
    Status _register_runtime_filter();
    // Get all arrived runtime filters at Open phase.
    Status _acquire_runtime_filter();
    // Append late-arrival runtime filters to the vconjunct_ctx.
    Status _append_rf_into_conjuncts(const VExprSPtrs& vexprs);

    void _init_profile(RuntimeProfile* profile);

    void _prepare_rf_timer(RuntimeProfile* profile);

    // For runtime filters
    struct RuntimeFilterContext {
        RuntimeFilterContext() : apply_mark(false), runtime_filter(nullptr) {}
        RuntimeFilterContext(IRuntimeFilter* rf) : apply_mark(false), runtime_filter(rf) {}
        // set to true if this runtime filter is already applied to vconjunct_ctx_ptr
        bool apply_mark;
        IRuntimeFilter* runtime_filter;
    };

    std::vector<RuntimeFilterContext> _runtime_filter_ctxs;
    // Set to true if the runtime filter is ready.
    std::vector<bool> _runtime_filter_ready_flag;
    doris::Mutex _rf_locks;
    phmap::flat_hash_set<VExprSPtr> _rf_vexpr_set;
    RuntimeState* _state;

private:
    int32_t _filter_id;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::list<vectorized::VExprContextSPtr> _probe_ctxs;

    const RowDescriptor& _row_descriptor_ref;

    VExprContextSPtrs& _conjuncts_ref;

    // True means all runtime filters are applied to scanners
    bool _is_all_rf_applied = true;
    std::shared_ptr<std::atomic_bool> _blocked_by_rf;

    RuntimeProfile::Counter* _acquire_runtime_filter_timer = nullptr;
};

} // namespace doris::vectorized