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

#include "pipeline/dependency.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

// this class used in ScanNode or MultiCastDataStreamSource
/**
 * init -> acquire_runtime_filter -> try_append_late_arrival_runtime_filter
 */
class RuntimeFilterConsumerHelper {
public:
    RuntimeFilterConsumerHelper(const int32_t node_id,
                                const std::vector<TRuntimeFilterDesc>& runtime_filters,
                                const RowDescriptor& row_descriptor);
    ~RuntimeFilterConsumerHelper() = default;

    Status init(RuntimeState* state, RuntimeProfile* profile, bool need_local_merge,
                std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>&
                        runtime_filter_dependencies,
                const int id, const int node_id, const std::string& name);
    // Get all arrived runtime filters at Open phase which will be push down to storage.
    // Called by Operator.
    Status acquire_runtime_filter(vectorized::VExprContextSPtrs& conjuncts);
    // The un-arrival filters will be checked every time the scanner is scheduled.
    // And once new runtime filters arrived, we will use it to do operator's filtering.
    // Called by Scanner.
    Status try_append_late_arrival_runtime_filter(int* arrived_rf_num,
                                                  vectorized::VExprContextSPtrs& conjuncts);

private:
    // Register and get all runtime filters at Init phase.
    Status _register_runtime_filter(bool need_local_merge);

    // Append late-arrival runtime filters to the vconjunct_ctx.
    Status _append_rf_into_conjuncts(const std::vector<vectorized::VRuntimeFilterPtr>& vexprs,
                                     vectorized::VExprContextSPtrs& conjuncts);

    void _init_dependency(
            std::vector<std::shared_ptr<pipeline::RuntimeFilterDependency>>& dependencies,
            const int id, const int node_id, const std::string& name);

    std::vector<std::shared_ptr<RuntimeFilterConsumer>> _consumers;
    std::mutex _rf_locks;
    RuntimeState* _state = nullptr;

    int32_t _node_id;
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    const RowDescriptor& _row_descriptor_ref;

    // True means all runtime filters are applied to scanners
    bool _is_all_rf_applied = true;
    std::shared_ptr<std::atomic_bool> _blocked_by_rf;

    RuntimeProfile::Counter* _acquire_runtime_filter_timer = nullptr;
    std::unique_ptr<RuntimeProfile> _profile;
};

} // namespace doris