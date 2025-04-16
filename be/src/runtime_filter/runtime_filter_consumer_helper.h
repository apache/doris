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

#include <mutex>

#include "pipeline/dependency.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {
#include "common/compile_check_begin.h"
// this class used in ScanNode or MultiCastDataStreamSource
/**
 * init -> acquire_runtime_filter -> try_append_late_arrival_runtime_filter
 */
class RuntimeFilterConsumerHelper {
public:
    RuntimeFilterConsumerHelper(const std::vector<TRuntimeFilterDesc>& runtime_filters);
    ~RuntimeFilterConsumerHelper() = default;

    Status init(RuntimeState* state, bool need_local_merge, int32_t node_id, int32_t operator_id,
                std::vector<std::shared_ptr<pipeline::Dependency>>& dependencies,
                const std::string& name);
    // Get all arrived runtime filters at Open phase which will be push down to storage.
    // Called by Operator.
    Status acquire_runtime_filter(RuntimeState* state, vectorized::VExprContextSPtrs& conjuncts,
                                  const RowDescriptor& row_descriptor);
    // The un-arrival filters will be checked every time the scanner is scheduled.
    // And once new runtime filters arrived, we will use it to do operator's filtering.
    // Called by Scanner.
    Status try_append_late_arrival_runtime_filter(RuntimeState* state, int* arrived_rf_num,
                                                  vectorized::VExprContextSPtrs& conjuncts,
                                                  const RowDescriptor& row_descriptor);

    // Called by XXXLocalState::close()
    // parent_operator_profile is owned by LocalState so update it is safe at here.
    void collect_realtime_profile(RuntimeProfile* parent_operator_profile);

private:
    // Append late-arrival runtime filters to the vconjunct_ctx.
    Status _append_rf_into_conjuncts(RuntimeState* state,
                                     const std::vector<vectorized::VRuntimeFilterPtr>& vexprs,
                                     vectorized::VExprContextSPtrs& conjuncts,
                                     const RowDescriptor& row_descriptor);

    std::vector<std::shared_ptr<RuntimeFilterConsumer>> _consumers;
    std::mutex _rf_locks;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;

    // True means all runtime filters are applied to scanners
    bool _is_all_rf_applied = true;

    std::unique_ptr<RuntimeProfile::Counter> _acquire_runtime_filter_timer =
            std::make_unique<RuntimeProfile::Counter>(TUnit::TIME_NS, 0);
};
#include "common/compile_check_end.h"
} // namespace doris