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

#include "common/be_mock_util.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_mgr.h"
#include "runtime_filter/runtime_filter_producer.h"
#include "vec/core/block.h" // IWYU pragma: keep
#include "vec/exprs/vexpr_context.h"

namespace doris {
#include "common/compile_check_begin.h"
// this class used in hash join node
/**
 * init -> (skip_runtime_filters ->) send_filter_size -> build filter -> publish filter
 */
class RuntimeFilterProducerHelper {
public:
    virtual ~RuntimeFilterProducerHelper() = default;

    RuntimeFilterProducerHelper(bool should_build_hash_table, bool is_broadcast_join)
            : _should_build_hash_table(should_build_hash_table),
              _is_broadcast_join(is_broadcast_join) {}

#ifdef BE_TEST
    RuntimeFilterProducerHelper() : _should_build_hash_table(true), _is_broadcast_join(false) {}
#endif

    // create and register runtime filters producers
    Status init(RuntimeState* state, const vectorized::VExprContextSPtrs& build_expr_ctxs,
                const std::vector<TRuntimeFilterDesc>& runtime_filter_descs);

    // send local size to remote to sync global rf size if needed
    MOCK_FUNCTION Status
    send_filter_size(RuntimeState* state, uint64_t hash_table_size,
                     const std::shared_ptr<pipeline::CountedFinishDependency>& dependency);

    // skip all runtime filter process, send size and rf to remote imeediately, mainly used to make join spill instance do not block other instance
    MOCK_FUNCTION Status skip_process(RuntimeState* state);

    // build rf
    Status build(RuntimeState* state, const vectorized::Block* block, bool use_shared_table,
                 std::map<int, std::shared_ptr<RuntimeFilterWrapper>>& runtime_filters);
    // if task is terminated, rf also need to publish
    Status terminate(RuntimeState* state);
    // publish rf
    Status publish(RuntimeState* state);

    void collect_realtime_profile(RuntimeProfile* parent_operator_profile);

protected:
    virtual void _init_expr(const vectorized::VExprContextSPtrs& build_expr_ctxs,
                            const std::vector<TRuntimeFilterDesc>& runtime_filter_descs);
    Status _init_filters(RuntimeState* state, uint64_t local_hash_table_size);
    Status _insert(const vectorized::Block* block, size_t start);
    Status _publish(RuntimeState* state);

    std::vector<std::shared_ptr<RuntimeFilterProducer>> _producers;
    const bool _should_build_hash_table;
    std::unique_ptr<RuntimeProfile::Counter> _publish_runtime_filter_timer =
            std::make_unique<RuntimeProfile::Counter>(TUnit::TIME_NS, 0);
    std::unique_ptr<RuntimeProfile::Counter> _runtime_filter_compute_timer =
            std::make_unique<RuntimeProfile::Counter>(TUnit::TIME_NS, 0);

    // This flag is setted by skip_process
    // and read by many methods, not sure wheather there exists data race, so i use atomic
    std::atomic_bool _skip_runtime_filters_process = false;
    const bool _is_broadcast_join;

    std::vector<std::shared_ptr<vectorized::VExprContext>> _filter_expr_contexts;
};
#include "common/compile_check_end.h"
} // namespace doris
