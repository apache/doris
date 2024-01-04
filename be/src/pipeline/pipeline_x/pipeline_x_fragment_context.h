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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"

namespace doris {
class ExecNode;
class DataSink;
struct ReportStatusRequest;
class ExecEnv;
class RuntimeFilterMergeControllerEntity;
class TDataSink;
class TPipelineFragmentParams;

namespace pipeline {
struct LocalExchangeSinkDependency;

class PipelineXFragmentContext : public PipelineFragmentContext {
public:
    // Callback to report execution status of plan fragment.
    // 'profile' is the cumulative profile, 'done' indicates whether the execution
    // is done or still continuing.
    // Note: this does not take a const RuntimeProfile&, because it might need to call
    // functions like PrettyPrint() or to_thrift(), neither of which is const
    // because they take locks.
    PipelineXFragmentContext(const TUniqueId& query_id, const int fragment_id,
                             std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
                             const std::function<void(RuntimeState*, Status*)>& call_back,
                             const report_status_callback& report_status_cb);

    ~PipelineXFragmentContext() override;

    void instance_ids(std::vector<TUniqueId>& ins_ids) const override {
        ins_ids.resize(_fragment_instance_ids.size());
        for (size_t i = 0; i < _fragment_instance_ids.size(); i++) {
            ins_ids[i] = _fragment_instance_ids[i];
        }
    }

    void instance_ids(std::vector<string>& ins_ids) const override {
        ins_ids.resize(_fragment_instance_ids.size());
        for (size_t i = 0; i < _fragment_instance_ids.size(); i++) {
            ins_ids[i] = print_id(_fragment_instance_ids[i]);
        }
    }

    void add_merge_controller_handler(
            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) override {
        _merge_controller_handlers.emplace_back(handler);
    }

    //    bool is_canceled() const { return _runtime_state->is_cancelled(); }

    // Prepare global information including global states and the unique operator tree shared by all pipeline tasks.
    Status prepare(const doris::TPipelineFragmentParams& request) override;

    Status submit() override;

    void close_if_prepare_failed() override;
    void close_sink() override;

    void cancel(const PPlanFragmentCancelReason& reason = PPlanFragmentCancelReason::INTERNAL_ERROR,
                const std::string& msg = "") override;

    Status send_report(bool) override;

    RuntimeFilterMgr* get_runtime_filter_mgr(UniqueId fragment_instance_id) override {
        DCHECK(_runtime_filter_mgr_map.contains(fragment_instance_id));
        return _runtime_filter_mgr_map[fragment_instance_id].get();
    }

    [[nodiscard]] int next_operator_id() { return _operator_id++; }

    [[nodiscard]] int max_operator_id() const { return _operator_id; }

    [[nodiscard]] int next_sink_operator_id() { return _sink_operator_id++; }

    [[nodiscard]] int max_sink_operator_id() const { return _sink_operator_id; }

    std::string debug_string() override;

private:
    void _close_fragment_instance() override;
    Status _build_pipeline_tasks(const doris::TPipelineFragmentParams& request) override;
    Status _add_local_exchange(int pip_idx, int idx, int node_id, ObjectPool* pool,
                               PipelinePtr cur_pipe, DataDistribution data_distribution,
                               bool* do_local_exchange, int num_buckets,
                               const std::map<int, int>& bucket_seq_to_instance_idx,
                               const bool ignore_data_distribution);
    void _inherit_pipeline_properties(const DataDistribution& data_distribution,
                                      PipelinePtr pipe_with_source, PipelinePtr pipe_with_sink);
    Status _add_local_exchange_impl(int idx, ObjectPool* pool, PipelinePtr cur_pipe,
                                    PipelinePtr new_pipe, DataDistribution data_distribution,
                                    bool* do_local_exchange, int num_buckets,
                                    const std::map<int, int>& bucket_seq_to_instance_idx,
                                    const bool ignore_data_distribution);

    [[nodiscard]] Status _build_pipelines(ObjectPool* pool,
                                          const doris::TPipelineFragmentParams& request,
                                          const DescriptorTbl& descs, OperatorXPtr* root,
                                          PipelinePtr cur_pipe);
    Status _create_tree_helper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                               const doris::TPipelineFragmentParams& request,
                               const DescriptorTbl& descs, OperatorXPtr parent, int* node_idx,
                               OperatorXPtr* root, PipelinePtr& cur_pipe, int child_idx);

    Status _create_operator(ObjectPool* pool, const TPlanNode& tnode,
                            const doris::TPipelineFragmentParams& request,
                            const DescriptorTbl& descs, OperatorXPtr& op, PipelinePtr& cur_pipe,
                            int parent_idx, int child_idx);
    template <bool is_intersect>
    Status _build_operators_for_set_operation_node(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs, OperatorXPtr& op,
                                                   PipelinePtr& cur_pipe, int parent_idx,
                                                   int child_idx);

    Status _create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                             const std::vector<TExpr>& output_exprs,
                             const TPipelineFragmentParams& params, const RowDescriptor& row_desc,
                             RuntimeState* state, DescriptorTbl& desc_tbl,
                             PipelineId cur_pipeline_id);
    Status _plan_local_exchange(int num_buckets,
                                const std::map<int, int>& bucket_seq_to_instance_idx);
    Status _plan_local_exchange(int num_buckets, int pip_idx, PipelinePtr pip,
                                const std::map<int, int>& bucket_seq_to_instance_idx,
                                const bool ignore_data_distribution);

    bool _has_inverted_index_or_partial_update(TOlapTableSink sink);

    bool _enable_local_shuffle() const { return _runtime_state->enable_local_shuffle(); }

    OperatorXPtr _root_op = nullptr;
    // this is a [n * m] matrix. n is parallelism of pipeline engine and m is the number of pipelines.
    std::vector<std::vector<std::unique_ptr<PipelineXTask>>> _tasks;

    bool _use_global_rf = false;

    // It is used to manage the lifecycle of RuntimeFilterMergeController
    std::vector<std::shared_ptr<RuntimeFilterMergeControllerEntity>> _merge_controller_handlers;

    // TODO: remove the _sink and _multi_cast_stream_sink_senders to set both
    // of it in pipeline task not the fragment_context
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    DataSinkOperatorXPtr _sink = nullptr;
#ifdef __clang__
#pragma clang diagnostic pop
#endif

    std::atomic_bool _canceled = false;

    // `_dag` manage dependencies between pipelines by pipeline ID. the indices will be blocked by members
    std::map<PipelineId, std::vector<PipelineId>> _dag;

    // We use preorder traversal to create an operator tree. When we meet a join node, we should
    // build probe operator and build operator in separate pipelines. To do this, we should build
    // ProbeSide first, and use `_pipelines_to_build` to store which pipeline the build operator
    // is in, so we can build BuildSide once we complete probe side.
    struct pipeline_parent_map {
        std::map<int, std::vector<PipelinePtr>> _build_side_pipelines;
        void push(int parent_node_id, PipelinePtr pipeline) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                _build_side_pipelines.insert({parent_node_id, {pipeline}});
            } else {
                _build_side_pipelines[parent_node_id].push_back(pipeline);
            }
        }
        void pop(PipelinePtr& cur_pipe, int parent_node_id, int child_idx) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                return;
            }
            DCHECK(_build_side_pipelines.contains(parent_node_id));
            auto& child_pipeline = _build_side_pipelines[parent_node_id];
            DCHECK(child_idx < child_pipeline.size());
            cur_pipe = child_pipeline[child_idx];
        }
        void clear() { _build_side_pipelines.clear(); }
    } _pipeline_parent_map;

    std::mutex _state_map_lock;

    int _operator_id = 0;
    int _sink_operator_id = 0;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                            std::shared_ptr<LocalExchangeSinkDependency>>>
            _op_id_to_le_state;

    // UniqueId -> runtime mgr
    std::map<UniqueId, std::unique_ptr<RuntimeFilterMgr>> _runtime_filter_mgr_map;

    //Here are two types of runtime states:
    //    - _runtime state is at the Fragment level.
    //    - _task_runtime_states is at the task level, unique to each task.

    std::vector<TUniqueId> _fragment_instance_ids;
    // Local runtime states for each task
    std::vector<std::unique_ptr<RuntimeState>> _task_runtime_states;

    std::vector<std::unique_ptr<RuntimeFilterParamsContext>> _runtime_filter_states;
};

} // namespace pipeline
} // namespace doris
