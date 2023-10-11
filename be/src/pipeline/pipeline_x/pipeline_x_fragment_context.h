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
                             const report_status_callback& report_status_cb,
                             bool group_commit = false);

    ~PipelineXFragmentContext() override;

    void instance_ids(std::vector<TUniqueId>& ins_ids) const override {
        ins_ids.resize(_runtime_states.size());
        for (size_t i = 0; i < _runtime_states.size(); i++) {
            ins_ids[i] = _runtime_states[i]->fragment_instance_id();
        }
    }

    void instance_ids(std::vector<string>& ins_ids) const override {
        ins_ids.resize(_runtime_states.size());
        for (size_t i = 0; i < _runtime_states.size(); i++) {
            ins_ids[i] = print_id(_runtime_states[i]->fragment_instance_id());
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

    RuntimeState* get_runtime_state(UniqueId fragment_instance_id) override {
        std::lock_guard<std::mutex> l(_state_map_lock);
        if (_instance_id_to_runtime_state.count(fragment_instance_id) > 0) {
            return _instance_id_to_runtime_state[fragment_instance_id];
        } else {
            return _runtime_state.get();
        }
    }

    int next_operator_id() { return _operator_id++; }

private:
    void _close_action() override;
    Status _build_pipeline_tasks(const doris::TPipelineFragmentParams& request) override;

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
                            const DescriptorTbl& descs, OperatorXPtr& node, PipelinePtr& cur_pipe,
                            int parent_idx, int child_idx);

    Status _create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                             const std::vector<TExpr>& output_exprs,
                             const TPipelineFragmentParams& params, const RowDescriptor& row_desc,
                             RuntimeState* state, DescriptorTbl& desc_tbl,
                             PipelineId cur_pipeline_id);
    OperatorXPtr _root_op = nullptr;
    // this is a [n * m] matrix. n is parallelism of pipeline engine and m is the number of pipelines.
    std::vector<std::vector<std::unique_ptr<PipelineXTask>>> _tasks;

    // Local runtime states for each pipeline task.
    std::vector<std::unique_ptr<RuntimeState>> _runtime_states;

    // It is used to manage the lifecycle of RuntimeFilterMergeController
    std::vector<std::shared_ptr<RuntimeFilterMergeControllerEntity>> _merge_controller_handlers;

    // TODO: remove the _sink and _multi_cast_stream_sink_senders to set both
    // of it in pipeline task not the fragment_context
    DataSinkOperatorXPtr _sink;

    std::atomic_bool _canceled = false;

    // `_dag` manage dependencies between pipelines by pipeline ID
    std::map<PipelineId, std::vector<PipelineId>> _dag;

    // We use preorder traversal to create an operator tree. When we meet a join node, we should
    // build probe operator and build operator in separate pipelines. To do this, we should build
    // ProbeSide first, and use `_pipelines_to_build` to store which pipeline the build operator
    // is in, so we can build BuildSide once we complete probe side.
    std::map<int, PipelinePtr> _build_side_pipelines;

    std::map<UniqueId, RuntimeState*> _instance_id_to_runtime_state;
    std::mutex _state_map_lock;
    std::map<int, std::vector<PipelinePtr>> _union_child_pipelines;
    // The number of operators is generally greater than the number of plan nodes,
    // so we need additional ID and cannot rely solely on plan node ID.
    int _operator_id = {1000000};
};

} // namespace pipeline
} // namespace doris
