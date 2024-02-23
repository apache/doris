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

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_task.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/task_group/task_group.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"
#include "vec/sink/vresult_sink.h"

namespace doris {
class QueryContext;
class RuntimeState;
namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

class TaskQueue;
class PriorityTaskQueue;
class Dependency;

// The class do the pipeline task. Minest schdule union by task scheduler
class PipelineXTask : public PipelineTask {
public:
    PipelineXTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                  PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
                  std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>,
                                          std::shared_ptr<Dependency>>>
                          le_state_map,
                  int task_idx);

    Status prepare(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    Status prepare(const TPipelineInstanceParams& local_params, const TDataSink& tsink,
                   QueryContext* query_ctx);

    Status execute(bool* eos) override;

    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close(Status exec_status) override;

    Status close_sink(Status exec_status);
    bool source_can_read() override {
        if (_dry_run) {
            return true;
        }
        return _read_blocked_dependency() == nullptr;
    }

    bool runtime_filters_are_ready_or_timeout() override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Should not reach here!");
        return false;
    }

    bool sink_can_write() override { return _write_blocked_dependency() == nullptr; }

    void finalize() override;

    bool is_finished() const { return _finished.load(); }

    std::string debug_string() override;

    bool is_pending_finish() override { return _finish_blocked_dependency() != nullptr; }

    std::vector<DependencySPtr>& get_downstream_dependency() { return _downstream_dependency; }
    std::map<int, std::shared_ptr<BasicSharedState>>& get_shared_states() { return _shared_states; }

    void add_upstream_dependency(std::vector<DependencySPtr>& multi_upstream_dependency,
                                 std::map<int, std::shared_ptr<BasicSharedState>>& shared_states) {
        for (auto dep : multi_upstream_dependency) {
            int dst_id = dep->id();
            if (!_upstream_dependency.contains(dst_id)) {
                _upstream_dependency.insert({dst_id, {dep}});
            } else {
                _upstream_dependency[dst_id].push_back(dep);
            }

            if (shared_states.contains(dst_id) && !_shared_states.contains(dst_id)) {
                // Shared state is created by upstream task's sink operator and shared by source operator of this task.
                _shared_states.insert({dst_id, shared_states[dst_id]});
            } else if (_shared_states.contains(dst_id) && !shared_states.contains(dst_id)) {
                // Shared state is created by this task's source operator and shared by upstream task's sink operator.
                shared_states.insert({dst_id, _shared_states[dst_id]});
            }
        }
    }

    std::vector<DependencySPtr>& get_upstream_dependency(int id) {
        if (_upstream_dependency.find(id) == _upstream_dependency.end()) {
            _upstream_dependency.insert({id, {}});
        }
        return _upstream_dependency[id];
    }

    BasicSharedState* get_shared_state(int id) {
        if (!_shared_states.contains(id)) {
            return nullptr;
        }
        return _shared_states[id].get();
    }

    bool is_pipelineX() const override { return true; }

    void wake_up();

    DataSinkOperatorXPtr sink() const { return _sink; }

    OperatorXPtr source() const { return _source; }

    OperatorXs operatorXs() { return _operators; }

    int task_id() const { return _index; };

    void clear_blocking_state() {
        if (!_finished && get_state() != PipelineTaskState::PENDING_FINISH && _blocked_dep) {
            _blocked_dep->set_ready();
            _blocked_dep = nullptr;
        }
    }

    bool has_dependency() override {
        _blocked_dep = _execution_dep->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            static_cast<Dependency*>(_blocked_dep)->start_watcher();
            return true;
        }
        return false;
    }

private:
    Dependency* _write_blocked_dependency() {
        _blocked_dep = _write_dependencies->is_blocked_by(this);
        if (_blocked_dep != nullptr) {
            static_cast<Dependency*>(_blocked_dep)->start_watcher();
            return _blocked_dep;
        }
        return nullptr;
    }

    Dependency* _finish_blocked_dependency() {
        for (auto* fin_dep : _finish_dependencies) {
            _blocked_dep = fin_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Dependency* _read_blocked_dependency() {
        for (auto* op_dep : _read_dependencies) {
            _blocked_dep = op_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Status _extract_dependencies();
    void set_close_pipeline_time() override {}
    void _init_profile() override;
    void _fresh_profile_counter() override;
    Status _open() override;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    std::vector<Dependency*> _read_dependencies;
    Dependency* _write_dependencies;
    std::vector<Dependency*> _finish_dependencies;
    RuntimeFilterDependency* _filter_dependency;

    // Write dependencies of upstream pipeline tasks.
    DependencyMap _upstream_dependency;
    // Read dependencies of this pipeline task.
    std::map<int, DependencySPtr> _source_dependency;
    // Write dependencies of this pipeline tasks.
    std::vector<DependencySPtr> _downstream_dependency;
    // All shared states of this pipeline task.
    std::map<int, std::shared_ptr<BasicSharedState>> _shared_states;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            _le_state_map;
    int _task_idx;
    bool _dry_run = false;

    Dependency* _blocked_dep = nullptr;

    Dependency* _execution_dep = nullptr;

    std::atomic<bool> _finished {false};
    std::mutex _release_lock;
};

} // namespace doris::pipeline
