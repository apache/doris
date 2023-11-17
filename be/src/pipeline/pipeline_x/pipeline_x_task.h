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

// The class do the pipeline task. Minest schdule union by task scheduler
class PipelineXTask : public PipelineTask {
public:
    PipelineXTask(PipelinePtr& pipeline, uint32_t task_id, RuntimeState* state,
                  PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
                  std::shared_ptr<LocalExchangeSharedState> local_exchange_state, int task_idx);

    Status prepare(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    Status prepare(RuntimeState* state, const TPipelineInstanceParams& local_params,
                   const TDataSink& tsink);

    Status execute(bool* eos) override;

    // Try to close this pipeline task. If there are still some resources need to be released after `try_close`,
    // this task will enter the `PENDING_FINISH` state.
    Status try_close(Status exec_status) override;
    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close(Status exec_status) override;

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

    Status finalize() override;

    std::string debug_string() override;

    bool is_pending_finish() override { return _finish_blocked_dependency() != nullptr; }

    std::vector<DependencySPtr>& get_downstream_dependency() { return _downstream_dependency; }

    void add_upstream_dependency(std::vector<DependencySPtr>& multi_upstream_dependency) {
        for (auto dep : multi_upstream_dependency) {
            int dst_id = dep->id();
            if (!_upstream_dependency.contains(dst_id)) {
                _upstream_dependency.insert({dst_id, {dep}});
            } else {
                _upstream_dependency[dst_id].push_back(dep);
            }
        }
    }

    void release_dependency() override {
        std::vector<DependencySPtr> {}.swap(_downstream_dependency);
        DependencyMap {}.swap(_upstream_dependency);

        _local_exchange_state = nullptr;
    }

    std::vector<DependencySPtr>& get_upstream_dependency(int id) {
        if (_upstream_dependency.find(id) == _upstream_dependency.end()) {
            _upstream_dependency.insert({id, {DependencySPtr {}}});
        }
        return _upstream_dependency[id];
    }

    bool is_pipelineX() const override { return true; }

    void try_wake_up(Dependency* wake_up_dep);

    DataSinkOperatorXPtr sink() const { return _sink; }

    OperatorXPtr source() const { return _source; }

    OperatorXs operatorXs() { return _operators; }

    bool push_blocked_task_to_queue() {
        /**
         * Push task into blocking queue if:
         * 1. `_use_blocking_queue` is true.
         * 2. Or this task is blocked by FE two phase execution (BLOCKED_FOR_DEPENDENCY).
         */
        return _use_blocking_queue || get_state() == PipelineTaskState::BLOCKED_FOR_DEPENDENCY;
    }
    void set_use_blocking_queue(bool use_blocking_queue) {
        if (_blocked_dep->is_or_dep()) {
            _use_blocking_queue = true;
            return;
        }
        _use_blocking_queue = use_blocking_queue;
    }

private:
    Dependency* _write_blocked_dependency() {
        _blocked_dep = _write_dependencies->write_blocked_by(this);
        if (_blocked_dep != nullptr) {
            set_use_blocking_queue(false);
            static_cast<WriteDependency*>(_blocked_dep)->start_write_watcher();
            return _blocked_dep;
        }
        return nullptr;
    }

    Dependency* _finish_blocked_dependency() {
        for (auto* fin_dep : _finish_dependencies) {
            _blocked_dep = fin_dep->finish_blocked_by(this);
            if (_blocked_dep != nullptr) {
                set_use_blocking_queue(false);
                static_cast<FinishDependency*>(_blocked_dep)->start_finish_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Dependency* _read_blocked_dependency() {
        for (auto* op_dep : _read_dependencies) {
            _blocked_dep = op_dep->read_blocked_by(this);
            if (_blocked_dep != nullptr) {
                // TODO(gabriel):
                set_use_blocking_queue(true);
                _blocked_dep->start_read_watcher();
                return _blocked_dep;
            }
        }
        return nullptr;
    }

    Status _extract_dependencies();
    void _make_run();
    void set_close_pipeline_time() override {}
    void _init_profile() override;
    void _fresh_profile_counter() override;
    using DependencyMap = std::map<int, std::vector<DependencySPtr>>;
    Status _open() override;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    std::vector<Dependency*> _read_dependencies;
    WriteDependency* _write_dependencies;
    std::vector<FinishDependency*> _finish_dependencies;
    RuntimeFilterDependency* _filter_dependency;

    DependencyMap _upstream_dependency;

    std::vector<DependencySPtr> _downstream_dependency;
    std::shared_ptr<LocalExchangeSharedState> _local_exchange_state;
    int _task_idx;
    bool _dry_run = false;

    Dependency* _blocked_dep {nullptr};

    std::atomic<bool> _use_blocking_queue {true};
};

} // namespace doris::pipeline
