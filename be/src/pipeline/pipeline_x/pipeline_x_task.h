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
#include "runtime/workload_group/workload_group.h"
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
    Status close(Status exec_status, bool close_sink = true) override;

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

    bool is_finished() const override { return _finished.load(); }

    std::string debug_string() override;

    bool is_pending_finish() override { return _finish_blocked_dependency() != nullptr; }

    std::shared_ptr<BasicSharedState> get_source_shared_state() {
        return _op_shared_states.contains(_source->operator_id())
                       ? _op_shared_states[_source->operator_id()]
                       : nullptr;
    }

    void inject_shared_state(std::shared_ptr<BasicSharedState> shared_state) {
        if (!shared_state) {
            return;
        }
        // Shared state is created by upstream task's sink operator and shared by source operator of this task.
        for (auto& op : _operators) {
            if (shared_state->related_op_ids.contains(op->operator_id())) {
                _op_shared_states.insert({op->operator_id(), shared_state});
                return;
            }
        }
        if (shared_state->related_op_ids.contains(_sink->dests_id().front())) {
            DCHECK(_sink_shared_state == nullptr);
            _sink_shared_state = shared_state;
        }
    }

    std::shared_ptr<BasicSharedState> get_sink_shared_state() { return _sink_shared_state; }

    BasicSharedState* get_op_shared_state(int id) {
        if (!_op_shared_states.contains(id)) {
            return nullptr;
        }
        return _op_shared_states[id].get();
    }

    bool is_pipelineX() const override { return true; }

    void wake_up();

    DataSinkOperatorXPtr sink() const { return _sink; }

    OperatorXPtr source() const { return _source; }

    OperatorXs operatorXs() { return _operators; }

    int task_id() const { return _index; };

    void clear_blocking_state() override {
        _state->get_query_ctx()->get_execution_dependency()->set_always_ready();
        // We use a lock to assure all dependencies are not deconstructed here.
        std::unique_lock<std::mutex> lc(_dependency_lock);
        if (!_finished) {
            _execution_dep->set_always_ready();
            for (auto* dep : _filter_dependencies) {
                dep->set_always_ready();
            }
            for (auto* dep : _read_dependencies) {
                dep->set_always_ready();
            }
            for (auto* dep : _write_dependencies) {
                dep->set_always_ready();
            }
            for (auto* dep : _finish_dependencies) {
                dep->set_always_ready();
            }
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

    void stop_if_finished() {
        if (_sink->is_finished(_state)) {
            clear_blocking_state();
        }
    }

    static bool should_revoke_memory(RuntimeState* state, int64_t revocable_mem_bytes);

    bool wake_up_early() const { return _wake_up_early; }

    void set_wake_up_early() override { _wake_up_early = true; }

private:
    friend class RuntimeFilterDependency;
    Dependency* _write_blocked_dependency() {
        for (auto* op_dep : _write_dependencies) {
            _blocked_dep = op_dep->is_blocked_by(this);
            if (_blocked_dep != nullptr) {
                _blocked_dep->start_watcher();
                return _blocked_dep;
            }
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

    Dependency* _runtime_filter_blocked_dependency() {
        for (auto* op_dep : _filter_dependencies) {
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
    std::vector<Dependency*> _write_dependencies;
    std::vector<Dependency*> _finish_dependencies;
    std::vector<Dependency*> _filter_dependencies;

    // All shared states of this pipeline task.
    std::map<int, std::shared_ptr<BasicSharedState>> _op_shared_states;
    std::shared_ptr<BasicSharedState> _sink_shared_state;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            _le_state_map;
    int _task_idx;
    bool _dry_run = false;

    Dependency* _blocked_dep = nullptr;

    Dependency* _execution_dep = nullptr;

    std::atomic<bool> _finished {false};
    std::mutex _dependency_lock;
    std::atomic<bool> _wake_up_early = false;
    std::atomic<bool> _eos = false;
};

} // namespace doris::pipeline
