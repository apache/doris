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
    PipelineXTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state,
                  PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile);

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
        for (auto& op : _operators) {
            auto dep = op->wait_for_dependency(_state);
            if (dep != nullptr) {
                dep->start_read_watcher();
                return false;
            }
        }
        return true;
    }

    bool runtime_filters_are_ready_or_timeout() override {
        return _source->runtime_filters_are_ready_or_timeout(_state);
    }

    bool sink_can_write() override {
        auto dep = _sink->wait_for_dependency(_state);
        if (dep != nullptr) {
            dep->start_write_watcher();
            return false;
        }
        return true;
    }

    Status finalize() override;

    std::string debug_string() override;

    bool is_pending_finish() override {
        for (auto& op : _operators) {
            auto dep = op->finish_blocked_by(_state);
            if (dep != nullptr) {
                dep->start_finish_watcher();
                return true;
            }
        }
        auto dep = _sink->finish_blocked_by(_state);
        if (dep != nullptr) {
            dep->start_finish_watcher();
            return true;
        }
        return false;
    }

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

    std::vector<DependencySPtr>& get_upstream_dependency(int id) {
        if (_upstream_dependency.find(id) == _upstream_dependency.end()) {
            _upstream_dependency.insert({id, {DependencySPtr {}}});
        }
        return _upstream_dependency[id];
    }

private:
    void set_close_pipeline_time() override {}
    void _init_profile() override;
    void _fresh_profile_counter() override;
    using DependencyMap = std::map<int, std::vector<DependencySPtr>>;
    Status _open() override;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    DependencyMap _upstream_dependency;

    std::vector<DependencySPtr> _downstream_dependency;

    bool _dry_run = false;
};

} // namespace doris::pipeline
