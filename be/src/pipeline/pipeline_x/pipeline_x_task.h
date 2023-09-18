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

    Status prepare(RuntimeState* state, const TPipelineInstanceParams& local_params);

    Status execute(bool* eos) override;

    // Try to close this pipeline task. If there are still some resources need to be released after `try_close`,
    // this task will enter the `PENDING_FINISH` state.
    Status try_close() override;
    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close() override;

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

    bool sink_can_write() override { return _sink->can_write(_state); }

    Status finalize() override;

    std::string debug_string() override;

    bool is_pending_finish() override {
        bool source_ret = _source->is_pending_finish(_state);
        if (source_ret) {
            return true;
        } else {
            set_src_pending_finish_time();
        }

        bool sink_ret = _sink->is_pending_finish(_state);
        if (sink_ret) {
            return true;
        } else {
            set_dst_pending_finish_time();
        }
        return false;
    }

    DependencySPtr& get_downstream_dependency() { return _downstream_dependency; }
    void set_upstream_dependency(DependencySPtr& upstream_dependency) {
        if (_upstream_dependency.contains(upstream_dependency->id())) {
            upstream_dependency = _upstream_dependency[upstream_dependency->id()];
        } else {
            _upstream_dependency.insert({upstream_dependency->id(), upstream_dependency});
        }
    }

    Dependency* get_upstream_dependency(int id) {
        return _upstream_dependency.find(id) == _upstream_dependency.end()
                       ? (Dependency*)nullptr
                       : _upstream_dependency.find(id)->second.get();
    }

private:
    void set_close_pipeline_time() override {}
    void _init_profile() override;
    void _fresh_profile_counter() override;
    using DependencyMap = std::map<int, DependencySPtr>;
    Status _open() override;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    DependencyMap _upstream_dependency;
    DependencySPtr _downstream_dependency;

    bool _dry_run = false;
};

} // namespace doris::pipeline
