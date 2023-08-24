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
                  PipelineFragmentContext* fragment_context, RuntimeProfile* parent_profile,
                  const std::vector<TScanRangeParams>& scan_ranges, const int sender_id);

    Status prepare(RuntimeState* state) override;

    Status execute(bool* eos) override;

    // Try to close this pipeline task. If there are still some resources need to be released after `try_close`,
    // this task will enter the `PENDING_FINISH` state.
    Status try_close() override;
    // if the pipeline create a bunch of pipeline task
    // must be call after all pipeline task is finish to release resource
    Status close() override;

    bool source_can_read() override {
        return _source->can_read(_state) || _ignore_blocking_source();
    }

    bool runtime_filters_are_ready_or_timeout() override {
        return _source->runtime_filters_are_ready_or_timeout();
    }

    bool sink_can_write() override { return _sink->can_write(_state) || _ignore_blocking_sink(); }

    Status finalize() override;

    OperatorXPtr get_rootx() { return _root; }

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
        _upstream_dependency.insert({upstream_dependency->id(), upstream_dependency});
    }

private:
    [[nodiscard]] bool _ignore_blocking_sink() { return _root->can_terminate_early(_state); }

    [[nodiscard]] bool _ignore_blocking_source() {
        for (size_t i = 1; i < _operators.size(); i++) {
            if (_operators[i]->can_terminate_early(_state)) {
                return true;
            }
        }
        return false;
    }

    using DependencyMap = std::map<int, DependencySPtr>;
    Status _open() override;

    const std::vector<TScanRangeParams> _scan_ranges;

    OperatorXs _operators; // left is _source, right is _root
    OperatorXPtr _source;
    OperatorXPtr _root;
    DataSinkOperatorXPtr _sink;

    const int _sender_id;

    DependencyMap _upstream_dependency;
    DependencySPtr _downstream_dependency;
};
} // namespace doris::pipeline
