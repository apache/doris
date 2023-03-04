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

#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/operator.h"

namespace doris::pipeline {

class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
using PipelineId = uint32_t;

class Pipeline : public std::enable_shared_from_this<Pipeline> {
    friend class PipelineTask;

public:
    Pipeline() = delete;
    explicit Pipeline(PipelineId pipeline_id, std::weak_ptr<PipelineFragmentContext> context)
            : _complete_dependency(0),
              _pipeline_id(pipeline_id),
              _context(context),
              _can_steal(true) {
        _init_profile();
    }

    void add_dependency(std::shared_ptr<Pipeline>& pipeline) {
        pipeline->_parents.push_back(shared_from_this());
        _dependencies.push_back(pipeline);
    }

    // If all dependencies are finished, this pipeline task should be scheduled.
    // e.g. Hash join probe task will be scheduled once Hash join build task is finished.
    bool finish_one_dependency(int dependency_core_id) {
        DCHECK(_complete_dependency < _dependencies.size());
        bool finish = _complete_dependency.fetch_add(1) == _dependencies.size() - 1;
        if (finish) {
            _previous_schedule_id = dependency_core_id;
        }
        return finish;
    }

    bool has_dependency() { return _complete_dependency.load() < _dependencies.size(); }

    Status add_operator(OperatorBuilderPtr& op);

    Status set_sink(OperatorBuilderPtr& sink_operator);

    OperatorBuilderBase* sink() { return _sink.get(); }

    Status build_operators(Operators&);

    RuntimeProfile* pipeline_profile() { return _pipeline_profile.get(); }

    bool can_steal() const { return _can_steal; }

    void disable_task_steal() { _can_steal = false; }

private:
    void _init_profile();
    std::atomic<uint32_t> _complete_dependency;

    OperatorBuilders _operator_builders; // left is _source, right is _root
    OperatorBuilderPtr _sink;            // put block to sink

    std::vector<std::shared_ptr<Pipeline>> _parents;
    std::vector<std::shared_ptr<Pipeline>> _dependencies;

    PipelineId _pipeline_id;
    std::weak_ptr<PipelineFragmentContext> _context;
    bool _can_steal;
    int _previous_schedule_id = -1;

    std::unique_ptr<RuntimeProfile> _pipeline_profile;
};

} // namespace doris::pipeline