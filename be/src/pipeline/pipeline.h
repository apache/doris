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

#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "util/runtime_profile.h"

namespace doris {
namespace pipeline {
class PipelineFragmentContext;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

class Pipeline;

using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
using PipelineId = uint32_t;

class Pipeline : public std::enable_shared_from_this<Pipeline> {
    friend class PipelineTask;
    friend class PipelineXTask;

public:
    Pipeline() = delete;
    explicit Pipeline(PipelineId pipeline_id, std::weak_ptr<PipelineFragmentContext> context)
            : _pipeline_id(pipeline_id), _context(context) {
        _init_profile();
    }

    void add_dependency(std::shared_ptr<Pipeline>& pipeline) {
        pipeline->_parents.push_back({_operator_builders.size(), weak_from_this()});
        _dependencies.push_back({_operator_builders.size(), pipeline});
    }

    // If all dependencies are finished, this pipeline task should be scheduled.
    // e.g. Hash join probe task will be scheduled once Hash join build task is finished.
    void finish_one_dependency(int dep_opr, int dependency_core_id) {
        std::lock_guard l(_depend_mutex);
        if (!_operators.empty() && _operators[dep_opr - 1]->can_terminate_early()) {
            _always_can_read = true;
            _always_can_write = (dep_opr == _operators.size());

            for (int i = 0; i < _dependencies.size(); ++i) {
                if (dep_opr == _dependencies[i].first) {
                    _dependencies.erase(_dependencies.begin(), _dependencies.begin() + i + 1);
                    break;
                }
            }
        } else {
            for (int i = 0; i < _dependencies.size(); ++i) {
                if (dep_opr == _dependencies[i].first) {
                    _dependencies.erase(_dependencies.begin() + i);
                    break;
                }
            }
        }

        if (_dependencies.empty()) {
            _previous_schedule_id = dependency_core_id;
        }
    }

    bool has_dependency() {
        std::lock_guard l(_depend_mutex);
        return !_dependencies.empty();
    }

    Status add_operator(OperatorBuilderPtr& op);

    // Add operators for pipelineX
    Status add_operator(OperatorXPtr& op);
    // prepare operators for pipelineX
    Status prepare(RuntimeState* state);

    Status set_sink(OperatorBuilderPtr& sink_operator);
    Status set_sink(DataSinkOperatorXPtr& sink_operator);

    OperatorBuilderBase* sink() { return _sink.get(); }
    DataSinkOperatorXBase* sink_x() { return _sink_x.get(); }
    OperatorXs& operator_xs() { return operatorXs; }
    DataSinkOperatorXPtr sink_shared_pointer() { return _sink_x; }

    Status build_operators();

    RuntimeProfile* pipeline_profile() { return _pipeline_profile.get(); }

    [[nodiscard]] const RowDescriptor& output_row_desc() const {
        return operatorXs.back()->row_desc();
    }

    [[nodiscard]] PipelineId id() const { return _pipeline_id; }
    void set_is_root_pipeline() { _is_root_pipeline = true; }
    bool is_root_pipeline() const { return _is_root_pipeline; }
    void set_collect_query_statistics_with_every_batch() {
        _collect_query_statistics_with_every_batch = true;
    }
    bool collect_query_statistics_with_every_batch() const {
        return _collect_query_statistics_with_every_batch;
    }

private:
    void _init_profile();

    OperatorBuilders _operator_builders; // left is _source, right is _root
    OperatorBuilderPtr _sink;            // put block to sink

    std::mutex _depend_mutex;
    std::vector<std::pair<int, std::weak_ptr<Pipeline>>> _parents;
    std::vector<std::pair<int, std::shared_ptr<Pipeline>>> _dependencies;

    PipelineId _pipeline_id;
    std::weak_ptr<PipelineFragmentContext> _context;
    int _previous_schedule_id = -1;

    std::unique_ptr<RuntimeProfile> _pipeline_profile;

    // Operators for pipelineX. All pipeline tasks share operators from this.
    // [SourceOperator -> ... -> SinkOperator]
    OperatorXs operatorXs;
    DataSinkOperatorXPtr _sink_x;

    std::shared_ptr<ObjectPool> _obj_pool;

    Operators _operators;
    /**
     * Consider the query plan below:
     *
     *      ExchangeSource     JoinBuild1
     *            \              /
     *         JoinProbe1 (Right Outer)    JoinBuild2
     *                   \                   /
     *                 JoinProbe2 (Right Outer)
     *                          |
     *                        Sink
     *
     * Assume JoinBuild1/JoinBuild2 outputs 0 rows, this pipeline task should not be blocked by ExchangeSource
     * because we have a determined conclusion that JoinProbe1/JoinProbe2 will also output 0 rows.
     *
     * Assume JoinBuild2 outputs > 0 rows, this pipeline task may be blocked by Sink because JoinProbe2 will
     * produce more data.
     *
     * Assume both JoinBuild2 outputs 0 rows this pipeline task should not be blocked by ExchangeSource
     * and Sink because JoinProbe2 will always produce 0 rows and terminate early.
     *
     * In a nutshell, we should follow the rules:
     * 1. if any operator in pipeline can terminate early, this task should never be blocked by source operator.
     * 2. if the last operator (except sink) can terminate early, this task should never be blocked by sink operator.
     */
    bool _always_can_read = false;
    bool _always_can_write = false;
    bool _is_root_pipeline = false;
    bool _collect_query_statistics_with_every_batch = false;
};

} // namespace doris::pipeline
