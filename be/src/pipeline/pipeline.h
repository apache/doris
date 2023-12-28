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

namespace doris::pipeline {

class PipelineFragmentContext;
class Pipeline;

using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
using PipelineId = uint32_t;

class Pipeline : public std::enable_shared_from_this<Pipeline> {
    friend class PipelineTask;
    friend class PipelineXTask;

public:
    Pipeline() = delete;
    explicit Pipeline(PipelineId pipeline_id, int num_tasks,
                      std::weak_ptr<PipelineFragmentContext> context)
            : _pipeline_id(pipeline_id), _context(context), _num_tasks(num_tasks) {
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

    Status set_sink_builder(OperatorBuilderPtr& sink_operator_builder);
    Status set_sink(DataSinkOperatorXPtr& sink_operator);

    OperatorBuilderBase* get_sink_builder() { return _sink_builder.get(); }
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
    [[nodiscard]] bool collect_query_statistics_with_every_batch() const {
        return _collect_query_statistics_with_every_batch;
    }

    static bool is_hash_exchange(ExchangeType idx) {
        return idx == ExchangeType::HASH_SHUFFLE || idx == ExchangeType::BUCKET_HASH_SHUFFLE;
    }

    bool need_to_local_exchange(const DataDistribution target_data_distribution) const {
        if (target_data_distribution.distribution_type != ExchangeType::BUCKET_HASH_SHUFFLE &&
            target_data_distribution.distribution_type != ExchangeType::HASH_SHUFFLE) {
            return true;
        } else if (operatorXs.front()->ignore_data_hash_distribution()) {
            if (_data_distribution.distribution_type ==
                        target_data_distribution.distribution_type &&
                (_data_distribution.partition_exprs.empty() ||
                 target_data_distribution.partition_exprs.empty())) {
                return true;
            }
            return _data_distribution.distribution_type !=
                           target_data_distribution.distribution_type ||
                   _data_distribution.partition_exprs != target_data_distribution.partition_exprs;
        } else {
            return _data_distribution.distribution_type !=
                           target_data_distribution.distribution_type &&
                   !(is_hash_exchange(_data_distribution.distribution_type) &&
                     is_hash_exchange(target_data_distribution.distribution_type));
        }
    }
    void init_data_distribution() {
        set_data_distribution(operatorXs.front()->required_data_distribution());
    }
    void set_data_distribution(const DataDistribution& data_distribution) {
        _data_distribution = data_distribution;
    }
    const DataDistribution& data_distribution() const { return _data_distribution; }

    std::vector<std::shared_ptr<Pipeline>>& children() { return _children; }
    void set_children(std::shared_ptr<Pipeline> child) { _children.push_back(child); }
    void set_children(std::vector<std::shared_ptr<Pipeline>> children) { _children = children; }

    void incr_created_tasks() { _num_tasks_created++; }
    bool need_to_create_task() const { return _num_tasks > _num_tasks_created; }
    void set_num_tasks(int num_tasks) {
        _num_tasks = num_tasks;
        for (auto& op : operatorXs) {
            op->set_parallel_tasks(_num_tasks);
        }
    }
    int num_tasks() const { return _num_tasks; }

    std::string debug_string() {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer,
                       "Pipeline [id: {}, _num_tasks: {}, _num_tasks_created: {}]", _pipeline_id,
                       _num_tasks, _num_tasks_created);
        for (size_t i = 0; i < operatorXs.size(); i++) {
            fmt::format_to(debug_string_buffer, "\n{}", operatorXs[i]->debug_string(i));
        }
        fmt::format_to(debug_string_buffer, "\n{}", _sink_x->debug_string(operatorXs.size()));
        return fmt::to_string(debug_string_buffer);
    }

private:
    void _init_profile();

    OperatorBuilders _operator_builders; // left is _source, right is _root
    OperatorBuilderPtr _sink_builder;    // put block to sink

    std::mutex _depend_mutex;
    std::vector<std::pair<int, std::weak_ptr<Pipeline>>> _parents;
    std::vector<std::pair<int, std::shared_ptr<Pipeline>>> _dependencies;

    std::vector<std::shared_ptr<Pipeline>> _children;

    PipelineId _pipeline_id;
    std::weak_ptr<PipelineFragmentContext> _context;
    int _previous_schedule_id = -1;

    std::unique_ptr<RuntimeProfile> _pipeline_profile;

    // Operators for pipelineX. All pipeline tasks share operators from this.
    // [SourceOperator -> ... -> SinkOperator]
    OperatorXs operatorXs;
    DataSinkOperatorXPtr _sink_x = nullptr;

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

    // Input data distribution of this pipeline. We do local exchange when input data distribution
    // does not match the target data distribution.
    DataDistribution _data_distribution {ExchangeType::NOOP};

    // How many tasks should be created ?
    int _num_tasks = 1;
    // How many tasks are already created?
    int _num_tasks_created = 0;
};

} // namespace doris::pipeline
