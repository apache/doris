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

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"

namespace doris::pipeline {

class PipelineFragmentContext;
class Pipeline;

using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
using PipelineId = uint32_t;

class Pipeline : public std::enable_shared_from_this<Pipeline> {
    friend class PipelineTask;
    friend class PipelineFragmentContext;

public:
    explicit Pipeline(PipelineId pipeline_id, int num_tasks,
                      std::weak_ptr<PipelineFragmentContext> context)
            : _pipeline_id(pipeline_id), _num_tasks(num_tasks) {
        _init_profile();
    }

    // Add operators for pipelineX
    Status add_operator(OperatorPtr& op);
    // prepare operators for pipelineX
    Status prepare(RuntimeState* state);

    Status set_sink(DataSinkOperatorPtr& sink_operator);

    DataSinkOperatorXBase* sink() { return _sink.get(); }
    Operators& operators() { return _operators; }
    DataSinkOperatorPtr sink_shared_pointer() { return _sink; }

    [[nodiscard]] const RowDescriptor& output_row_desc() const {
        return _operators.back()->row_desc();
    }

    [[nodiscard]] PipelineId id() const { return _pipeline_id; }

    static bool is_hash_exchange(ExchangeType idx) {
        return idx == ExchangeType::HASH_SHUFFLE || idx == ExchangeType::BUCKET_HASH_SHUFFLE;
    }

    bool need_to_local_exchange(const DataDistribution target_data_distribution) const {
        if (target_data_distribution.distribution_type != ExchangeType::BUCKET_HASH_SHUFFLE &&
            target_data_distribution.distribution_type != ExchangeType::HASH_SHUFFLE) {
            return true;
        } else if (_operators.front()->ignore_data_hash_distribution()) {
            if (_data_distribution.distribution_type ==
                        target_data_distribution.distribution_type &&
                (_data_distribution.partition_exprs.empty() ||
                 target_data_distribution.partition_exprs.empty())) {
                return true;
            }
            return _data_distribution.distribution_type !=
                           target_data_distribution.distribution_type &&
                   !(is_hash_exchange(_data_distribution.distribution_type) &&
                     is_hash_exchange(target_data_distribution.distribution_type));
        } else {
            return _data_distribution.distribution_type !=
                           target_data_distribution.distribution_type &&
                   !(is_hash_exchange(_data_distribution.distribution_type) &&
                     is_hash_exchange(target_data_distribution.distribution_type));
        }
    }
    void init_data_distribution() {
        set_data_distribution(_operators.front()->required_data_distribution());
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
        for (auto& op : _operators) {
            op->set_parallel_tasks(_num_tasks);
        }
    }
    int num_tasks() const { return _num_tasks; }

    std::string debug_string() {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer,
                       "Pipeline [id: {}, _num_tasks: {}, _num_tasks_created: {}]", _pipeline_id,
                       _num_tasks, _num_tasks_created);
        for (size_t i = 0; i < _operators.size(); i++) {
            fmt::format_to(debug_string_buffer, "\n{}", _operators[i]->debug_string(i));
        }
        fmt::format_to(debug_string_buffer, "\n{}", _sink->debug_string(_operators.size()));
        return fmt::to_string(debug_string_buffer);
    }

private:
    void _init_profile();

    std::vector<std::pair<int, std::weak_ptr<Pipeline>>> _parents;
    std::vector<std::pair<int, std::shared_ptr<Pipeline>>> _dependencies;

    std::vector<std::shared_ptr<Pipeline>> _children;

    PipelineId _pipeline_id;
    int _previous_schedule_id = -1;

    // pipline id + operator names. init when:
    //  build_operators(), if pipeline;
    //  _build_pipelines() and _create_tree_helper(), if pipelineX.
    std::string _name;

    std::unique_ptr<RuntimeProfile> _pipeline_profile;

    // Operators for pipelineX. All pipeline tasks share operators from this.
    // [SourceOperator -> ... -> SinkOperator]
    Operators _operators;
    DataSinkOperatorPtr _sink = nullptr;

    std::shared_ptr<ObjectPool> _obj_pool;

    // Input data distribution of this pipeline. We do local exchange when input data distribution
    // does not match the target data distribution.
    DataDistribution _data_distribution {ExchangeType::NOOP};

    // How many tasks should be created ?
    int _num_tasks = 1;
    // How many tasks are already created?
    int _num_tasks_created = 0;
};

} // namespace doris::pipeline
