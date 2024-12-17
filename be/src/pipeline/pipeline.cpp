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

#include "pipeline.h"

#include <memory>
#include <string>
#include <utility>

#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_task.h"

namespace doris::pipeline {

void Pipeline::_init_profile() {
    auto s = fmt::format("Pipeline (pipeline id={})", _pipeline_id);
    _pipeline_profile = std::make_unique<RuntimeProfile>(std::move(s));
}

Status Pipeline::build_operators() {
    _name.reserve(_operator_builders.size() * 10);
    _name.append(std::to_string(id()));

    OperatorPtr pre;
    for (auto& operator_t : _operator_builders) {
        auto o = operator_t->build_operator();
        if (pre) {
            RETURN_IF_ERROR(o->set_child(pre));
        }
        _operators.emplace_back(o);

        _name.push_back('-');
        _name.append(std::to_string(operator_t->id()));
        _name.append(o->get_name());

        pre = std::move(o);
    }
    return Status::OK();
}

Status Pipeline::add_operator(OperatorBuilderPtr& op) {
    if (_operator_builders.empty() && !op->is_source()) {
        return Status::InternalError("Should set source before other operator");
    }
    _operator_builders.emplace_back(op);
    return Status::OK();
}

Status Pipeline::add_operator(OperatorXPtr& op) {
    op->set_parallel_tasks(num_tasks());
    operatorXs.emplace_back(op);
    if (op->is_source()) {
        std::reverse(operatorXs.begin(), operatorXs.end());
    }
    return Status::OK();
}

Status Pipeline::prepare(RuntimeState* state) {
    // TODO
    RETURN_IF_ERROR(operatorXs.back()->prepare(state));
    RETURN_IF_ERROR(operatorXs.back()->open(state));
    RETURN_IF_ERROR(_sink_x->prepare(state));
    RETURN_IF_ERROR(_sink_x->open(state));
    return Status::OK();
}

Status Pipeline::set_sink_builder(OperatorBuilderPtr& sink_) {
    if (_sink_builder) {
        return Status::InternalError("set sink twice");
    }
    if (!sink_->is_sink()) {
        return Status::InternalError("should set a sink operator but {}", typeid(sink_).name());
    }
    _sink_builder = sink_;
    return Status::OK();
}

Status Pipeline::set_sink(DataSinkOperatorXPtr& sink) {
    if (_sink_x) {
        return Status::InternalError("set sink twice");
    }
    if (!sink->is_sink()) {
        return Status::InternalError("should set a sink operator but {}", typeid(sink).name());
    }
    _sink_x = sink;
    return Status::OK();
}

void Pipeline::make_all_runnable() {
    if (_sink_x->count_down_destination()) {
        for (auto* task : _tasks) {
            if (task) {
                task->set_wake_up_early();
            }
        }
        for (auto* task : _tasks) {
            if (task) {
                task->clear_blocking_state();
            }
        }
    }
}

} // namespace doris::pipeline
