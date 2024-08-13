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

namespace doris::pipeline {

void Pipeline::_init_profile() {
    auto s = fmt::format("Pipeline (pipeline id={})", _pipeline_id);
    _pipeline_profile = std::make_unique<RuntimeProfile>(std::move(s));
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
    RETURN_IF_ERROR(operatorXs.back()->prepare(state));
    RETURN_IF_ERROR(operatorXs.back()->open(state));
    RETURN_IF_ERROR(_sink_x->prepare(state));
    RETURN_IF_ERROR(_sink_x->open(state));
    _name.append(std::to_string(id()));
    _name.push_back('-');
    for (auto& op : operatorXs) {
        _name.append(std::to_string(op->node_id()));
        _name.append(op->get_name());
    }
    _name.push_back('-');
    _name.append(std::to_string(_sink_x->node_id()));
    _name.append(_sink_x->get_name());
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

} // namespace doris::pipeline