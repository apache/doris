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

#include <ostream>
#include <typeinfo>
#include <utility>

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

void Pipeline::_init_profile() {
    std::stringstream ss;
    ss << "Pipeline"
       << " (pipeline id=" << _pipeline_id << ")";
    _pipeline_profile.reset(new RuntimeProfile(ss.str()));
}

Status Pipeline::build_operators(Operators& operators) {
    OperatorPtr pre;
    for (auto& operator_t : _operator_builders) {
        auto o = operator_t->build_operator();
        if (pre) {
            o->set_child(pre);
        }
        operators.emplace_back(o);
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
    if (_operators.empty() && !op->is_source()) {
        return Status::InternalError("Should set source before other operator");
    }
    _operators.emplace_back(op);
    return Status::OK();
}

Status Pipeline::prepare(RuntimeState* state) {
    // TODO
    RETURN_IF_ERROR(_operators.back()->prepare(state));
    RETURN_IF_ERROR(_operators.back()->open(state));
    RETURN_IF_ERROR(_sink_x->prepare(state));
    RETURN_IF_ERROR(_sink_x->open(state));
    return Status::OK();
}

Status Pipeline::set_sink(OperatorBuilderPtr& sink_) {
    if (_sink) {
        return Status::InternalError("set sink twice");
    }
    if (!sink_->is_sink()) {
        return Status::InternalError("should set a sink operator but {}", typeid(sink_).name());
    }
    _sink = sink_;
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