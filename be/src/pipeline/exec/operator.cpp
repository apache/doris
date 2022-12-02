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

#include "operator.h"

namespace doris::pipeline {

Operator::Operator(OperatorBuilder* operator_builder)
        : _operator_builder(operator_builder), _is_closed(false) {}

bool Operator::is_sink() const {
    return _operator_builder->is_sink();
}

bool Operator::is_source() const {
    return _operator_builder->is_source();
}

Status Operator::init(ExecNode* exec_node, RuntimeState* state) {
    _runtime_profile.reset(new RuntimeProfile(_operator_builder->get_name()));
    if (exec_node) {
        exec_node->runtime_profile()->insert_child_head(_runtime_profile.get(), true);
    }
    return Status::OK();
}

Status Operator::prepare(RuntimeState* state) {
    _mem_tracker = std::make_unique<MemTracker>("Operator:" + _runtime_profile->name(),
                                                _runtime_profile.get());
    return Status::OK();
}

Status Operator::open(RuntimeState* state) {
    return Status::OK();
}

Status Operator::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    return Status::OK();
}

const RowDescriptor& Operator::row_desc() {
    return _operator_builder->row_desc();
}

void Operator::_fresh_exec_timer(doris::ExecNode* node) {
    node->_runtime_profile->total_time_counter()->update(
            _runtime_profile->total_time_counter()->value());
}

std::string Operator::debug_string() const {
    std::stringstream ss;
    ss << _operator_builder->get_name() << ", source: " << is_source();
    ss << ", sink: " << is_sink() << ", is closed: " << _is_closed;
    ss << ", is pending finish: " << is_pending_finish();
    return ss.str();
}

/////////////////////////////////////// OperatorBuilder ////////////////////////////////////////////////////////////

Status OperatorBuilder::prepare(doris::RuntimeState* state) {
    _state = state;
    // runtime filter, now dispose by NewOlapScanNode
    return Status::OK();
}

void OperatorBuilder::close(doris::RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
}

} // namespace doris::pipeline
