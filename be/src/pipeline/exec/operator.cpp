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

OperatorBase::OperatorBase(OperatorBuilderBase* operator_builder)
        : _operator_builder(operator_builder), _is_closed(false) {}

bool OperatorBase::is_sink() const {
    return _operator_builder->is_sink();
}

bool OperatorBase::is_source() const {
    return _operator_builder->is_source();
}

Status OperatorBase::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    return Status::OK();
}

const RowDescriptor& OperatorBase::row_desc() {
    return _operator_builder->row_desc();
}

std::string OperatorBase::debug_string() const {
    std::stringstream ss;
    ss << _operator_builder->get_name() << ", is source: " << is_source();
    ss << ", is sink: " << is_sink() << ", is closed: " << _is_closed;
    ss << ", is pending finish: " << is_pending_finish();
    return ss.str();
}

} // namespace doris::pipeline
