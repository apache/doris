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

#include "olap_table_sink_operator.h"

#include "common/status.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

OperatorPtr OlapTableSinkOperatorBuilder::build_operator() {
    return std::make_shared<OlapTableSinkOperator>(this, _sink);
}

Status OlapTableSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<Parent>();
    RETURN_IF_ERROR(_writer->init_properties(p._pool, p._group_commit));
    return Status::OK();
}

Status OlapTableSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (Base::_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(exec_time_counter());
    if (_closed) {
        return _close_status;
    }
    _close_status = Base::close(state, exec_status);
    return _close_status;
}

} // namespace doris::pipeline
