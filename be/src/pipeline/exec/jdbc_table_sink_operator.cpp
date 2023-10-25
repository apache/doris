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

#include "jdbc_table_sink_operator.h"

#include <memory>

#include "common/object_pool.h"
#include "pipeline/exec/operator.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

JdbcTableSinkOperatorX::JdbcTableSinkOperatorX(const RowDescriptor& row_desc, int operator_id,
                                               const std::vector<TExpr>& t_output_expr)
        : DataSinkOperatorX(operator_id, 0), _row_desc(row_desc), _t_output_expr(t_output_expr) {}

Status JdbcTableSinkOperatorX::init(const TDataSink& thrift_sink) {
    RETURN_IF_ERROR(DataSinkOperatorX<JdbcTableSinkLocalState>::init(thrift_sink));
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    return Status::OK();
}

Status JdbcTableSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<JdbcTableSinkLocalState>::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status JdbcTableSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<JdbcTableSinkLocalState>::open(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return Status::OK();
}

Status JdbcTableSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block,
                                    SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    RETURN_IF_ERROR(local_state.sink(state, block, source_state));
    return Status::OK();
}

FinishDependency* JdbcTableSinkOperatorX::finish_blocked_by(RuntimeState* state) const {
    auto& local_state = state->get_sink_local_state(operator_id())->cast<JdbcTableSinkLocalState>();
    return local_state._finish_dependency->finish_blocked_by();
}

} // namespace doris::pipeline
