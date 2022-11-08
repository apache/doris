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

#include "vec/sink/vjdbc_table_sink.h"

#ifdef LIBJVM
#include <gen_cpp/DataSinks_types.h>

#include <sstream>

#include "vec/core/materialize_block.h"
#include "vec/sink/vtable_sink.h"

namespace doris {
namespace vectorized {

VJdbcTableSink::VJdbcTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                               const std::vector<TExpr>& t_exprs)
        : VTableSink(pool, row_desc, t_exprs) {
    _name = "VJdbcTableSink";
}

Status VJdbcTableSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(VTableSink::init(t_sink));
    const TJdbcTableSink& t_jdbc_sink = t_sink.jdbc_table_sink;

    _jdbc_param.jdbc_url = t_jdbc_sink.jdbc_table.jdbc_url;
    _jdbc_param.user = t_jdbc_sink.jdbc_table.jdbc_user;
    _jdbc_param.passwd = t_jdbc_sink.jdbc_table.jdbc_password;
    _jdbc_param.driver_class = t_jdbc_sink.jdbc_table.jdbc_driver_class;
    _jdbc_param.driver_path = t_jdbc_sink.jdbc_table.jdbc_driver_url;
    _jdbc_param.driver_checksum = t_jdbc_sink.jdbc_table.jdbc_driver_checksum;
    _jdbc_param.resource_name = t_jdbc_sink.jdbc_table.jdbc_resource_name;
    _table_name = t_jdbc_sink.jdbc_table.jdbc_table_name;
    _use_transaction = t_jdbc_sink.use_transaction;
    _need_extra_convert = (t_jdbc_sink.table_type == TOdbcTableType::ORACLE);

    return Status::OK();
}

Status VJdbcTableSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJdbcTableSink::open");
    RETURN_IF_ERROR(VTableSink::open(state));

    // create writer
    _writer.reset(new JdbcConnector(_jdbc_param));
    RETURN_IF_ERROR(_writer->open(state, false));
    if (_use_transaction) {
        RETURN_IF_ERROR(_writer->begin_trans());
    }

    _writer->init_profile(_profile);
    return Status::OK();
}

Status VJdbcTableSink::send(RuntimeState* state, Block* block) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VJdbcTableSink::send");
    Status status = Status::OK();
    if (block == nullptr || block->rows() == 0) {
        return status;
    }

    auto output_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _output_vexpr_ctxs, *block, status);
    materialize_block_inplace(output_block);

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < output_block.rows()) {
        RETURN_IF_ERROR(_writer->append(_table_name, &output_block, _output_vexpr_ctxs,
                                        start_send_row, &num_row_sent, _need_extra_convert));
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

Status VJdbcTableSink::close(RuntimeState* state, Status exec_status) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VJdbcTableSink::close");
    RETURN_IF_ERROR(VTableSink::close(state, exec_status));
    if (exec_status.ok() && _use_transaction) {
        RETURN_IF_ERROR(_writer->finish_trans());
    }
    RETURN_IF_ERROR(_writer->close());
    return DataSink::close(state, exec_status);
}
} // namespace vectorized
} // namespace doris
#endif