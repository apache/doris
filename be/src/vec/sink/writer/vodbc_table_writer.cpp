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

#include "vodbc_table_writer.h"

#include <gen_cpp/DataSinks_types.h>
#include <stdint.h>

#include <sstream>

#include "util/binary_cast.hpp"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

ODBCConnectorParam VOdbcTableWriter::create_connect_param(const doris::TDataSink& t_sink) {
    const TOdbcTableSink& t_odbc_sink = t_sink.odbc_table_sink;

    ODBCConnectorParam odbc_param;
    odbc_param.connect_string = t_odbc_sink.connect_string;
    odbc_param.table_name = t_odbc_sink.table;
    odbc_param.use_transaction = t_odbc_sink.use_transaction;

    return odbc_param;
}

VOdbcTableWriter::VOdbcTableWriter(const doris::TDataSink& t_sink,
                                   const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), ODBCConnector(create_connect_param(t_sink)) {}

Status VOdbcTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));
    auto num_rows = output_block.rows();

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < num_rows) {
        RETURN_IF_ERROR(
                append(&output_block, _vec_output_expr_ctxs, start_send_row, &num_row_sent));
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

} // namespace vectorized
} // namespace doris