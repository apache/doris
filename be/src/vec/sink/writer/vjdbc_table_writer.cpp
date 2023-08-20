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

#include "vjdbc_table_writer.h"

#include <stdint.h>

#include <sstream>

#include "util/binary_cast.hpp"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

VJdbcTableWriter::VJdbcTableWriter(const JdbcConnectorParam& param,
                                   const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), JdbcConnector(param) {}

Status VJdbcTableWriter::append_block(vectorized::Block& block) {
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));
    auto num_rows = output_block.rows();

    uint32_t start_send_row = 0;
    uint32_t num_row_sent = 0;
    while (start_send_row < num_rows) {
        RETURN_IF_ERROR(append(&output_block, _vec_output_expr_ctxs, start_send_row, &num_row_sent,
                               _conn_param.table_type));
        start_send_row += num_row_sent;
        num_row_sent = 0;
    }

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
