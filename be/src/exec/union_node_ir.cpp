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

#include "exec/union_node.h"
#include "exprs/expr_context.h"
#include "runtime/tuple_row.h"

namespace doris {

void IR_ALWAYS_INLINE UnionNode::materialize_exprs(const std::vector<ExprContext*>& exprs,
                                                   TupleRow* row, uint8_t* tuple_buf,
                                                   RowBatch* dst_batch) {
    DCHECK(!dst_batch->at_capacity());
    Tuple* dst_tuple = reinterpret_cast<Tuple*>(tuple_buf);
    TupleRow* dst_row = dst_batch->get_row(dst_batch->add_row());
    // dst_tuple->materialize_exprs<false, false>(row, *_tuple_desc, exprs,
    dst_tuple->materialize_exprs<false>(row, *_tuple_desc, exprs, dst_batch->tuple_data_pool(),
                                        nullptr, nullptr);
    dst_row->set_tuple(0, dst_tuple);
    dst_batch->commit_last_row();
}

void UnionNode::materialize_batch(RowBatch* dst_batch, uint8_t** tuple_buf) {
    // Take all references to member variables out of the loop to reduce the number of
    // loads and stores.
    RowBatch* child_batch = _child_batch.get();
    int tuple_byte_size = _tuple_desc->byte_size();
    uint8_t* cur_tuple = *tuple_buf;
    const std::vector<ExprContext*>& child_exprs = _child_expr_lists[_child_idx];

    int num_rows_to_process = std::min(child_batch->num_rows() - _child_row_idx,
                                       dst_batch->capacity() - dst_batch->num_rows());
    FOREACH_ROW_LIMIT(child_batch, _child_row_idx, num_rows_to_process, batch_iter) {
        TupleRow* child_row = batch_iter.get();
        materialize_exprs(child_exprs, child_row, cur_tuple, dst_batch);
        cur_tuple += tuple_byte_size;
    }

    _child_row_idx += num_rows_to_process;
    *tuple_buf = cur_tuple;
}

Status UnionNode::get_error_msg(const std::vector<ExprContext*>& exprs) {
    for (auto expr_ctx : exprs) {
        std::string expr_error = expr_ctx->get_error_msg();
        if (!expr_error.empty()) {
            return Status::RuntimeError(expr_error);
        }
    }
    return Status::OK();
}

} // namespace doris
