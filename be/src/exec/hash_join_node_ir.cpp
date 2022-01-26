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

#include "common/utils.h"
#include "exec/hash_join_node.h"
#include "exec/hash_table.hpp"
#include "exprs/expr_context.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"

namespace doris {

// Functions in this file are cross compiled to IR with clang.

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool IR_NO_INLINE eval_other_join_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
    return ExecNode::eval_conjuncts(ctxs, num_ctxs, row);
}

// CreateOutputRow, EvalOtherJoinConjuncts, and EvalConjuncts are replaced by
// codegen.
int HashJoinNode::process_probe_batch(RowBatch* out_batch, RowBatch* probe_batch,
                                      int max_added_rows) {
    // This path does not handle full outer or right outer joins
    DCHECK(!_match_all_build);

    int row_idx = out_batch->add_rows(max_added_rows);
    DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
    uint8_t* out_row_mem = reinterpret_cast<uint8_t*>(out_batch->get_row(row_idx));
    TupleRow* out_row = reinterpret_cast<TupleRow*>(out_row_mem);

    int rows_returned = 0;
    int probe_rows = probe_batch->num_rows();

    ExprContext* const* other_conjunct_ctxs = &_other_join_conjunct_ctxs[0];
    int num_other_conjunct_ctxs = _other_join_conjunct_ctxs.size();

    ExprContext* const* conjunct_ctxs = &_conjunct_ctxs[0];
    int num_conjunct_ctxs = _conjunct_ctxs.size();

    while (true) {
        // Create output row for each matching build row
        while (_hash_tbl_iterator.has_next()) {
            TupleRow* matched_build_row = _hash_tbl_iterator.get_row();
            _hash_tbl_iterator.next<true>();
            create_output_row(out_row, _current_probe_row, matched_build_row);

            if (!eval_other_join_conjuncts(other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
                continue;
            }

            _matched_probe = true;

            // left_anti_join: equal match won't return
            if (_join_op == TJoinOp::LEFT_ANTI_JOIN) {
                _hash_tbl_iterator = _hash_tbl->end();
                break;
            }

            if (eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
                ++rows_returned;

                // Filled up out batch or hit limit
                if (UNLIKELY(rows_returned == max_added_rows)) {
                    goto end;
                }

                // Advance to next out row
                out_row_mem += out_batch->row_byte_size();
                out_row = reinterpret_cast<TupleRow*>(out_row_mem);
            }

            // Handle left semi-join
            if (_match_one_build) {
                _hash_tbl_iterator = _hash_tbl->end();
                break;
            }
        }

        // Handle left outer-join and left semi-join
        if ((!_matched_probe && _match_all_probe) ||
            ((!_matched_probe && _join_op == TJoinOp::LEFT_ANTI_JOIN))) {
            create_output_row(out_row, _current_probe_row, nullptr);
            _matched_probe = true;

            if (ExecNode::eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
                ++rows_returned;

                if (UNLIKELY(rows_returned == max_added_rows)) {
                    goto end;
                }

                // Advance to next out row
                out_row_mem += out_batch->row_byte_size();
                out_row = reinterpret_cast<TupleRow*>(out_row_mem);
            }
        }

        if (!_hash_tbl_iterator.has_next()) {
            // Advance to the next probe row
            if (UNLIKELY(_probe_batch_pos == probe_rows)) {
                goto end;
            }
            if (++_probe_counter % RELEASE_CONTEXT_COUNTER == 0) {
                ExprContext::free_local_allocations(_probe_expr_ctxs);
                ExprContext::free_local_allocations(_build_expr_ctxs);
            }
            _current_probe_row = probe_batch->get_row(_probe_batch_pos++);
            _hash_tbl_iterator = _hash_tbl->find(_current_probe_row);
            _matched_probe = false;
        }
    }

end:

    if (_match_one_build && _matched_probe) {
        _hash_tbl_iterator = _hash_tbl->end();
    }

    out_batch->commit_rows(rows_returned);
    return rows_returned;
}

// when build table has too many duplicated rows, the collisions will be very serious,
// so in some case will don't need to store duplicated value in hash table, we can build an unique one
Status HashJoinNode::process_build_batch(RuntimeState* state, RowBatch* build_batch) {
    // insert build row into our hash table
    if (_build_unique) {
        for (int i = 0; i < build_batch->num_rows(); ++i) {
            // _hash_tbl->insert_unique(build_batch->get_row(i));
            TupleRow* tuple_row = nullptr;
            if (_hash_tbl->emplace_key(build_batch->get_row(i), &tuple_row)) {
                build_batch->get_row(i)->deep_copy(tuple_row,
                                                   child(1)->row_desc().tuple_descriptors(),
                                                   _build_pool.get(), false);
            }
        }
    } else {
        // take ownership of tuple data of build_batch
        _build_pool->acquire_data(build_batch->tuple_data_pool(), false);

        for (int i = 0; i < build_batch->num_rows(); ++i) {
            _hash_tbl->insert(build_batch->get_row(i));
        }
    }
    return Status::OK();
}

} // namespace doris
