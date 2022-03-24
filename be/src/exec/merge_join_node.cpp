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

#include "exec/merge_join_node.h"

#include <sstream>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/in_predicate.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

template <class T>
int compare_value(const void* left_value, const void* right_value) {
    if (*(T*)left_value < *(T*)right_value) {
        return -1;
    } else if (*(T*)left_value == *(T*)right_value) {
        return 0;
    } else {
        return 1;
    }
}

template <class T>
int compare_value(const StringValue* left_value, const StringValue* right_value) {
    return left_value->compare(*right_value);
}

MergeJoinNode::MergeJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _out_batch(nullptr) {}

MergeJoinNode::~MergeJoinNode() {}

Status MergeJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    DCHECK(tnode.__isset.merge_join_node);
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const std::vector<TEqJoinCondition>& cmp_conjuncts = tnode.merge_join_node.cmp_conjuncts;

    for (int i = 0; i < cmp_conjuncts.size(); ++i) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, cmp_conjuncts[i].left, &ctx));
        _left_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, cmp_conjuncts[i].right, &ctx));
        _right_expr_ctxs.push_back(ctx);
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.merge_join_node.other_join_conjuncts,
                                            &_other_join_conjunct_ctxs));
    return Status::OK();
}

Status MergeJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    // build and probe exprs are evaluated in the context of the rows produced by our
    // right and left children, respectively
    RETURN_IF_ERROR(
            Expr::prepare(_left_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(
            Expr::prepare(_right_expr_ctxs, state, child(1)->row_desc(), expr_mem_tracker()));

    for (int i = 0; i < _left_expr_ctxs.size(); ++i) {
        switch (_left_expr_ctxs[i]->root()->type().type) {
        case TYPE_TINYINT:
            _cmp_func.push_back(compare_value<int8_t>);
            break;

        case TYPE_SMALLINT:
            _cmp_func.push_back(compare_value<int16_t>);
            break;

        case TYPE_INT:
            _cmp_func.push_back(compare_value<int32_t>);
            break;

        case TYPE_BIGINT:
            _cmp_func.push_back(compare_value<int64_t>);
            break;

        case TYPE_LARGEINT:
            _cmp_func.push_back(compare_value<__int128>);
            break;

        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            _cmp_func.push_back(compare_value<StringValue>);
            break;

        default:
            return Status::InternalError("unsupported compare type.");
            break;
        }
    }

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    RETURN_IF_ERROR(
            Expr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor, expr_mem_tracker()));

    _result_tuple_row_size = _row_descriptor.tuple_descriptors().size() * sizeof(Tuple*);
    // pre-compute the tuple index of build tuples in the output row

    _left_tuple_size = child(0)->row_desc().tuple_descriptors().size();
    _right_tuple_size = child(1)->row_desc().tuple_descriptors().size();
    _right_tuple_idx.reserve(_right_tuple_size);

    for (int i = 0; i < _right_tuple_size; ++i) {
        TupleDescriptor* right_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        _right_tuple_idx.push_back(_row_descriptor.get_tuple_idx(right_tuple_desc->id()));
    }

    _left_child_ctx.reset(new ChildReaderContext(row_desc(), state->batch_size()));
    _right_child_ctx.reset(new ChildReaderContext(row_desc(), state->batch_size()));

    return Status::OK();
}

Status MergeJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    Expr::close(_left_expr_ctxs, state);
    Expr::close(_right_expr_ctxs, state);
    Expr::close(_other_join_conjunct_ctxs, state);
    return ExecNode::close(state);
}

Status MergeJoinNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_left_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_right_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_other_join_conjunct_ctxs, state));

    _eos = false;
    // Open the probe-side child so that it may perform any initialisation in parallel.
    // Don't exit even if we see an error, we still need to wait for the build thread
    // to finish.
    RETURN_IF_ERROR(child(0)->open(state));
    RETURN_IF_ERROR(child(1)->open(state));

    RETURN_IF_ERROR(get_input_row(state, 0));
    RETURN_IF_ERROR(get_input_row(state, 1));

    return Status::OK();
}

Status MergeJoinNode::get_next(RuntimeState* state, RowBatch* out_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || _eos) {
        *eos = true;
        return Status::OK();
    }

    while (true) {
        int row_idx = out_batch->add_row();
        DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
        TupleRow* row = out_batch->get_row(row_idx);

        _out_batch = out_batch;
        RETURN_IF_ERROR(get_next_row(state, row, eos));

        if (*eos) {
            _eos = true;
            return Status::OK();
        }

        if (eval_conjuncts(&_other_join_conjunct_ctxs[0], _other_join_conjunct_ctxs.size(), row)) {
            out_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        }

        if (out_batch->is_full() || out_batch->at_resource_limit() || reached_limit()) {
            break;
        }
    }

    return Status::OK();
}

void MergeJoinNode::create_output_row(TupleRow* out, TupleRow* left, TupleRow* right) {
    if (left == nullptr) {
        memset(out, 0, _left_tuple_size);
    } else {
        memcpy(out, left, _left_tuple_size);
    }

    if (right != nullptr) {
        for (int i = 0; i < _right_tuple_size; ++i) {
            out->set_tuple(_right_tuple_idx[i], right->get_tuple(i));
        }
    } else {
        for (int i = 0; i < _right_tuple_size; ++i) {
            out->set_tuple(_right_tuple_idx[i], nullptr);
        }
    }
}

Status MergeJoinNode::compare_row(TupleRow* left_row, TupleRow* right_row, bool* is_lt) {
    if (left_row == nullptr) {
        *is_lt = false;
        return Status::OK();
    } else if (right_row == nullptr) {
        *is_lt = true;
        return Status::OK();
    }

    for (int i = 0; i < _left_expr_ctxs.size(); ++i) {
        void* left_value = _left_expr_ctxs[i]->get_value(left_row);
        void* right_value = _right_expr_ctxs[i]->get_value(right_row);
        int cmp_val = _cmp_func[i](left_value, right_value);

        if (cmp_val < 0) {
            *is_lt = true;
            return Status::OK();
        } else if (cmp_val == 0) {
            // do nothing
        } else {
            *is_lt = false;
            return Status::OK();
        }
    }

    // equal
    *is_lt = false;

    return Status::OK();
}

Status MergeJoinNode::get_next_row(RuntimeState* state, TupleRow* out_row, bool* eos) {
    TupleRow* left_row = _left_child_ctx->current_row;
    TupleRow* right_row = _right_child_ctx->current_row;

    if (left_row == nullptr && right_row == nullptr) {
        *eos = true;
        return Status::OK();
    }

    bool is_lt = true;
    RETURN_IF_ERROR(compare_row(left_row, right_row, &is_lt));

    if (is_lt) {
        create_output_row(out_row, left_row, nullptr);
        RETURN_IF_ERROR(get_input_row(state, 0));
    } else {
        create_output_row(out_row, nullptr, right_row);
        RETURN_IF_ERROR(get_input_row(state, 1));
    }

    return Status::OK();
}

Status MergeJoinNode::get_input_row(RuntimeState* state, int child_idx) {
    ChildReaderContext* ctx = nullptr;

    if (child_idx == 0) {
        ctx = _left_child_ctx.get();
    } else {
        ctx = _right_child_ctx.get();
    }

    // loop util read a valid data
    while (!ctx->is_eos && ctx->row_idx >= ctx->batch.num_rows()) {
        // transfer ownership before get new batch
        if (nullptr != _out_batch) {
            ctx->batch.transfer_resource_ownership(_out_batch);
        }

        if (child_idx == 0) {
            _left_child_ctx.reset(
                    new ChildReaderContext(child(child_idx)->row_desc(), state->batch_size()));
            ctx = _left_child_ctx.get();
        } else {
            _right_child_ctx.reset(
                    new ChildReaderContext(child(child_idx)->row_desc(), state->batch_size()));
            ctx = _right_child_ctx.get();
        }

        RETURN_IF_ERROR(child(child_idx)->get_next(state, &ctx->batch, &ctx->is_eos));
    }

    if (ctx->row_idx >= ctx->batch.num_rows()) {
        ctx->current_row = nullptr;
        return Status::OK();
    }

    ctx->current_row = ctx->batch.get_row(ctx->row_idx++);
    return Status::OK();
}

void MergeJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "MergeJoin(eos=" << (_eos ? "true" : "false")
         << " _left_child_pos=" << (_left_child_ctx.get() ? _left_child_ctx->row_idx : -1)
         << " _right_child_pos=" << (_right_child_ctx.get() ? _right_child_ctx->row_idx : -1)
         << " join_conjuncts=";
    *out << "Conjunct(";
    // << " left_exprs=" << Expr::debug_string(_left_exprs)
    // << " right_exprs=" << Expr::debug_string(_right_exprs);
    *out << ")";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris
