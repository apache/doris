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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/hash-join-node.cc
// and modified by Doris
#include "exec/hash_join_node.h"

#include <memory>
#include <sstream>

#include "common/utils.h"
#include "exec/hash_table.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_slots.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace doris {

HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _probe_counter(0),
          _probe_eos(false),
          _process_probe_batch_fn(nullptr),
          _anti_join_last_pos(nullptr) {
    _match_all_probe =
            (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN);
    _match_one_build = (_join_op == TJoinOp::LEFT_SEMI_JOIN);
    _match_all_build =
            (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN);
    _build_unique = _join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN;

    _runtime_filter_descs = tnode.runtime_filters;
}

HashJoinNode::~HashJoinNode() {
    // _probe_batch must be cleaned up in close() to ensure proper resource freeing.
    DCHECK(_probe_batch == nullptr);
}

Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;

    for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjuncts[i].left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, eq_join_conjuncts[i].right, &ctx));
        _build_expr_ctxs.push_back(ctx);
        if (eq_join_conjuncts[i].__isset.opcode &&
            eq_join_conjuncts[i].opcode == TExprOpcode::EQ_FOR_NULL) {
            _is_null_safe_eq_join.push_back(true);
        } else {
            _is_null_safe_eq_join.push_back(false);
        }
    }

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, tnode.hash_join_node.other_join_conjuncts,
                                            &_other_join_conjunct_ctxs));

    if (!_other_join_conjunct_ctxs.empty()) {
        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        _build_unique = false;
    }

    _runtime_filters.resize(_runtime_filter_descs.size());

    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::PRODUCER, _runtime_filter_descs[i], state->query_options()));
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(
                _runtime_filter_descs[i].filter_id, &_runtime_filters[i]));
    }

    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _build_pool.reset(new MemPool(mem_tracker()));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _build_rows_counter = ADD_COUNTER(runtime_profile(), "BuildRows", TUnit::UNIT);
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
    _probe_rows_counter = ADD_COUNTER(runtime_profile(), "ProbeRows", TUnit::UNIT);
    _hash_tbl_load_factor_counter =
            ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);
    _hash_table_list_min_size = ADD_COUNTER(runtime_profile(), "HashTableMinList", TUnit::UNIT);
    _hash_table_list_max_size = ADD_COUNTER(runtime_profile(), "HashTableMaxList", TUnit::UNIT);
    // build and probe exprs are evaluated in the context of the rows produced by our
    // right and left children, respectively
    RETURN_IF_ERROR(Expr::prepare(_build_expr_ctxs, state, child(1)->row_desc()));
    RETURN_IF_ERROR(Expr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    // _other_join_conjuncts are evaluated in the context of the rows produced by this node
    RETURN_IF_ERROR(Expr::prepare(_other_join_conjunct_ctxs, state, _row_descriptor));

    _result_tuple_row_size = _row_descriptor.tuple_descriptors().size() * sizeof(Tuple*);

    int num_left_tuples = child(0)->row_desc().tuple_descriptors().size();
    int num_build_tuples = child(1)->row_desc().tuple_descriptors().size();
    _probe_tuple_row_size = num_left_tuples * sizeof(Tuple*);
    _build_tuple_row_size = num_build_tuples * sizeof(Tuple*);

    // pre-compute the tuple index of build tuples in the output row
    _build_tuple_size = num_build_tuples;
    _build_tuple_idx.reserve(_build_tuple_size);

    for (int i = 0; i < _build_tuple_size; ++i) {
        TupleDescriptor* build_tuple_desc = child(1)->row_desc().tuple_descriptors()[i];
        auto tuple_idx = _row_descriptor.get_tuple_idx(build_tuple_desc->id());
        RETURN_IF_INVALID_TUPLE_IDX(build_tuple_desc->id(), tuple_idx);
        _build_tuple_idx.push_back(tuple_idx);
    }
    _probe_tuple_row_size = num_left_tuples * sizeof(Tuple*);
    _build_tuple_row_size = num_build_tuples * sizeof(Tuple*);

    // TODO: default buckets
    const bool stores_nulls =
            _join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN ||
            (std::find(_is_null_safe_eq_join.begin(), _is_null_safe_eq_join.end(), true) !=
             _is_null_safe_eq_join.end());
    _hash_tbl.reset(new HashTable(_build_expr_ctxs, _probe_expr_ctxs, _build_tuple_size,
                                  stores_nulls, _is_null_safe_eq_join, id(),
                                  BitUtil::RoundUpToPowerOfTwo(state->batch_size())));

    _probe_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size()));

    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    // Must reset _probe_batch in close() to release resources
    _probe_batch.reset(nullptr);

    if (_hash_tbl.get() != nullptr) {
        _hash_tbl->close();
    }
    if (_build_pool.get() != nullptr) {
        _build_pool->free_all();
    }

    Expr::close(_build_expr_ctxs, state);
    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_other_join_conjunct_ctxs, state);

    return ExecNode::close(state);
}

void HashJoinNode::probe_side_open_thread(RuntimeState* state, std::promise<Status>* status) {
    SCOPED_ATTACH_TASK(state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_shared());
    status->set_value(child(0)->open(state));
}

Status HashJoinNode::construct_hash_table(RuntimeState* state) {
    // Do a full scan of child(1) and store everything in _hash_tbl
    // The hash join node needs to keep in memory all build tuples, including the tuple
    // row ptrs.  The row ptrs are copied into the hash table's internal structure so they
    // don't need to be stored in the _build_pool.
    RowBatch build_batch(child(1)->row_desc(), state->batch_size());
    RETURN_IF_ERROR(child(1)->open(state));

    SCOPED_TIMER(_build_timer);
    Defer defer {[&] {
        COUNTER_SET(_build_rows_counter, _hash_tbl->size());
        COUNTER_SET(_build_buckets_counter, _hash_tbl->num_buckets());
        COUNTER_SET(_hash_tbl_load_factor_counter, _hash_tbl->load_factor());
        auto node = _hash_tbl->minmax_node();
        COUNTER_SET(_hash_table_list_min_size, node.first);
        COUNTER_SET(_hash_table_list_max_size, node.second);
    }};
    while (true) {
        RETURN_IF_CANCELLED(state);
        bool eos = true;
        RETURN_IF_ERROR(child(1)->get_next(state, &build_batch, &eos));
        RETURN_IF_ERROR(process_build_batch(state, &build_batch));
        VLOG_ROW << _hash_tbl->debug_string(true, &child(1)->row_desc());

        build_batch.reset();

        if (eos) {
            break;
        }
    }

    return Status::OK();
}

Status HashJoinNode::open(RuntimeState* state) {
    for (size_t i = 0; i < _runtime_filter_descs.size(); i++) {
        if (auto bf = _runtime_filters[i]->get_bloomfilter()) {
            RETURN_IF_ERROR(bf->init_with_fixed_length());
        }
    }
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_other_join_conjunct_ctxs, state));

    _eos = false;

    // TODO: fix problems with asynchronous cancellation
    // Kick-off the construction of the build-side table in a separate
    // thread, so that the left child can do any initialisation in parallel.
    // Only do this if we can get a thread token.  Otherwise, do this in the
    // main thread
    std::promise<Status> thread_status;
    add_runtime_exec_option("Hash Table Built Asynchronously");
    std::thread(bind(&HashJoinNode::probe_side_open_thread, this, state, &thread_status)).detach();

    if (!_runtime_filter_descs.empty()) {
        RuntimeFilterSlots runtime_filter_slots(_probe_expr_ctxs, _build_expr_ctxs,
                                                _runtime_filter_descs);
        Status st;
        do {
            st = construct_hash_table(state);
            if (UNLIKELY(!st.ok())) {
                break;
            }
            st = runtime_filter_slots.init(state, _hash_tbl->size());
            if (UNLIKELY(!st.ok())) {
                break;
            }
            {
                SCOPED_TIMER(_push_compute_timer);
                auto func = [&](TupleRow* row) { runtime_filter_slots.insert(row); };
                _hash_tbl->for_each_row(func);
            }
            COUNTER_UPDATE(_build_timer, _push_compute_timer->value());
            {
                SCOPED_TIMER(_push_down_timer);
                runtime_filter_slots.publish();
            }
        } while (false);
        VLOG_ROW << "runtime st: " << st;
        // Don't exit even if we see an error, we still need to wait for the probe thread
        // to finish.
        // If this return first, probe thread will use '_await_time_cost'
        // which is already destructor and then coredump.
        RETURN_IF_ERROR(thread_status.get_future().get());
        if (UNLIKELY(!st.ok())) {
            return st;
        }
    } else {
        // Blocks until ConstructHashTable has returned, after which
        // the hash table is fully constructed and we can start the probe
        // phase.
        RETURN_IF_ERROR(thread_status.get_future().get());
        RETURN_IF_ERROR(construct_hash_table(state));
    }

    // seed probe batch and _current_probe_row, etc.
    while (true) {
        RETURN_IF_ERROR(child(0)->get_next(state, _probe_batch.get(), &_probe_eos));
        COUNTER_UPDATE(_probe_rows_counter, _probe_batch->num_rows());
        _probe_batch_pos = 0;

        if (_probe_batch->num_rows() == 0) {
            if (_probe_eos) {
                _hash_tbl_iterator = _hash_tbl->begin();
                _eos = true;
                break;
            }

            _probe_batch->reset();
            continue;
        } else {
            _current_probe_row = _probe_batch->get_row(_probe_batch_pos++);
            VLOG_ROW << "probe row: " << get_probe_row_output_string(_current_probe_row);
            _matched_probe = false;
            _hash_tbl_iterator = _hash_tbl->find(_current_probe_row);
            break;
        }
    }

    return Status::OK();
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* out_batch, bool* eos) {
    RETURN_IF_CANCELLED(state);
    // In most cases, no additional memory overhead will be applied for at this stage,
    // but if the expression calculation in this node needs to apply for additional memory,
    // it may cause the memory to exceed the limit.
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // These cases are simpler and use a more efficient processing loop
    if (!(_match_all_build || _join_op == TJoinOp::RIGHT_SEMI_JOIN ||
          _join_op == TJoinOp::RIGHT_ANTI_JOIN)) {
        if (_eos) {
            *eos = true;
            return Status::OK();
        }

        return left_join_get_next(state, out_batch, eos);
    }

    ExprContext* const* other_conjunct_ctxs = &_other_join_conjunct_ctxs[0];
    int num_other_conjunct_ctxs = _other_join_conjunct_ctxs.size();

    ExprContext* const* conjunct_ctxs = &_conjunct_ctxs[0];
    int num_conjunct_ctxs = _conjunct_ctxs.size();

    // Explicitly manage the timer counter to avoid measuring time in the child
    // GetNext call.
    ScopedTimer<MonotonicStopWatch> probe_timer(_probe_timer);

    while (!_eos) {
        // create output rows as long as:
        // 1) we haven't already created an output row for the probe row and are doing
        //    a semi-join;
        // 2) there are more matching build rows
        VLOG_ROW << "probe row: " << get_probe_row_output_string(_current_probe_row);
        while (_hash_tbl_iterator.has_next()) {
            TupleRow* matched_build_row = _hash_tbl_iterator.get_row();
            VLOG_ROW << "matched_build_row: " << matched_build_row->to_string(child(1)->row_desc());

            if ((_join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN) &&
                _hash_tbl_iterator.matched()) {
                // We have already matched this build row, continue to next match.
                // _hash_tbl_iterator.next<true>();
                _hash_tbl_iterator.next<true>();
                continue;
            }

            int row_idx = out_batch->add_row();
            TupleRow* out_row = out_batch->get_row(row_idx);

            // right anti join
            // 1. find pos in hash table which meets equi-join
            // 2. judge if set matched with other join predicates
            // 3. scans hash table to choose row which is't set matched and meets conjuncts
            if (_join_op == TJoinOp::RIGHT_ANTI_JOIN) {
                create_output_row(out_row, _current_probe_row, matched_build_row);
                if (eval_conjuncts(other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
                    _hash_tbl_iterator.set_matched();
                }
                _hash_tbl_iterator.next<true>();
                continue;
            } else {
                // right semi join
                // 1. find pos in hash table which meets equi-join and set_matched
                // 2. check if the row meets other join predicates
                // 3. check if the row meets conjuncts
                // right join and full join
                // 1. find pos in hash table which meets equi-join
                // 2. check if the row meets other join predicates
                // 3. check if the row meets conjuncts
                // 4. output left and right meeting other predicates and conjuncts
                // 5. if full join, output left meeting and right no meeting other
                // join predicates and conjuncts
                // 6. output left no meeting and right meeting other join predicate
                // and conjuncts
                create_output_row(out_row, _current_probe_row, matched_build_row);
            }

            if (!eval_conjuncts(other_conjunct_ctxs, num_other_conjunct_ctxs, out_row)) {
                _hash_tbl_iterator.next<true>();
                continue;
            }

            if (_join_op == TJoinOp::RIGHT_SEMI_JOIN) {
                _hash_tbl_iterator.set_matched();
            }

            // we have a match for the purpose of the (outer?) join as soon as we
            // satisfy the JOIN clause conjuncts
            _matched_probe = true;

            if (_match_all_build) {
                // remember that we matched this build row
                _joined_build_rows.insert(matched_build_row);
                VLOG_ROW << "joined build row: " << matched_build_row;
            }

            _hash_tbl_iterator.next<true>();
            if (eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
                out_batch->commit_last_row();
                VLOG_ROW << "match row: " << out_row->to_string(row_desc());
                ++_num_rows_returned;
                COUNTER_SET(_rows_returned_counter, _num_rows_returned);

                if (out_batch->is_full() || reached_limit()) {
                    *eos = reached_limit();
                    return Status::OK();
                }
            }
        }

        // check whether we need to output the current probe row before
        // getting a new probe batch
        if (_match_all_probe && !_matched_probe) {
            int row_idx = out_batch->add_row();
            TupleRow* out_row = out_batch->get_row(row_idx);
            create_output_row(out_row, _current_probe_row, nullptr);

            if (eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
                out_batch->commit_last_row();
                VLOG_ROW << "match row: " << out_row->to_string(row_desc());
                ++_num_rows_returned;
                COUNTER_SET(_rows_returned_counter, _num_rows_returned);
                _matched_probe = true;

                if (out_batch->is_full() || reached_limit()) {
                    *eos = reached_limit();
                    return Status::OK();
                }
            }
        }

        if (_probe_batch_pos == _probe_batch->num_rows()) {
            // pass on resources, out_batch might still need them
            _probe_batch->transfer_resource_ownership(out_batch);
            _probe_batch_pos = 0;

            if (out_batch->is_full() || out_batch->at_resource_limit()) {
                return Status::OK();
            }

            // get new probe batch
            if (!_probe_eos) {
                while (true) {
                    probe_timer.stop();
                    RETURN_IF_ERROR(child(0)->get_next(state, _probe_batch.get(), &_probe_eos));
                    probe_timer.start();

                    if (_probe_batch->num_rows() == 0) {
                        // Empty batches can still contain IO buffers, which need to be passed up to
                        // the caller; transferring resources can fill up out_batch.
                        _probe_batch->transfer_resource_ownership(out_batch);

                        if (_probe_eos) {
                            _eos = true;
                            break;
                        }

                        if (out_batch->is_full() || out_batch->at_resource_limit()) {
                            return Status::OK();
                        }

                        continue;
                    } else {
                        COUNTER_UPDATE(_probe_rows_counter, _probe_batch->num_rows());
                        break;
                    }
                }
            } else {
                _eos = true;
            }

            // finish up right outer join
            if (_eos && (_match_all_build || _join_op == TJoinOp::RIGHT_ANTI_JOIN)) {
                _hash_tbl_iterator = _hash_tbl->begin();
            }
        }

        if (_eos) {
            break;
        }

        // join remaining rows in probe _batch
        _current_probe_row = _probe_batch->get_row(_probe_batch_pos++);
        VLOG_ROW << "probe row: " << get_probe_row_output_string(_current_probe_row);
        _matched_probe = false;
        _hash_tbl_iterator = _hash_tbl->find(_current_probe_row);
    }

    *eos = true;
    if (_match_all_build || _join_op == TJoinOp::RIGHT_ANTI_JOIN) {
        // output remaining unmatched build rows
        TupleRow* build_row = nullptr;
        if (_join_op == TJoinOp::RIGHT_ANTI_JOIN) {
            if (_anti_join_last_pos != nullptr) {
                _hash_tbl_iterator = *_anti_join_last_pos;
            } else {
                _hash_tbl_iterator = _hash_tbl->begin();
            }
        }
        while (!out_batch->is_full() && _hash_tbl_iterator.has_next()) {
            build_row = _hash_tbl_iterator.get_row();

            if (_match_all_build) {
                if (_joined_build_rows.find(build_row) != _joined_build_rows.end()) {
                    _hash_tbl_iterator.next<false>();
                    continue;
                }
            } else if (_join_op == TJoinOp::RIGHT_ANTI_JOIN) {
                if (_hash_tbl_iterator.matched()) {
                    _hash_tbl_iterator.next<false>();
                    continue;
                }
            }

            int row_idx = out_batch->add_row();
            TupleRow* out_row = out_batch->get_row(row_idx);
            create_output_row(out_row, nullptr, build_row);
            if (eval_conjuncts(conjunct_ctxs, num_conjunct_ctxs, out_row)) {
                out_batch->commit_last_row();
                VLOG_ROW << "match row: " << out_row->to_string(row_desc());
                ++_num_rows_returned;
                COUNTER_SET(_rows_returned_counter, _num_rows_returned);

                if (reached_limit()) {
                    *eos = true;
                    return Status::OK();
                }
            }
            _hash_tbl_iterator.next<false>();
        }
        if (_join_op == TJoinOp::RIGHT_ANTI_JOIN) {
            _anti_join_last_pos = &_hash_tbl_iterator;
        }
        // we're done if there are no more rows left to check
        *eos = !_hash_tbl_iterator.has_next();
    }

    return Status::OK();
}

Status HashJoinNode::left_join_get_next(RuntimeState* state, RowBatch* out_batch, bool* eos) {
    *eos = _eos;

    ScopedTimer<MonotonicStopWatch> probe_timer(_probe_timer);
    Defer defer {[&] { COUNTER_SET(_rows_returned_counter, _num_rows_returned); }};

    while (!_eos) {
        // Compute max rows that should be added to out_batch
        int64_t max_added_rows = out_batch->capacity() - out_batch->num_rows();

        if (limit() != -1) {
            max_added_rows = std::min(max_added_rows, limit() - rows_returned());
        }

        // Continue processing this row batch
        _num_rows_returned += process_probe_batch(out_batch, _probe_batch.get(), max_added_rows);

        if (reached_limit() || out_batch->is_full()) {
            *eos = reached_limit();
            break;
        }

        // Check to see if we're done processing the current probe batch
        if (!_hash_tbl_iterator.has_next() && _probe_batch_pos == _probe_batch->num_rows()) {
            _probe_batch->transfer_resource_ownership(out_batch);
            _probe_batch_pos = 0;

            if (out_batch->is_full() || out_batch->at_resource_limit()) {
                break;
            }

            if (_probe_eos) {
                *eos = _eos = true;
                break;
            } else {
                probe_timer.stop();
                RETURN_IF_ERROR(child(0)->get_next(state, _probe_batch.get(), &_probe_eos));
                probe_timer.start();
                COUNTER_UPDATE(_probe_rows_counter, _probe_batch->num_rows());
            }
        }
    }

    return Status::OK();
}

std::string HashJoinNode::get_probe_row_output_string(TupleRow* probe_row) {
    std::stringstream out;
    out << "[";
    int* _build_tuple_idx_ptr = &_build_tuple_idx[0];

    for (int i = 0; i < row_desc().tuple_descriptors().size(); ++i) {
        if (i != 0) {
            out << " ";
        }

        int* is_build_tuple =
                std::find(_build_tuple_idx_ptr, _build_tuple_idx_ptr + _build_tuple_size, i);

        if (is_build_tuple != _build_tuple_idx_ptr + _build_tuple_size) {
            out << Tuple::to_string(nullptr, *row_desc().tuple_descriptors()[i]);
        } else {
            out << Tuple::to_string(probe_row->get_tuple(i), *row_desc().tuple_descriptors()[i]);
        }
    }

    out << "]";
    return out.str();
}

void HashJoinNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "_hashJoin(eos=" << (_eos ? "true" : "false") << " probe_batch_pos=" << _probe_batch_pos
         << " hash_tbl=";
    *out << string(indentation_level * 2, ' ');
    *out << "HashTbl(";
    // << " build_exprs=" << Expr::debug_string(_build_expr_ctxs)
    // << " probe_exprs=" << Expr::debug_string(_probe_expr_ctxs);
    *out << ")";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

// This function is replaced by codegen
void HashJoinNode::create_output_row(TupleRow* out, TupleRow* probe, TupleRow* build) {
    uint8_t* out_ptr = reinterpret_cast<uint8_t*>(out);
    if (probe == nullptr) {
        memset(out_ptr, 0, _probe_tuple_row_size);
    } else {
        memcpy(out_ptr, probe, _probe_tuple_row_size);
    }

    if (build == nullptr) {
        memset(out_ptr + _probe_tuple_row_size, 0, _build_tuple_row_size);
    } else {
        memcpy(out_ptr + _probe_tuple_row_size, build, _build_tuple_row_size);
    }
}

// Wrapper around ExecNode's eval conjuncts with a different function name.
// This lets us distinguish between the join conjuncts vs. non-join conjuncts
// for codegen.
// Note: don't declare this static.  LLVM will pick the fastcc calling convention and
// we will not be able to replace the functions with codegen'd versions.
// TODO: explicitly set the calling convention?
// TODO: investigate using fastcc for all codegen internal functions?
bool eval_other_join_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
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
        RETURN_IF_ERROR(_hash_tbl->resize_buckets_ahead(build_batch->num_rows()));
        for (int i = 0; i < build_batch->num_rows(); ++i) {
            _hash_tbl->insert_without_check(build_batch->get_row(i));
        }
    }
    return Status::OK();
}

} // namespace doris
