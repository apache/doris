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

#include "exec/except_node.h"

#include "exec/hash_table.hpp"
#include "exprs/expr.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

namespace doris {
ExceptNode::ExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

Status ExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.except_node);
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    DCHECK_GE(_children.size(), 2);
    // Create result_expr_ctx_lists_ from thrift exprs.
    auto& result_texpr_lists = tnode.except_node.result_expr_lists;
    for (auto& texprs : result_texpr_lists) {
        std::vector<ExprContext*> ctxs;
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }
    return Status::OK();
}

Status ExceptNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _build_pool.reset(new MemPool(mem_tracker()));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    for (size_t i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(Expr::prepare(_child_expr_lists[i], state, child(i)->row_desc(),
                                      expr_mem_tracker()));
    }
    _build_tuple_size = child(0)->row_desc().tuple_descriptors().size();
    _build_tuple_row_size = _build_tuple_size * sizeof(Tuple*);
    _build_tuple_idx.reserve(_build_tuple_size);

    for (int i = 0; i < _build_tuple_size; ++i) {
        TupleDescriptor* build_tuple_desc = child(0)->row_desc().tuple_descriptors()[i];
        _build_tuple_idx.push_back(_row_descriptor.get_tuple_idx(build_tuple_desc->id()));
    }
    _find_nulls = std::vector<bool>(_build_tuple_size, true);
    return Status::OK();
}
Status ExceptNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    for (auto& exprs : _child_expr_lists) {
        Expr::close(exprs, state);
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    // Must reset _probe_batch in close() to release resources
    _probe_batch.reset(NULL);

    if (_memory_used_counter != NULL && _hash_tbl.get() != NULL) {
        COUNTER_UPDATE(_memory_used_counter, _build_pool->peak_allocated_bytes());
        COUNTER_UPDATE(_memory_used_counter, _hash_tbl->byte_size());
    }
    if (_hash_tbl.get() != NULL) {
        _hash_tbl->close();
    }
    if (_build_pool.get() != NULL) {
        _build_pool->free_all();
    }
    return ExecNode::close(state);
}
Status ExceptNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    // open result expr lists.
    for (const vector<ExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(Expr::open(exprs, state));
    }
    // initial build hash table, use _child_expr_lists[0] as probe is used for remove duplicted
    _hash_tbl.reset(new HashTable(_child_expr_lists[0], _child_expr_lists[0], _build_tuple_size,
                                  true, _find_nulls, id(), mem_tracker(), 1024));
    RowBatch build_batch(child(0)->row_desc(), state->batch_size(), mem_tracker());
    RETURN_IF_ERROR(child(0)->open(state));

    bool eos = false;
    while (!eos) {
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next(state, &build_batch, &eos));
        // take ownership of tuple data of build_batch
        _build_pool->acquire_data(build_batch.tuple_data_pool(), false);
        RETURN_IF_LIMIT_EXCEEDED(state, " Except, while constructing the hash table.");
        // build hash table and remvoe duplicate items
        for (int i = 0; i < build_batch.num_rows(); ++i) {
            _hash_tbl->insert_unique(build_batch.get_row(i));
        }
        VLOG_ROW << "hash table content: " << _hash_tbl->debug_string(true, &child(0)->row_desc());
        build_batch.reset();
    }
    // if a table is empty, the result must be empty

    if (_hash_tbl->size() == 0) {
        _hash_tbl_iterator = _hash_tbl->begin();
        return Status::OK();
    }

    for (int i = 1; i < _children.size(); ++i) {
        // rebuid hash table, for first time will rebuild with the no duplicated _hash_tbl,
        if (i > 1) {
            SCOPED_TIMER(_build_timer);
            std::unique_ptr<HashTable> temp_tbl(
                    new HashTable(_child_expr_lists[0], _child_expr_lists[i], _build_tuple_size,
                                  true, _find_nulls, id(), mem_tracker(), 1024));
            _hash_tbl_iterator = _hash_tbl->begin();
            uint32_t previous_hash = -1;
            while (_hash_tbl_iterator.has_next()) {
                if (previous_hash != _hash_tbl_iterator.get_hash()) {
                    previous_hash = _hash_tbl_iterator.get_hash();
                    if (!_hash_tbl_iterator.matched()) {
                        temp_tbl->insert(_hash_tbl_iterator.get_row());
                    }
                }
                _hash_tbl_iterator.next<false>();
            }
            _hash_tbl.swap(temp_tbl);
            temp_tbl->close();
        }
        // probe
        _probe_batch.reset(new RowBatch(child(i)->row_desc(), state->batch_size(), mem_tracker()));
        ScopedTimer<MonotonicStopWatch> probe_timer(_probe_timer);
        RETURN_IF_ERROR(child(i)->open(state));
        eos = false;
        while (!eos) {
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(child(i)->get_next(state, _probe_batch.get(), &eos));
            RETURN_IF_LIMIT_EXCEEDED(state, " Except , while probing the hash table.");
            for (int j = 0; j < _probe_batch->num_rows(); ++j) {
                _hash_tbl_iterator = _hash_tbl->find(_probe_batch->get_row(j));
                if (_hash_tbl_iterator != _hash_tbl->end()) {
                    _hash_tbl_iterator.set_matched();
                }
            }
            _probe_batch->reset();
        }
        // if a table is empty, the result must be empty
        if (_hash_tbl->size() == 0) {
            break;
        }
    }
    _hash_tbl_iterator = _hash_tbl->begin();
    return Status::OK();
}

Status ExceptNode::get_next(RuntimeState* state, RowBatch* out_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    *eos = true;
    if (reached_limit()) {
        return Status::OK();
    }
    uint32_t previous_hash = -1;
    TupleRow* previous_row = nullptr;
    while (_hash_tbl_iterator.has_next()) {
        if (!_hash_tbl_iterator.matched()) {
            if (previous_hash != _hash_tbl_iterator.get_hash() ||
                !equals(previous_row, _hash_tbl_iterator.get_row())) {
                int row_idx = out_batch->add_row();
                TupleRow* out_row = out_batch->get_row(row_idx);
                uint8_t* out_ptr = reinterpret_cast<uint8_t*>(out_row);
                memcpy(out_ptr, _hash_tbl_iterator.get_row(), _build_tuple_row_size);
                out_batch->commit_last_row();
                ++_num_rows_returned;
            }
        }
        previous_hash = _hash_tbl_iterator.get_hash();
        previous_row = _hash_tbl_iterator.get_row();
        _hash_tbl_iterator.next<false>();

        *eos = !_hash_tbl_iterator.has_next() || reached_limit();
        if (out_batch->is_full() || out_batch->at_resource_limit() || *eos) {
            return Status::OK();
        }
    }
    return Status::OK();
}

bool ExceptNode::equals(TupleRow* row, TupleRow* other) {
    DCHECK(!(row == nullptr && other == nullptr));
    if (row == nullptr || other == nullptr) {
        return false;
    }
    for (int i = 0; i < _child_expr_lists[0].size(); ++i) {
        void* val_row = _child_expr_lists[0][i]->get_value(row);
        void* val_other = _child_expr_lists[0][i]->get_value(other);
        if (val_row == nullptr && val_other == nullptr) {
            continue;
        } else if (val_row == nullptr || val_other == nullptr) {
            return false;
        } else if (!RawValue::eq(val_row, val_other, _child_expr_lists[0][i]->root()->type())) {
            return false;
        }
    }

    return true;
}

} // namespace doris
