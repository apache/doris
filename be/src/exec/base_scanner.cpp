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

#include "base_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "common/utils.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"

namespace doris {

BaseScanner::BaseScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : _state(state),
          _params(params),
          _counter(counter),
          _src_tuple(nullptr),
          _src_tuple_row(nullptr),
#if BE_TEST
          _mem_tracker(new MemTracker()),
#else
          _mem_tracker(MemTracker::create_tracker(
                  -1, state->query_type() == TQueryType::LOAD
                              ? "BaseScanner:" + std::to_string(state->load_job_id())
                              : "BaseScanner:Select")),
#endif
          _mem_pool(std::make_unique<MemPool>(_mem_tracker.get())),
          _dest_tuple_desc(nullptr),
          _pre_filter_texprs(pre_filter_texprs),
          _strict_mode(false),
          _line_counter(0),
          _profile(profile),
          _rows_read_counter(nullptr),
          _read_timer(nullptr),
          _materialize_timer(nullptr),
          _success(false),
          _scanner_eof(false) {
}

Status BaseScanner::open() {
    RETURN_IF_ERROR(init_expr_ctxes());
    if (_params.__isset.strict_mode) {
        _strict_mode = _params.strict_mode;
    }
    if (_strict_mode && !_params.__isset.dest_sid_to_src_sid_without_trans) {
        return Status::InternalError("Slot map of dest to src must be set in strict mode");
    }
    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_profile, "MaterializeTupleTime(*)");
    return Status::OK();
}

Status BaseScanner::init_expr_ctxes() {
    // Construct _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
            _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status::InternalError(ss.str());
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (auto slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status::InternalError(ss.str());
        }
        _src_slot_descs.emplace_back(it->second);
    }
    // Construct source tuple and tuple row
    _src_tuple = (Tuple*)_mem_pool->allocate(src_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*)_mem_pool->allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
    _row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    // preceding filter expr should be initialized by using `_row_desc`, which is the source row descriptor
    if (!_pre_filter_texprs.empty()) {
        if (_state->enable_vectorized_exec()) {
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                    _state->obj_pool(), _pre_filter_texprs, &_vpre_filter_ctxs));
            RETURN_IF_ERROR(vectorized::VExpr::prepare(_vpre_filter_ctxs, _state, *_row_desc,
                                                       _mem_tracker));
            RETURN_IF_ERROR(vectorized::VExpr::open(_vpre_filter_ctxs, _state));
        } else {
            RETURN_IF_ERROR(Expr::create_expr_trees(_state->obj_pool(), _pre_filter_texprs,
                                                    &_pre_filter_ctxs));
            RETURN_IF_ERROR(Expr::prepare(_pre_filter_ctxs, _state, *_row_desc, _mem_tracker));
            RETURN_IF_ERROR(Expr::open(_pre_filter_ctxs, _state));
        }
    }

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown dest tuple descriptor, tuple_id=" << _params.dest_tuple_id;
        return Status::InternalError(ss.str());
    }

    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            std::stringstream ss;
            ss << "No expr for dest slot, id=" << slot_desc->id()
               << ", name=" << slot_desc->col_name();
            return Status::InternalError(ss.str());
        }

        if (_state->enable_vectorized_exec()) {
            vectorized::VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
            RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker));
            RETURN_IF_ERROR(ctx->open(_state));
            _dest_vexpr_ctx.emplace_back(ctx);
        } else {
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
            RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker));
            RETURN_IF_ERROR(ctx->open(_state));
            _dest_expr_ctx.emplace_back(ctx);
        }
        if (has_slot_id_map) {
            auto it = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
            if (it == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _src_slot_descs_order_by_dest.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                    std::stringstream ss;
                    ss << "No src slot " << it->second << " in src slot descs";
                    return Status::InternalError(ss.str());
                }
                _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
            }
        }
    }
    return Status::OK();
}

Status BaseScanner::fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool, bool* fill_tuple) {
    RETURN_IF_ERROR(_fill_dest_tuple(dest_tuple, mem_pool));
    if (_success) {
        free_expr_local_allocations();
        *fill_tuple = true;
    } else {
        *fill_tuple = false;
    }
    return Status::OK();
}

Status BaseScanner::_fill_dest_tuple(Tuple* dest_tuple, MemPool* mem_pool) {
    // filter src tuple by preceding filter first
    if (!ExecNode::eval_conjuncts(&_pre_filter_ctxs[0], _pre_filter_ctxs.size(), _src_tuple_row)) {
        _counter->num_rows_unselected++;
        _success = false;
        return Status::OK();
    }

    // convert and fill dest tuple
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        ExprContext* ctx = _dest_expr_ctx[dest_index];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            // Only when the expr return value is null, we will check the error message.
            std::string expr_error = ctx->get_error_msg();
            if (!expr_error.empty()) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string { return expr_error; }, &_scanner_eof));
                _counter->num_rows_filtered++;
                // The ctx is reused, so must clear the error state and message.
                ctx->clear_error_msg();
                _success = false;
                return Status::OK();
            }
            // If _strict_mode is false, _src_slot_descs_order_by_dest size could be zero
            if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index] != nullptr) &&
                !_src_tuple->is_null(
                        _src_slot_descs_order_by_dest[dest_index]->null_indicator_offset())) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string {
                            // Type of the slot is must be Varchar in _src_tuple.
                            StringValue* raw_value = _src_tuple->get_string_slot(
                                    _src_slot_descs_order_by_dest[dest_index]->tuple_offset());
                            std::string raw_string;
                            if (raw_value != nullptr) { //is not null then get raw value
                                raw_string = raw_value->to_string();
                            }
                            fmt::memory_buffer error_msg;
                            fmt::format_to(error_msg,
                                           "column({}) value is incorrect while strict mode is {}, "
                                           "src value is {}",
                                           slot_desc->col_name(), _strict_mode, raw_string);
                            return fmt::to_string(error_msg);
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }
            if (!slot_desc->is_nullable()) {
                RETURN_IF_ERROR(_state->append_error_msg_to_file(
                        [&]() -> std::string {
                            return _src_tuple_row->to_string(*(_row_desc.get()));
                        },
                        [&]() -> std::string {
                            fmt::memory_buffer error_msg;
                            fmt::format_to(
                                    error_msg,
                                    "column({}) values is null while columns is not nullable",
                                    slot_desc->col_name());
                            return fmt::to_string(error_msg);
                        },
                        &_scanner_eof));
                _counter->num_rows_filtered++;
                _success = false;
                return Status::OK();
            }
            dest_tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        if (slot_desc->is_nullable()) {
            dest_tuple->set_not_null(slot_desc->null_indicator_offset());
        }
        void* slot = dest_tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), mem_pool);
        continue;
    }
    _success = true;
    return Status::OK();
}

Status BaseScanner::filter_block(vectorized::Block* temp_block, size_t slot_num) {
    // filter block
    if (!_vpre_filter_ctxs.empty()) {
        for (auto _vpre_filter_ctx : _vpre_filter_ctxs) {
            auto old_rows = temp_block->rows();
            RETURN_IF_ERROR(
                    vectorized::VExprContext::filter_block(_vpre_filter_ctx, temp_block, slot_num));
            _counter->num_rows_unselected += old_rows - temp_block->rows();
        }
    }
    return Status::OK();
}

Status BaseScanner::execute_exprs(vectorized::Block* output_block, vectorized::Block* temp_block) {
    // Do vectorized expr here
    Status status;
    if (!_dest_vexpr_ctx.empty()) {
        *output_block = vectorized::VExprContext::get_output_block_after_execute_exprs(
                _dest_vexpr_ctx, *temp_block, status);
        if (UNLIKELY(output_block->rows() == 0)) {
            return status;
        }
    }

    return Status::OK();
}

Status BaseScanner::fill_dest_block(vectorized::Block* dest_block,
                                    std::vector<vectorized::MutableColumnPtr>& columns) {
    if (columns.empty() || columns[0]->size() == 0) {
        return Status::OK();
    }

    std::unique_ptr<vectorized::Block> temp_block(new vectorized::Block());
    auto n_columns = 0;
    for (const auto slot_desc : _src_slot_descs) {
        temp_block->insert(vectorized::ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                             slot_desc->get_data_type_ptr(),
                                                             slot_desc->col_name()));
    }

    RETURN_IF_ERROR(BaseScanner::filter_block(temp_block.get(), _dest_tuple_desc->slots().size()));

    if (_dest_vexpr_ctx.empty()) {
        *dest_block = *temp_block;
    } else {
        RETURN_IF_ERROR(BaseScanner::execute_exprs(dest_block, temp_block.get()));
    }

    return Status::OK();
}

void BaseScanner::fill_slots_of_columns_from_path(
        int start, const std::vector<std::string>& columns_from_path) {
    // values of columns from path can not be null
    for (int i = 0; i < columns_from_path.size(); ++i) {
        auto slot_desc = _src_slot_descs.at(i + start);
        _src_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = _src_tuple->get_slot(slot_desc->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string& column_from_path = columns_from_path[i];
        str_slot->ptr = const_cast<char*>(column_from_path.c_str());
        str_slot->len = column_from_path.size();
    }
}

void BaseScanner::free_expr_local_allocations() {
    if (++_line_counter % RELEASE_CONTEXT_COUNTER == 0) {
        ExprContext::free_local_allocations(_dest_expr_ctx);
    }
}

void BaseScanner::close() {
    if (!_pre_filter_ctxs.empty()) {
        Expr::close(_pre_filter_ctxs, _state);
    }

    if (_state->enable_vectorized_exec() && !_vpre_filter_ctxs.empty()) {
        vectorized::VExpr::close(_vpre_filter_ctxs, _state);
    }
}

} // namespace doris
