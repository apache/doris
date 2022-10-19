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

#include "common/utils.h"
#include "exec/exec_node.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

BaseScanner::BaseScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : _state(state),
          _params(params),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          _next_range(0),
          _counter(counter),
          _src_tuple(nullptr),
          _src_tuple_row(nullptr),
          _mem_pool(std::make_unique<MemPool>()),
          _dest_tuple_desc(nullptr),
          _pre_filter_texprs(pre_filter_texprs),
          _strict_mode(false),
          _line_counter(0),
          _profile(profile),
          _rows_read_counter(nullptr),
          _read_timer(nullptr),
          _materialize_timer(nullptr),
          _success(false),
          _scanner_eof(false) {}

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

    DCHECK(!_ranges.empty());
    const auto& range = _ranges[0];
    _num_of_columns_from_file = range.__isset.num_of_columns_from_file
                                        ? implicit_cast<int>(range.num_of_columns_from_file)
                                        : implicit_cast<int>(_src_slot_descs.size());

    // check consistency
    if (range.__isset.num_of_columns_from_file) {
        int size = range.columns_from_path.size();
        for (const auto& r : _ranges) {
            if (r.columns_from_path.size() != size) {
                return Status::InternalError("ranges have different number of columns.");
            }
        }
    }
    return Status::OK();
}

void BaseScanner::reg_conjunct_ctxs(const TupleId& tupleId,
                                    const std::vector<ExprContext*>& conjunct_ctxs) {
    _conjunct_ctxs = conjunct_ctxs;
    _tupleId = tupleId;
}

Status BaseScanner::init_expr_ctxes() {
    // Construct _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
            _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        return Status::InternalError("Unknown source tuple descriptor, tuple_id={}",
                                     _params.src_tuple_id);
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    std::unordered_map<SlotDescriptor*, int> src_slot_desc_to_index {};
    for (int i = 0, len = src_tuple_desc->slots().size(); i < len; ++i) {
        auto* slot_desc = src_tuple_desc->slots()[i];
        src_slot_desc_to_index.emplace(slot_desc, i);
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}", slot_id);
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
            // for vectorized, preceding filter exprs should be compounded to one passed from fe.
            DCHECK(_pre_filter_texprs.size() == 1);
            _vpre_filter_ctx_ptr.reset(new doris::vectorized::VExprContext*);
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                    _state->obj_pool(), _pre_filter_texprs[0], _vpre_filter_ctx_ptr.get()));
            RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->prepare(_state, *_row_desc));
            RETURN_IF_ERROR((*_vpre_filter_ctx_ptr)->open(_state));
        } else {
            RETURN_IF_ERROR(Expr::create_expr_trees(_state->obj_pool(), _pre_filter_texprs,
                                                    &_pre_filter_ctxs));
            RETURN_IF_ERROR(Expr::prepare(_pre_filter_ctxs, _state, *_row_desc));
            RETURN_IF_ERROR(Expr::open(_pre_filter_ctxs, _state));
        }
    }

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        return Status::InternalError("Unknown dest tuple descriptor, tuple_id={}",
                                     _params.dest_tuple_id);
    }

    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            return Status::InternalError("No expr for dest slot, id={}, name={}", slot_desc->id(),
                                         slot_desc->col_name());
        }

        if (_state->enable_vectorized_exec()) {
            vectorized::VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
            RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get()));
            RETURN_IF_ERROR(ctx->open(_state));
            _dest_vexpr_ctx.emplace_back(ctx);
        } else {
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
            RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get()));
            RETURN_IF_ERROR(ctx->open(_state));
            _dest_expr_ctx.emplace_back(ctx);
        }
        if (has_slot_id_map) {
            auto it1 = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
            if (it1 == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _src_slot_descs_order_by_dest.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it1->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                    return Status::InternalError("No src slot {} in src slot descs", it1->second);
                }
                _dest_slot_to_src_slot_index.emplace(_src_slot_descs_order_by_dest.size(),
                                                     src_slot_desc_to_index[_src_slot_it->second]);
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
    }
    _success = true;
    return Status::OK();
}

Status BaseScanner::_filter_src_block() {
    auto origin_column_num = _src_block.columns();
    // filter block
    auto old_rows = _src_block.rows();
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_vpre_filter_ctx_ptr, &_src_block,
                                                           origin_column_num));
    _counter->num_rows_unselected += old_rows - _src_block.rows();
    return Status::OK();
}

Status BaseScanner::_materialize_dest_block(vectorized::Block* dest_block) {
    // Do vectorized expr here
    int ctx_idx = 0;
    size_t rows = _src_block.rows();
    auto filter_column = vectorized::ColumnUInt8::create(rows, 1);
    auto& filter_map = filter_column->get_data();
    auto origin_column_num = _src_block.columns();

    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx++;

        auto* ctx = _dest_vexpr_ctx[dest_index];
        int result_column_id = -1;
        // PT1 => dest primitive type
        RETURN_IF_ERROR(ctx->execute(&_src_block, &result_column_id));
        bool is_origin_column = result_column_id < origin_column_num;
        auto column_ptr =
                is_origin_column && _src_block_mem_reuse
                        ? _src_block.get_by_position(result_column_id).column->clone_resized(rows)
                        : _src_block.get_by_position(result_column_id).column;

        DCHECK(column_ptr != nullptr);

        // because of src_slot_desc is always be nullable, so the column_ptr after do dest_expr
        // is likely to be nullable
        if (LIKELY(column_ptr->is_nullable())) {
            auto nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            for (int i = 0; i < rows; ++i) {
                if (filter_map[i] && nullable_column->is_null_at(i)) {
                    if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index]) &&
                        !_src_block.get_by_position(_dest_slot_to_src_slot_index[dest_index])
                                 .column->is_null_at(i)) {
                        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                                [&]() -> std::string {
                                    return _src_block.dump_one_line(i, _num_of_columns_from_file);
                                },
                                [&]() -> std::string {
                                    auto raw_value =
                                            _src_block.get_by_position(ctx_idx).column->get_data_at(
                                                    i);
                                    std::string raw_string = raw_value.to_string();
                                    fmt::memory_buffer error_msg;
                                    fmt::format_to(error_msg,
                                                   "column({}) value is incorrect while strict "
                                                   "mode is {}, "
                                                   "src value is {}",
                                                   slot_desc->col_name(), _strict_mode, raw_string);
                                    return fmt::to_string(error_msg);
                                },
                                &_scanner_eof));
                        filter_map[i] = false;
                    } else if (!slot_desc->is_nullable()) {
                        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                                [&]() -> std::string {
                                    return _src_block.dump_one_line(i, _num_of_columns_from_file);
                                },
                                [&]() -> std::string {
                                    fmt::memory_buffer error_msg;
                                    fmt::format_to(error_msg,
                                                   "column({}) values is null while columns is not "
                                                   "nullable",
                                                   slot_desc->col_name());
                                    return fmt::to_string(error_msg);
                                },
                                &_scanner_eof));
                        filter_map[i] = false;
                    }
                }
            }
            if (!slot_desc->is_nullable()) column_ptr = nullable_column->get_nested_column_ptr();
        } else if (slot_desc->is_nullable()) {
            column_ptr = vectorized::make_nullable(column_ptr);
        }
        dest_block->insert(vectorized::ColumnWithTypeAndName(
                std::move(column_ptr), slot_desc->get_data_type_ptr(), slot_desc->col_name()));
    }

    // after do the dest block insert operation, clear _src_block to remove the reference of origin column
    if (_src_block_mem_reuse) {
        _src_block.clear_column_data(origin_column_num);
    } else {
        _src_block.clear();
    }

    size_t dest_size = dest_block->columns();
    // do filter
    dest_block->insert(vectorized::ColumnWithTypeAndName(
            std::move(filter_column), std::make_shared<vectorized::DataTypeUInt8>(),
            "filter column"));
    RETURN_IF_ERROR(vectorized::Block::filter_block(dest_block, dest_size, dest_size));
    _counter->num_rows_filtered += rows - dest_block->rows();

    return Status::OK();
}

// TODO: opt the reuse of src_block or dest_block column. some case we have to
// shallow copy the column of src_block to dest block
Status BaseScanner::_init_src_block() {
    if (_src_block.is_empty_column()) {
        for (auto i = 0; i < _num_of_columns_from_file; ++i) {
            SlotDescriptor* slot_desc = _src_slot_descs[i];
            if (slot_desc == nullptr) {
                continue;
            }
            auto data_type = slot_desc->get_data_type_ptr();
            auto column_ptr = data_type->create_column();
            column_ptr->reserve(_state->batch_size());
            _src_block.insert(vectorized::ColumnWithTypeAndName(std::move(column_ptr), data_type,
                                                                slot_desc->col_name()));
        }
    }

    return Status::OK();
}

Status BaseScanner::_fill_dest_block(vectorized::Block* dest_block, bool* eof) {
    *eof = _scanner_eof;
    _fill_columns_from_path();
    if (LIKELY(_src_block.rows() > 0)) {
        RETURN_IF_ERROR(BaseScanner::_filter_src_block());
        RETURN_IF_ERROR(BaseScanner::_materialize_dest_block(dest_block));
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
        auto* str_slot = reinterpret_cast<StringValue*>(slot);
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

    if (_vpre_filter_ctx_ptr) {
        (*_vpre_filter_ctx_ptr)->close(_state);
    }
}

void BaseScanner::_fill_columns_from_path() {
    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.num_of_columns_from_file) {
        size_t start = range.num_of_columns_from_file;
        size_t rows = _src_block.rows();

        for (size_t i = 0; i < range.columns_from_path.size(); ++i) {
            auto slot_desc = _src_slot_descs.at(i + start);
            if (slot_desc == nullptr) continue;
            auto is_nullable = slot_desc->is_nullable();
            auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_VARCHAR,
                                                                                      is_nullable);
            auto data_column = data_type->create_column();
            const std::string& column_from_path = range.columns_from_path[i];
            for (size_t j = 0; j < rows; ++j) {
                data_column->insert_data(const_cast<char*>(column_from_path.c_str()),
                                         column_from_path.size());
            }
            _src_block.insert(vectorized::ColumnWithTypeAndName(std::move(data_column), data_type,
                                                                slot_desc->col_name()));
        }
    }
}

bool BaseScanner::is_null(const Slice& slice) {
    return slice.size == 2 && slice.data[0] == '\\' && slice.data[1] == 'N';
}

bool BaseScanner::is_array(const Slice& slice) {
    return slice.size > 1 && slice.data[0] == '[' && slice.data[slice.size - 1] == ']';
}

bool BaseScanner::check_array_format(std::vector<Slice>& split_values) {
    // if not the array format, filter this line and return error url
    auto dest_slot_descs = _dest_tuple_desc->slots();
    for (int j = 0; j < split_values.size() && j < dest_slot_descs.size(); ++j) {
        auto dest_slot_desc = dest_slot_descs[j];
        if (!dest_slot_desc->is_materialized()) {
            continue;
        }
        const Slice& value = split_values[j];
        if (dest_slot_desc->type().is_array_type() && !is_null(value) && !is_array(value)) {
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string { return std::string(value.data, value.size); },
                    [&]() -> std::string {
                        fmt::memory_buffer err_msg;
                        fmt::format_to(err_msg, "Invalid format for array column({})",
                                       dest_slot_desc->col_name());
                        return fmt::to_string(err_msg);
                    },
                    &_scanner_eof));
            _counter->num_rows_filtered++;
            return false;
        }
    }
    return true;
}

} // namespace doris
