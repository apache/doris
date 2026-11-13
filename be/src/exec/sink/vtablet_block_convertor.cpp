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

#include "exec/sink/vtablet_block_convertor.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exec/sink/tablet_column_validator.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris {

// !FIXME: Here we should consider using MutableBlock, due to potential data reorganization
Status OlapTableBlockConvertor::validate_and_convert_block(RuntimeState* state, Block* input_block,
                                                           std::shared_ptr<Block>& block,
                                                           VExprContextSPtrs output_vexpr_ctxs,
                                                           size_t rows, bool& has_filtered_rows) {
    DCHECK(input_block->rows() > 0);

    block = Block::create_shared(input_block->get_columns_with_type_and_name());
    if (!output_vexpr_ctxs.empty()) {
        // Do vectorized expr here to speed up load
        RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(
                output_vexpr_ctxs, *input_block, block.get()));
    }

    if (_is_partial_update_and_auto_inc) {
        // If this load is partial update and this table has a auto inc column,
        // e.g. table schema: k1, v1, v2(auto inc)
        // 1. insert columns include auto inc column
        // e.g. insert into table (k1, v2) value(a, 1);
        // we do nothing.
        // 2. insert columns do not include auto inc column
        // e.g. insert into table (k1, v1) value(a, a);
        // we need to fill auto_inc_cols by creating a new column.
        if (!_auto_inc_col_idx.has_value()) {
            RETURN_IF_ERROR(_partial_update_fill_auto_inc_cols(block.get(), rows));
        }
    } else if (_auto_inc_col_idx.has_value()) {
        // fill the valus for auto-increment columns
        DCHECK_EQ(_is_partial_update_and_auto_inc, false);
        RETURN_IF_ERROR(_fill_auto_inc_cols(block.get(), rows));
    }

    int filtered_rows = 0;
    {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_map.clear();
        _filter_map.resize(rows, 0);
        _tablet_validate_context.state = state;
        auto st = _validate_data(block.get(), rows, filtered_rows);
        _num_filtered_rows += filtered_rows;
        has_filtered_rows = filtered_rows > 0;
        if (!st.ok()) {
            return st;
        }
        _convert_to_dest_desc_block(block.get());
    }

    return Status::OK();
}

void OlapTableBlockConvertor::init_autoinc_info(int64_t db_id, int64_t table_id, int batch_size,
                                                bool is_partial_update_and_auto_inc,
                                                int32_t auto_increment_column_unique_id) {
    _batch_size = batch_size;
    if (is_partial_update_and_auto_inc) {
        _is_partial_update_and_auto_inc = is_partial_update_and_auto_inc;
        _auto_inc_id_buffer = GlobalAutoIncBuffers::GetInstance()->get_auto_inc_buffer(
                db_id, table_id, auto_increment_column_unique_id);
        return;
    }
    for (size_t idx = 0; idx < _output_tuple_desc->slots().size(); idx++) {
        if (_output_tuple_desc->slots()[idx]->is_auto_increment()) {
            _auto_inc_col_idx = idx;
            _auto_inc_id_buffer = GlobalAutoIncBuffers::GetInstance()->get_auto_inc_buffer(
                    db_id, table_id, _output_tuple_desc->slots()[idx]->col_unique_id());
            _auto_inc_id_buffer->set_batch_size_at_least(_batch_size);
            break;
        }
    }
}

Status OlapTableBlockConvertor::_validate_data(Block* block, const size_t rows,
                                               int& filtered_rows) {
    filtered_rows = 0;
    Defer defer {[&] {
        for (size_t i = 0; i < rows; ++i) {
            filtered_rows += _filter_map[i];
        }
    }};
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        // VRowDistribution's _deal_missing_map() relies on these slots being full columns.
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        auto& column = block->get_by_position(i).column;

        DCHECK_EQ(column->size(), rows);
        _tablet_validate_context.error_prefix.clear();
        fmt::format_to(_tablet_validate_context.error_prefix, "column_name[{}], ",
                       desc->col_name());
        column = DORIS_TRY(tablet_validate_column(column, desc->type(), _tablet_validate_context));
    }
    return Status::OK();
}

void OlapTableBlockConvertor::_convert_to_dest_desc_block(doris::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size() && i < block->columns(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        if (desc->is_nullable() != block->get_by_position(i).type->is_nullable()) {
            if (desc->is_nullable()) {
                block->get_by_position(i).type = make_nullable(block->get_by_position(i).type);
                block->get_by_position(i).column = make_nullable(block->get_by_position(i).column);
            } else {
                block->get_by_position(i).type =
                        assert_cast<const DataTypeNullable&>(*block->get_by_position(i).type)
                                .get_nested_type();
                block->get_by_position(i).column =
                        assert_cast<const ColumnNullable&>(*block->get_by_position(i).column)
                                .get_nested_column_ptr();
            }
        }
    }
}

Status OlapTableBlockConvertor::_fill_auto_inc_cols(Block* block, size_t rows) {
    size_t idx = _auto_inc_col_idx.value();
    SlotDescriptor* slot = _output_tuple_desc->slots()[idx];
    DCHECK(slot->type()->get_primitive_type() == PrimitiveType::TYPE_BIGINT);
    DCHECK(!slot->is_nullable());

    size_t null_value_count = 0;
    auto dst_column = ColumnInt64::create();
    ColumnInt64::Container& dst_values = dst_column->get_data();

    ColumnPtr src_column_ptr = block->get_by_position(idx).column;
    if (const auto* const_column = check_and_get_column<ColumnConst>(src_column_ptr.get())) {
        // for insert stmt like "insert into tbl1 select null,col1,col2,... from tbl2" or
        // "insert into tbl1 select 1,col1,col2,... from tbl2", the type of literal's column
        // will be `ColumnConst`
        if (const_column->is_null_at(0)) {
            // the input of autoinc column are all null literals
            // fill the column with generated ids
            null_value_count = rows;
            std::vector<std::pair<int64_t, size_t>> res;
            RETURN_IF_ERROR(_auto_inc_id_buffer->sync_request_ids(null_value_count, &res));
            for (auto [start, length] : res) {
                _auto_inc_id_allocator.insert_ids(start, length);
            }

            for (size_t i = 0; i < rows; i++) {
                dst_values.emplace_back(_auto_inc_id_allocator.next_id());
            }
        } else {
            // the input of autoinc column are all int64 literals
            // fill the column with that literal
            int64_t value = const_column->get_int(0);
            dst_values.resize_fill(rows, value);
        }
    } else if (const auto* src_nullable_column =
                       check_and_get_column<ColumnNullable>(src_column_ptr.get())) {
        auto src_nested_column_ptr = src_nullable_column->get_nested_column_ptr();
        const auto& null_map_data = src_nullable_column->get_null_map_data();
        dst_values.reserve(rows);
        for (size_t i = 0; i < rows; i++) {
            null_value_count += null_map_data[i];
        }
        std::vector<std::pair<int64_t, size_t>> res;
        RETURN_IF_ERROR(_auto_inc_id_buffer->sync_request_ids(null_value_count, &res));
        for (auto [start, length] : res) {
            _auto_inc_id_allocator.insert_ids(start, length);
        }

        for (size_t i = 0; i < rows; i++) {
            dst_values.emplace_back((null_map_data[i] != 0) ? _auto_inc_id_allocator.next_id()
                                                            : src_nested_column_ptr->get_int(i));
        }
    } else {
        return Status::OK();
    }
    block->get_by_position(idx).column = std::move(dst_column);
    block->get_by_position(idx).type = remove_nullable(slot->type());
    return Status::OK();
}

Status OlapTableBlockConvertor::_partial_update_fill_auto_inc_cols(Block* block, size_t rows) {
    auto dst_column = ColumnInt64::create();
    ColumnInt64::Container& dst_values = dst_column->get_data();
    size_t null_value_count = rows;
    std::vector<std::pair<int64_t, size_t>> res;
    RETURN_IF_ERROR(_auto_inc_id_buffer->sync_request_ids(null_value_count, &res));
    for (auto [start, length] : res) {
        _auto_inc_id_allocator.insert_ids(start, length);
    }

    for (size_t i = 0; i < rows; i++) {
        dst_values.emplace_back(_auto_inc_id_allocator.next_id());
    }
    block->insert(ColumnWithTypeAndName(std::move(dst_column), std::make_shared<DataTypeInt64>(),
                                        BeConsts::PARTIAL_UPDATE_AUTO_INC_COL));
    return Status::OK();
}

} // namespace doris
