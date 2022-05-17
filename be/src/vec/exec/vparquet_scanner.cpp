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

#include "vec/exec/vparquet_scanner.h"

#include "exec/parquet_reader.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris::vectorized {

VParquetScanner::VParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                                 const TBrokerScanRangeParams& params,
                                 const std::vector<TBrokerRangeDesc>& ranges,
                                 const std::vector<TNetworkAddress>& broker_addresses,
                                 const std::vector<TExpr>& pre_filter_texprs,
                                 ScannerCounter* counter)
        : ParquetScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs,
                         counter),
          _batch(nullptr),
          _arrow_batch_cur_idx(0),
          _num_of_columns_from_file(0) {}
VParquetScanner::~VParquetScanner() {}

Status VParquetScanner::open() {
    RETURN_IF_ERROR(ParquetScanner::open());
    if (_ranges.empty()) {
        return Status::OK();
    }
    auto range = _ranges[0];
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

// get next available arrow batch
Status VParquetScanner::_next_arrow_batch() {
    _arrow_batch_cur_idx = 0;
    // first, init file reader
    if (_cur_file_reader == nullptr || _cur_file_eof) {
        RETURN_IF_ERROR(open_next_reader());
        _cur_file_eof = false;
    }
    // second, loop until find available arrow batch or EOF
    while (!_scanner_eof) {
        RETURN_IF_ERROR(_cur_file_reader->next_batch(&_batch, _src_slot_descs, &_cur_file_eof));
        if (_cur_file_eof) {
            RETURN_IF_ERROR(open_next_reader());
            _cur_file_eof = false;
            continue;
        }
        if (_batch->num_rows() == 0) {
            continue;
        }
        return Status::OK();
    }
    return Status::EndOfFile("EOF");
}

Status VParquetScanner::_init_arrow_batch_if_necessary() {
    // 1. init batch if first time
    // 2. reset reader if end of file
    Status status;
    if (_scanner_eof) {
        return Status::EndOfFile("EOF");
    }
    if (_batch == nullptr || _arrow_batch_cur_idx >= _batch->num_rows()) {
        return _next_arrow_batch();
    }
    return status;
}

Status VParquetScanner::_init_src_block(Block* block) {
    size_t batch_pos = 0;
    block->clear();
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descs[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _batch->column(batch_pos++).get();
        // let src column be nullable for simplify converting
        // TODO, support not nullable for exec efficiently
        auto is_nullable = true;
        DataTypePtr data_type =
                DataTypeFactory::instance().create_data_type(array->type()->id(), is_nullable);
        if (data_type == nullptr) {
            return Status::NotSupported(
                    fmt::format("Not support arrow type:{}", array->type()->name()));
        }
        MutableColumnPtr data_column = data_type->create_column();
        block->insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    return Status::OK();
}

Status VParquetScanner::get_next(vectorized::Block* block, bool* eof) {
    // overall of type converting:
    // arrow type ==arrow_column_to_doris_column==> primitive type(PT0) ==cast_src_block==>
    // primitive type(PT1) ==materialize_block==> dest primitive type

    // first, we need to convert the arrow type to the corresponding internal type,
    // such as arrow::INT16 to TYPE_SMALLINT(PT0).
    // why need first step? we cannot convert the arrow type to type in src desc directly,
    // it's too hard to achieve.

    // second, convert PT0 to the type in src desc, such as TYPE_SMALLINT to TYPE_VARCHAR.(PT1)
    // why need second step? the materialize step only accepts types specified in src desc.

    // finally, through the materialized, convert to the type in dest desc, such as TYPE_DATETIME.
    SCOPED_TIMER(_read_timer);
    // init arrow batch
    {
        Status st = _init_arrow_batch_if_necessary();
        if (!st.ok()) {
            if (!st.is_end_of_file()) {
                return st;
            }
            *eof = true;
            return Status::OK();
        }
    }
    Block src_block;
    RETURN_IF_ERROR(_init_src_block(&src_block));
    // convert arrow batch to block until reach the batch_size
    while (!_scanner_eof) {
        // cast arrow type to PT0 and append it to src block
        // for example: arrow::Type::INT16 => TYPE_SMALLINT
        RETURN_IF_ERROR(_append_batch_to_src_block(&src_block));
        // finalize the src block if full
        if (src_block.rows() >= _state->batch_size()) {
            break;
        }
        auto status = _next_arrow_batch();
        // if ok, append the batch to the src columns
        if (status.ok()) {
            continue;
        }
        // return error if not EOF
        if (!status.is_end_of_file()) {
            return status;
        }
        _cur_file_eof = true;
        break;
    }
    COUNTER_UPDATE(_rows_read_counter, src_block.rows());
    SCOPED_TIMER(_materialize_timer);
    // cast PT0 => PT1
    // for example: TYPE_SMALLINT => TYPE_VARCHAR
    RETURN_IF_ERROR(_cast_src_block(&src_block));
    // range of current file
    _fill_columns_from_path(&src_block);
    RETURN_IF_ERROR(_eval_conjunts(&src_block));
    // materialize, src block => dest columns
    RETURN_IF_ERROR(_materialize_block(&src_block, block));
    *eof = _scanner_eof;
    return Status::OK();
}

// eval conjuncts, for example: t1 > 1
Status VParquetScanner::_eval_conjunts(Block* block) {
    for (auto& vctx : _vpre_filter_ctxs) {
        size_t orig_rows = block->rows();
        RETURN_IF_ERROR(VExprContext::filter_block(vctx, block, block->columns()));
        _counter->num_rows_unselected += orig_rows - block->rows();
    }
    return Status::OK();
}

void VParquetScanner::_fill_columns_from_path(Block* block) {
    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.num_of_columns_from_file) {
        size_t start = range.num_of_columns_from_file;
        size_t rows = block->rows();
        for (size_t i = 0; i < range.columns_from_path.size(); ++i) {
            auto slot_desc = _src_slot_descs.at(i + start);
            if (slot_desc == nullptr) continue;
            auto is_nullable = slot_desc->is_nullable();
            DataTypePtr data_type =
                    DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, is_nullable);
            MutableColumnPtr data_column = data_type->create_column();
            const std::string& column_from_path = range.columns_from_path[i];
            for (size_t i = 0; i < rows; ++i) {
                data_column->insert_data(const_cast<char*>(column_from_path.c_str()),
                                         column_from_path.size());
            }
            block->insert(ColumnWithTypeAndName(std::move(data_column), data_type,
                                                slot_desc->col_name()));
        }
    }
}

Status VParquetScanner::_materialize_block(Block* block, Block* dest_block) {
    int ctx_idx = 0;
    size_t orig_rows = block->rows();
    auto filter_column = ColumnUInt8::create(orig_rows, 1);
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx++;

        VExprContext* ctx = _dest_vexpr_ctx[dest_index];
        int result_column_id = 0;
        // PT1 => dest primitive type
        RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
        ColumnPtr& ptr = block->safe_get_by_position(result_column_id).column;
        if (!slot_desc->is_nullable()) {
            if (auto* nullable_column = check_and_get_column<ColumnNullable>(*ptr)) {
                if (nullable_column->has_null()) {
                    // fill filter if src has null value and dest column is not nullable
                    IColumn::Filter& filter = assert_cast<ColumnUInt8&>(*filter_column).get_data();
                    const ColumnPtr& null_column_ptr = nullable_column->get_null_map_column_ptr();
                    const auto& column_data =
                            assert_cast<const ColumnUInt8&>(*null_column_ptr).get_data();
                    for (size_t i = 0; i < null_column_ptr->size(); ++i) {
                        filter[i] &= !column_data[i];
                    }
                }
                ptr = nullable_column->get_nested_column_ptr();
            }
        }
        dest_block->insert(vectorized::ColumnWithTypeAndName(
                std::move(ptr), slot_desc->get_data_type_ptr(), slot_desc->col_name()));
    }
    size_t dest_size = dest_block->columns();
    // do filter
    dest_block->insert(vectorized::ColumnWithTypeAndName(
            std::move(filter_column), std::make_shared<vectorized::DataTypeUInt8>(),
            "filter column"));
    RETURN_IF_ERROR(Block::filter_block(dest_block, dest_size, dest_size));
    _counter->num_rows_filtered += orig_rows - dest_block->rows();
    return Status::OK();
}

// arrow type ==arrow_column_to_doris_column==> primitive type(PT0) ==cast_src_block==>
// primitive type(PT1) ==materialize_block==> dest primitive type
Status VParquetScanner::_cast_src_block(Block* block) {
    // cast primitive type(PT0) to primitive type(PT1)
    for (size_t i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descs[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto& arg = block->get_by_name(slot_desc->col_name());
        // remove nullable here, let the get_function decide whether nullable
        auto return_type = slot_desc->get_data_type_ptr();
        ColumnsWithTypeAndName arguments {
                arg,
                {DataTypeString().create_column_const(
                         arg.column->size(), remove_nullable(return_type)->get_family_name()),
                 std::make_shared<DataTypeString>(), ""}};
        auto func_cast =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, return_type);
        RETURN_IF_ERROR(func_cast->execute(nullptr, *block, {i}, i, arg.column->size()));
        block->get_by_position(i).type = std::move(return_type);
    }
    return Status::OK();
}

Status VParquetScanner::_append_batch_to_src_block(Block* block) {
    size_t num_elements = std::min<size_t>((_state->batch_size() - block->rows()),
                                           (_batch->num_rows() - _arrow_batch_cur_idx));
    size_t column_pos = 0;
    for (auto i = 0; i < _num_of_columns_from_file; ++i) {
        SlotDescriptor* slot_desc = _src_slot_descs[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _batch->column(column_pos++).get();
        auto& column_with_type_and_name = block->get_by_name(slot_desc->col_name());
        RETURN_IF_ERROR(arrow_column_to_doris_column(array, _arrow_batch_cur_idx,
                                                     column_with_type_and_name, num_elements,
                                                     _state->timezone()));
    }

    _arrow_batch_cur_idx += num_elements;
    return Status::OK();
}

} // namespace doris::vectorized
