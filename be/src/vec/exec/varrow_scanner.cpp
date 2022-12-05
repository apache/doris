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

#include "exec/arrow/parquet_reader.h"
#include "exprs/expr.h"
#include "io/file_factory.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/vorc_scanner.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris::vectorized {

VArrowScanner::VArrowScanner(RuntimeState* state, RuntimeProfile* profile,
                             const TBrokerScanRangeParams& params,
                             const std::vector<TBrokerRangeDesc>& ranges,
                             const std::vector<TNetworkAddress>& broker_addresses,
                             const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          // _splittable(params.splittable),
          _cur_file_reader(nullptr),
          _cur_file_eof(false),
          _batch(nullptr),
          _arrow_batch_cur_idx(0) {
    _filtered_row_groups_counter = ADD_COUNTER(_profile, "FileFilteredRowGroups", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "FileFilteredRows", TUnit::UNIT);
    _filtered_bytes_counter = ADD_COUNTER(_profile, "FileFilteredBytes", TUnit::BYTES);
    _total_rows_counter = ADD_COUNTER(_profile, "FileTotalRows", TUnit::UNIT);
    _total_groups_counter = ADD_COUNTER(_profile, "FileTotalRowGroups", TUnit::UNIT);
}

VArrowScanner::~VArrowScanner() {
    close();
}

Status VArrowScanner::_open_next_reader() {
    // open_file_reader
    if (_cur_file_reader != nullptr) {
        delete _cur_file_reader;
        _cur_file_reader = nullptr;
    }

    while (true) {
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TBrokerRangeDesc& range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                range.file_type, _state->exec_env(), _profile, _broker_addresses,
                _params.properties, range, range.start_offset, file_reader));
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        int32_t num_of_columns_from_file = _src_slot_descs.size();
        if (range.__isset.num_of_columns_from_file) {
            num_of_columns_from_file = range.num_of_columns_from_file;
        }
        _cur_file_reader =
                _new_arrow_reader(_src_slot_descs, file_reader.release(), num_of_columns_from_file,
                                  range.start_offset, range.size);
        auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
        Status status =
                _cur_file_reader->init_reader(tuple_desc, _conjunct_ctxs, _state->timezone());

        if (status.is_end_of_file()) {
            continue;
        } else {
            if (!status.ok()) {
                return Status::InternalError(" file: {} error:{}", range.path,
                                             status.get_error_msg());
            } else {
                update_profile(_cur_file_reader->statistics());
                return status;
            }
        }
    }
}

void VArrowScanner::update_profile(std::shared_ptr<Statistics>& statistics) {
    COUNTER_UPDATE(_total_groups_counter, statistics->total_groups);
    COUNTER_UPDATE(_filtered_row_groups_counter, statistics->filtered_row_groups);
    COUNTER_UPDATE(_total_rows_counter, statistics->total_rows);
    COUNTER_UPDATE(_filtered_rows_counter, statistics->filtered_rows);
    COUNTER_UPDATE(_filtered_bytes_counter, statistics->filtered_total_bytes);
}

Status VArrowScanner::open() {
    RETURN_IF_ERROR(BaseScanner::open());
    if (_ranges.empty()) {
        return Status::OK();
    }
    return Status::OK();
}

// get next available arrow batch
Status VArrowScanner::_next_arrow_batch() {
    _arrow_batch_cur_idx = 0;
    // first, init file reader
    if (_cur_file_reader == nullptr || _cur_file_eof) {
        RETURN_IF_ERROR(_open_next_reader());
        _cur_file_eof = false;
    }
    // second, loop until find available arrow batch or EOF
    while (!_scanner_eof) {
        RETURN_IF_ERROR(_cur_file_reader->next_batch(&_batch, &_cur_file_eof));
        if (_cur_file_eof) {
            RETURN_IF_ERROR(_open_next_reader());
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

Status VArrowScanner::_init_arrow_batch_if_necessary() {
    // 1. init batch if first time
    // 2. reset reader if end of file
    Status status = Status::OK();
    if (_scanner_eof) {
        return Status::EndOfFile("EOF");
    }
    if (_batch == nullptr || _arrow_batch_cur_idx >= _batch->num_rows()) {
        return _next_arrow_batch();
    }
    return status;
}

Status VArrowScanner::_init_src_block() {
    size_t batch_pos = 0;
    _src_block.clear();
    if (_batch->num_columns() < _num_of_columns_from_file) {
        LOG(WARNING) << "some columns not found in the file, num_columns_obtained: "
                     << _batch->num_columns()
                     << " num_columns_required: " << _num_of_columns_from_file;
        return Status::InvalidArgument("some columns not found in the file");
    }
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
                DataTypeFactory::instance().create_data_type(array->type().get(), is_nullable);
        if (data_type == nullptr) {
            return Status::NotSupported(
                    fmt::format("Not support arrow type:{}", array->type()->name()));
        }
        MutableColumnPtr data_column = data_type->create_column();
        _src_block.insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot_desc->col_name()));
    }
    return Status::OK();
}

Status VArrowScanner::get_next(vectorized::Block* block, bool* eof) {
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

    RETURN_IF_ERROR(_init_src_block());
    // convert arrow batch to block until reach the batch_size
    while (!_scanner_eof) {
        // cast arrow type to PT0 and append it to src block
        // for example: arrow::Type::INT16 => TYPE_SMALLINT
        RETURN_IF_ERROR(_append_batch_to_src_block(&_src_block));
        // finalize the src block if full
        if (_src_block.rows() >= _state->batch_size()) {
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
    COUNTER_UPDATE(_rows_read_counter, _src_block.rows());
    SCOPED_TIMER(_materialize_timer);
    // cast PT0 => PT1
    // for example: TYPE_SMALLINT => TYPE_VARCHAR
    RETURN_IF_ERROR(_cast_src_block(&_src_block));

    // materialize, src block => dest columns
    return _fill_dest_block(block, eof);
}

// arrow type ==arrow_column_to_doris_column==> primitive type(PT0) ==cast_src_block==>
// primitive type(PT1) ==materialize_block==> dest primitive type
Status VArrowScanner::_cast_src_block(Block* block) {
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

Status VArrowScanner::_append_batch_to_src_block(Block* block) {
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
        RETURN_IF_ERROR(arrow_column_to_doris_column(
                array, _arrow_batch_cur_idx, column_with_type_and_name.column,
                column_with_type_and_name.type, num_elements, _state->timezone_obj()));
    }

    _arrow_batch_cur_idx += num_elements;
    return Status::OK();
}

void VArrowScanner::close() {
    BaseScanner::close();
    if (_cur_file_reader != nullptr) {
        delete _cur_file_reader;
        _cur_file_reader = nullptr;
    }
}

} // namespace doris::vectorized
