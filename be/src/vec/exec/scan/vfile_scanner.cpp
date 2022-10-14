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

#include "vec/exec/scan/vfile_scanner.h"

#include <fmt/format.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <vec/data_types/data_type_factory.hpp>

#include "common/logging.h"
#include "common/utils.h"
#include "exec/arrow/orc_reader.h"
#include "exec/text_converter.hpp"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/csv/vcsv_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

VFileScanner::VFileScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                           const TFileScanRange& scan_range, RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit),
          _params(scan_range.params),
          _ranges(scan_range.ranges),
          _next_range(0),
          _cur_reader(nullptr),
          _cur_reader_eof(false),
          _mem_pool(std::make_unique<MemPool>()),
          _profile(profile),
          _strict_mode(false) {}

Status VFileScanner::prepare(
        VExprContext** vconjunct_ctx_ptr,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;

    _get_block_timer = ADD_TIMER(_parent->_scanner_profile, "FileScannerGetBlockTime");
    _cast_to_input_block_timer =
            ADD_TIMER(_parent->_scanner_profile, "FileScannerCastInputBlockTime");
    _fill_path_columns_timer =
            ADD_TIMER(_parent->_scanner_profile, "FileScannerFillPathColumnTime");
    _fill_missing_columns_timer =
            ADD_TIMER(_parent->_scanner_profile, "FileScannerFillMissingColumnTime");
    _pre_filter_timer = ADD_TIMER(_parent->_scanner_profile, "FileScannerPreFilterTimer");
    _convert_to_output_block_timer =
            ADD_TIMER(_parent->_scanner_profile, "FileScannerConvertOuputBlockTime");

    if (vconjunct_ctx_ptr != nullptr) {
        // Copy vconjunct_ctx_ptr from scan node to this scanner's _vconjunct_ctx.
        RETURN_IF_ERROR((*vconjunct_ctx_ptr)->clone(_state, &_vconjunct_ctx));
    }

    if (_is_load) {
        _src_block_mem_reuse = true;
        _src_row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                              std::vector<TupleId>({_input_tuple_desc->id()}),
                                              std::vector<bool>({false})));
        // prepare pre filters
        if (_params.__isset.pre_filter_exprs) {
            _pre_conjunct_ctx_ptr.reset(new doris::vectorized::VExprContext*);
            RETURN_IF_ERROR(doris::vectorized::VExpr::create_expr_tree(
                    _state->obj_pool(), _params.pre_filter_exprs, _pre_conjunct_ctx_ptr.get()));
            RETURN_IF_ERROR((*_pre_conjunct_ctx_ptr)->prepare(_state, *_src_row_desc));
            RETURN_IF_ERROR((*_pre_conjunct_ctx_ptr)->open(_state));
        }
    }

    _default_val_row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                                  std::vector<TupleId>({_real_tuple_desc->id()}),
                                                  std::vector<bool>({false})));

    return Status::OK();
}

Status VFileScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VScanner::open(state));
    RETURN_IF_ERROR(_init_expr_ctxes());
    return Status::OK();
}

// For query:
//                              [exist cols]  [non-exist cols]  [col from path]  input  output
//                              A     B    C  D                 E
// _init_src_block              x     x    x  x                 x                -      x
// get_next_block               x     x    x  -                 -                -      x
// _cast_to_input_block         -     -    -  -                 -                -      -
// _fill_columns_from_path      -     -    -  -                 x                -      x
// _fill_missing_columns        -     -    -  x                 -                -      x
// _convert_to_output_block     -     -    -  -                 -                -      -
//
// For load:
//                              [exist cols]  [non-exist cols]  [col from path]  input  output
//                              A     B    C  D                 E
// _init_src_block              x     x    x  x                 x                x      -
// get_next_block               x     x    x  -                 -                x      -
// _cast_to_input_block         x     x    x  -                 -                x      -
// _fill_columns_from_path      -     -    -  -                 x                x      -
// _fill_missing_columns        -     -    -  x                 -                x      -
// _convert_to_output_block     -     -    -  -                 -                -      x
Status VFileScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    do {
        if (_cur_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(_get_next_reader());
        }

        if (_scanner_eof) {
            *eof = true;
            return Status::OK();
        }

        // Init src block for load job based on the data file schema (e.g. parquet)
        // For query job, simply set _src_block_ptr to block.
        size_t read_rows = 0;
        RETURN_IF_ERROR(_init_src_block(block));
        {
            SCOPED_TIMER(_get_block_timer);
            // Read next block.
            // Some of column in block may not be filled (column not exist in file)
            RETURN_IF_ERROR(
                    _cur_reader->get_next_block(_src_block_ptr, &read_rows, &_cur_reader_eof));
        }

        // use read_rows instead of _src_block_ptr->rows(), because the first column of _src_block_ptr
        // may not be filled after calling `get_next_block()`, so _src_block_ptr->rows() may return wrong result.
        if (read_rows > 0) {
            // Convert the src block columns type to string in-place.
            RETURN_IF_ERROR(_cast_to_input_block(block));
            // Fill rows in src block with partition columns from path. (e.g. Hive partition columns)
            RETURN_IF_ERROR(_fill_columns_from_path(read_rows));
            // Fill columns not exist in file with null or default value
            RETURN_IF_ERROR(_fill_missing_columns(read_rows));
            // Apply _pre_conjunct_ctx_ptr to filter src block.
            RETURN_IF_ERROR(_pre_filter_src_block());
            // Convert src block to output block (dest block), string to dest data type and apply filters.
            RETURN_IF_ERROR(_convert_to_output_block(block));
            break;
        }
    } while (true);

    // Update filtered rows and unselected rows for load, reset counter.
    {
        state->update_num_rows_load_filtered(_counter.num_rows_filtered);
        state->update_num_rows_load_unselected(_counter.num_rows_unselected);
        _reset_counter();
    }

    return Status::OK();
}

Status VFileScanner::_init_src_block(Block* block) {
    if (!_is_load) {
        _src_block_ptr = block;
        return Status::OK();
    }

    // if (_src_block_init) {
    //     _src_block.clear_column_data();
    //     _src_block_ptr = &_src_block;
    //     return Status::OK();
    // }

    _src_block.clear();
    size_t idx = 0;
    // slots in _input_tuple_desc contains all slots describe in load statement, eg:
    // -H "columns: k1, k2, tmp1, k3 = tmp1 + 1"
    // _input_tuple_desc will contains: k1, k2, tmp1
    // and some of them are from file, such as k1 and k2, and some of them may not exist in file, such as tmp1
    // _input_tuple_desc also contains columns from path
    for (auto& slot : _input_tuple_desc->slots()) {
        DataTypePtr data_type;
        auto it = _name_to_col_type.find(slot->col_name());
        if (it == _name_to_col_type.end()) {
            // not exist in file, using type from _input_tuple_desc
            data_type =
                    DataTypeFactory::instance().create_data_type(slot->type(), slot->is_nullable());
        } else {
            data_type = DataTypeFactory::instance().create_data_type(it->second, true);
        }
        if (data_type == nullptr) {
            return Status::NotSupported(fmt::format("Not support arrow type:{}", slot->col_name()));
        }
        MutableColumnPtr data_column = data_type->create_column();
        _src_block.insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot->col_name()));
        _src_block_name_to_idx.emplace(slot->col_name(), idx++);
    }
    _src_block_ptr = &_src_block;
    _src_block_init = true;
    return Status::OK();
}

Status VFileScanner::_cast_to_input_block(Block* block) {
    if (!_is_load) {
        return Status::OK();
    }
    SCOPED_TIMER(_cast_to_input_block_timer);
    // cast primitive type(PT0) to primitive type(PT1)
    size_t idx = 0;
    for (auto& slot_desc : _input_tuple_desc->slots()) {
        if (_name_to_col_type.find(slot_desc->col_name()) == _name_to_col_type.end()) {
            // skip columns which does not exist in file
            continue;
        }
        auto& arg = _src_block_ptr->get_by_name(slot_desc->col_name());
        // remove nullable here, let the get_function decide whether nullable
        auto return_type = slot_desc->get_data_type_ptr();
        ColumnsWithTypeAndName arguments {
                arg,
                {DataTypeString().create_column_const(
                         arg.column->size(), remove_nullable(return_type)->get_family_name()),
                 std::make_shared<DataTypeString>(), ""}};
        auto func_cast =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, return_type);
        idx = _src_block_name_to_idx[slot_desc->col_name()];
        RETURN_IF_ERROR(
                func_cast->execute(nullptr, *_src_block_ptr, {idx}, idx, arg.column->size()));
        _src_block_ptr->get_by_position(idx).type = std::move(return_type);
    }
    return Status::OK();
}

Status VFileScanner::_fill_columns_from_path(size_t rows) {
    const TFileRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.columns_from_path && !_partition_slot_descs.empty()) {
        SCOPED_TIMER(_fill_path_columns_timer);
        for (const auto& slot_desc : _partition_slot_descs) {
            if (slot_desc == nullptr) continue;
            auto it = _partition_slot_index_map.find(slot_desc->id());
            if (it == std::end(_partition_slot_index_map)) {
                std::stringstream ss;
                ss << "Unknown source slot descriptor, slot_id=" << slot_desc->id();
                return Status::InternalError(ss.str());
            }
            const std::string& column_from_path = range.columns_from_path[it->second];

            auto doris_column = _src_block_ptr->get_by_name(slot_desc->col_name()).column;
            IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());

            for (size_t j = 0; j < rows; ++j) {
                _text_converter->write_vec_column(slot_desc, col_ptr,
                                                  const_cast<char*>(column_from_path.c_str()),
                                                  column_from_path.size(), true, false);
            }
        }
    }
    return Status::OK();
}

Status VFileScanner::_fill_missing_columns(size_t rows) {
    if (_missing_cols.empty()) {
        return Status::OK();
    }

    SCOPED_TIMER(_fill_missing_columns_timer);
    for (auto slot_desc : _real_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        if (_missing_cols.find(slot_desc->col_name()) == _missing_cols.end()) {
            continue;
        }

        auto it = _col_default_value_ctx.find(slot_desc->col_name());
        if (it == _col_default_value_ctx.end()) {
            return Status::InternalError("failed to find default value expr for slot: {}",
                                         slot_desc->col_name());
        }
        if (it->second == nullptr) {
            // no default column, fill with null
            auto nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    (*std::move(_src_block_ptr->get_by_name(slot_desc->col_name()).column))
                            .mutate()
                            .get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto* ctx = it->second;
            auto origin_column_num = _src_block_ptr->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(_src_block_ptr, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                std::move(*_src_block_ptr->get_by_position(result_column_id).column)
                        .mutate()
                        ->resize(rows);
                auto result_column_ptr = _src_block_ptr->get_by_position(result_column_id).column;
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = _src_block_ptr->get_by_name(slot_desc->col_name()).type;
                bool is_nullable = origin_column_type->is_nullable();
                _src_block_ptr->replace_by_position(
                        _src_block_ptr->get_position_by_name(slot_desc->col_name()),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                _src_block_ptr->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

Status VFileScanner::_pre_filter_src_block() {
    if (!_is_load) {
        return Status::OK();
    }
    if (_pre_conjunct_ctx_ptr) {
        SCOPED_TIMER(_pre_filter_timer);
        auto origin_column_num = _src_block_ptr->columns();
        auto old_rows = _src_block_ptr->rows();
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_pre_conjunct_ctx_ptr,
                                                               _src_block_ptr, origin_column_num));
        _counter.num_rows_unselected += old_rows - _src_block.rows();
    }
    return Status::OK();
}

Status VFileScanner::_convert_to_output_block(Block* block) {
    if (!_is_load) {
        return Status::OK();
    }

    SCOPED_TIMER(_convert_to_output_block_timer);
    // The block is passed from scanner context's free blocks,
    // which is initialized by src columns.
    // But for load job, the block should be filled with dest columns.
    // So need to clear it first.
    block->clear();

    int ctx_idx = 0;
    size_t rows = _src_block.rows();
    auto filter_column = vectorized::ColumnUInt8::create(rows, 1);
    auto& filter_map = filter_column->get_data();
    auto origin_column_num = _src_block.columns();

    for (auto slot_desc : _output_tuple_desc->slots()) {
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
        // column_ptr maybe a ColumnConst, convert it to a normal column
        column_ptr = column_ptr->convert_to_full_column_if_const();

        DCHECK(column_ptr != nullptr);

        // because of src_slot_desc is always be nullable, so the column_ptr after do dest_expr
        // is likely to be nullable
        if (LIKELY(column_ptr->is_nullable())) {
            const ColumnNullable* nullable_column =
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
            if (!slot_desc->is_nullable()) {
                column_ptr = remove_nullable(column_ptr);
            }
        } else if (slot_desc->is_nullable()) {
            column_ptr = make_nullable(column_ptr);
        }
        block->insert(dest_index, vectorized::ColumnWithTypeAndName(std::move(column_ptr),
                                                                    slot_desc->get_data_type_ptr(),
                                                                    slot_desc->col_name()));
    }

    // after do the dest block insert operation, clear _src_block to remove the reference of origin column
    if (_src_block_mem_reuse) {
        _src_block.clear_column_data(origin_column_num);
    } else {
        _src_block.clear();
    }

    size_t dest_size = block->columns();
    // do filter
    block->insert(vectorized::ColumnWithTypeAndName(std::move(filter_column),
                                                    std::make_shared<vectorized::DataTypeUInt8>(),
                                                    "filter column"));
    RETURN_IF_ERROR(vectorized::Block::filter_block(block, dest_size, dest_size));
    _counter.num_rows_filtered += rows - block->rows();

    return Status::OK();
}

Status VFileScanner::_get_next_reader() {
    while (true) {
        _cur_reader.reset(nullptr);
        _src_block_init = false;
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TFileRangeDesc& range = _ranges[_next_range++];

        // 1. create file reader
        // TODO: Each format requires its own FileReader to achieve a special access mode,
        //  so create the FileReader inner the format.
        std::unique_ptr<FileReader> file_reader;
        if (_params.format_type != TFileFormatType::FORMAT_PARQUET) {
            RETURN_IF_ERROR(FileFactory::create_file_reader(_state->exec_env(), _profile, _params,
                                                            range, file_reader));
            RETURN_IF_ERROR(file_reader->open());
            if (file_reader->size() == 0) {
                file_reader->close();
                continue;
            }
        }

        // 2. create reader for specific format
        // TODO: add csv, json, avro
        Status init_status;
        switch (_params.format_type) {
        case TFileFormatType::FORMAT_PARQUET: {
            _cur_reader.reset(new ParquetReader(
                    _profile, _params, range, _file_col_names, _state->query_options().batch_size,
                    const_cast<cctz::time_zone*>(&_state->timezone_obj())));
            init_status =
                    ((ParquetReader*)(_cur_reader.get()))->init_reader(_colname_to_value_range);
            break;
        }
        case TFileFormatType::FORMAT_ORC: {
            _cur_reader.reset(new ORCReaderWrap(_state, _file_slot_descs, file_reader.release(),
                                                _num_of_columns_from_file, range.start_offset,
                                                range.size, false));
            init_status =
                    ((ORCReaderWrap*)(_cur_reader.get()))
                            ->init_reader(_real_tuple_desc, _conjunct_ctxs, _state->timezone());
            break;
        }
        case TFileFormatType::FORMAT_CSV_PLAIN:
        case TFileFormatType::FORMAT_CSV_GZ:
        case TFileFormatType::FORMAT_CSV_BZ2:
        case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        case TFileFormatType::FORMAT_CSV_LZOP:
        case TFileFormatType::FORMAT_CSV_DEFLATE: {
            _cur_reader.reset(new CsvReader(_state, _profile, &_counter, _params, range,
                                            _file_slot_descs, file_reader.release()));
            init_status = ((CsvReader*)(_cur_reader.get()))->init_reader();
            break;
        }
        default:
            return Status::InternalError("Not supported file format: {}", _params.format_type);
        }

        if (init_status.is_end_of_file()) {
            continue;
        } else if (!init_status.ok()) {
            return Status::InternalError("failed to init reader for file {}, err: {}", range.path,
                                         init_status.get_error_msg());
        }

        _name_to_col_type.clear();
        _missing_cols.clear();
        _cur_reader->get_columns(&_name_to_col_type, &_missing_cols);
        if (!_missing_cols.empty() && _is_load && VLOG_NOTICE_IS_ON) {
            fmt::memory_buffer col_buf;
            for (auto& col : _missing_cols) {
                fmt::format_to(col_buf, " {}", col);
            }
            VLOG_NOTICE << fmt::format("Unknown columns:{} in file {}", fmt::to_string(col_buf),
                                       range.path);
        }
        _cur_reader_eof = false;
        break;
    }
    return Status::OK();
}

Status VFileScanner::_init_expr_ctxes() {
    DCHECK(!_ranges.empty());

    std::map<SlotId, int> full_src_index_map;
    std::map<SlotId, SlotDescriptor*> full_src_slot_map;
    int index = 0;
    for (const auto& slot_desc : _real_tuple_desc->slots()) {
        full_src_slot_map.emplace(slot_desc->id(), slot_desc);
        full_src_index_map.emplace(slot_desc->id(), index++);
    }

    _num_of_columns_from_file = _params.num_of_columns_from_file;
    for (const auto& slot_info : _params.required_slots) {
        auto slot_id = slot_info.slot_id;
        auto it = full_src_slot_map.find(slot_id);
        if (it == std::end(full_src_slot_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status::InternalError(ss.str());
        }
        if (slot_info.is_file_slot) {
            _file_slot_descs.emplace_back(it->second);
            auto iti = full_src_index_map.find(slot_id);
            _file_slot_index_map.emplace(slot_id, iti->second);
            _file_slot_name_map.emplace(it->second->col_name(), iti->second);
            _file_col_names.push_back(it->second->col_name());
        } else {
            _partition_slot_descs.emplace_back(it->second);
            auto iti = full_src_index_map.find(slot_id);
            _partition_slot_index_map.emplace(slot_id, iti->second - _num_of_columns_from_file);
        }
    }

    if (_is_load) {
        // follow desc expr map and src default value expr map is only for load task.
        bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
        int idx = 0;
        for (auto slot_desc : _output_tuple_desc->slots()) {
            if (!slot_desc->is_materialized()) {
                continue;
            }
            auto it = _params.expr_of_dest_slot.find(slot_desc->id());
            if (it == std::end(_params.expr_of_dest_slot)) {
                return Status::InternalError("No expr for dest slot, id={}, name={}",
                                             slot_desc->id(), slot_desc->col_name());
            }

            vectorized::VExprContext* ctx = nullptr;
            if (!it->second.nodes.empty()) {
                RETURN_IF_ERROR(
                        vectorized::VExpr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
                RETURN_IF_ERROR(ctx->prepare(_state, *_src_row_desc));
                RETURN_IF_ERROR(ctx->open(_state));
            }
            _dest_vexpr_ctx.emplace_back(ctx);
            _dest_slot_name_to_idx[slot_desc->col_name()] = idx++;

            if (has_slot_id_map) {
                auto it1 = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
                if (it1 == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                    _src_slot_descs_order_by_dest.emplace_back(nullptr);
                } else {
                    auto _src_slot_it = full_src_slot_map.find(it1->second);
                    if (_src_slot_it == std::end(full_src_slot_map)) {
                        return Status::InternalError("No src slot {} in src slot descs",
                                                     it1->second);
                    }
                    _dest_slot_to_src_slot_index.emplace(_src_slot_descs_order_by_dest.size(),
                                                         full_src_index_map[_src_slot_it->first]);
                    _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
                }
            }
        }

        for (auto slot_desc : _real_tuple_desc->slots()) {
            if (!slot_desc->is_materialized()) {
                continue;
            }
            vectorized::VExprContext* ctx = nullptr;
            auto it = _params.default_value_of_src_slot.find(slot_desc->id());
            if (it != std::end(_params.default_value_of_src_slot)) {
                if (!it->second.nodes.empty()) {
                    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(_state->obj_pool(),
                                                                        it->second, &ctx));
                    RETURN_IF_ERROR(ctx->prepare(_state, *_default_val_row_desc));
                    RETURN_IF_ERROR(ctx->open(_state));
                }
                // if expr is empty, the default value will be null
                _col_default_value_ctx.emplace(slot_desc->col_name(), ctx);
            }
        }
    }
    return Status::OK();
}

Status VFileScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    for (auto ctx : _dest_vexpr_ctx) {
        if (ctx != nullptr) {
            ctx->close(state);
        }
    }

    for (auto it : _col_default_value_ctx) {
        if (it.second != nullptr) {
            it.second->close(state);
        }
    }

    if (_pre_conjunct_ctx_ptr) {
        (*_pre_conjunct_ctx_ptr)->close(state);
    }

    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}

} // namespace doris::vectorized
