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

#include "vec/exec/file_arrow_scanner.h"

#include <memory>

#include "exec/arrow/parquet_reader.h"
#include "io/buffered_reader.h"
#include "io/file_factory.h"
#include "io/hdfs_reader_writer.h"
#include "runtime/descriptors.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris::vectorized {

FileArrowScanner::FileArrowScanner(RuntimeState* state, RuntimeProfile* profile,
                                   const TFileScanRangeParams& params,
                                   const std::vector<TFileRangeDesc>& ranges,
                                   const std::vector<TExpr>& pre_filter_texprs,
                                   ScannerCounter* counter)
        : FileScanner(state, profile, params, ranges, pre_filter_texprs, counter),
          _cur_file_reader(nullptr),
          _cur_file_eof(false),
          _batch(nullptr),
          _arrow_batch_cur_idx(0) {
    _convert_arrow_block_timer = ADD_TIMER(_profile, "ConvertArrowBlockTimer");
}

FileArrowScanner::~FileArrowScanner() {
    FileArrowScanner::close();
}

Status FileArrowScanner::_open_next_reader() {
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
        const TFileRangeDesc& range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _params, range.path,
                                                        range.start_offset, range.file_size, 0,
                                                        file_reader));
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        int32_t num_of_columns_from_file = _file_slot_descs.size();

        _cur_file_reader =
                _new_arrow_reader(_file_slot_descs, file_reader.release(), num_of_columns_from_file,
                                  range.start_offset, range.size);

        auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
        Status status =
                _cur_file_reader->init_reader(tuple_desc, _conjunct_ctxs, _state->timezone());
        if (status.is_end_of_file()) {
            continue;
        } else {
            if (!status.ok()) {
                std::stringstream ss;
                ss << " file: " << range.path << " error:" << status.get_error_msg();
                return Status::InternalError(ss.str());
            } else {
                _update_profile(_cur_file_reader->statistics());
                return status;
            }
        }
    }
}

Status FileArrowScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());
    if (_ranges.empty()) {
        return Status::OK();
    }
    return Status::OK();
}

// get next available arrow batch
Status FileArrowScanner::_next_arrow_batch() {
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

Status FileArrowScanner::_init_arrow_batch_if_necessary() {
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

Status FileArrowScanner::get_next(vectorized::Block* block, bool* eof) {
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

    RETURN_IF_ERROR(init_block(block));
    // convert arrow batch to block until reach the batch_size
    while (!_scanner_eof) {
        // cast arrow type to PT0 and append it to block
        // for example: arrow::Type::INT16 => TYPE_SMALLINT
        RETURN_IF_ERROR(_append_batch_to_block(block));
        // finalize the block if full
        if (_rows >= _state->batch_size()) {
            break;
        }
        auto status = _next_arrow_batch();
        // if ok, append the batch to the columns
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

    return finalize_block(block, eof);
}

Status FileArrowScanner::_append_batch_to_block(Block* block) {
    SCOPED_TIMER(_convert_arrow_block_timer);
    size_t num_elements = std::min<size_t>((_state->batch_size() - block->rows()),
                                           (_batch->num_rows() - _arrow_batch_cur_idx));
    for (auto i = 0; i < _file_slot_descs.size(); ++i) {
        SlotDescriptor* slot_desc = _file_slot_descs[i];
        if (slot_desc == nullptr) {
            continue;
        }
        std::string real_column_name = _cur_file_reader->is_case_sensitive()
                                               ? slot_desc->col_name()
                                               : slot_desc->col_name_lower_case();
        auto* array = _batch->GetColumnByName(real_column_name).get();
        auto& column_with_type_and_name = block->get_by_name(slot_desc->col_name());
        RETURN_IF_ERROR(arrow_column_to_doris_column(
                array, _arrow_batch_cur_idx, column_with_type_and_name.column,
                column_with_type_and_name.type, num_elements, _state->timezone_obj()));
    }
    _rows += num_elements;
    _arrow_batch_cur_idx += num_elements;
    return _fill_columns_from_path(block, num_elements);
}

void VFileParquetScanner::_update_profile(std::shared_ptr<Statistics>& statistics) {
    COUNTER_UPDATE(_filtered_row_groups_counter, statistics->filtered_row_groups);
    COUNTER_UPDATE(_filtered_rows_counter, statistics->filtered_rows);
    COUNTER_UPDATE(_filtered_bytes_counter, statistics->filtered_total_bytes);
    COUNTER_UPDATE(_total_rows_counter, statistics->total_rows);
    COUNTER_UPDATE(_total_groups_counter, statistics->total_groups);
    COUNTER_UPDATE(_total_bytes_counter, statistics->total_bytes);
}

void FileArrowScanner::close() {
    FileScanner::close();
    if (_cur_file_reader != nullptr) {
        delete _cur_file_reader;
        _cur_file_reader = nullptr;
    }
}

VFileParquetScanner::VFileParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                                         const TFileScanRangeParams& params,
                                         const std::vector<TFileRangeDesc>& ranges,
                                         const std::vector<TExpr>& pre_filter_texprs,
                                         ScannerCounter* counter)
        : FileArrowScanner(state, profile, params, ranges, pre_filter_texprs, counter) {
    _init_profiles(profile);
}

ArrowReaderWrap* VFileParquetScanner::_new_arrow_reader(
        const std::vector<SlotDescriptor*>& file_slot_descs, FileReader* file_reader,
        int32_t num_of_columns_from_file, int64_t range_start_offset, int64_t range_size) {
    return new ParquetReaderWrap(_state, file_slot_descs, file_reader, num_of_columns_from_file,
                                 range_start_offset, range_size, false);
}

void VFileParquetScanner::_init_profiles(RuntimeProfile* profile) {
    _filtered_row_groups_counter = ADD_COUNTER(_profile, "ParquetRowGroupsFiltered", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "ParquetRowsFiltered", TUnit::UNIT);
    _filtered_bytes_counter = ADD_COUNTER(_profile, "ParquetBytesFiltered", TUnit::BYTES);
    _total_rows_counter = ADD_COUNTER(_profile, "ParquetRowsTotal", TUnit::UNIT);
    _total_groups_counter = ADD_COUNTER(_profile, "ParquetRowGroupsTotal", TUnit::UNIT);
    _total_bytes_counter = ADD_COUNTER(_profile, "ParquetBytesTotal", TUnit::BYTES);
}

VFileORCScanner::VFileORCScanner(RuntimeState* state, RuntimeProfile* profile,
                                 const TFileScanRangeParams& params,
                                 const std::vector<TFileRangeDesc>& ranges,
                                 const std::vector<TExpr>& pre_filter_texprs,
                                 ScannerCounter* counter)
        : FileArrowScanner(state, profile, params, ranges, pre_filter_texprs, counter) {}

ArrowReaderWrap* VFileORCScanner::_new_arrow_reader(
        const std::vector<SlotDescriptor*>& file_slot_descs, FileReader* file_reader,
        int32_t num_of_columns_from_file, int64_t range_start_offset, int64_t range_size) {
    return new ORCReaderWrap(_state, file_slot_descs, file_reader, num_of_columns_from_file,
                             range_start_offset, range_size, false);
}

} // namespace doris::vectorized
