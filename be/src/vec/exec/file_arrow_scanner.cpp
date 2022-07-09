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

#include "exec/arrow/parquet_reader.h"
#include "io/buffered_reader.h"
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
          _arrow_batch_cur_idx(0) {}

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

        FileReader* hdfs_reader = nullptr;
        RETURN_IF_ERROR(HdfsReaderWriter::create_reader(range.hdfs_params, range.path,
                                                        range.start_offset, &hdfs_reader));
        file_reader.reset(new BufferedReader(_profile, hdfs_reader));
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        int32_t num_of_columns_from_file = _file_slot_descs.size();

        _cur_file_reader = _new_arrow_reader(file_reader.release(), _state->batch_size(),
                                             num_of_columns_from_file);

        auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_tupleId);
        Status status = _cur_file_reader->init_reader(tuple_desc, _file_slot_descs, _conjunct_ctxs,
                                                      _state->timezone());
        if (status.is_end_of_file()) {
            continue;
        } else {
            if (!status.ok()) {
                std::stringstream ss;
                ss << " file: " << range.path << " error:" << status.get_error_msg();
                return Status::InternalError(ss.str());
            } else {
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
    Status status;
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
    size_t num_elements = std::min<size_t>((_state->batch_size() - block->rows()),
                                           (_batch->num_rows() - _arrow_batch_cur_idx));
    for (auto i = 0; i < _file_slot_descs.size(); ++i) {
        SlotDescriptor* slot_desc = _file_slot_descs[i];
        if (slot_desc == nullptr) {
            continue;
        }
        auto* array = _batch->GetColumnByName(slot_desc->col_name()).get();
        auto& column_with_type_and_name = block->get_by_name(slot_desc->col_name());
        RETURN_IF_ERROR(arrow_column_to_doris_column(
                array, _arrow_batch_cur_idx, column_with_type_and_name.column,
                column_with_type_and_name.type, num_elements, _state->timezone()));
    }
    _rows += num_elements;
    _arrow_batch_cur_idx += num_elements;
    return Status::OK();
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
        : FileArrowScanner(state, profile, params, ranges, pre_filter_texprs, counter) {}

ArrowReaderWrap* VFileParquetScanner::_new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                                        int32_t num_of_columns_from_file) {
    return new ParquetReaderWrap(_profile, file_reader, batch_size, num_of_columns_from_file);
}

VFileORCScanner::VFileORCScanner(RuntimeState* state, RuntimeProfile* profile,
                                 const TFileScanRangeParams& params,
                                 const std::vector<TFileRangeDesc>& ranges,
                                 const std::vector<TExpr>& pre_filter_texprs,
                                 ScannerCounter* counter)
        : FileArrowScanner(state, profile, params, ranges, pre_filter_texprs, counter) {}

ArrowReaderWrap* VFileORCScanner::_new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                                    int32_t num_of_columns_from_file) {
    return new ORCReaderWrap(file_reader, batch_size, num_of_columns_from_file);
}

} // namespace doris::vectorized
