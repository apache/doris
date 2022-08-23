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

#include "vec/exec/scan/new_file_arrow_scanner.h"
#include "vec/utils/arrow_column_to_doris_column.h"
#include "io/file_factory.h"
#include "exec/arrow/parquet_reader.h"
#include "exec/arrow/orc_reader.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

    NewFileArrowScanner::NewFileArrowScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                                   const TFileScanRange& scan_range, MemTracker* tracker, RuntimeProfile* profile)
            : NewFileScanner(state, parent, limit, scan_range, tracker, profile),
              _cur_file_reader(nullptr),
              _cur_file_eof(false),
              _batch(nullptr),
              _arrow_batch_cur_idx(0){}

    Status NewFileArrowScanner::open(RuntimeState* state) {
        RETURN_IF_ERROR(NewFileScanner::open(state));
        // SCOPED_TIMER(_parent->_reader_init_timer);
        // SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

        // _runtime_filter_marks.resize(_parent->runtime_filter_descs().size(), false);
        return Status::OK();
    }

    Status NewFileArrowScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
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

        *eof = false;
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

        if (_scanner_eof && block->rows() == 0) {
            *eof = true;
        }
//        return finalize_block(block, eof);
        return Status::OK();
    }

    Status NewFileArrowScanner::_init_arrow_batch_if_necessary() {
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

    Status NewFileArrowScanner::_append_batch_to_block(Block* block) {
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

    Status NewFileArrowScanner::_next_arrow_batch() {
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

    Status NewFileArrowScanner::_open_next_reader() {
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

            RETURN_IF_ERROR(FileFactory::create_file_reader(_state->exec_env(), _profile, _params,
                                                            range, file_reader));
            RETURN_IF_ERROR(file_reader->open());
            if (file_reader->size() == 0) {
                file_reader->close();
                continue;
            }

            int32_t num_of_columns_from_file = _file_slot_descs.size();

            _cur_file_reader =
                    _new_arrow_reader(file_reader.release(), _state->batch_size(),
                                      num_of_columns_from_file, range.start_offset, range.size);

            auto tuple_desc = _state->desc_tbl().get_tuple_descriptor(_parent->output_tuple_id());
            // TODO _conjunct_ctxs is empty for now. _conjunct_ctxs is not empty.
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
//                    _update_profile(_cur_file_reader->statistics());
                    return status;
                }
            }
        }
    }

    NewFileParquetScanner::NewFileParquetScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                                                 const TFileScanRange& scan_range, MemTracker* tracker,
                                                 RuntimeProfile* profile)
            : NewFileArrowScanner(state, parent, limit, scan_range, tracker, profile) {
//        _init_profiles(profile);
    }

    ArrowReaderWrap* NewFileParquetScanner::_new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                                            int32_t num_of_columns_from_file,
                                                            int64_t range_start_offset,
                                                            int64_t range_size) {
        return new ParquetReaderWrap(file_reader, batch_size, num_of_columns_from_file,
                                     range_start_offset, range_size, false);
    }

    NewFileORCScanner::NewFileORCScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                                         const TFileScanRange& scan_range, MemTracker* tracker, RuntimeProfile* profile)
            : NewFileArrowScanner(state, parent, limit, scan_range, tracker, profile) {}

    ArrowReaderWrap* NewFileORCScanner::_new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                                        int32_t num_of_columns_from_file,
                                                        int64_t range_start_offset,
                                                        int64_t range_size) {
        return new ORCReaderWrap(file_reader, batch_size, num_of_columns_from_file, range_start_offset,
                                 range_size, false);
    }
}
