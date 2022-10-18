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

#include "vec/exec/scan/new_file_text_scanner.h"

#include "exec/plain_text_line_reader.h"
#include "io/file_factory.h"
#include "util/utf8_check.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

NewFileTextScanner::NewFileTextScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                                       const TFileScanRange& scan_range, RuntimeProfile* profile,
                                       const std::vector<TExpr>& pre_filter_texprs)
        : NewFileScanner(state, parent, limit, scan_range, profile, pre_filter_texprs),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_line_reader_eof(false),
          _skip_lines(0),
          _success(false) {}

Status NewFileTextScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(NewFileScanner::open(state));
    if (_ranges.empty()) {
        return Status::OK();
    }
    _split_values.reserve(sizeof(Slice) * _file_slot_descs.size());
    return Status::OK();
}

Status NewFileTextScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    if (!_is_load) {
        RETURN_IF_ERROR(init_block(block));
    }
    const int batch_size = state->batch_size();
    *eof = false;
    int current_rows = _rows;
    while (_rows < batch_size && !_scanner_eof) {
        if (_cur_line_reader == nullptr || _cur_line_reader_eof) {
            RETURN_IF_ERROR(_open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                continue;
            }
        }
        const uint8_t* ptr = nullptr;
        size_t size = 0;
        RETURN_IF_ERROR(_cur_line_reader->read_line(&ptr, &size, &_cur_line_reader_eof));
        if (_skip_lines > 0) {
            _skip_lines--;
            continue;
        }
        if (size == 0) {
            // Read empty row, just continue
            continue;
        }
        {
            COUNTER_UPDATE(_rows_read_counter, 1);
            RETURN_IF_ERROR(_fill_file_columns(Slice(ptr, size), block));
        }
        if (_cur_line_reader_eof) {
            RETURN_IF_ERROR(_fill_columns_from_path(block, _rows - current_rows));
            current_rows = _rows;
        }
    }
    if (_scanner_eof && block->rows() == 0) {
        *eof = true;
    }
    return Status::OK();
}

Status NewFileTextScanner::_fill_file_columns(const Slice& line, vectorized::Block* _block) {
    RETURN_IF_ERROR(_line_split_to_values(line));
    if (!_success) {
        // If not success, which means we met an invalid row, return.
        return Status::OK();
    }

    for (int i = 0; i < _split_values.size(); ++i) {
        auto slot_desc = _file_slot_descs[i];
        const Slice& value = _split_values[i];

        auto doris_column = _block->get_by_name(slot_desc->col_name()).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        _text_converter->write_vec_column(slot_desc, col_ptr, value.data, value.size, true, false);
    }
    _rows++;
    return Status::OK();
}

Status NewFileTextScanner::_line_split_to_values(const Slice& line) {
    if (!validate_utf8(line.data, line.size)) {
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                []() -> std::string { return "Unable to display"; },
                []() -> std::string {
                    fmt::memory_buffer error_msg;
                    fmt::format_to(error_msg, "{}", "Unable to display");
                    return fmt::to_string(error_msg);
                },
                &_scanner_eof));
        _success = false;
        return Status::OK();
    }

    RETURN_IF_ERROR(_split_line(line));

    _success = true;
    return Status::OK();
}

Status NewFileTextScanner::_open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_open_file_reader());
    RETURN_IF_ERROR(_open_line_reader());
    _next_range++;

    return Status::OK();
}

Status NewFileTextScanner::_open_file_reader() {
    const TFileRangeDesc& range = _ranges[_next_range];
    RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _params, range.path,
                                                    range.start_offset, range.file_size, 0,
                                                    _cur_file_reader));
    return _cur_file_reader->open();
}

Status NewFileTextScanner::_open_line_reader() {
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    const TFileRangeDesc& range = _ranges[_next_range];
    int64_t size = range.size;
    if (range.start_offset != 0) {
        if (_params.format_type != TFileFormatType::FORMAT_CSV_PLAIN) {
            std::stringstream ss;
            ss << "For now we do not support split compressed file";
            return Status::InternalError(ss.str());
        }
        size += 1;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    // open line reader
    switch (_params.format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        _cur_line_reader = new PlainTextLineReader(_profile, _cur_file_reader.get(), nullptr, size,
                                                   _line_delimiter, _line_delimiter_length);
        break;
    default: {
        std::stringstream ss;
        ss << "Unknown format type, cannot init line reader, type=" << _params.format_type;
        return Status::InternalError(ss.str());
    }
    }

    _cur_line_reader_eof = false;

    return Status::OK();
}

Status NewFileTextScanner::_split_line(const Slice& line) {
    _split_values.clear();
    std::vector<Slice> tmp_split_values;
    tmp_split_values.reserve(_num_of_columns_from_file);

    const char* value = line.data;
    size_t start = 0;     // point to the start pos of next col value.
    size_t curpos = 0;    // point to the start pos of separator matching sequence.
    size_t p1 = 0;        // point to the current pos of separator matching sequence.
    size_t non_space = 0; // point to the last pos of non_space charactor.

    // Separator: AAAA
    //
    //    p1
    //     ▼
    //     AAAA
    //   1000AAAA2000AAAA
    //   ▲   ▲
    // Start │
    //     curpos

    while (curpos < line.size) {
        if (*(value + curpos + p1) != _value_separator[p1]) {
            // Not match, move forward:
            curpos += (p1 == 0 ? 1 : p1);
            p1 = 0;
        } else {
            p1++;
            if (p1 == _value_separator_length) {
                // Match a separator
                non_space = curpos;
                // Trim tailing spaces. Be consistent with hive and trino's behavior.
                if (_state->trim_tailing_spaces_for_external_table_query()) {
                    while (non_space > start && *(value + non_space - 1) == ' ') {
                        non_space--;
                    }
                }
                tmp_split_values.emplace_back(value + start, non_space - start);
                start = curpos + _value_separator_length;
                curpos = start;
                p1 = 0;
                non_space = 0;
            }
        }
    }

    CHECK(curpos == line.size) << curpos << " vs " << line.size;
    non_space = curpos;
    if (_state->trim_tailing_spaces_for_external_table_query()) {
        while (non_space > start && *(value + non_space - 1) == ' ') {
            non_space--;
        }
    }

    tmp_split_values.emplace_back(value + start, non_space - start);
    for (const auto& slot : _file_slot_descs) {
        auto it = _file_slot_index_map.find(slot->id());
        if (it == std::end(_file_slot_index_map)) {
            std::stringstream ss;
            ss << "Unknown _file_slot_index_map, slot_id=" << slot->id();
            return Status::InternalError(ss.str());
        }
        int index = it->second;
        CHECK(index < _num_of_columns_from_file) << index << " vs " << _num_of_columns_from_file;
        _split_values.emplace_back(tmp_split_values[index]);
    }
    return Status::OK();
}

Status NewFileTextScanner::_convert_to_output_block(Block* output_block) {
    if (_input_block_ptr == output_block) {
        return Status::OK();
    }
    if (LIKELY(_input_block_ptr->rows() > 0)) {
        RETURN_IF_ERROR(_materialize_dest_block(output_block));
    }
    return Status::OK();
}
} // namespace doris::vectorized
