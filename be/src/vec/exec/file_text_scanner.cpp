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

#include "vec/exec/file_text_scanner.h"

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>

#include <iostream>

#include "exec/exec_node.h"
#include "exec/plain_text_line_reader.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exprs/expr_context.h"
#include "io/buffered_reader.h"
#include "io/hdfs_reader_writer.h"
#include "util/types.h"
#include "util/utf8_check.h"

namespace doris::vectorized {

FileTextScanner::FileTextScanner(RuntimeState* state, RuntimeProfile* profile,
                                 const TFileScanRangeParams& params,
                                 const std::vector<TFileRangeDesc>& ranges,
                                 const std::vector<TExpr>& pre_filter_texprs,
                                 ScannerCounter* counter)
        : FileScanner(state, profile, params, ranges, pre_filter_texprs, counter),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_line_reader_eof(false),
          _skip_lines(0),
          _success(false)

{
    if (params.__isset.text_params) {
        auto text_params = params.text_params;
        if (text_params.__isset.column_separator_str) {
            _value_separator = text_params.column_separator_str;
            _value_separator_length = _value_separator.length();
        }
        if (text_params.__isset.line_delimiter_str) {
            _line_delimiter = text_params.line_delimiter_str;
            _line_delimiter_length = _line_delimiter.length();
        }
    }
}

FileTextScanner::~FileTextScanner() {
    close();
}

Status FileTextScanner::open() {
    RETURN_IF_ERROR(FileScanner::open());

    if (_ranges.empty()) {
        return Status::OK();
    }
    _split_values.reserve(sizeof(Slice) * _file_slot_descs.size());
    return Status::OK();
}

void FileTextScanner::close() {
    FileScanner::close();

    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }
}

Status FileTextScanner::get_next(Block* block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    RETURN_IF_ERROR(init_block(block));

    const int batch_size = _state->batch_size();

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
        std::unique_ptr<const uint8_t> u_ptr;
        u_ptr.reset(ptr);
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
    }

    return finalize_block(block, eof);
}

Status FileTextScanner::_fill_file_columns(const Slice& line, vectorized::Block* _block) {
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

Status FileTextScanner::_open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_open_file_reader());
    RETURN_IF_ERROR(_open_line_reader());
    _next_range++;

    return Status::OK();
}

Status FileTextScanner::_open_file_reader() {
    const TFileRangeDesc& range = _ranges[_next_range];

    FileReader* hdfs_reader = nullptr;
    RETURN_IF_ERROR(HdfsReaderWriter::create_reader(range.hdfs_params, range.path,
                                                    range.start_offset, &hdfs_reader));
    _cur_file_reader.reset(new BufferedReader(_profile, hdfs_reader));
    return _cur_file_reader->open();
}

Status FileTextScanner::_open_line_reader() {
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    const TFileRangeDesc& range = _ranges[_next_range];
    int64_t size = range.size;
    if (range.start_offset != 0) {
        if (range.format_type != TFileFormatType::FORMAT_CSV_PLAIN) {
            std::stringstream ss;
            ss << "For now we do not support split compressed file";
            return Status::InternalError(ss.str());
        }
        size += 1;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    // open line reader
    switch (range.format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        _cur_line_reader = new PlainTextLineReader(_profile, _cur_file_reader.get(), nullptr, size,
                                                   _line_delimiter, _line_delimiter_length);
        break;
    default: {
        std::stringstream ss;
        ss << "Unknown format type, cannot init line reader, type=" << range.format_type;
        return Status::InternalError(ss.str());
    }
    }

    _cur_line_reader_eof = false;

    return Status::OK();
}

Status FileTextScanner::_line_split_to_values(const Slice& line) {
    if (!validate_utf8(line.data, line.size)) {
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                []() -> std::string { return "Unable to display"; },
                []() -> std::string {
                    fmt::memory_buffer error_msg;
                    fmt::format_to(error_msg, "{}", "Unable to display");
                    return fmt::to_string(error_msg);
                },
                &_scanner_eof));
        _counter->num_rows_filtered++;
        _success = false;
        return Status::OK();
    }

    RETURN_IF_ERROR(_split_line(line));

    _success = true;
    return Status::OK();
}

Status FileTextScanner::_split_line(const Slice& line) {
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

} // namespace doris::vectorized
