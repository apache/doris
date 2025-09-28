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

#include "text_reader.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exec/line_reader.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/s3_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void HiveTextFieldSplitter::do_split(const Slice& line, std::vector<Slice>* splitted_values) {
    if (_value_sep_len == 1) {
        _split_field_single_char(line, splitted_values);
    } else {
        _split_field_multi_char(line, splitted_values);
    }
}

void HiveTextFieldSplitter::_split_field_single_char(const Slice& line,
                                                     std::vector<Slice>* splitted_values) {
    const char* data = line.data;
    const size_t size = line.size;
    size_t value_start = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == _value_sep[0]) {
            // hive will escape the field separator in string
            if (_escape_char != 0 && i > 0 && data[i - 1] == _escape_char) {
                continue;
            }
            process_value_func(data, value_start, i - value_start, _trimming_char, splitted_values);
            value_start = i + _value_sep_len;
        }
    }
    process_value_func(data, value_start, size - value_start, _trimming_char, splitted_values);
}

void HiveTextFieldSplitter::_split_field_multi_char(const Slice& line,
                                                    std::vector<Slice>* splitted_values) {
    const char* data = line.data;
    const size_t size = line.size;
    size_t start = 0;

    std::vector<int> next(_value_sep_len);
    next[0] = -1;
    for (int i = 1, j = -1; i < (int)_value_sep_len; i++) {
        while (j >= 0 && _value_sep[i] != _value_sep[j + 1]) {
            j = next[j];
        }
        if (_value_sep[i] == _value_sep[j + 1]) {
            j++;
        }
        next[i] = j;
    }

    // KMP search
    for (int i = 0, j = -1; i < (int)size; i++) {
        while (j >= 0 && data[i] != _value_sep[j + 1]) {
            j = next[j];
        }
        if (data[i] == _value_sep[j + 1]) {
            j++;
        }
        if (j == (int)_value_sep_len - 1) {
            size_t curpos = i - _value_sep_len + 1;
            if (_escape_char != 0 && curpos > 0 && data[curpos - 1] == _escape_char) {
                j = next[j];
                continue;
            }

            if (curpos >= start) {
                process_value_func(data, start, curpos - start, _trimming_char, splitted_values);
                start = curpos + _value_sep_len;
            }

            j = next[j];
        }
    }
    process_value_func(data, start, size - start, _trimming_char, splitted_values);
}

TextReader::TextReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                       const TFileScanRangeParams& params, const TFileRangeDesc& range,
                       const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx)
        : CsvReader(state, profile, counter, params, range, file_slot_descs, io_ctx) {}

Status TextReader::_init_options() {
    // get column_separator and line_delimiter
    _value_separator = _params.file_attributes.text_params.column_separator;
    _value_separator_length = _value_separator.size();
    _line_delimiter = _params.file_attributes.text_params.line_delimiter;
    _line_delimiter_length = _line_delimiter.size();

    if (_params.file_attributes.text_params.__isset.escape) {
        _escape = _params.file_attributes.text_params.escape;
    }
    _options.escape_char = _escape;

    _options.collection_delim = _params.file_attributes.text_params.collection_delimiter[0];
    _options.map_key_delim = _params.file_attributes.text_params.mapkv_delimiter[0];

    _options.null_format = _params.file_attributes.text_params.null_format.data();
    _options.null_len = _params.file_attributes.text_params.null_format.length();

    return Status::OK();
}

Status TextReader::_deserialize_one_cell(DataTypeSerDeSPtr serde, IColumn& column, Slice& slice) {
    return serde->deserialize_one_cell_from_hive_text(column, slice, _options);
}

Status TextReader::_create_line_reader() {
    std::shared_ptr<TextLineReaderContextIf> text_line_reader_ctx;

    text_line_reader_ctx = std::make_shared<PlainTextLineReaderCtx>(_line_delimiter,
                                                                    _line_delimiter_length, false);

    _fields_splitter = std::make_unique<HiveTextFieldSplitter>(
            false, false, _value_separator, _value_separator_length, -1, _escape);

    _line_reader =
            NewPlainTextLineReader::create_unique(_profile, _file_reader, _decompressor.get(),
                                                  text_line_reader_ctx, _size, _start_offset);

    return Status::OK();
}

Status TextReader::_validate_line(const Slice& line, bool* success) {
    // text file do not need utf8 check
    *success = true;
    return Status::OK();
}

Status TextReader::_deserialize_nullable_string(IColumn& column, Slice& slice) {
    auto& null_column = assert_cast<ColumnNullable&>(column);
    if (slice.compare(Slice(_options.null_format, _options.null_len)) == 0) {
        null_column.insert_data(nullptr, 0);
        return Status::OK();
    }
    static DataTypeStringSerDe stringSerDe;
    auto st = stringSerDe.deserialize_one_cell_from_hive_text(null_column.get_nested_column(),
                                                              slice, _options);
    if (!st.ok()) {
        // fill null if fail
        null_column.insert_data(nullptr, 0); // 0 is meaningless here
        return Status::OK();
    }
    // fill not null if success
    null_column.get_null_map_data().push_back(0);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
