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

#include "format_v2/delimited_text/text_reader.h"

#include <cstring>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "format/file_reader/new_plain_text_line_reader.h"
#include "runtime/descriptors.h"
#include "util/decompressor.h"

namespace doris::format::text {
namespace {

bool starts_with_at(const Slice& line, size_t pos, const std::string& needle) {
    return !needle.empty() && pos + needle.size() <= line.size &&
           std::memcmp(line.data + pos, needle.data(), needle.size()) == 0;
}

} // namespace

TextReader::TextReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                       std::unique_ptr<io::FileDescription>& file_description,
                       std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                       const TFileScanRangeParams* scan_params,
                       const std::vector<SlotDescriptor*>& file_slot_descs,
                       TFileCompressType::type range_compress_type,
                       std::optional<TUniqueId> stream_load_id)
        : DelimitedTextReader(system_properties, file_description, std::move(io_ctx), profile,
                              scan_params, file_slot_descs, range_compress_type,
                              std::move(stream_load_id), "Text") {}

TextReader::~TextReader() = default;

Status TextReader::_init_format_state() {
    _file_compress_type =
            _range_compress_type != TFileCompressType::UNKNOWN
                    ? _range_compress_type
                    : (_scan_params->__isset.compress_type ? _scan_params->compress_type
                                                           : TFileCompressType::PLAIN);

    const auto& text_params = _scan_params->file_attributes.text_params;
    _value_separator = text_params.column_separator;
    _line_delimiter = text_params.line_delimiter;
    if (text_params.__isset.escape) {
        _escape = text_params.escape;
    }
    _options.escape_char = _escape;
    _options.collection_delim =
            text_params.collection_delimiter.empty() ? ',' : text_params.collection_delimiter[0];
    _options.map_key_delim =
            text_params.mapkv_delimiter.empty() ? ':' : text_params.mapkv_delimiter[0];
    if (text_params.__isset.null_format) {
        _options.null_format = text_params.null_format.data();
        _options.null_len = text_params.null_format.length();
    }
    return Status::OK();
}

Status TextReader::_create_decompressor() {
    return Decompressor::create_decompressor(_file_compress_type, &_decompressor);
}

Status TextReader::_create_line_reader() {
    auto text_line_reader_ctx = std::make_shared<PlainTextLineReaderCtx>(
            _line_delimiter, _line_delimiter.size(), false);
    _line_reader = NewPlainTextLineReader::create_unique(
            _profile, _file_reader, _decompressor.get(), std::move(text_line_reader_ctx), _size,
            _start_offset);
    return Status::OK();
}

void TextReader::_split_line(const Slice& line) {
    _split_values.clear();
    if (_value_separator.size() == 1) {
        _split_line_single_char(line);
    } else {
        _split_line_multi_char(line);
    }
}

void TextReader::_split_line_single_char(const Slice& line) {
    size_t value_start = 0;
    for (size_t i = 0; i < line.size; ++i) {
        if (line.data[i] == _value_separator[0]) {
            // Hive text lets a string escape the field separator. The backslash remains in the
            // field slice so deserialize_one_cell_from_hive_text() can unescape the final value.
            if (_escape != 0 && i > 0 && line.data[i - 1] == _escape) {
                continue;
            }
            _split_values.emplace_back(line.data + value_start, i - value_start);
            value_start = i + _value_separator.size();
        }
    }
    _split_values.emplace_back(line.data + value_start, line.size - value_start);
}

void TextReader::_split_line_multi_char(const Slice& line) {
    size_t value_start = 0;
    size_t i = 0;
    while (i < line.size) {
        if (starts_with_at(line, i, _value_separator)) {
            if (_escape != 0 && i > 0 && line.data[i - 1] == _escape) {
                ++i;
                continue;
            }
            _split_values.emplace_back(line.data + value_start, i - value_start);
            i += _value_separator.size();
            value_start = i;
            continue;
        }
        ++i;
    }
    _split_values.emplace_back(line.data + value_start, line.size - value_start);
}

Status TextReader::_deserialize_one_cell(const RequestedColumn& column, IColumn* output,
                                         Slice value) {
    DORIS_CHECK(output != nullptr);
    if (column.nullable_string_fast_path) {
        auto& null_column = assert_cast<ColumnNullable&>(*output);
        if (_options.null_len > 0 && value.size == _options.null_len &&
            std::memcmp(value.data, _options.null_format, value.size) == 0) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
        static DataTypeStringSerDe string_serde(TYPE_STRING);
        auto status = string_serde.deserialize_one_cell_from_hive_text(
                null_column.get_nested_column(), value, _options);
        if (!status.ok()) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
        null_column.get_null_map_data().push_back(0);
        return Status::OK();
    }
    return column.serde->deserialize_one_cell_from_hive_text(*output, value, _options);
}

} // namespace doris::format::text
