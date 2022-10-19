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

#include "exec/broker_scanner.h"

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>

#include <iostream>
#include <sstream>

#include "common/consts.h"
#include "exec/decompressor.h"
#include "exec/plain_binary_line_reader.h"
#include "exec/plain_text_line_reader.h"
#include "io/file_factory.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/tuple.h"
#include "util/string_util.h"
#include "util/utf8_check.h"

namespace doris {

BrokerScanner::BrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                             const TBrokerScanRangeParams& params,
                             const std::vector<TBrokerRangeDesc>& ranges,
                             const std::vector<TNetworkAddress>& broker_addresses,
                             const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_decompressor(nullptr),
          _cur_line_reader_eof(false),
          _skip_lines(0) {
    if (params.__isset.column_separator_length && params.column_separator_length > 1) {
        _value_separator = params.column_separator_str;
        _value_separator_length = params.column_separator_length;
    } else {
        _value_separator.push_back(static_cast<char>(params.column_separator));
        _value_separator_length = 1;
    }
    if (params.__isset.line_delimiter_length && params.line_delimiter_length > 1) {
        _line_delimiter = params.line_delimiter_str;
        _line_delimiter_length = params.line_delimiter_length;
    } else {
        _line_delimiter.push_back(static_cast<char>(params.line_delimiter));
        _line_delimiter_length = 1;
    }
    _split_values.reserve(sizeof(Slice) * params.src_slot_ids.size());
}

BrokerScanner::~BrokerScanner() {
    close();
}

Status BrokerScanner::open() {
    RETURN_IF_ERROR(BaseScanner::open()); // base default function
    return Status::OK();
}

Status BrokerScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) {
    SCOPED_TIMER(_read_timer);
    // Get one line
    while (!_scanner_eof) {
        if (_cur_line_reader == nullptr || _cur_line_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
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
            SCOPED_TIMER(_materialize_timer);
            RETURN_IF_ERROR(_convert_one_row(Slice(ptr, size), tuple, tuple_pool, fill_tuple));
            break; // break always
        }
    }

    *eof = _scanner_eof;
    return Status::OK();
}

Status BrokerScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(open_file_reader());
    RETURN_IF_ERROR(open_line_reader());
    _next_range++;

    return Status::OK();
}

Status BrokerScanner::open_file_reader() {
    const TBrokerRangeDesc& range = _ranges[_next_range];
    int64_t start_offset = range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }
    //means first range, skip
    if (start_offset == 0 && range.header_type.size() > 0) {
        std::string header_type = to_lower(range.header_type);
        if (header_type == BeConsts::CSV_WITH_NAMES) {
            _skip_lines = 1;
        } else if (header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
            _skip_lines = 2;
        }
    }

    if (range.file_type == TFileType::FILE_STREAM) {
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(range.load_id, _cur_file_reader_s));
        _real_reader = _cur_file_reader_s.get();
    } else {
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                range.file_type, _state->exec_env(), _profile, _broker_addresses,
                _params.properties, range, start_offset, _cur_file_reader));
        _real_reader = _cur_file_reader.get();
    }
    return _real_reader->open();
}

Status BrokerScanner::create_decompressor(TFileFormatType::type type) {
    if (_cur_decompressor != nullptr) {
        delete _cur_decompressor;
        _cur_decompressor = nullptr;
    }

    CompressType compress_type;
    switch (type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_JSON:
    case TFileFormatType::FORMAT_PROTO:
        compress_type = CompressType::UNCOMPRESSED;
        break;
    case TFileFormatType::FORMAT_CSV_GZ:
        compress_type = CompressType::GZIP;
        break;
    case TFileFormatType::FORMAT_CSV_BZ2:
        compress_type = CompressType::BZIP2;
        break;
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        compress_type = CompressType::LZ4FRAME;
        break;
    case TFileFormatType::FORMAT_CSV_LZOP:
        compress_type = CompressType::LZOP;
        break;
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        compress_type = CompressType::DEFLATE;
        break;
    default: {
        return Status::InternalError("Unknown format type, cannot inference compress type, type={}",
                                     type);
    }
    }
    RETURN_IF_ERROR(Decompressor::create_decompressor(compress_type, &_cur_decompressor));

    return Status::OK();
}

Status BrokerScanner::open_line_reader() {
    if (_cur_decompressor != nullptr) {
        delete _cur_decompressor;
        _cur_decompressor = nullptr;
    }

    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    int64_t size = range.size;
    if (range.start_offset != 0) {
        if (range.format_type != TFileFormatType::FORMAT_CSV_PLAIN) {
            return Status::InternalError("For now we do not support split compressed file");
        }
        size += 1;
        // not first range will always skip one line
        _skip_lines = 1;
    }

    // create decompressor.
    // _decompressor may be nullptr if this is not a compressed file
    RETURN_IF_ERROR(create_decompressor(range.format_type));

    _file_format_type = range.format_type;
    // open line reader
    switch (range.format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        _cur_line_reader = new PlainTextLineReader(_profile, _real_reader, _cur_decompressor, size,
                                                   _line_delimiter, _line_delimiter_length);
        break;
    case TFileFormatType::FORMAT_PROTO:
        _cur_line_reader = new PlainBinaryLineReader(_real_reader);
        break;
    default: {
        return Status::InternalError("Unknown format type, cannot init line reader, type={}",
                                     range.format_type);
    }
    }

    _cur_line_reader_eof = false;

    return Status::OK();
}

void BrokerScanner::close() {
    BaseScanner::close();
    if (_cur_decompressor != nullptr) {
        delete _cur_decompressor;
        _cur_decompressor = nullptr;
    }

    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }
}

void BrokerScanner::split_line(const Slice& line) {
    _split_values.clear();
    if (_file_format_type == TFileFormatType::FORMAT_PROTO) {
        PDataRow** ptr = reinterpret_cast<PDataRow**>(line.data);
        PDataRow* row = *ptr;
        for (const PDataColumn& col : (row)->col()) {
            int len = col.value().size();
            uint8_t* buf = new uint8_t[len];
            memcpy(buf, col.value().c_str(), len);
            _split_values.emplace_back(buf, len);
        }
        delete row;
        delete[] ptr;
    } else {
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
                    _split_values.emplace_back(value + start, non_space - start);
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
        _split_values.emplace_back(value + start, non_space - start);
    }
}

void BrokerScanner::fill_fix_length_string(const Slice& value, MemPool* pool, char** new_value_p,
                                           const int new_value_length) {
    if (new_value_length != 0 && value.size < new_value_length) {
        *new_value_p = reinterpret_cast<char*>(pool->allocate(new_value_length));

        // 'value' is guaranteed not to be nullptr
        memcpy(*new_value_p, value.data, value.size);
        for (int i = value.size; i < new_value_length; ++i) {
            (*new_value_p)[i] = '\0';
        }
    }
}

// Following format are included.
//      .123    1.23    123.   -1.23
// ATTN: The decimal point and (for negative numbers) the "-" sign are not counted.
//      like '.123', it will be regarded as '0.123', but it match decimal(3, 3)
bool BrokerScanner::check_decimal_input(const Slice& slice, int precision, int scale,
                                        std::stringstream* error_msg) {
    const char* value = slice.data;
    size_t value_length = slice.size;

    if (value_length > (precision + 2)) {
        (*error_msg) << "the length of decimal value is overflow. "
                     << "precision in schema: (" << precision << ", " << scale << "); "
                     << "value: [" << slice.to_string() << "]; "
                     << "str actual length: " << value_length << ";";
        return false;
    }

    // ignore leading spaces and trailing spaces
    int begin_index = 0;
    while (begin_index < value_length && std::isspace(value[begin_index])) {
        ++begin_index;
    }
    int end_index = value_length - 1;
    while (end_index >= begin_index && std::isspace(value[end_index])) {
        --end_index;
    }

    if (value[begin_index] == '+' || value[begin_index] == '-') {
        ++begin_index;
    }

    int point_index = -1;
    for (int i = begin_index; i <= end_index; ++i) {
        if (value[i] == '.') {
            point_index = i;
        }
    }

    int value_int_len = 0;
    int value_frac_len = 0;
    value_int_len = point_index - begin_index;
    value_frac_len = end_index - point_index;

    if (point_index == -1) {
        // an int value: like 123
        value_int_len = end_index - begin_index + 1;
        value_frac_len = 0;
    } else {
        value_int_len = point_index - begin_index;
        value_frac_len = end_index - point_index;
    }

    if (value_int_len > (precision - scale)) {
        (*error_msg) << "the int part length longer than schema precision [" << precision << "]. "
                     << "value [" << slice.to_string() << "]. ";
        return false;
    } else if (value_frac_len > scale) {
        (*error_msg) << "the frac part length longer than schema scale [" << scale << "]. "
                     << "value [" << slice.to_string() << "]. ";
        return false;
    }
    return true;
}

// Convert one row to this tuple
Status BrokerScanner::_convert_one_row(const Slice& line, Tuple* tuple, MemPool* tuple_pool,
                                       bool* fill_tuple) {
    RETURN_IF_ERROR(_line_to_src_tuple(line));
    if (!_success) {
        // If not success, which means we met an invalid row, return.
        *fill_tuple = false;
        return Status::OK();
    }

    return fill_dest_tuple(tuple, tuple_pool, fill_tuple);
}

Status BrokerScanner::_line_split_to_values(const Slice& line) {
    bool is_proto_format = _file_format_type == TFileFormatType::FORMAT_PROTO;
    if (!is_proto_format && !validate_utf8(line.data, line.size)) {
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

    split_line(line);

    // range of current file
    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    bool read_by_column_def = false;
    if (range.__isset.read_by_column_def) {
        read_by_column_def = range.read_by_column_def;
    }
    const std::vector<std::string>& columns_from_path = range.columns_from_path;
    // read data by column defination, resize _split_values to _src_solt_size
    if (read_by_column_def) {
        // fill slots by NULL
        while (_split_values.size() + columns_from_path.size() < _src_slot_descs.size()) {
            _split_values.emplace_back(_split_values.back().get_data(), 0);
        }
        // remove redundant slots
        while (_split_values.size() + columns_from_path.size() > _src_slot_descs.size()) {
            _split_values.pop_back();
        }
    } else {
        if (_split_values.size() + columns_from_path.size() < _src_slot_descs.size()) {
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string {
                        return is_proto_format ? "" : std::string(line.data, line.size);
                    },
                    [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "{}",
                                       "actual column number is less than schema column number.");
                        fmt::format_to(error_msg, "actual number: {}, column separator: [{}], ",
                                       _split_values.size(), _value_separator);
                        fmt::format_to(error_msg, "line delimiter: [{}], schema number: {}; ",
                                       _line_delimiter, _src_slot_descs.size());
                        return fmt::to_string(error_msg);
                    },
                    &_scanner_eof));
            _counter->num_rows_filtered++;
            _success = false;
            return Status::OK();
        } else if (_split_values.size() + columns_from_path.size() > _src_slot_descs.size()) {
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string {
                        return is_proto_format ? "" : std::string(line.data, line.size);
                    },
                    [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "{}",
                                       "actual column number is more than schema column number.");
                        fmt::format_to(error_msg, "actual number: {}, column separator: [{}], ",
                                       _split_values.size(), _value_separator);
                        fmt::format_to(error_msg, "line delimiter: [{}], schema number: {}; ",
                                       _line_delimiter, _src_slot_descs.size());
                        return fmt::to_string(error_msg);
                    },
                    &_scanner_eof));
            _counter->num_rows_filtered++;
            _success = false;
            return Status::OK();
        }
    }

    _success = true;
    return Status::OK();
}

// Convert one row to this tuple
Status BrokerScanner::_line_to_src_tuple(const Slice& line) {
    RETURN_IF_ERROR(_line_split_to_values(line));
    if (!_success) {
        return Status::OK();
    }

    if (!check_array_format(_split_values)) {
        return Status::OK();
    }

    for (int i = 0; i < _split_values.size(); ++i) {
        auto slot_desc = _src_slot_descs[i];
        const Slice& value = _split_values[i];
        if (slot_desc->is_nullable() && is_null(value)) {
            _src_tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        _src_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = _src_tuple->get_slot(slot_desc->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = value.data;
        str_slot->len = value.size;
    }

    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    if (range.__isset.num_of_columns_from_file) {
        fill_slots_of_columns_from_path(range.num_of_columns_from_file, range.columns_from_path);
    }

    return Status::OK();
}

} // namespace doris
