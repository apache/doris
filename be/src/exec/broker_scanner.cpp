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

#include <iostream>
#include <sstream>

#include "exec/broker_reader.h"
#include "exec/buffered_reader.h"
#include "exec/decompressor.h"
#include "exec/exec_node.h"
#include "exec/local_file_reader.h"
#include "exec/plain_text_line_reader.h"
#include "exec/s3_reader.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "util/utf8_check.h"

#if defined(__x86_64__)
#include "exec/hdfs_file_reader.h"
#endif

namespace doris {

BrokerScanner::BrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                             const TBrokerScanRangeParams& params,
                             const std::vector<TBrokerRangeDesc>& ranges,
                             const std::vector<TNetworkAddress>& broker_addresses,
                             const std::vector<ExprContext*>& pre_filter_ctxs,
                             ScannerCounter* counter)
        : BaseScanner(state, profile, params, pre_filter_ctxs, counter),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_decompressor(nullptr),
          _next_range(0),
          _cur_line_reader_eof(false),
          _scanner_eof(false),
          _skip_next_line(false) {
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
    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
    if (_text_converter == nullptr) {
        return Status::InternalError("No memory error.");
    }
    return Status::OK();
}

Status BrokerScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
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
        if (_skip_next_line) {
            _skip_next_line = false;
            continue;
        }
        if (size == 0) {
            // Read empty row, just continue
            continue;
        }
        {
            COUNTER_UPDATE(_rows_read_counter, 1);
            SCOPED_TIMER(_materialize_timer);
            if (convert_one_row(Slice(ptr, size), tuple, tuple_pool)) {
                free_expr_local_allocations();
                break;
            }
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
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
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    int64_t start_offset = range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }
    switch (range.file_type) {
    case TFileType::FILE_LOCAL: {
        LocalFileReader* file_reader = new LocalFileReader(range.path, start_offset);
        RETURN_IF_ERROR(file_reader->open());
        _cur_file_reader = file_reader;
        break;
    }
    case TFileType::FILE_HDFS: {
#if defined(__x86_64__)
        BufferedReader* file_reader =
                new BufferedReader(_profile, new HdfsFileReader(range.hdfs_params, range.path, start_offset));
        RETURN_IF_ERROR(file_reader->open());
        _cur_file_reader = file_reader;
        break;
#else
        return Status::InternalError("HdfsFileReader do not support on non x86 platform");
#endif
    }
    case TFileType::FILE_BROKER: {
        BrokerReader* broker_reader =
                new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
                                 range.path, start_offset);
        RETURN_IF_ERROR(broker_reader->open());
        _cur_file_reader = broker_reader;
        break;
    }
    case TFileType::FILE_S3: {
        BufferedReader* s3_reader =
                new BufferedReader(_profile, new S3Reader(_params.properties, range.path, start_offset));
        RETURN_IF_ERROR(s3_reader->open());
        _cur_file_reader = s3_reader;
        break;
    }
    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG_NOTICE << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
    default: {
        std::stringstream ss;
        ss << "Unknown file type, type=" << range.file_type;
        return Status::InternalError(ss.str());
    }
    }
    return Status::OK();
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
        std::stringstream ss;
        ss << "Unknown format type, cannot inference compress type, type=" << type;
        return Status::InternalError(ss.str());
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
            std::stringstream ss;
            ss << "For now we do not support split compressed file";
            return Status::InternalError(ss.str());
        }
        size += 1;
        _skip_next_line = true;
    } else {
        _skip_next_line = false;
    }

    // create decompressor.
    // _decompressor may be NULL if this is not a compressed file
    RETURN_IF_ERROR(create_decompressor(range.format_type));

    // open line reader
    switch (range.format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        _cur_line_reader = new PlainTextLineReader(_profile, _cur_file_reader, _cur_decompressor,
                                                   size, _line_delimiter, _line_delimiter_length);
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

void BrokerScanner::close() {
    if (_cur_decompressor != nullptr) {
        delete _cur_decompressor;
        _cur_decompressor = nullptr;
    }

    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }
}

void BrokerScanner::split_line(const Slice& line) {
    _split_values.clear();
    const char* value = line.data;
    size_t start = 0;  // point to the start pos of next col value.
    size_t curpos = 0; // point to the start pos of separator matching sequence.
    size_t p1 = 0;     // point to the current pos of separator matching sequence.

    // Separator: AAAA
    //
    //   curpos
    //     ▼
    //     AAAA
    //   1000AAAA2000AAAA
    //   ▲   ▲
    // Start │
    //       p1

    while (curpos < line.size) {
        if (*(value + curpos + p1) != _value_separator[p1]) {
            // Not match, move forward:
            curpos += (p1 == 0 ? 1 : p1);
            p1 = 0;
        } else {
            p1++;
            if (p1 == _value_separator_length) {
                // Match a separator
                _split_values.emplace_back(value + start, curpos - start);
                start = curpos + _value_separator_length;
                curpos = start;
                p1 = 0;
            }
        }
    }

    CHECK(curpos == line.size) << curpos << " vs " << line.size;
    _split_values.emplace_back(value + start, curpos - start);
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

bool is_null(const Slice& slice) {
    return slice.size == 2 && slice.data[0] == '\\' && slice.data[1] == 'N';
}

// Convert one row to this tuple
bool BrokerScanner::convert_one_row(const Slice& line, Tuple* tuple, MemPool* tuple_pool) {
    if (!line_to_src_tuple(line)) {
        return false;
    }

    return fill_dest_tuple(tuple, tuple_pool);
}

// Convert one row to this tuple
bool BrokerScanner::line_to_src_tuple(const Slice& line) {
    if (!validate_utf8(line.data, line.size)) {
        std::stringstream error_msg;
        error_msg << "data is not encoded by UTF-8";
        _state->append_error_msg_to_file("Unable to display", error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    }

    split_line(line);

    // range of current file
    const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
    const std::vector<std::string>& columns_from_path = range.columns_from_path;
    if (_split_values.size() + columns_from_path.size() < _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is less than schema column number. "
                  << "actual number: " << _split_values.size() << " column separator: ["
                  << _value_separator << "], "
                  << "line delimiter: [" << _line_delimiter << "], "
                  << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size), error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    } else if (_split_values.size() + columns_from_path.size() > _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is more than schema column number. "
                  << "actual number: " << _split_values.size() << " column separator: ["
                  << _value_separator << "], "
                  << "line delimiter: [" << _line_delimiter << "], "
                  << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size), error_msg.str());
        _counter->num_rows_filtered++;
        return false;
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

    if (range.__isset.num_of_columns_from_file) {
        fill_slots_of_columns_from_path(range.num_of_columns_from_file, columns_from_path);
    }

    return true;
}

} // namespace doris
