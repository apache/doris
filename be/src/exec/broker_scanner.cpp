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

#include <sstream>
#include <iostream>

#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "exprs/expr.h"
#include "exec/text_converter.h"
#include "exec/text_converter.hpp"
#include "exec/plain_text_line_reader.h"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"
#include "exec/decompressor.h"

namespace doris {

BrokerScanner::BrokerScanner(RuntimeState* state,
                             RuntimeProfile* profile,
                             const TBrokerScanRangeParams& params, 
                             const std::vector<TBrokerRangeDesc>& ranges,
                             const std::vector<TNetworkAddress>& broker_addresses,
                             BrokerScanCounter* counter) : 
        _state(state),
        _profile(profile),
        _params(params),
        _ranges(ranges),
        _broker_addresses(broker_addresses),
        // _splittable(params.splittable),
        _value_separator(static_cast<char>(params.column_separator)),
        _line_delimiter(static_cast<char>(params.line_delimiter)),
        _cur_file_reader(nullptr),
        _cur_line_reader(nullptr),
        _cur_decompressor(nullptr),
        _next_range(0),
        _cur_line_reader_eof(false),
        _scanner_eof(false),
        _skip_next_line(false),
        _src_tuple(nullptr),
        _src_tuple_row(nullptr),
#if BE_TEST
        _mem_tracker(new MemTracker()),
        _mem_pool(_mem_tracker.get()),
#else 
        _mem_tracker(new MemTracker(-1, "Broker Scanner", state->instance_mem_tracker())),
        _mem_pool(_state->instance_mem_tracker()),
#endif
        _dest_tuple_desc(nullptr),
        _counter(counter),
        _rows_read_counter(nullptr),
        _read_timer(nullptr),
        _materialize_timer(nullptr) {
}

BrokerScanner::~BrokerScanner() {
    close();
}

Status BrokerScanner::init_expr_ctxes() {
    // Constcut _src_slot_descs
    const TupleDescriptor* src_tuple_desc = 
        _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status(ss.str());
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (auto slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status(ss.str());
        }
        _src_slot_descs.emplace_back(it->second);
    }
    // Construct source tuple and tuple row
    _src_tuple = (Tuple*) _mem_pool.allocate(src_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*) _mem_pool.allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
    _row_desc.reset(new RowDescriptor(_state->desc_tbl(), 
                                      std::vector<TupleId>({_params.src_tuple_id}), 
                                      std::vector<bool>({false})));

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown dest tuple descriptor, tuple_id=" << _params.dest_tuple_id;
        return Status(ss.str());
    }

    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            std::stringstream ss;
            ss << "No expr for dest slot, id=" << slot_desc->id() 
                << ", name=" << slot_desc->col_name();
            return Status(ss.str());
        }
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
        RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker.get()));
        RETURN_IF_ERROR(ctx->open(_state));
        _dest_expr_ctx.emplace_back(ctx);
    }

    return Status::OK;
}

Status BrokerScanner::open() {
    RETURN_IF_ERROR(init_expr_ctxes());
    _text_converter.reset(new(std::nothrow) TextConverter('\\'));
    if (_text_converter == nullptr) {
        return Status("No memory error.");
    }

    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_profile, "MaterializeTupleTime(*)");

    return Status::OK;
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
        RETURN_IF_ERROR(_cur_line_reader->read_line(
                &ptr, &size, &_cur_line_reader_eof));
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
            _counter->num_rows_total++;
            if (convert_one_row(Slice(ptr, size), tuple, tuple_pool)) {
                break;
            }
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK;
}

Status BrokerScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK;
    }

    RETURN_IF_ERROR(open_file_reader());
    RETURN_IF_ERROR(open_line_reader());
    _next_range++;
    
    return Status::OK;
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
    case TFileType::FILE_BROKER: {
        BrokerReader* broker_reader = new BrokerReader(
            _state->exec_env(), _broker_addresses, _params.properties, range.path, start_offset);
        RETURN_IF_ERROR(broker_reader->open());
        _cur_file_reader = broker_reader;
        break;
    }
    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG(3) << "unknown stream load id: " << UniqueId(range.load_id);
            return Status("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
    default: {
        std::stringstream ss;
        ss << "Unknown file type, type=" << range.file_type;
        return Status(ss.str());
    }
    }
    return Status::OK;
}

Status BrokerScanner::create_decompressor(TFileFormatType::type type) {
    if (_cur_decompressor == nullptr) {
        delete _cur_decompressor;
        _cur_decompressor = nullptr;
    }

    CompressType compress_type;
    switch (type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
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
    default: {
        std::stringstream ss;
        ss << "Unknown format type, type=" << type;
        return Status(ss.str());
    }
    }
    RETURN_IF_ERROR(Decompressor::create_decompressor(
            compress_type, &_cur_decompressor));

    return Status::OK;
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
            return Status(ss.str());
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
        _cur_line_reader = new PlainTextLineReader(
                _profile,
                _cur_file_reader, _cur_decompressor,
                size, _line_delimiter);
        break;
    default: {
        std::stringstream ss;
        ss << "Unknown format type, type=" << range.format_type;
        return Status(ss.str());
    }
    }

    _cur_line_reader_eof = false;

    return Status::OK;
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
    Expr::close(_dest_expr_ctx, _state);
}

void BrokerScanner::split_line(
        const Slice& line, std::vector<Slice>* values) {
    // line-begin char and line-end char are considered to be 'delimeter'
    const char* value = line.data;
    const char* ptr = line.data;
    for (size_t i = 0; i < line.size; ++i, ++ptr) {
        if (*ptr == _value_separator) {
            values->emplace_back(value, ptr - value);
            value = ptr + 1;
        }
    }
    values->emplace_back(value, ptr - value);
}

void BrokerScanner::fill_fix_length_string(
        const Slice& value, MemPool* pool,
        char** new_value_p, const int new_value_length) {
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
bool BrokerScanner::check_decimal_input(
        const Slice& slice,
        int precision, int scale,
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
    value_frac_len = end_index- point_index;

    if (point_index == -1) {
        // an int value: like 123
        value_int_len = end_index - begin_index + 1;
        value_frac_len = 0;
    } else {
        value_int_len = point_index - begin_index;
        value_frac_len = end_index- point_index;
    }

    if (value_int_len > (precision - scale)) {
        (*error_msg) << "the int part length longer than schema precision ["
                << precision << "]. "
                << "value [" << slice.to_string() << "]. ";
        return false;
    } else if (value_frac_len > scale) {
        (*error_msg) << "the frac part length longer than schema scale ["
                << scale << "]. "
                << "value [" << slice.to_string() << "]. ";
        return false;
    }
    return true;
}

bool is_null(const Slice& slice) {
    return slice.size == 2 && 
        slice.data[0] == '\\' && 
        slice.data[1] == 'N';
}

// Writes a slot in _tuple from an value containing text data.
bool BrokerScanner::write_slot(
        const std::string& column_name, const TColumnType& column_type,
        const Slice& value, const SlotDescriptor* slot,
        Tuple* tuple, MemPool* tuple_pool,
        std::stringstream* error_msg) {

    if (value.size == 0 && !slot->type().is_string_type()) {
        (*error_msg) << "the length of input should not be 0. "
                << "column_name: " << column_name << "; "
                << "type: " << slot->type();
        return false;
    }

    char* value_to_convert = value.data;
    size_t value_to_convert_length = value.size;

    // Fill all the spaces if it is 'TYPE_CHAR' type
    if (slot->type().is_string_type()) {
        int char_len = column_type.len;
        if (value.size > char_len) {
            (*error_msg) << "the length of input is too long than schema. "
                    << "column_name: " << column_name << "; "
                    << "input_str: [" << value.to_string() << "] "
                    << "type: " << slot->type() << "; "
                    << "schema length: " << char_len << "; "
                    << "actual length: " << value.size << "; ";
            return false;
        }
        if (slot->type().type == TYPE_CHAR && value.size < char_len) {
            if (!is_null(value)) {
                fill_fix_length_string(
                        value, tuple_pool,
                        &value_to_convert, char_len);
                value_to_convert_length = char_len;
            }
        }
    } else if (slot->type().is_decimal_type()) {
        bool is_success = check_decimal_input(
            value, column_type.precision, column_type.scale, error_msg);
        if (is_success == false) {
            return false;
        }
    }

    if (!_text_converter->write_slot(
            slot, tuple, value_to_convert, value_to_convert_length,
            true, false, tuple_pool)) {
        (*error_msg) << "convert csv string to "
            << slot->type() << " failed. "
            << "column_name: " << column_name << "; "
            << "input_str: [" << value.to_string() << "]; ";
        return false;
    }

    return true;
}

// Convert one row to this tuple
bool BrokerScanner::convert_one_row(
        const Slice& line,
        Tuple* tuple, MemPool* tuple_pool) {
    if (!line_to_src_tuple(line)) {
        return false;
    }
    return fill_dest_tuple(line, tuple, tuple_pool);
}

// Convert one row to this tuple
bool BrokerScanner::line_to_src_tuple(const Slice& line) {
    std::vector<Slice> values;
    {
        split_line(line, &values);
    }

    if (values.size() < _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is less than schema column number. "
            << "actual number: " << values.size() << " sep: " << _value_separator << ", "
            << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size),
                                         error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    } else if (values.size() > _src_slot_descs.size()) {
        std::stringstream error_msg;
        error_msg << "actual column number is more than schema column number. "
            << "actual number: " << values.size() << " sep: " << _value_separator << ", "
            << "schema number: " << _src_slot_descs.size() << "; ";
        _state->append_error_msg_to_file(std::string(line.data, line.size),
                                         error_msg.str());
        _counter->num_rows_filtered++;
        return false;
    }

    for (int i = 0; i < values.size(); ++i) {
        auto slot_desc = _src_slot_descs[i];
        const Slice& value = values[i];
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

    return true;
}

bool BrokerScanner::fill_dest_tuple(const Slice& line, Tuple* dest_tuple, MemPool* mem_pool) {
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        ExprContext* ctx = _dest_expr_ctx[ctx_idx++];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            if (slot_desc->is_nullable()) {
                dest_tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            } else {
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is null";
                _state->append_error_msg_to_file(
                    std::string(line.data, line.size), error_msg.str());
                _counter->num_rows_filtered++;
                return false;
            }
        }
        dest_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = dest_tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), mem_pool);
    }
    return true;
}

}
