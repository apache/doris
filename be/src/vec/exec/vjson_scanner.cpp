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

#include "vec/exec/vjson_scanner.h"

#include <algorithm>
#include <fmt/format.h>

#include "env/env.h"
#include "exec/broker_reader.h"
#include "exec/buffered_reader.h"
#include "exec/local_file_reader.h"
#include "exec/plain_text_line_reader.h"
#include "exec/s3_reader.h"
#include "exprs/expr.h"
#include "exprs/json_functions.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/time.h"

namespace doris::vectorized {

VJsonScanner::VJsonScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : JsonScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_vjson_reader(nullptr),
          _next_range(0),
          _cur_reader_eof(false),
          _read_json_by_line(false) { 
}

VJsonScanner::~VJsonScanner() {
    close();
}

Status VJsonScanner::open() {
    RETURN_IF_ERROR(BaseScanner::open());
    return Status::OK();
}

void VJsonScanner::close() { 
    BaseScanner::close();
    if (_cur_vjson_reader != nullptr) {
        delete _cur_vjson_reader;
        _cur_vjson_reader = nullptr;
    }
}

Status VJsonScanner::get_next(vectorized::Block& output_block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    Status status = Status::OK();
    const int batch_size = _state->batch_size();
    size_t slot_num = _src_slot_descs.size();
    std::shared_ptr<vectorized::Block> temp_block(new vectorized::Block());
    std::vector<vectorized::MutableColumnPtr> columns(slot_num);
    auto string_type = make_nullable(std::make_shared<DataTypeString>());
    for (int i = 0; i < slot_num; i++) {
        columns[i] = string_type->create_column();
    }

    // Get one line
    while (columns[0]->size() < batch_size && !_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }
        
        if (_read_json_by_line && _skip_next_line) {
            size_t size = 0;
            const uint8_t* line_ptr = nullptr;
            RETURN_IF_ERROR(_cur_line_reader->read_line(&line_ptr, &size, &_cur_reader_eof));
            _skip_next_line = false;
            continue;
        }

        bool is_empty_row = false;
        RETURN_IF_ERROR(_cur_vjson_reader->read_json_column(columns, _src_slot_descs,  
                                                        &is_empty_row, &_cur_reader_eof));
        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
    }

    if (columns[0]->size() > 0) {
        if (!_dest_vexpr_ctx.empty()) {
            auto n_columns = 0;
            for (const auto slot_desc : _src_slot_descs) {
                temp_block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
            }

            // filter src tuple by preceding filter first
            if (!_vpre_filter_ctxs.empty()) {
                for (auto _vpre_filter_ctx : _vpre_filter_ctxs) {
                    RETURN_IF_ERROR(VExprContext::filter_block(_vpre_filter_ctx, &output_block, slot_num));
                }
            }

            // Do vectorized expr here to speed up load
            output_block = VExprContext::get_output_block_after_execute_exprs(_dest_vexpr_ctx,
                             *(temp_block.get()), status);
            if (UNLIKELY(output_block.rows() == 0)) {
                return status;
            }
        } else {
            auto n_columns = 0;
            for (const auto slot_desc : _src_slot_descs) {
                output_block.insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
            }
            
            // filter src tuple by preceding filter first
            if (!_vpre_filter_ctxs.empty()) { 
                for (auto _vpre_filter_ctx : _vpre_filter_ctxs) {
                    RETURN_IF_ERROR(VExprContext::filter_block(_vpre_filter_ctx, &output_block, slot_num));
                }
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

Status VJsonScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    
    // init the file reader
    RETURN_IF_ERROR(JsonScanner::open_file_reader());
    _cur_file_reader = JsonScanner::get_cur_file_reader();
    _cur_reader_eof = JsonScanner::get_cur_reader_eof();
    _read_json_by_line = JsonScanner::get_read_json_by_line();

    // init line reader
    if (_read_json_by_line) {
        RETURN_IF_ERROR(JsonScanner::open_line_reader());
        _cur_line_reader = JsonScanner::get_cur_line_reader();
        _cur_reader_eof = JsonScanner::get_cur_reader_eof();
        _skip_next_line = JsonScanner::get_skip_next_line();
    }

    RETURN_IF_ERROR(open_vjson_reader());
    _next_range++;

    return Status::OK();
}

Status VJsonScanner::open_vjson_reader() {
    if (_cur_vjson_reader != nullptr) {
        delete _cur_vjson_reader;
        _cur_vjson_reader = nullptr;
    }
    std::string json_root = "";
    std::string jsonpath = "";
    bool strip_outer_array = false;
    bool num_as_string = false;
    bool fuzzy_parse = false;

    const TBrokerRangeDesc& range = _ranges[_next_range];

    if (range.__isset.jsonpaths) {
        jsonpath = range.jsonpaths;
    }
    if (range.__isset.json_root) {
        json_root = range.json_root;
    }
    if (range.__isset.strip_outer_array) {
        strip_outer_array = range.strip_outer_array;
    }
    if (range.__isset.num_as_string) {
        num_as_string = range.num_as_string;
    }
    if (range.__isset.fuzzy_parse) {
        fuzzy_parse = range.fuzzy_parse;
    }
    
    if (_read_json_by_line) {
        _cur_vjson_reader = new VJsonReader(_state, _counter, _profile, strip_outer_array, num_as_string,
                               fuzzy_parse, &_scanner_eof, nullptr, _cur_line_reader);
    } else {
        _cur_vjson_reader =  new VJsonReader(_state, _counter, _profile, strip_outer_array, num_as_string,
                                        fuzzy_parse, &_scanner_eof, _cur_file_reader);
    }

    RETURN_IF_ERROR(_cur_vjson_reader->init(jsonpath, json_root));
    return Status::OK();
}

VJsonReader::VJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                        bool strip_outer_array, bool num_as_string,bool fuzzy_parse,
                        bool* scanner_eof, FileReader* file_reader, LineReader* line_reader)
               : JsonReader(state, counter, profile, strip_outer_array, num_as_string, fuzzy_parse,
                            scanner_eof, file_reader, line_reader),
                _vhandle_json_callback(nullptr),
                _next_line(0),
                _total_lines(0),
                _state(state),
                _counter(counter),
                _profile(profile),
                _strip_outer_array(strip_outer_array),
                _fuzzy_parse(fuzzy_parse),
                _json_doc(nullptr),
                _scanner_eof(scanner_eof) {
}

VJsonReader::~VJsonReader() {

}

Status VJsonReader::init(const std::string& jsonpath, const std::string& json_root) {
    // parse jsonpath
    if (!jsonpath.empty()) {
        Status st = JsonReader::_generate_json_paths(jsonpath, &_parsed_jsonpaths);
        RETURN_IF_ERROR(st);
    }
    if (!json_root.empty()) {
        JsonFunctions::parse_json_paths(json_root, &_parsed_json_root);
    }

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _vhandle_json_callback = &VJsonReader::_vhandle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _vhandle_json_callback = &VJsonReader::_vhandle_flat_array_complex_json;
        } else {
            _vhandle_json_callback = &VJsonReader::_vhandle_nested_complex_json;
        }
    }
    
    return Status::OK();
}

Status VJsonReader::read_json_column(std::vector<MutableColumnPtr>& columns, 
                                    const std::vector<SlotDescriptor*>& slot_descs,
                                    bool* is_empty_row, bool* eof) {
    return (this->*_vhandle_json_callback)(columns, slot_descs, is_empty_row, eof);
}

Status VJsonReader::_vhandle_simple_json(std::vector<MutableColumnPtr>& columns, 
                                                    const std::vector<SlotDescriptor*>& slot_descs,
                                                    bool* is_empty_row, bool* eof) {
    do {
        bool valid = false;
        if (_next_line >= _total_lines) { // parse json and generic document
            size_t size = 0;
            Status st = JsonReader::_parse_json_doc(&size, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st); // terminate if encounter other errors
            if (size == 0 || *eof) {          // read all data, then return
                *is_empty_row = true;
                return Status::OK();
            }
            _name_map.clear();
            rapidjson::Value* objectValue = nullptr;
            _json_doc = VJsonReader::get_json_doc();
            if (_json_doc->IsArray()) {
                _total_lines = _json_doc->Size();
                if (_total_lines == 0) {
                    // may be passing an empty json, such as "[]"
                    RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return JsonReader::_print_json_value(*_json_doc); },
                            [&]() -> std::string { return "Empty json line"; }, _scanner_eof));
                    _counter->num_rows_filtered++;
                    if (*_scanner_eof) {
                        *is_empty_row = true;
                        return Status::OK();
                    }
                    continue;
                }
                objectValue = &(*_json_doc)[0];
            } else {
                _total_lines = 1; // only one row
                objectValue = _json_doc;
            }
            _next_line = 0;
            if (_fuzzy_parse) {
                for (auto v : slot_descs) {
                    for (int i = 0; i < objectValue->MemberCount(); ++i) {
                        auto it = objectValue->MemberBegin() + i;
                        if (v->col_name() == it->name.GetString()) {
                            _name_map[v->col_name()] = i;
                            break;
                        }
                    }
                }
            }
        }

        if (_json_doc->IsArray()) { // handle case 1
            rapidjson::Value& objectValue = (*_json_doc)[_next_line]; // json object
            RETURN_IF_ERROR(_set_column_value(objectValue, columns, slot_descs, &valid));
        } else { // handle case 2
            RETURN_IF_ERROR(_set_column_value(*_json_doc, columns, slot_descs, &valid));
        }
        _next_line++;
        if (!valid) {
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                return Status::OK();
            }
            continue;
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

// for simple format json
// set valid to true and return OK if succeed.
// set valid to false and return OK if we met an invalid row.
// return other status if encounter other problmes.
Status VJsonReader::_set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                                    const std::vector<SlotDescriptor*>& slot_descs, bool* valid) {
    if (!objectValue.IsObject()) {
        // Here we expect the incoming `objectValue` to be a Json Object, such as {"key" : "value"},
        // not other type of Json format.
        RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return JsonReader::_print_json_value(objectValue); },
                [&]() -> std::string { return "Expect json object value"; }, _scanner_eof));
        _counter->num_rows_filtered++;
        *valid = false; // current row is invalid
        return Status::OK();
    }

    int nullcount = 0;
    int ctx_idx = 0;
    for (auto slot_desc : slot_descs) {
        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        rapidjson::Value::ConstMemberIterator it = objectValue.MemberEnd();

        if (_fuzzy_parse) {
            auto idx_it = _name_map.find(slot_desc->col_name());
            if (idx_it != _name_map.end() && idx_it->second < objectValue.MemberCount()) {
                it = objectValue.MemberBegin() + idx_it->second;
            }
        } else {
            it = objectValue.FindMember(
                    rapidjson::Value(slot_desc->col_name().c_str(), slot_desc->col_name().size()));
        }

        if (it != objectValue.MemberEnd()) {
            const rapidjson::Value& value = it->value;
            RETURN_IF_ERROR(_write_data_to_column(&value, slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        } else { // not found
            if (slot_desc->is_nullable()) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                nullable_column->insert_data(nullptr, 0);
                nullcount++;
                // LOG(INFO) << "not found in objectValue";
            } else {
               RETURN_IF_ERROR( _state->append_error_msg_to_file([&]() -> std::string { return JsonReader::_print_json_value(objectValue); },
                        [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "The column `{}` is not nullable, but it's not found in jsondata.", slot_desc->col_name());
                        return fmt::to_string(error_msg);
                        }, _scanner_eof));
                _counter->num_rows_filtered++;
                *valid = false; // current row is invalid
                break;
            }
        }
    }

    if (nullcount == slot_descs.size()) {
        RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return JsonReader::_print_json_value(objectValue); },
                [&]() -> std::string { return "All fields is null, this is a invalid row."; }, _scanner_eof));
        _counter->num_rows_filtered++;
        *valid = false;
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

Status VJsonReader::_write_data_to_column(rapidjson::Value::ConstValueIterator value, SlotDescriptor* slot_desc,
                                        vectorized::IColumn* column_ptr, bool* valid) {
    const char* str_value = nullptr;
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;
    
    if (slot_desc->is_nullable()) {
        auto* nullable_column =
            reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        nullable_column->get_null_map_data().push_back(0);
        column_ptr = &nullable_column->get_nested_column();
    }

    switch (value->GetType()) {
    case rapidjson::Type::kStringType:
        str_value = value->GetString();
        wbytes = strlen(str_value);
        break;
    case rapidjson::Type::kNumberType:
        if (value->IsUint()) { 
            wbytes = sprintf((char *)tmp_buf, "%u", value->GetUint());
            str_value = (char *)tmp_buf;
        } else if (value->IsInt()) {
            wbytes = sprintf((char *)tmp_buf, "%d", value->GetInt());
            str_value = (char *)tmp_buf;
        } else if (value->IsUint64()) {
            wbytes = sprintf((char *)tmp_buf, "%lu", value->GetUint64());
            str_value = (char *)tmp_buf;
        } else if (value->IsInt64()) {
            wbytes = sprintf((char *)tmp_buf, "%ld", value->GetInt64());
            str_value = (char *)tmp_buf;
        } else {
            wbytes = sprintf((char *)tmp_buf, "%f", value->GetDouble());
            str_value = (char *)tmp_buf;
        }
        break;
    case rapidjson::Type::kFalseType:
        wbytes = 1;
        str_value = (char *)"0";
        break;
    case rapidjson::Type::kTrueType:
        wbytes = 1;
        str_value = (char *)"1";
        break;
    case rapidjson::Type::kNullType:
        if (slot_desc->is_nullable()) {
            auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
            nullable_column->insert_data(nullptr, 0);
        } else {
            RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return JsonReader::_print_json_value(*value); },
                    [&]() -> std::string {
                    fmt::memory_buffer error_msg;
                    fmt::format_to(error_msg, "Json value is null, but the column `{}` is not nullable.", slot_desc->col_name());
                    return fmt::to_string(error_msg);
                    }, _scanner_eof));
            _counter->num_rows_filtered++;
            *valid = false;
            return Status::OK();
        }
        break;
    default:
        // for other type like array or object. we convert it to string to save
        std::string json_str = JsonReader::_print_json_value(*value);
        wbytes = json_str.size();
        str_value = json_str.c_str();
        break;
    }
    
    RETURN_IF_ERROR(_insert_to_column(column_ptr, slot_desc, str_value, wbytes));

    *valid = true;
    return Status::OK();
}

Status VJsonReader::_insert_to_column(vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                    const char* value_ptr, int32_t& wbytes) {
    switch (slot_desc->type().type) {
    case TYPE_BOOLEAN: {
        assert_cast<ColumnVector<UInt8>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_TINYINT: {
        assert_cast<ColumnVector<Int8>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_SMALLINT: {
        assert_cast<ColumnVector<Int16>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_INT: {
        assert_cast<ColumnVector<Int32>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_BIGINT: {
        assert_cast<ColumnVector<Int64>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_LARGEINT: {
        assert_cast<ColumnVector<Int128>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_FLOAT: {
        assert_cast<ColumnVector<Float32>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_DOUBLE: {
        assert_cast<ColumnVector<Float64>*>(column_ptr)->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_CHAR: {
        assert_cast<ColumnString*>(column_ptr)->insert_data(value_ptr, wbytes);
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        assert_cast<ColumnString*>(column_ptr)->insert_data(value_ptr, wbytes);
        break;
    }
    case TYPE_OBJECT: {
        Slice slice(value_ptr, wbytes);
        // insert_default()
        auto* target_column = assert_cast<ColumnBitmap*>(column_ptr);

        target_column->insert_default();
        BitmapValue* pvalue = nullptr;
        int pos = target_column->size() - 1;
        pvalue = &target_column->get_element(pos);

        if (slice.size != 0) {
            BitmapValue value;
            value.deserialize(slice.data);
            *pvalue = std::move(value);
        } else {
            *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice.data));
        }
        break;
    }
    case TYPE_HLL: {
        Slice slice(value_ptr, wbytes);
        auto* target_column = assert_cast<ColumnHLL*>(column_ptr);

        target_column->insert_default();
        HyperLogLog* pvalue = nullptr;
        int pos = target_column->size() - 1;
        pvalue = &target_column->get_element(pos);
        if (slice.size != 0) {
            HyperLogLog value;
            value.deserialize(slice);
            *pvalue = std::move(value);
        } else {
            *pvalue = std::move(*reinterpret_cast<HyperLogLog*>(slice.data));
        }
        break;
    }
    case TYPE_DECIMALV2: {
        assert_cast<ColumnDecimal<Decimal128>*>(column_ptr)
                ->insert_data(value_ptr, 0);
        break;
    }
    case TYPE_DATETIME: {
        Slice slice(value_ptr, wbytes);
        DateTimeValue value = *reinterpret_cast<DateTimeValue*>(slice.data);
        VecDateTimeValue date;
        date.convert_dt_to_vec_dt(&value);
        assert_cast<ColumnVector<Int64>*>(column_ptr)
                ->insert_data(reinterpret_cast<char*>(&date), 0);
        break;
    }
    case TYPE_DATE: {
        Slice slice(value_ptr, wbytes);
        DateTimeValue value = *reinterpret_cast<DateTimeValue*>(slice.data);
        VecDateTimeValue date;
        date.convert_dt_to_vec_dt(&value);
        assert_cast<ColumnVector<Int64>*>(column_ptr)
                ->insert_data(reinterpret_cast<char*>(&date), 0);
        break;
    }
    default: {
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        break;
    }
    }
    return Status::OK();
}

Status VJsonReader::_vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                                    const std::vector<SlotDescriptor*>& slot_descs,
                                                    bool* is_empty_row, bool* eof) {
    do {
        if (_next_line >= _total_lines) {
            size_t size = 0;
            Status st = JsonReader::_parse_json_doc(&size, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st); // terminate if encounter other errors
            if (size == 0 || *eof) {          // read all data, then return
                *is_empty_row = true;
                return Status::OK();
            }
            _json_doc = JsonReader::get_json_doc();
            _total_lines = _json_doc->Size();
            _next_line = 0;

            if (_total_lines == 0) {
                // meet an empty json array.
                *is_empty_row = true;
                continue;
            }
        }
        rapidjson::Value& objectValue = (*_json_doc)[_next_line++];
        bool valid = true;
        RETURN_IF_ERROR(_write_columns_by_jsonpath(objectValue, slot_descs, columns, &valid));
        if (!valid) {
            continue; // process next line
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

Status VJsonReader::_vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                                const std::vector<SlotDescriptor*>& slot_descs,
                                                bool* is_empty_row, bool* eof) {
    while (true) {
        size_t size = 0;
        Status st = JsonReader::_parse_json_doc(&size, eof);
        if (st.is_data_quality_error()) {
            continue; // continue to read next
        }
        RETURN_IF_ERROR(st);
        if (size == 0 || *eof) {
            *is_empty_row = true;
            return Status::OK(); // read over,then return
        }
        *is_empty_row = false;
        break; // read a valid row
    }
    bool valid = true;
    _json_doc = JsonReader::get_json_doc();
    RETURN_IF_ERROR(_write_columns_by_jsonpath(*_json_doc, slot_descs, columns, &valid));
    if (!valid) {
        // there is only one line in this case, so if it return false, just set is_empty_row true
        // so that the caller will continue reading next line.
        *is_empty_row = true;
    }
    return Status::OK();
}

Status VJsonReader::_write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                             const std::vector<SlotDescriptor*>& slot_descs,
                                             std::vector<MutableColumnPtr>& columns,
                                             bool* valid) {
    int nullcount = 0;
    int ctx_idx = 0;
    size_t column_num = slot_descs.size();
    for (size_t i = 0; i < column_num; i++) {
        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        _origin_json_doc_ptr = JsonReader::get_origin_json_doc();
        rapidjson::Value* json_values = nullptr;
        bool wrap_explicitly = false;
        if (LIKELY(i < _parsed_jsonpaths.size())) {
            json_values = JsonFunctions::get_json_array_from_parsed_json(
                    _parsed_jsonpaths[i], &objectValue, _origin_json_doc_ptr->GetAllocator(), &wrap_explicitly);
        }

        if (json_values == nullptr) {
            // not match in jsondata.
            if (slot_descs[i]->is_nullable()) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                nullable_column->insert_data(nullptr, 0);
                nullcount++;
            } else {
                RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return _print_json_value(objectValue); },
                        [&]() -> std::string {
                        fmt::memory_buffer error_msg;
                        fmt::format_to(error_msg, "The column `{}` is not nullable, but it's not found in jsondata.", slot_descs[i]->col_name());
                        return fmt::to_string(error_msg);
                        }, _scanner_eof));
                _counter->num_rows_filtered++;
                *valid = false; // current row is invalid
                break;
            }
        } else {
            CHECK(json_values->IsArray());
            if (json_values->Size() == 1 && wrap_explicitly) {
                // NOTICE1: JsonFunctions::get_json_array_from_parsed_json() will wrap the single json object with an array.
                // so here we unwrap the array to get the real element.
                // if json_values' size > 1, it means we just match an array, not a wrapped one, so no need to unwrap.
                json_values = &((*json_values)[0]);
            }
            // RETURN_IF_ERROR(_write_data_to_tuple(json_values, slot_descs[i], tuple, tuple_pool, valid));
            RETURN_IF_ERROR(_write_data_to_column(json_values, slot_descs[i], column_ptr, valid));
            if (!(*valid)) {
                break;
            }
        }
    }
    if (nullcount == column_num) {
        RETURN_IF_ERROR(_state->append_error_msg_to_file([&]() -> std::string { return _print_json_value(objectValue); },
                [&]() -> std::string { return "All fields is null or not matched, this is a invalid row."; }, _scanner_eof));
        _counter->num_rows_filtered++;
        *valid = false;
    }
    return Status::OK();
}

} // namespace doris::vectorized
