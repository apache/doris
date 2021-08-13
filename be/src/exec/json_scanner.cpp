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

#include "exec/json_scanner.h"

#include <algorithm>

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
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace doris {

JsonScanner::JsonScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<ExprContext*>& pre_filter_ctxs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, pre_filter_ctxs, counter),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          _cur_file_reader(nullptr),
          _cur_line_reader(nullptr),
          _cur_json_reader(nullptr),
          _next_range(0),
          _cur_reader_eof(false),
          _scanner_eof(false),
          _read_json_by_line(false) {
    if (params.__isset.line_delimiter_length && params.line_delimiter_length > 1) {
        _line_delimiter = params.line_delimiter_str;
        _line_delimiter_length = params.line_delimiter_length;
    } else {
        _line_delimiter.push_back(static_cast<char>(params.line_delimiter));
        _line_delimiter_length = 1;
    }
}

JsonScanner::~JsonScanner() {
    close();
}

Status JsonScanner::open() {
    return BaseScanner::open();
}

Status JsonScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) {
    SCOPED_TIMER(_read_timer);
    // Get one line
    while (!_scanner_eof) {
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
        RETURN_IF_ERROR(_cur_json_reader->read_json_row(_src_tuple, _src_slot_descs, tuple_pool,
                                                    &is_empty_row, &_cur_reader_eof));

        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        if (fill_dest_tuple(tuple, tuple_pool)) {
            break; // break if true
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

Status JsonScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(open_file_reader());
    if (_read_json_by_line) {
        RETURN_IF_ERROR(open_line_reader());
    }
    RETURN_IF_ERROR(open_json_reader());
    _next_range++;

    return Status::OK();
}

Status JsonScanner::open_file_reader() {
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
    if (range.__isset.read_json_by_line) {
        _read_json_by_line = range.read_json_by_line;
    }
    switch (range.file_type) {
    case TFileType::FILE_LOCAL: {
        LocalFileReader* file_reader = new LocalFileReader(range.path, start_offset);
        RETURN_IF_ERROR(file_reader->open());
        _cur_file_reader = file_reader;
        break;
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
    _cur_reader_eof = false;
    return Status::OK();
}

Status JsonScanner::open_line_reader() {
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];
    int64_t size = range.size;
    if (range.start_offset != 0) {
        size += 1;
        _skip_next_line = true;
    } else {
        _skip_next_line = false;
    }
    _cur_line_reader = new PlainTextLineReader(_profile, _cur_file_reader, nullptr,
                                               size, _line_delimiter, _line_delimiter_length);
    _cur_reader_eof = false;
    return Status::OK();
}

Status JsonScanner::open_json_reader() {
    if (_cur_json_reader != nullptr) {
        delete _cur_json_reader;
        _cur_json_reader = nullptr;
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
        _cur_json_reader =
                new JsonReader(_state, _counter, _profile, strip_outer_array, num_as_string,
                               fuzzy_parse, nullptr, _cur_line_reader);
    } else {
        _cur_json_reader =  new JsonReader(_state, _counter, _profile, strip_outer_array, num_as_string,
                                           fuzzy_parse, _cur_file_reader);
    }

    RETURN_IF_ERROR(_cur_json_reader->init(jsonpath, json_root));
    return Status::OK();
}

void JsonScanner::close() {
    if (_cur_json_reader != nullptr) {
        delete _cur_json_reader;
        _cur_json_reader = nullptr;
    }
    if (_cur_line_reader != nullptr) {
        delete _cur_line_reader;
        _cur_line_reader = nullptr;
    }
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
        } else {
            delete _cur_file_reader;
        }
        _cur_file_reader = nullptr;
    }
}

////// class JsonDataInternal
JsonDataInternal::JsonDataInternal(rapidjson::Value* v) : _json_values(v) {
    if (v != nullptr) {
        _iterator = v->Begin();
    }
}

rapidjson::Value::ConstValueIterator JsonDataInternal::get_next() {
    if (is_null() || _json_values->End() == _iterator) {
        return nullptr;
    }
    return _iterator++;
}

////// class JsonReader
JsonReader::JsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                       bool strip_outer_array, bool num_as_string,bool fuzzy_parse,
                       FileReader* file_reader, LineReader* line_reader)
        : _handle_json_callback(nullptr),
          _next_line(0),
          _total_lines(0),
          _state(state),
          _counter(counter),
          _profile(profile),
          _file_reader(file_reader),
          _line_reader(line_reader),
          _closed(false),
          _strip_outer_array(strip_outer_array),
          _num_as_string(num_as_string),
          _fuzzy_parse(fuzzy_parse),
          _json_doc(nullptr) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
}

JsonReader::~JsonReader() {
    _close();
}

Status JsonReader::init(const std::string& jsonpath, const std::string& json_root) {
    // parse jsonpath
    if (!jsonpath.empty()) {
        Status st = _generate_json_paths(jsonpath, &_parsed_jsonpaths);
        RETURN_IF_ERROR(st);
    }
    if (!json_root.empty()) {
        JsonFunctions::parse_json_paths(json_root, &_parsed_json_root);
    }

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _handle_json_callback = &JsonReader::_handle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _handle_json_callback = &JsonReader::_handle_flat_array_complex_json;
        } else {
            _handle_json_callback = &JsonReader::_handle_nested_complex_json;
        }
    }
    return Status::OK();
}

Status JsonReader::_generate_json_paths(const std::string& jsonpath,
                                        std::vector<std::vector<JsonPath>>* vect) {
    rapidjson::Document jsonpaths_doc;
    if (!jsonpaths_doc.Parse(jsonpath.c_str(), jsonpath.length()).HasParseError()) {
        if (!jsonpaths_doc.IsArray()) {
            return Status::InvalidArgument("Invalid json path: " + jsonpath);
        } else {
            for (int i = 0; i < jsonpaths_doc.Size(); i++) {
                const rapidjson::Value& path = jsonpaths_doc[i];
                if (!path.IsString()) {
                    return Status::InvalidArgument("Invalid json path: " + jsonpath);
                }
                std::vector<JsonPath> parsed_paths;
                JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
                vect->push_back(std::move(parsed_paths));
            }
            return Status::OK();
        }
    } else {
        return Status::InvalidArgument("Invalid json path: " + jsonpath);
    }
}

void JsonReader::_close() {
    if (_closed) {
        return;
    }
    _closed = true;
}

// read one json string from line read or file read and parse it to json doc.
// return Status::DataQualityError() if data has quality error.
// return other error if encounter other problemes.
// return Status::OK() if parse succeed or reach EOF.
Status JsonReader::_parse_json_doc(size_t* size, bool* eof) {
    // read a whole message
    SCOPED_TIMER(_file_read_timer);
    const uint8_t* json_str = nullptr;
    std::unique_ptr<uint8_t[]> json_str_ptr;
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, size, eof));
    } else {
        int64_t length = 0;
        RETURN_IF_ERROR(_file_reader->read_one_message(&json_str_ptr, &length));
        json_str = json_str_ptr.get();
        *size = length;
        if (length == 0) {
            *eof = true;
        }
    }

    _bytes_read_counter += *size;
    if (*eof) {
        return Status::OK();
    }

    bool has_parse_error = false;
    // parse jsondata to JsonDoc

    // As the issue: https://github.com/Tencent/rapidjson/issues/1458
    // Now, rapidjson only support uint64_t, So lagreint load cause bug. We use kParseNumbersAsStringsFlag.
    if (_num_as_string) {
        has_parse_error =
                _origin_json_doc
                        .Parse<rapidjson::kParseNumbersAsStringsFlag>((char*)json_str, *size)
                        .HasParseError();
    } else {
        has_parse_error = _origin_json_doc.Parse((char*)json_str, *size).HasParseError();
    }

    if (has_parse_error) {
        std::stringstream str_error;
        str_error << "Parse json data for JsonDoc failed. code = "
                  << _origin_json_doc.GetParseError() << ", error-info:"
                  << rapidjson::GetParseError_En(_origin_json_doc.GetParseError());
        _state->append_error_msg_to_file(std::string((char*)json_str, *size),
                                         str_error.str());
        _counter->num_rows_filtered++;
        return Status::DataQualityError(str_error.str());
    }

    // set json root
    if (_parsed_json_root.size() != 0) {
        _json_doc = JsonFunctions::get_json_object_from_parsed_json(
                _parsed_json_root, &_origin_json_doc, _origin_json_doc.GetAllocator());
        if (_json_doc == nullptr) {
            std::stringstream str_error;
            str_error << "JSON Root not found.";
            _state->append_error_msg_to_file(_print_json_value(_origin_json_doc), str_error.str());
            _counter->num_rows_filtered++;
            return Status::DataQualityError(str_error.str());
        }
    } else {
        _json_doc = &_origin_json_doc;
    }

    if (_json_doc->IsArray() && !_strip_outer_array) {
        std::stringstream str_error;
        str_error << "JSON data is array-object, `strip_outer_array` must be TRUE.";
        _state->append_error_msg_to_file(_print_json_value(_origin_json_doc), str_error.str());
        _counter->num_rows_filtered++;
        return Status::DataQualityError(str_error.str());
    }

    if (!_json_doc->IsArray() && _strip_outer_array) {
        std::stringstream str_error;
        str_error << "JSON data is not an array-object, `strip_outer_array` must be FALSE.";
        _state->append_error_msg_to_file(_print_json_value(_origin_json_doc), str_error.str());
        _counter->num_rows_filtered++;
        return Status::DataQualityError(str_error.str());
    }

    return Status::OK();
}

std::string JsonReader::_print_json_value(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

std::string JsonReader::_print_jsonpath(const std::vector<JsonPath>& path) {
    std::stringstream ss;
    for (auto& p : path) {
        ss << p.to_string() << ".";
    }
    return ss.str();
}

void JsonReader::_fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                            const uint8_t* value, int32_t len) {
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    memcpy(str_slot->ptr, value, len);
    str_slot->len = len;
}

void JsonReader::_write_data_to_tuple(rapidjson::Value::ConstValueIterator value,
                                      SlotDescriptor* desc, Tuple* tuple, MemPool* tuple_pool,
                                      bool* valid) {
    const char* str_value = nullptr;
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;
    switch (value->GetType()) {
    case rapidjson::Type::kStringType:
        str_value = value->GetString();
        _fill_slot(tuple, desc, tuple_pool, (uint8_t*)str_value, strlen(str_value));
        break;
    case rapidjson::Type::kNumberType:
        if (value->IsUint()) {
            wbytes = sprintf((char*)tmp_buf, "%u", value->GetUint());
            _fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
        } else if (value->IsInt()) {
            wbytes = sprintf((char*)tmp_buf, "%d", value->GetInt());
            _fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
        } else if (value->IsUint64()) {
            wbytes = sprintf((char*)tmp_buf, "%lu", value->GetUint64());
            _fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
        } else if (value->IsInt64()) {
            wbytes = sprintf((char*)tmp_buf, "%ld", value->GetInt64());
            _fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
        } else {
            wbytes = sprintf((char*)tmp_buf, "%f", value->GetDouble());
            _fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
        }
        break;
    case rapidjson::Type::kFalseType:
        _fill_slot(tuple, desc, tuple_pool, (uint8_t*)"0", 1);
        break;
    case rapidjson::Type::kTrueType:
        _fill_slot(tuple, desc, tuple_pool, (uint8_t*)"1", 1);
        break;
    case rapidjson::Type::kNullType:
        if (desc->is_nullable()) {
            tuple->set_null(desc->null_indicator_offset());
        } else {
            std::stringstream str_error;
            str_error << "Json value is null, but the column `" << desc->col_name()
                      << "` is not nullable.";
            _state->append_error_msg_to_file(_print_json_value(*value), str_error.str());
            _counter->num_rows_filtered++;
            *valid = false;
            return;
        }
        break;
    default:
        // for other type like array or object. we convert it to string to save
        std::string json_str = _print_json_value(*value);
        _fill_slot(tuple, desc, tuple_pool, (uint8_t*)json_str.c_str(), json_str.length());
        break;
    }
    *valid = true;
}

// for simple format json
void JsonReader::_set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple,
                                  const std::vector<SlotDescriptor*>& slot_descs,
                                  MemPool* tuple_pool, bool* valid) {
    if (!objectValue.IsObject()) {
        // Here we expect the incoming `objectValue` to be a Json Object, such as {"key" : "value"},
        // not other type of Json format.
        _state->append_error_msg_to_file(_print_json_value(objectValue),
                                         "Expect json object value");
        _counter->num_rows_filtered++;
        *valid = false; // current row is invalid
        return;
    }

    int nullcount = 0;
    for (auto v : slot_descs) {
        rapidjson::Value::ConstMemberIterator it = objectValue.MemberEnd();
        if (_fuzzy_parse) {
            auto idx_it = _name_map.find(v->col_name());
            if (idx_it != _name_map.end() && idx_it->second < objectValue.MemberCount()) {
                it = objectValue.MemberBegin() + idx_it->second;
            }
        } else {
            it = objectValue.FindMember(
                    rapidjson::Value(v->col_name().c_str(), v->col_name().size()));
        }
        if (it != objectValue.MemberEnd()) {
            const rapidjson::Value& value = it->value;
            _write_data_to_tuple(&value, v, tuple, tuple_pool, valid);
            if (!(*valid)) {
                return;
            }
        } else { // not found
            if (v->is_nullable()) {
                tuple->set_null(v->null_indicator_offset());
                nullcount++;
            } else {
                std::stringstream str_error;
                str_error << "The column `" << v->col_name()
                          << "` is not nullable, but it's not found in jsondata.";
                _state->append_error_msg_to_file(_print_json_value(objectValue), str_error.str());
                _counter->num_rows_filtered++;
                *valid = false; // current row is invalid
                break;
            }
        }
    }

    if (nullcount == slot_descs.size()) {
        _state->append_error_msg_to_file(_print_json_value(objectValue),
                                         "All fields is null, this is a invalid row.");
        _counter->num_rows_filtered++;
        *valid = false;
        return;
    }
    *valid = true;
    return;
}

/**
 * handle input a simple json.
 * A json is a simple json only when user not specifying the json path.
 * For example:
 *  case 1. [{"column1":"value1", "column2":10}, {"column1":"
", "column2":30}]
 *  case 2. {"column1":"value1", "column2":10}
 */
Status JsonReader::_handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                       MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    do {
        bool valid = false;
        if (_next_line >= _total_lines) { // parse json and generic document
            size_t size = 0;
            Status st = _parse_json_doc(&size, eof);
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
            if (_json_doc->IsArray()) {
                _total_lines = _json_doc->Size();
                if (_total_lines == 0) {
                    // may be passing an empty json, such as "[]"
                    std::stringstream str_error;
                    str_error << "Empty json line";
                    _state->append_error_msg_to_file(_print_json_value(*_json_doc),
                                                     str_error.str());
                    _counter->num_rows_filtered++;
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

        if (_json_doc->IsArray()) {                                   // handle case 1
            rapidjson::Value& objectValue = (*_json_doc)[_next_line]; // json object
            _set_tuple_value(objectValue, tuple, slot_descs, tuple_pool, &valid);
        } else { // handle case 2
            _set_tuple_value(*_json_doc, tuple, slot_descs, tuple_pool, &valid);
        }
        _next_line++;
        if (!valid) {
            continue;
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

bool JsonReader::_write_values_by_jsonpath(rapidjson::Value& objectValue, MemPool* tuple_pool,
                                           Tuple* tuple,
                                           const std::vector<SlotDescriptor*>& slot_descs) {
    int nullcount = 0;
    bool valid = true;
    size_t column_num = slot_descs.size();

    for (size_t i = 0; i < column_num; i++) {
        rapidjson::Value* json_values = nullptr;
        if (LIKELY(i < _parsed_jsonpaths.size())) {
            json_values = JsonFunctions::get_json_array_from_parsed_json(
                    _parsed_jsonpaths[i], &objectValue, _origin_json_doc.GetAllocator());
        }

        if (json_values == nullptr) {
            // not match in jsondata.
            if (slot_descs[i]->is_nullable()) {
                tuple->set_null(slot_descs[i]->null_indicator_offset());
                nullcount++;
            } else {
                std::stringstream str_error;
                str_error << "The column `" << slot_descs[i]->col_name()
                          << "` is not nullable, but it's not found in jsondata.";
                _state->append_error_msg_to_file(_print_json_value(objectValue), str_error.str());
                _counter->num_rows_filtered++;
                valid = false; // current row is invalid
                break;
            }
        } else {
            CHECK(json_values->IsArray());
            CHECK(json_values->Size() >= 1);
            if (json_values->Size() == 1) {
                // NOTICE1: JsonFunctions::get_json_array_from_parsed_json() will wrap the single json object with an array.
                // so here we unwrap the array to get the real element.
                // if json_values' size > 1, it means we just match an array, not a wrapped one, so no need to unwrap.
                json_values = &((*json_values)[0]);
            }
            _write_data_to_tuple(json_values, slot_descs[i], tuple, tuple_pool, &valid);
            if (!valid) {
                break;
            }
        }
    }
    if (nullcount == column_num) {
        _state->append_error_msg_to_file(
                _print_json_value(objectValue),
                "All fields is null or not matched, this is a invalid row.");
        _counter->num_rows_filtered++;
        valid = false;
    }
    return valid;
}

/**
 * for example:
 * {
 *    "data": {"a":"a1", "b":"b1", "c":"c1"}
 * }
 * In this scene, generate only one row
 */
Status JsonReader::_handle_nested_complex_json(Tuple* tuple,
                                               const std::vector<SlotDescriptor*>& slot_descs,
                                               MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    while (true) {
        size_t size = 0;
        Status st = _parse_json_doc(&size, eof);
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
    if (!_write_values_by_jsonpath(*_json_doc, tuple_pool, tuple, slot_descs)) {
        // there is only one line in this case, so if it return false, just set is_empty_row true
        // so that the caller will continue reading next line.
        *is_empty_row = true;
    }
    return Status::OK();
}

/**
 * flat array for json. _json_doc should be an array
 * For example:
 *  [{"column1":"value1", "column2":10}, {"column1":"value2", "column2":30}]
 * Result:
 *      column1    column2
 *      ------------------
 *      value1     10
 *      value2     30
 */
Status JsonReader::_handle_flat_array_complex_json(Tuple* tuple,
                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                   MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    do {
        if (_next_line >= _total_lines) {
            size_t size = 0;
            Status st = _parse_json_doc(&size, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st); // terminate if encounter other errors
            if (size == 0 || *eof) {          // read all data, then return
                *is_empty_row = true;
                return Status::OK();
            }
            _total_lines = _json_doc->Size();
            _next_line = 0;

            if (_total_lines == 0) {
                // meet an empty json array.
                *is_empty_row = true;
                continue;
            }
        }
        rapidjson::Value& objectValue = (*_json_doc)[_next_line++];
        if (!_write_values_by_jsonpath(objectValue, tuple_pool, tuple, slot_descs)) {
            continue; // process next line
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

Status JsonReader::read_json_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                        MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    return (this->*_handle_json_callback)(tuple, slot_descs, tuple_pool, is_empty_row, eof);
}

} // namespace doris
