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
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "exprs/expr.h"
#include "env/env.h"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"
#include "exprs/json_functions.h"

namespace doris {

JsonScanner::JsonScanner(RuntimeState* state,
                         RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         ScannerCounter* counter) : BaseScanner(state, profile, params, counter),
                          _ranges(ranges),
                          _broker_addresses(broker_addresses),
                          _cur_file_reader(nullptr),
                          _next_range(0),
                          _cur_file_eof(false),
                          _scanner_eof(false) {

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
        if (_cur_file_reader == nullptr || _cur_file_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
            _cur_file_eof = false;
        }
        RETURN_IF_ERROR(_cur_file_reader->read(_src_tuple, _src_slot_descs, tuple_pool, &_cur_file_eof));

        if (_cur_file_eof) {
            continue; // read next file
        }
        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        if (fill_dest_tuple(Slice(), tuple, tuple_pool)) {
            break;// break if true
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
    if (_cur_file_reader != nullptr) {
        delete _cur_file_reader;
        _cur_file_reader = nullptr;
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
        }
    }
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    const TBrokerRangeDesc& range = _ranges[_next_range++];
    int64_t start_offset = range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }
    FileReader *file = nullptr;
    switch (range.file_type) {
    case TFileType::FILE_LOCAL: {
        LocalFileReader* file_reader = new LocalFileReader(range.path, start_offset);
        RETURN_IF_ERROR(file_reader->open());
        file = file_reader;
        break;
    }
    case TFileType::FILE_BROKER: {
        BrokerReader* broker_reader = new BrokerReader(
            _state->exec_env(), _broker_addresses, _params.properties, range.path, start_offset);
        RETURN_IF_ERROR(broker_reader->open());
        file = broker_reader;
        break;
    }

    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG(3) << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
        file = _stream_load_pipe.get();
        break;
    }
    default: {
        std::stringstream ss;
        ss << "Unknown file type, type=" << range.file_type;
        return Status::InternalError(ss.str());
    }
    }

    std::string jsonpath = "";
    bool strip_outer_array = false;
    if (range.__isset.jsonpaths) {
        jsonpath = range.jsonpaths;
    }
    if (range.__isset.strip_outer_array) {
        strip_outer_array = range.strip_outer_array;
    }
    _cur_file_reader = new JsonReader(_state, _counter, _profile, file, jsonpath, strip_outer_array);

    return Status::OK();
}

void JsonScanner::close() {
    if (_cur_file_reader != nullptr) {
        delete _cur_file_reader;
        _cur_file_reader = nullptr;
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
        }
    }
}

////// class JsonDataInternal
JsonDataInternal::JsonDataInternal(rapidjson::Value* v) :
        _json_values(v), _iterator(v->Begin()) {
}

JsonDataInternal::~JsonDataInternal() {

}

rapidjson::Value::ConstValueIterator JsonDataInternal::get_next() {
    if (_json_values->End() == _iterator) {
        return nullptr;
    }
    return _iterator++;
}


////// class JsonReader
JsonReader::JsonReader(
        RuntimeState* state, ScannerCounter* counter,
        RuntimeProfile* profile,
        FileReader* file_reader,
        std::string& jsonpath,
        bool strip_outer_array) :
            _next_line(0),
            _total_lines(0),
            _state(state),
            _counter(counter),
            _profile(profile),
            _file_reader(file_reader),
            _closed(false),
            _strip_outer_array(strip_outer_array) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "FileReadTime");

    init_jsonpath(jsonpath);
}

JsonReader::~JsonReader() {
    close();
}

void JsonReader::init_jsonpath(std::string& jsonpath) {
    //parse jsonpath
    if (!jsonpath.empty()) {
        if (!_jsonpaths_doc.Parse(jsonpath.c_str()).HasParseError()) {
            if (!_jsonpaths_doc.IsArray()) {
                _parse_jsonpath_flag = -1;// failed, has none object
            } else {
                _parse_jsonpath_flag = 1;// success
            }
        } else {
            _parse_jsonpath_flag = -1;// parse failed
        }
    } else {
        _parse_jsonpath_flag = 0;
    }
    return ;
}

void JsonReader::close() {
    if (_closed) {
        return;
    }
    if (typeid(*_file_reader) == typeid(doris::BrokerReader) || typeid(*_file_reader) == typeid(doris::LocalFileReader)) {
        _file_reader->close();
        delete _file_reader;
    }
    _closed = true;
}

Status JsonReader::parse_json_doc(bool* eof) {
    // read a whole message, must be delete json_str by `delete[]`
    uint8_t* json_str = nullptr;
    size_t length = 0;
    RETURN_IF_ERROR(_file_reader->read_one_message(&json_str, &length));
    if (length == 0) {
        *eof = true;
        return Status::OK();
    }
    //  parse jsondata to JsonDoc
    if (_json_doc.Parse((char*)json_str, length).HasParseError()) {
        delete[] json_str;
        std::stringstream str_error;
        str_error << "Parse json data for JsonDoc is failed. code = " << _json_doc.GetParseError()
                << ", error-info:" << rapidjson::GetParseError_En(_json_doc.GetParseError());
        return Status::InternalError(str_error.str());
    }

    if (!_json_doc.IsArray() && _strip_outer_array) {
        delete[] json_str;
        return Status::InternalError("JSON ROOT node is array-object, `strip_outer_array` must be TRUE.");
    }

    delete[] json_str;
    return Status::OK();
}

size_t JsonReader::get_data_by_jsonpath(const std::vector<SlotDescriptor*>& slot_descs) {
    size_t max_lines = 0;
    //iterator jsonpath to find object and save it to Map
    _jmap.clear();

    for (int i = 0; i < _jsonpaths_doc.Size(); i++) {
        const rapidjson::Value& path = _jsonpaths_doc[i];
        if (!path.IsString()) {
            return -1;
        }

        // if jsonValues is null, because not match in jsondata.
        rapidjson::Value* json_values = JsonFunctions::get_json_array_from_parsed_json(path.GetString(), &_json_doc, _json_doc.GetAllocator());
        if (json_values == nullptr) {
            return -1;
        }
        max_lines = std::max(max_lines, (size_t)json_values->Size());
        _jmap.emplace(slot_descs[i]->col_name(), json_values);
    }

    return max_lines;
}

void JsonReader::fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value, int32_t len) {
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    memcpy(str_slot->ptr, value, len);
    str_slot->len = len;
    return;
}

Status JsonReader::write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc, Tuple* tuple, MemPool* tuple_pool) {
    const char* str_value = nullptr;
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;
    switch (value->GetType()) {
        case rapidjson::Type::kStringType:
            str_value = value->GetString();
            fill_slot(tuple, desc, tuple_pool, (uint8_t*)str_value, strlen(str_value));
            break;
        case rapidjson::Type::kNumberType:
            if (value->IsUint()) {
                wbytes = sprintf((char*)tmp_buf, "%u", value->GetUint());
                fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
            } else if (value->IsInt()) {
                wbytes = sprintf((char*)tmp_buf, "%d", value->GetInt());
                fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
            } else if (value->IsUint64()) {
                wbytes = sprintf((char*)tmp_buf, "%lu", value->GetUint64());
                fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
            } else if (value->IsInt64()) {
                wbytes = sprintf((char*)tmp_buf, "%ld", value->GetInt64());
                fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
            } else {
                wbytes = sprintf((char*)tmp_buf, "%f", value->GetDouble());
                fill_slot(tuple, desc, tuple_pool, tmp_buf, wbytes);
            }
            break;
        case rapidjson::Type::kFalseType:
            fill_slot(tuple, desc, tuple_pool, (uint8_t*)"0", 1);
            break;
        case rapidjson::Type::kTrueType:
            fill_slot(tuple, desc, tuple_pool, (uint8_t*)"1", 1);
            break;
        case rapidjson::Type::kNullType:
            if (desc->is_nullable()) {
                tuple->set_null(desc->null_indicator_offset());
            } else {
                std::stringstream str_error;
                str_error << "Json value is null, but the column `" << desc->col_name() << "` is not nullable.";
                return Status::RuntimeError(str_error.str());
            }
            break;
        default:
            std::stringstream str_error;
            str_error << "Invalid JsonType " << value->GetType() << ", Column Name `" << desc->col_name() << "`.";
            return Status::RuntimeError(str_error.str());
    }
    return Status::OK();
}

Status JsonReader::set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid) {
    int nullcount = 0;
    for (auto v : slot_descs) {
        if (objectValue.HasMember(v->col_name().c_str())) {
            rapidjson::Value& value = objectValue[v->col_name().c_str()];
            RETURN_IF_ERROR(write_data_to_tuple(&value, v, tuple, tuple_pool));
        } else {
            if (v->is_nullable()) {
                tuple->set_null(v->null_indicator_offset());
                nullcount++;
            } else  {
                std::stringstream str_error;
                str_error << "The column `" << v->col_name() << "` is not nullable, but it's not found in jsondata.";
                _state->append_error_msg_to_file("", str_error.str());
                _counter->num_rows_filtered++;
                *valid = false; // current row is invalid
                break;
            }
        }
    }

    if (nullcount == slot_descs.size()) {
        _state->append_error_msg_to_file("", "The all fields is null, this is a invalid row.");
        _counter->num_rows_filtered++;
        *valid = false;
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

/**
 * handle input a simple json
 * For example:
 *  case 1. [{"colunm1":"value1", "colunm2":10}, {"colunm1":"value2", "colunm2":30}]
 *  case 2. {"colunm1":"value1", "colunm2":10}
 */
Status JsonReader::handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof) {
    do {
        bool valid = false;
        if (_next_line >= _total_lines) {//parse json and generic document
            RETURN_IF_ERROR(parse_json_doc(eof));
            if (*eof) {// read all data, then return
                return Status::OK();
            }
            if (_json_doc.IsArray() ) {
                _total_lines = _json_doc.Size();
            } else {
                _total_lines = 1; // only one row
            }
            _next_line = 0;
        }

        if (_json_doc.IsArray()) {//handle case 1
            rapidjson::Value& objectValue = _json_doc[_next_line];// json object
            RETURN_IF_ERROR(set_tuple_value(objectValue, tuple, slot_descs, tuple_pool, &valid));
        } else {// handle case 2
            RETURN_IF_ERROR(set_tuple_value(_json_doc, tuple, slot_descs, tuple_pool, &valid));
        }
        _next_line++;
        if (!valid) {
            continue;
        }
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

Status JsonReader::set_tuple_value_from_map(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid) {
    std::unordered_map<std::string, JsonDataInternal>::iterator it_map;
    for (auto v : slot_descs) {
        it_map = _jmap.find(v->col_name());
        if (it_map == _jmap.end()) {
            return Status::RuntimeError("The column name of table is not foud in jsonpath.");
        }
        rapidjson::Value::ConstValueIterator value = it_map->second.get_next();
        if (value == nullptr) {
            if (v->is_nullable()) {
                tuple->set_null(v->null_indicator_offset());
            } else  {
                std::stringstream str_error;
                str_error << "The column `" << it_map->first << "` is not nullable, but it's not found in jsondata.";
                _state->append_error_msg_to_file("", str_error.str());
                _counter->num_rows_filtered++;
                *valid = false; // current row is invalid
                break;
            }
        } else {
            RETURN_IF_ERROR(write_data_to_tuple(value, v, tuple, tuple_pool));
        }
    }
    *valid = true;
    return Status::OK();
}

Status JsonReader::handle_nest_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof) {
    do {
        bool valid = false;
        if (_next_line >= _total_lines) {
            RETURN_IF_ERROR(parse_json_doc(eof));
            if (*eof) {
                return Status::OK();
            }
            _total_lines = get_data_by_jsonpath(slot_descs);
            if (_total_lines == -1) {
                return Status::InternalError("Parse json data is failed.");
            } else if (_total_lines == 0) {
                *eof = true;
                return Status::OK();
            }
            _next_line = 0;
        }

        RETURN_IF_ERROR(set_tuple_value_from_map(tuple, slot_descs, tuple_pool, &valid));
        _next_line++;
        if (!valid) {// read a invalid row, then read next one
            continue;
        }
        break; // read a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

/**
 * flat array for json
 * For example:
 *  [{"colunm1":"value1", "colunm2":10}, {"colunm1":"value2", "colunm2":30}]
 * Result:
 *      colunm1    colunm2
 *      ------------------
 *      value1     10
 *      value2     30
 */
Status JsonReader::handle_flat_array_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof) {
    do {
        if (_next_line >= _total_lines) {//parse json and generic document
            RETURN_IF_ERROR(parse_json_doc(eof));
            if (*eof) {// read all data, then return
                return Status::OK();
            }
            _total_lines = _json_doc.Size();
            _next_line = 0;
        }
        int nullcount = 0;
        bool valid = true;
        size_t limit = std::min(slot_descs.size(), (size_t)_jsonpaths_doc.Size());
        rapidjson::Value& objectValue = _json_doc[_next_line];

        for (size_t i = 0; i < limit; i++) {
            const rapidjson::Value& path = _jsonpaths_doc[i];
            if (!path.IsString()) {
                return Status::InternalError("Jsonpath is not string.");
            }

            // if jsonValues is null, because not match in jsondata.
            rapidjson::Value* json_values = JsonFunctions::get_json_array_from_parsed_json(path.GetString(), &objectValue, _json_doc.GetAllocator());
            if (json_values == nullptr) {
                if (slot_descs[i]->is_nullable()) {
                    tuple->set_null(slot_descs[i]->null_indicator_offset());
                    nullcount++;
                } else  {
                    std::stringstream str_error;
                    str_error << "The column `" << slot_descs[i]->col_name() << "` is not nullable, but it's not found in jsondata.";
                    _state->append_error_msg_to_file("", str_error.str());
                    _counter->num_rows_filtered++;
                    valid = false; // current row is invalid
                    break;
                }
            } else {
                RETURN_IF_ERROR(write_data_to_tuple(json_values, slot_descs[i], tuple, tuple_pool));
            }
        }
        _next_line++;
        if (!valid) {
            continue;
        }
        if (nullcount == _jsonpaths_doc.Size()) {
            _state->append_error_msg_to_file("", "The all fields is null, this is a invalid row.");
            _counter->num_rows_filtered++;
            continue;
        }
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

Status JsonReader::handle_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof) {
    if (_strip_outer_array) {
        return handle_flat_array_complex_json(tuple, slot_descs, tuple_pool, eof);
    } else {
        return handle_nest_complex_json(tuple, slot_descs, tuple_pool, eof);
    }
}

Status JsonReader::read(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof) {
    if (_parse_jsonpath_flag == -1) {
        return Status::InternalError("Parse jsonpath is failed.");
    } else if (_parse_jsonpath_flag == 0) {// input a simple json-string
        return handle_simple_json(tuple, slot_descs, tuple_pool, eof);
    } else {// input a complex json-string and a json-path
        return handle_complex_json(tuple,  slot_descs, tuple_pool, eof);
    }
}


} // end of namespace
