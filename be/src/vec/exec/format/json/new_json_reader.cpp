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

#include "vec/exec/format/json/new_json_reader.h"

#include "common/compiler_util.h"
#include "exec/plain_text_line_reader.h"
#include "exprs/json_functions.h"
#include "io/file_factory.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exec/scan/vscanner.h"
namespace doris::vectorized {

NewJsonReader::NewJsonReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                             const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs, bool* scanner_eof)
        : _vhandle_json_callback(nullptr),
          _state(state),
          _profile(profile),
          _counter(counter),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _file_reader(nullptr),
          _file_reader_s(nullptr),
          _real_file_reader(nullptr),
          _line_reader(nullptr),
          _reader_eof(false),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator),
          _scanner_eof(scanner_eof) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
}

NewJsonReader::NewJsonReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs)
        : _vhandle_json_callback(nullptr),
          _state(nullptr),
          _profile(profile),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _file_reader(nullptr),
          _file_reader_s(nullptr),
          _real_file_reader(nullptr),
          _line_reader(nullptr),
          _reader_eof(false),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator) {}

Status NewJsonReader::init_reader() {
    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader());
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_open_line_reader());
    }

    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _vhandle_json_callback = &NewJsonReader::_vhandle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _vhandle_json_callback = &NewJsonReader::_vhandle_flat_array_complex_json;
        } else {
            _vhandle_json_callback = &NewJsonReader::_vhandle_nested_complex_json;
        }
    }
    return Status::OK();
}

Status NewJsonReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_reader_eof == true) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = _state->batch_size();
    auto columns = block->mutate_columns();

    while (columns[0]->size() < batch_size && !_reader_eof) {
        if (UNLIKELY(_read_json_by_line && _skip_first_line)) {
            size_t size = 0;
            const uint8_t* line_ptr = nullptr;
            RETURN_IF_ERROR(_line_reader->read_line(&line_ptr, &size, &_reader_eof));
            _skip_first_line = false;
            continue;
        }

        bool is_empty_row = false;

        RETURN_IF_ERROR(_read_json_column(columns, _file_slot_descs, &is_empty_row, &_reader_eof));
        ++(*read_rows);
        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
    }

    columns.clear();
    return Status::OK();
}

Status NewJsonReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status NewJsonReader::get_parsered_schema(std::vector<std::string>* col_names,
                                          std::vector<TypeDescriptor>* col_types) {
    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader());
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_open_line_reader());
    }

    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());

    bool eof = false;
    const uint8_t* json_str = nullptr;
    std::unique_ptr<uint8_t[]> json_str_ptr;
    size_t size = 0;
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, &size, &eof));
    } else {
        int64_t length = 0;
        RETURN_IF_ERROR(_real_file_reader->read_one_message(&json_str_ptr, &length));
        json_str = json_str_ptr.get();
        size = length;
        if (length == 0) {
            eof = true;
        }
    }

    if (size == 0 || eof) {
        return Status::EndOfFile("Empty file.");
    }

    // clear memory here.
    _value_allocator.Clear();
    _parse_allocator.Clear();
    bool has_parse_error = false;

    // parse jsondata to JsonDoc
    // As the issue: https://github.com/Tencent/rapidjson/issues/1458
    // Now, rapidjson only support uint64_t, So lagreint load cause bug. We use kParseNumbersAsStringsFlag.
    if (_num_as_string) {
        has_parse_error =
                _origin_json_doc.Parse<rapidjson::kParseNumbersAsStringsFlag>((char*)json_str, size)
                        .HasParseError();
    } else {
        has_parse_error = _origin_json_doc.Parse((char*)json_str, size).HasParseError();
    }

    if (has_parse_error) {
        return Status::DataQualityError(
                "Parse json data for JsonDoc failed. code: {}, error info: {}",
                _origin_json_doc.GetParseError(),
                rapidjson::GetParseError_En(_origin_json_doc.GetParseError()));
    }

    // set json root
    if (_parsed_json_root.size() != 0) {
        _json_doc = JsonFunctions::get_json_object_from_parsed_json(
                _parsed_json_root, &_origin_json_doc, _origin_json_doc.GetAllocator());
        if (_json_doc == nullptr) {
            return Status::DataQualityError("JSON Root not found.");
        }
    } else {
        _json_doc = &_origin_json_doc;
    }

    if (_json_doc->IsArray() && !_strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is array-object, `strip_outer_array` must be TRUE.");
    } else if (!_json_doc->IsArray() && _strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
    }

    rapidjson::Value* objectValue = nullptr;
    if (_json_doc->IsArray()) {
        if (_json_doc->Size() == 0) {
            // may be passing an empty json, such as "[]"
            return Status::InternalError("Empty first json line");
        }
        objectValue = &(*_json_doc)[0];
    } else {
        objectValue = _json_doc;
    }

    // use jsonpaths to col_names
    if (_parsed_jsonpaths.size() > 0) {
        for (size_t i = 0; i < _parsed_jsonpaths.size(); ++i) {
            size_t len = _parsed_jsonpaths[i].size();
            if (len == 0) {
                return Status::InvalidArgument("It's invalid jsonpaths.");
            }
            std::string key = _parsed_jsonpaths[i][len - 1].key;
            col_names->emplace_back(key);
            col_types->emplace_back(TypeDescriptor::create_string_type());
        }
        return Status::OK();
    }

    for (int i = 0; i < objectValue->MemberCount(); ++i) {
        auto it = objectValue->MemberBegin() + i;
        col_names->emplace_back(it->name.GetString());
        col_types->emplace_back(TypeDescriptor::create_string_type());
    }
    return Status::OK();
}

Status NewJsonReader::_get_range_params() {
    if (!_params.__isset.file_attributes) {
        return Status::InternalError("BE cat get file_attributes");
    }

    // get line_delimiter
    if (_params.file_attributes.__isset.text_params &&
        _params.file_attributes.text_params.__isset.line_delimiter) {
        _line_delimiter = _params.file_attributes.text_params.line_delimiter;
        _line_delimiter_length = _line_delimiter.size();
    }

    if (_params.file_attributes.__isset.jsonpaths) {
        _jsonpaths = _params.file_attributes.jsonpaths;
    }
    if (_params.file_attributes.__isset.json_root) {
        _json_root = _params.file_attributes.json_root;
    }
    if (_params.file_attributes.__isset.read_json_by_line) {
        _read_json_by_line = _params.file_attributes.read_json_by_line;
    }
    if (_params.file_attributes.__isset.strip_outer_array) {
        _strip_outer_array = _params.file_attributes.strip_outer_array;
    }
    if (_params.file_attributes.__isset.num_as_string) {
        _num_as_string = _params.file_attributes.num_as_string;
    }
    if (_params.file_attributes.__isset.fuzzy_parse) {
        _fuzzy_parse = _params.file_attributes.fuzzy_parse;
    }
    return Status::OK();
}

Status NewJsonReader::_open_file_reader() {
    int64_t start_offset = _range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }

    if (_params.file_type == TFileType::FILE_STREAM) {
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, _file_reader_s));
        _real_file_reader = _file_reader_s.get();
    } else {
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                _profile, _params, _range.path, start_offset, _range.file_size, 0, _file_reader));
        _real_file_reader = _file_reader.get();
    }
    return _real_file_reader->open();
}

Status NewJsonReader::_open_line_reader() {
    int64_t size = _range.size;
    if (_range.start_offset != 0) {
        // When we fetch range doesn't start from 0, size will += 1.

        // TODO(ftw): check what if file_reader is stream_pipe? Is `size+=1` is correct?
        size += 1;
        _skip_first_line = true;
    } else {
        _skip_first_line = false;
    }
    _line_reader.reset(new PlainTextLineReader(_profile, _real_file_reader, nullptr, size,
                                               _line_delimiter, _line_delimiter_length));
    return Status::OK();
}

Status NewJsonReader::_parse_jsonpath_and_json_root() {
    // parse jsonpaths
    if (!_jsonpaths.empty()) {
        rapidjson::Document jsonpaths_doc;
        if (!jsonpaths_doc.Parse(_jsonpaths.c_str(), _jsonpaths.length()).HasParseError()) {
            if (!jsonpaths_doc.IsArray()) {
                return Status::InvalidArgument("Invalid json path: {}", _jsonpaths);
            } else {
                for (int i = 0; i < jsonpaths_doc.Size(); i++) {
                    const rapidjson::Value& path = jsonpaths_doc[i];
                    if (!path.IsString()) {
                        return Status::InvalidArgument("Invalid json path: {}", _jsonpaths);
                    }
                    std::vector<JsonPath> parsed_paths;
                    JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
                    _parsed_jsonpaths.push_back(std::move(parsed_paths));
                }
            }
        } else {
            return Status::InvalidArgument("Invalid json path: {}", _jsonpaths);
        }
    }

    // parse jsonroot
    if (!_json_root.empty()) {
        JsonFunctions::parse_json_paths(_json_root, &_parsed_json_root);
    }
    return Status::OK();
}

Status NewJsonReader::_read_json_column(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof) {
    return (this->*_vhandle_json_callback)(columns, slot_descs, is_empty_row, eof);
}

Status NewJsonReader::_vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                           const std::vector<SlotDescriptor*>& slot_descs,
                                           bool* is_empty_row, bool* eof) {
    do {
        bool valid = false;
        if (_next_row >= _total_rows) { // parse json and generic document
            Status st = _parse_json(is_empty_row, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                return Status::OK();
            }
            _name_map.clear();
            rapidjson::Value* objectValue = nullptr;
            if (_json_doc->IsArray()) {
                _total_rows = _json_doc->Size();
                if (_total_rows == 0) {
                    // may be passing an empty json, such as "[]"
                    RETURN_IF_ERROR(_append_error_msg(*_json_doc, "Empty json line", "", nullptr));

                    // TODO(ftw): check _reader_eof??
                    if (_reader_eof) {
                        *is_empty_row = true;
                        return Status::OK();
                    }
                    continue;
                }
                objectValue = &(*_json_doc)[0];
            } else {
                _total_rows = 1; // only one row
                objectValue = _json_doc;
            }
            _next_row = 0;
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

        if (_json_doc->IsArray()) {                                  // handle case 1
            rapidjson::Value& objectValue = (*_json_doc)[_next_row]; // json object
            RETURN_IF_ERROR(_set_column_value(objectValue, columns, slot_descs, &valid));
        } else { // handle case 2
            RETURN_IF_ERROR(_set_column_value(*_json_doc, columns, slot_descs, &valid));
        }
        _next_row++;
        if (!valid) {
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                // TODO(ftw): check *eof=true?
                *eof = true;
                return Status::OK();
            }
            continue;
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_row <= _total_rows);
    return Status::OK();
}

Status NewJsonReader::_vhandle_flat_array_complex_json(
        std::vector<MutableColumnPtr>& columns, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
    do {
        if (_next_row >= _total_rows) {
            Status st = _parse_json(is_empty_row, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                if (st == Status::OK()) {
                    return Status::OK();
                }
                if (_total_rows == 0) {
                    continue;
                }
            }
        }
        rapidjson::Value& objectValue = (*_json_doc)[_next_row++];
        bool valid = true;
        RETURN_IF_ERROR(_write_columns_by_jsonpath(objectValue, slot_descs, columns, &valid));
        if (!valid) {
            continue; // process next line
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_row <= _total_rows);
    return Status::OK();
}

Status NewJsonReader::_vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                   bool* is_empty_row, bool* eof) {
    while (true) {
        Status st = _parse_json(is_empty_row, eof);
        if (st.is_data_quality_error()) {
            continue; // continue to read next
        }
        RETURN_IF_ERROR(st);
        if (*is_empty_row == true) {
            return Status::OK();
        }
        *is_empty_row = false;
        break; // read a valid row
    }
    bool valid = true;
    RETURN_IF_ERROR(_write_columns_by_jsonpath(*_json_doc, slot_descs, columns, &valid));
    if (!valid) {
        // there is only one line in this case, so if it return false, just set is_empty_row true
        // so that the caller will continue reading next line.
        *is_empty_row = true;
    }
    return Status::OK();
}

Status NewJsonReader::_parse_json(bool* is_empty_row, bool* eof) {
    size_t size = 0;
    RETURN_IF_ERROR(_parse_json_doc(&size, eof));

    // read all data, then return
    if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    if (!_parsed_jsonpaths.empty() && _strip_outer_array) {
        _total_rows = _json_doc->Size();
        _next_row = 0;

        if (_total_rows == 0) {
            // meet an empty json array.
            *is_empty_row = true;
        }
    }
    return Status::OK();
}

// read one json string from line reader or file reader and parse it to json doc.
// return Status::DataQualityError() if data has quality error.
// return other error if encounter other problems.
// return Status::OK() if parse succeed or reach EOF.
Status NewJsonReader::_parse_json_doc(size_t* size, bool* eof) {
    // read a whole message
    SCOPED_TIMER(_file_read_timer);
    const uint8_t* json_str = nullptr;
    std::unique_ptr<uint8_t[]> json_str_ptr;
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, size, eof));
    } else {
        int64_t length = 0;
        RETURN_IF_ERROR(_real_file_reader->read_one_message(&json_str_ptr, &length));
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

    // clear memory here.
    _value_allocator.Clear();
    _parse_allocator.Clear();
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
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       _origin_json_doc.GetParseError(),
                       rapidjson::GetParseError_En(_origin_json_doc.GetParseError()));
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return std::string((char*)json_str, *size); },
                [&]() -> std::string { return fmt::to_string(error_msg); }, _scanner_eof));
        _counter->num_rows_filtered++;
        if (*_scanner_eof) {
            // Case A: if _scanner_eof is set to true in "append_error_msg_to_file", which means
            // we meet enough invalid rows and the scanner should be stopped.
            // So we set eof to true and return OK, the caller will stop the process as we meet the end of file.
            *eof = true;
            return Status::OK();
        }
        return Status::DataQualityError(fmt::to_string(error_msg));
    }

    // set json root
    if (_parsed_json_root.size() != 0) {
        _json_doc = JsonFunctions::get_json_object_from_parsed_json(
                _parsed_json_root, &_origin_json_doc, _origin_json_doc.GetAllocator());
        if (_json_doc == nullptr) {
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "{}", "JSON Root not found.");
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string { return _print_json_value(_origin_json_doc); },
                    [&]() -> std::string { return fmt::to_string(error_msg); }, _scanner_eof));
            _counter->num_rows_filtered++;
            if (*_scanner_eof) {
                // Same as Case A
                *eof = true;
                return Status::OK();
            }
            return Status::DataQualityError(fmt::to_string(error_msg));
        }
    } else {
        _json_doc = &_origin_json_doc;
    }

    if (_json_doc->IsArray() && !_strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is array-object, `strip_outer_array` must be TRUE.");
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return _print_json_value(_origin_json_doc); },
                [&]() -> std::string { return fmt::to_string(error_msg); }, _scanner_eof));
        _counter->num_rows_filtered++;
        if (*_scanner_eof) {
            // Same as Case A
            *eof = true;
            return Status::OK();
        }
        return Status::DataQualityError(fmt::to_string(error_msg));
    }

    if (!_json_doc->IsArray() && _strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return _print_json_value(_origin_json_doc); },
                [&]() -> std::string { return fmt::to_string(error_msg); }, _scanner_eof));
        _counter->num_rows_filtered++;
        if (*_scanner_eof) {
            // Same as Case A
            *eof = true;
            return Status::OK();
        }
        return Status::DataQualityError(fmt::to_string(error_msg));
    }

    return Status::OK();
}

// for simple format json
// set valid to true and return OK if succeed.
// set valid to false and return OK if we met an invalid row.
// return other status if encounter other problems.
Status NewJsonReader::_set_column_value(rapidjson::Value& objectValue,
                                        std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* valid) {
    if (!objectValue.IsObject()) {
        // Here we expect the incoming `objectValue` to be a Json Object, such as {"key" : "value"},
        // not other type of Json format.
        RETURN_IF_ERROR(_append_error_msg(objectValue, "Expect json object value", "", valid));
        return Status::OK();
    }

    int ctx_idx = 0;
    bool has_valid_value = false;
    size_t cur_row_count = columns[0]->size();
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

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
            has_valid_value = true;
        } else { // not found
            // When the entire row has no valid value, this row should be filtered,
            // so the default value cannot be directly inserted here
            if (!slot_desc->is_nullable()) {
                RETURN_IF_ERROR(_append_error_msg(
                        objectValue,
                        "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_desc->col_name(), valid));
                break;
            }
        }
    }
    if (!has_valid_value) {
        RETURN_IF_ERROR(_append_error_msg(objectValue, "All fields is null, this is a invalid row.",
                                          "", valid));
        return Status::OK();
    }
    ctx_idx = 0;
    int nullcount = 0;
    // fill missing slot
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        if (column_ptr->size() < cur_row_count + 1) {
            DCHECK(column_ptr->size() == cur_row_count);
            column_ptr->assume_mutable()->insert_default();
            ++nullcount;
        }
        DCHECK(column_ptr->size() == cur_row_count + 1);
    }
    // There is at least one valid value here
    DCHECK(nullcount < columns.size());
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                            SlotDescriptor* slot_desc,
                                            vectorized::IColumn* column_ptr, bool* valid) {
    const char* str_value = nullptr;
    char tmp_buf[128] = {0};
    int32_t wbytes = 0;
    std::string json_str;

    vectorized::ColumnNullable* nullable_column = nullptr;
    if (slot_desc->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        // kNullType will put 1 into the Null map, so there is no need to push 0 for kNullType.
        if (value->GetType() != rapidjson::Type::kNullType) {
            nullable_column->get_null_map_data().push_back(0);
        } else {
            nullable_column->insert_default();
        }
        column_ptr = &nullable_column->get_nested_column();
    }

    switch (value->GetType()) {
    case rapidjson::Type::kStringType:
        str_value = value->GetString();
        wbytes = strlen(str_value);
        break;
    case rapidjson::Type::kNumberType:
        if (value->IsUint()) {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%u", value->GetUint());
        } else if (value->IsInt()) {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d", value->GetInt());
        } else if (value->IsUint64()) {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRIu64, value->GetUint64());
        } else if (value->IsInt64()) {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRId64, value->GetInt64());
        } else {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%f", value->GetDouble());
        }
        str_value = tmp_buf;
        break;
    case rapidjson::Type::kFalseType:
        wbytes = 1;
        str_value = (char*)"0";
        break;
    case rapidjson::Type::kTrueType:
        wbytes = 1;
        str_value = (char*)"1";
        break;
    case rapidjson::Type::kNullType:
        if (!slot_desc->is_nullable()) {
            RETURN_IF_ERROR(_append_error_msg(
                    *value, "Json value is null, but the column `{}` is not nullable.",
                    slot_desc->col_name(), valid));
            return Status::OK();
        }
        // return immediately to prevent from repeatedly insert_data
        *valid = true;
        return Status::OK();
    default:
        // for other type like array or object. we convert it to string to save
        json_str = NewJsonReader::_print_json_value(*value);
        wbytes = json_str.size();
        str_value = json_str.c_str();
        break;
    }

    // TODO: if the vexpr can support another 'slot_desc type' than 'TYPE_VARCHAR',
    // we need use a function to support these types to insert data in columns.
    DCHECK(slot_desc->type().type == TYPE_VARCHAR || slot_desc->type().type == TYPE_STRING)
            << slot_desc->type().type << ", query id: " << print_id(_state->query_id());
    assert_cast<ColumnString*>(column_ptr)->insert_data(str_value, wbytes);

    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                                 const std::vector<SlotDescriptor*>& slot_descs,
                                                 std::vector<MutableColumnPtr>& columns,
                                                 bool* valid) {
    int ctx_idx = 0;
    bool has_valid_value = false;
    size_t cur_row_count = columns[0]->size();
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int i = ctx_idx++;
        auto* column_ptr = columns[i].get();
        rapidjson::Value* json_values = nullptr;
        bool wrap_explicitly = false;
        if (LIKELY(i < _parsed_jsonpaths.size())) {
            json_values = JsonFunctions::get_json_array_from_parsed_json(
                    _parsed_jsonpaths[i], &objectValue, _origin_json_doc.GetAllocator(),
                    &wrap_explicitly);
        }

        if (json_values == nullptr) {
            // not match in jsondata.
            if (!slot_descs[i]->is_nullable()) {
                RETURN_IF_ERROR(_append_error_msg(
                        objectValue,
                        "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_descs[i]->col_name(), valid));
                return Status::OK();
            }
        } else {
            CHECK(json_values->IsArray());
            if (json_values->Size() == 1 && wrap_explicitly) {
                // NOTICE1: JsonFunctions::get_json_array_from_parsed_json() will wrap the single json object with an array.
                // so here we unwrap the array to get the real element.
                // if json_values' size > 1, it means we just match an array, not a wrapped one, so no need to unwrap.
                json_values = &((*json_values)[0]);
            }
            RETURN_IF_ERROR(_write_data_to_column(json_values, slot_descs[i], column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
            has_valid_value = true;
        }
    }
    if (!has_valid_value) {
        RETURN_IF_ERROR(_append_error_msg(
                objectValue, "All fields is null or not matched, this is a invalid row.", "",
                valid));
        return Status::OK();
    }
    ctx_idx = 0;
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        if (column_ptr->size() < cur_row_count + 1) {
            DCHECK(column_ptr->size() == cur_row_count);
            column_ptr->assume_mutable()->insert_default();
        }
        DCHECK(column_ptr->size() == cur_row_count + 1);
    }
    return Status::OK();
}

Status NewJsonReader::_append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                                        std::string col_name, bool* valid) {
    std::string err_msg;
    if (!col_name.empty()) {
        fmt::memory_buffer error_buf;
        fmt::format_to(error_buf, error_msg, col_name);
        err_msg = fmt::to_string(error_buf);
    } else {
        err_msg = error_msg;
    }

    RETURN_IF_ERROR(_state->append_error_msg_to_file(
            [&]() -> std::string { return NewJsonReader::_print_json_value(objectValue); },
            [&]() -> std::string { return err_msg; }, _scanner_eof));

    // TODO(ftw): check here？
    if (*_scanner_eof == true) {
        _reader_eof = true;
    }

    _counter->num_rows_filtered++;
    if (valid != nullptr) {
        // current row is invalid
        *valid = false;
    }
    return Status::OK();
}

std::string NewJsonReader::_print_json_value(const rapidjson::Value& value) {
    rapidjson::StringBuffer buffer;
    buffer.Clear();
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return std::string(buffer.GetString());
}

} // namespace doris::vectorized
