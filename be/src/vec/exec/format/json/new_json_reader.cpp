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

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <inttypes.h>
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <map>
#include <memory>
#include <ostream>
#include <string_view>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "exprs/json_functions.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/stream_load_pipe.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "vec/core/block.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/json/simd_json_parser.h"
// dynamic table
#include "common/config.h"
#include "io/fs/file_reader.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/json/json_parser.h"
#include "vec/json/parse2column.h"

namespace doris {
namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
} // namespace doris

namespace doris::vectorized {
using namespace ErrorCode;

NewJsonReader::NewJsonReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                             const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs, bool* scanner_eof,
                             io::IOContext* io_ctx)
        : _vhandle_json_callback(nullptr),
          _state(state),
          _profile(profile),
          _counter(counter),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _file_system(nullptr),
          _file_reader(nullptr),
          _line_reader(nullptr),
          _reader_eof(false),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator),
          _scanner_eof(scanner_eof),
          _current_offset(0),
          _io_ctx(io_ctx) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
    _init_system_properties();
    _init_file_description();
}

NewJsonReader::NewJsonReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             io::IOContext* io_ctx)
        : _vhandle_json_callback(nullptr),
          _state(nullptr),
          _profile(profile),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _line_reader(nullptr),
          _reader_eof(false),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator),
          _io_ctx(io_ctx) {
    _init_system_properties();
    _init_file_description();
}

void NewJsonReader::_init_system_properties() {
    if (_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _range.file_type;
    } else {
        _system_properties.system_type = _params.file_type;
    }
    _system_properties.properties = _params.properties;
    _system_properties.hdfs_params = _params.hdfs_params;
    if (_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                                   _params.broker_addresses.end());
    }
}

void NewJsonReader::_init_file_description() {
    _file_description.path = _range.path;
    _file_description.file_size = _range.__isset.file_size ? _range.file_size : -1;

    if (_range.__isset.fs_name) {
        _file_description.fs_name = _range.fs_name;
    }
}

Status NewJsonReader::init_reader(
        const std::unordered_map<std::string, vectorized::VExprContextSPtr>&
                col_default_value_ctx) {
    // generate _col_default_value_map
    RETURN_IF_ERROR(_get_column_default_value(_file_slot_descs, col_default_value_ctx));

#ifdef __AVX2__
    if (config::enable_simdjson_reader) {
        RETURN_IF_ERROR(_simdjson_init_reader());
        return Status::OK();
    }
#endif

    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader(false));
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
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        _slot_desc_index[_file_slot_descs[i]->col_name()] = i;
    }
    return Status::OK();
}

Status NewJsonReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_reader_eof == true) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = std::max(_state->batch_size(), (int)_MIN_BATCH_SIZE);

    while (block->rows() < batch_size && !_reader_eof) {
        if (UNLIKELY(_read_json_by_line && _skip_first_line)) {
            size_t size = 0;
            const uint8_t* line_ptr = nullptr;
            RETURN_IF_ERROR(_line_reader->read_line(&line_ptr, &size, &_reader_eof, _io_ctx));
            _skip_first_line = false;
            continue;
        }

        bool is_empty_row = false;

        RETURN_IF_ERROR(
                _read_json_column(_state, *block, _file_slot_descs, &is_empty_row, &_reader_eof));
        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
        ++(*read_rows);
    }

    return Status::OK();
}

Status NewJsonReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status NewJsonReader::get_parsed_schema(std::vector<std::string>* col_names,
                                        std::vector<TypeDescriptor>* col_types) {
    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader(true));
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
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, &size, &eof, _io_ctx));
    } else {
        size_t read_size = 0;
        RETURN_IF_ERROR(_read_one_message(&json_str_ptr, &read_size));
        json_str = json_str_ptr.get();
        size = read_size;
        if (read_size == 0) {
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

Status NewJsonReader::_open_file_reader(bool need_schema) {
    int64_t start_offset = _range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }

    _current_offset = start_offset;

    if (_params.file_type == TFileType::FILE_STREAM) {
        // Due to http_stream needs to pre read a portion of the data to parse column information, so it is set to true here
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state,
                                                        need_schema));
    } else {
        _file_description.mtime = _range.__isset.modification_time ? _range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        RETURN_IF_ERROR(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options, &_file_system,
                &_file_reader, io::DelegateReader::AccessMode::SEQUENTIAL, _io_ctx,
                io::PrefetchRange(_range.start_offset, _range.size)));
    }
    return Status::OK();
}

Status NewJsonReader::_open_line_reader() {
    int64_t size = _range.size;
    if (_range.start_offset != 0) {
        // When we fetch range doesn't start from 0, size will += 1.
        size += 1;
        _skip_first_line = true;
    } else {
        _skip_first_line = false;
    }
    _line_reader = NewPlainTextLineReader::create_unique(
            _profile, _file_reader, nullptr,
            std::make_shared<PlainTextLineReaderCtx>(_line_delimiter, _line_delimiter_length), size,
            _current_offset);
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

Status NewJsonReader::_read_json_column(RuntimeState* state, Block& block,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof) {
    return (this->*_vhandle_json_callback)(state, block, slot_descs, is_empty_row, eof);
}

Status NewJsonReader::_vhandle_simple_json(RuntimeState* /*state*/, Block& block,
                                           const std::vector<SlotDescriptor*>& slot_descs,
                                           bool* is_empty_row, bool* eof) {
    do {
        bool valid = false;
        if (_next_row >= _total_rows) { // parse json and generic document
            Status st = _parse_json(is_empty_row, eof);
            if (st.is<DATA_QUALITY_ERROR>()) {
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
            RETURN_IF_ERROR(_set_column_value(objectValue, block, slot_descs, &valid));
        } else { // handle case 2
            RETURN_IF_ERROR(_set_column_value(*_json_doc, block, slot_descs, &valid));
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
        RuntimeState* /*state*/, Block& block, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
    do {
        if (_next_row >= _total_rows) {
            Status st = _parse_json(is_empty_row, eof);
            if (st.is<DATA_QUALITY_ERROR>()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                if (st.ok()) {
                    return st;
                }
                if (_total_rows == 0) {
                    continue;
                }
            }
        }
        rapidjson::Value& objectValue = (*_json_doc)[_next_row++];
        bool valid = true;
        RETURN_IF_ERROR(_write_columns_by_jsonpath(objectValue, slot_descs, block, &valid));
        if (!valid) {
            continue; // process next line
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_row <= _total_rows);
    return Status::OK();
}

Status NewJsonReader::_vhandle_nested_complex_json(RuntimeState* /*state*/, Block& block,
                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                   bool* is_empty_row, bool* eof) {
    while (true) {
        Status st = _parse_json(is_empty_row, eof);
        if (st.is<DATA_QUALITY_ERROR>()) {
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
    RETURN_IF_ERROR(_write_columns_by_jsonpath(*_json_doc, slot_descs, block, &valid));
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
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, size, eof, _io_ctx));
    } else {
        RETURN_IF_ERROR(_read_one_message(&json_str_ptr, size));
        json_str = json_str_ptr.get();
        if (*size == 0) {
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
Status NewJsonReader::_set_column_value(rapidjson::Value& objectValue, Block& block,
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
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        auto* column_ptr = block.get_by_position(dest_index).column->assume_mutable().get();
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
        } else {
            // not found, filling with default value
            RETURN_IF_ERROR(_fill_missing_column(slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        }
    }
    if (!has_valid_value) {
        // there is no valid value in json line but has filled with default value before
        // so remove this line in block
        for (int i = 0; i < block.columns(); ++i) {
            auto column = block.get_by_position(i).column->assume_mutable();
            column->pop_back(1);
        }
        RETURN_IF_ERROR(_append_error_msg(objectValue, "All fields is null, this is a invalid row.",
                                          "", valid));
        return Status::OK();
    }
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
        wbytes = value->GetStringLength();
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
        } else if (value->IsFloat() || value->IsDouble()) {
            auto end = fmt::format_to(tmp_buf, "{}", value->GetDouble());
            wbytes = end - tmp_buf;
        } else {
            return Status::InternalError("It should not here.");
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
                                                 Block& block, bool* valid) {
    int ctx_idx = 0;
    bool has_valid_value = false;
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int i = ctx_idx++;
        auto* column_ptr = block.get_by_position(i).column->assume_mutable().get();
        rapidjson::Value* json_values = nullptr;
        bool wrap_explicitly = false;
        if (LIKELY(i < _parsed_jsonpaths.size())) {
            json_values = JsonFunctions::get_json_array_from_parsed_json(
                    _parsed_jsonpaths[i], &objectValue, _origin_json_doc.GetAllocator(),
                    &wrap_explicitly);
        }
        if (json_values != nullptr) {
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
        } else {
            // not found, filling with default value
            RETURN_IF_ERROR(_fill_missing_column(slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        }
    }
    if (!has_valid_value) {
        // there is no valid value in json line but has filled with default value before
        // so remove this line in block
        for (int i = 0; i < block.columns(); ++i) {
            auto column = block.get_by_position(i).column->assume_mutable();
            column->pop_back(1);
        }
        RETURN_IF_ERROR(_append_error_msg(
                objectValue, "All fields is null or not matched, this is a invalid row.", "",
                valid));
        return Status::OK();
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

Status NewJsonReader::_read_one_message(std::unique_ptr<uint8_t[]>* file_buf, size_t* read_size) {
    switch (_params.file_type) {
    case TFileType::FILE_LOCAL:
        [[fallthrough]];
    case TFileType::FILE_HDFS:
        [[fallthrough]];
    case TFileType::FILE_S3: {
        size_t file_size = _file_reader->size();
        file_buf->reset(new uint8_t[file_size]);
        Slice result(file_buf->get(), file_size);
        RETURN_IF_ERROR(_file_reader->read_at(_current_offset, result, read_size, _io_ctx));
        _current_offset += *read_size;
        break;
    }
    case TFileType::FILE_STREAM: {
        RETURN_IF_ERROR((dynamic_cast<io::StreamLoadPipe*>(_file_reader.get()))
                                ->read_one_message(file_buf, read_size));
        break;
    }
    default: {
        return Status::NotSupported("no supported file reader type: {}", _params.file_type);
    }
    }
    return Status::OK();
}
// ---------SIMDJSON----------
// simdjson, replace none simdjson function if it is ready
Status NewJsonReader::_simdjson_init_reader() {
    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader(false));
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_open_line_reader());
    }

    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _vhandle_json_callback = &NewJsonReader::_simdjson_handle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _vhandle_json_callback = &NewJsonReader::_simdjson_handle_flat_array_complex_json;
        } else {
            _vhandle_json_callback = &NewJsonReader::_simdjson_handle_nested_complex_json;
        }
    }
    _ondemand_json_parser = std::make_unique<simdjson::ondemand::parser>();
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        _slot_desc_index[_file_slot_descs[i]->col_name()] = i;
    }
    _simdjson_ondemand_padding_buffer.resize(_padded_size);
    _simdjson_ondemand_unscape_padding_buffer.resize(_padded_size);
    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_simple_json(RuntimeState* /*state*/, Block& block,
                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                   bool* is_empty_row, bool* eof) {
    // simple json
    simdjson::ondemand::object objectValue;
    size_t num_rows = block.rows();
    do {
        bool valid = false;
        try {
            if (_next_row >= _total_rows) { // parse json and generic document
                Status st = _simdjson_parse_json(is_empty_row, eof);
                if (st.is<DATA_QUALITY_ERROR>()) {
                    continue; // continue to read next
                }
                RETURN_IF_ERROR(st);
                if (*is_empty_row == true) {
                    return Status::OK();
                }
                if (_json_value.type() == simdjson::ondemand::json_type::array) {
                    _array = _json_value.get_array();
                    _array_iter = _array.begin();

                    _total_rows = _array.count_elements();
                    if (_total_rows == 0) {
                        // may be passing an empty json, such as "[]"
                        RETURN_IF_ERROR(_append_error_msg(nullptr, "Empty json line", "", nullptr));
                        if (*_scanner_eof) {
                            *is_empty_row = true;
                            return Status::OK();
                        }
                        continue;
                    }
                } else {
                    _total_rows = 1; // only one row
                    objectValue = _json_value;
                }
                _next_row = 0;
            }

            if (_json_value.type() == simdjson::ondemand::json_type::array) { // handle case 1
                objectValue = *_array_iter;
                RETURN_IF_ERROR(
                        _simdjson_set_column_value(&objectValue, block, slot_descs, &valid));
                if (_array_iter == _array.end()) {
                    // Hint to read next json doc
                    _next_row = _total_rows + 1;
                    break;
                }
                ++_array_iter;
            } else { // handle case 2
                // objectValue = _json_value.get_object();
                RETURN_IF_ERROR(
                        _simdjson_set_column_value(&objectValue, block, slot_descs, &valid));
            }
            _next_row++;
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
        } catch (simdjson::simdjson_error& e) {
            // prevent from endless loop
            _next_row = _total_rows + 1;
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Parse json data failed. code: {}, error info: {}", e.error(),
                           e.what());
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string {
                        return std::string(_simdjson_ondemand_padding_buffer.data(),
                                           _original_doc_size);
                    },
                    [&]() -> std::string { return fmt::to_string(error_msg); }, eof));
            _counter->num_rows_filtered++;
            // Before continuing to process other rows, we need to first clean the fail parsed row.
            for (int i = 0; i < block.columns(); ++i) {
                auto column = block.get_by_position(i).column->assume_mutable();
                if (column->size() > num_rows) {
                    column->pop_back(column->size() - num_rows);
                }
            }
            if (!valid) {
                if (*_scanner_eof) {
                    // When _scanner_eof is true and valid is false, it means that we have encountered
                    // unqualified data and decided to stop the scan.
                    *is_empty_row = true;
                    return Status::OK();
                }
                continue;
            }
            continue;
        }
    } while (_next_row <= _total_rows);
    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_flat_array_complex_json(
        RuntimeState* /*state*/, Block& block, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
// Advance one row in array list, if it is the endpoint, stop advance and break the loop
#define ADVANCE_ROW()                  \
    if (_array_iter == _array.end()) { \
        _next_row = _total_rows + 1;   \
        break;                         \
    }                                  \
    ++_array_iter;                     \
    ++_next_row;

    // array complex json
    size_t num_rows = block.rows();
    simdjson::ondemand::object cur;
    do {
        try {
            if (_next_row >= _total_rows) {
                Status st = _simdjson_parse_json(is_empty_row, eof);
                if (st.is<DATA_QUALITY_ERROR>()) {
                    continue; // continue to read next
                }
                RETURN_IF_ERROR(st);
                if (*is_empty_row == true) {
                    if (st.ok()) {
                        return st;
                    }
                    if (_total_rows == 0) {
                        continue;
                    }
                }
                _array = _json_value.get_array();
                _array_iter = _array.begin();
            }

            bool valid = true;
            cur = (*_array_iter).get_object();
            // extract root
            if (_parsed_json_root.size() != 0) {
                simdjson::ondemand::value val;
                Status st = JsonFunctions::extract_from_object(cur, _parsed_json_root, &val);
                if (UNLIKELY(!st.ok())) {
                    if (st.is<DATA_QUALITY_ERROR>()) {
                        RETURN_IF_ERROR(_append_error_msg(nullptr, st.to_string(), "", nullptr));
                        ADVANCE_ROW();
                        continue;
                    }
                    return st;
                }
                if (val.type() != simdjson::ondemand::json_type::object) {
                    RETURN_IF_ERROR(_append_error_msg(nullptr, "Not object item", "", nullptr));
                    ADVANCE_ROW();
                    continue;
                }
                cur = val.get_object();
            }
            RETURN_IF_ERROR(_simdjson_write_columns_by_jsonpath(&cur, slot_descs, block, &valid));
            ADVANCE_ROW();
            if (!valid) {
                continue; // process next line
            }
            *is_empty_row = false;
            break; // get a valid row, then break
        } catch (simdjson::simdjson_error& e) {
            // prevent from endless loop
            _next_row = _total_rows + 1;
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Parse json data failed. code: {}, error info: {}", e.error(),
                           e.what());
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string {
                        return std::string(_simdjson_ondemand_padding_buffer.data(),
                                           _original_doc_size);
                    },
                    [&]() -> std::string { return fmt::to_string(error_msg); }, eof));
            _counter->num_rows_filtered++;
            // Before continuing to process other rows, we need to first clean the fail parsed row.
            for (int i = 0; i < block.columns(); ++i) {
                auto column = block.get_by_position(i).column->assume_mutable();
                if (column->size() > num_rows) {
                    column->pop_back(column->size() - num_rows);
                }
            }
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                return Status::OK();
            }
            continue;
        }
    } while (_next_row <= _total_rows);
    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_nested_complex_json(
        RuntimeState* /*state*/, Block& block, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
    // nested complex json
    while (true) {
        size_t num_rows = block.rows();
        simdjson::ondemand::object cur;
        try {
            Status st = _simdjson_parse_json(is_empty_row, eof);
            if (st.is<DATA_QUALITY_ERROR>()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                return Status::OK();
            }
            *is_empty_row = false;
            bool valid = true;
            if (_json_value.type() != simdjson::ondemand::json_type::object) {
                RETURN_IF_ERROR(_append_error_msg(nullptr, "Not object item", "", nullptr));
                continue;
            }
            cur = _json_value.get_object();
            st = _simdjson_write_columns_by_jsonpath(&cur, slot_descs, block, &valid);
            if (!st.ok()) {
                RETURN_IF_ERROR(_append_error_msg(nullptr, st.to_string(), "", nullptr));
                // Before continuing to process other rows, we need to first clean the fail parsed row.
                for (int i = 0; i < block.columns(); ++i) {
                    auto column = block.get_by_position(i).column->assume_mutable();
                    if (column->size() > num_rows) {
                        column->pop_back(column->size() - num_rows);
                    }
                }
                continue;
            }
            if (!valid) {
                // there is only one line in this case, so if it return false, just set is_empty_row true
                // so that the caller will continue reading next line.
                *is_empty_row = true;
            }
            break; // read a valid row
        } catch (simdjson::simdjson_error& e) {
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Parse json data failed. code: {}, error info: {}", e.error(),
                           e.what());
            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                    [&]() -> std::string {
                        return std::string(_simdjson_ondemand_padding_buffer.data(),
                                           _original_doc_size);
                    },
                    [&]() -> std::string { return fmt::to_string(error_msg); }, eof));
            _counter->num_rows_filtered++;
            // Before continuing to process other rows, we need to first clean the fail parsed row.
            for (int i = 0; i < block.columns(); ++i) {
                auto column = block.get_by_position(i).column->assume_mutable();
                if (column->size() > num_rows) {
                    column->pop_back(column->size() - num_rows);
                }
            }
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                return Status::OK();
            }
            continue;
        }
    }
    return Status::OK();
}

size_t NewJsonReader::_column_index(const StringRef& name, size_t key_index) {
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.
    if (_prev_positions.size() > key_index && _prev_positions[key_index] &&
        name == _prev_positions[key_index]->get_first()) {
        return _prev_positions[key_index]->get_second();
    } else {
        auto* it = _slot_desc_index.find(name);
        if (it) {
            if (key_index < _prev_positions.size()) {
                _prev_positions[key_index] = it;
            }
            return it->get_second();
        } else {
            return size_t(-1);
        }
    }
}

Status NewJsonReader::_simdjson_set_column_value(simdjson::ondemand::object* value, Block& block,
                                                 const std::vector<SlotDescriptor*>& slot_descs,
                                                 bool* valid) {
    // set
    _seen_columns.assign(block.columns(), false);
    size_t cur_row_count = block.rows();
    bool has_valid_value = false;
    // iterate through object, simdjson::ondemond will parsing on the fly
    size_t key_index = 0;
    for (auto field : *value) {
        std::string_view key = field.unescaped_key();
        StringRef name_ref(key.data(), key.size());
        const size_t column_index = _column_index(name_ref, key_index++);
        if (UNLIKELY(ssize_t(column_index) < 0)) {
            // This key is not exist in slot desc, just ignore
            continue;
        }
        simdjson::ondemand::value val = field.value();
        auto* column_ptr = block.get_by_position(column_index).column->assume_mutable().get();
        RETURN_IF_ERROR(
                _simdjson_write_data_to_column(val, slot_descs[column_index], column_ptr, valid));
        if (!(*valid)) {
            return Status::OK();
        }
        _seen_columns[column_index] = true;
        has_valid_value = true;
    }
    if (!has_valid_value) {
        RETURN_IF_ERROR(
                _append_error_msg(value, "All fields is null, this is a invalid row.", "", valid));
        return Status::OK();
    }

    // fill missing slot
    int nullcount = 0;
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        if (_seen_columns[i]) {
            continue;
        }
        auto slot_desc = slot_descs[i];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto* column_ptr = block.get_by_position(i).column->assume_mutable().get();
        if (column_ptr->size() < cur_row_count + 1) {
            DCHECK(column_ptr->size() == cur_row_count);
            RETURN_IF_ERROR(_fill_missing_column(slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
            ++nullcount;
        }
        DCHECK(column_ptr->size() == cur_row_count + 1);
    }

#ifndef NDEBUG
    // Check all columns rows matched
    for (size_t i = 0; i < block.columns(); ++i) {
        DCHECK_EQ(block.get_by_position(i).column->size(), cur_row_count + 1);
    }
#endif
    // There is at least one valid value here
    DCHECK(nullcount < block.columns());
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_simdjson_write_data_to_column(simdjson::ondemand::value& value,
                                                     SlotDescriptor* slot_desc,
                                                     vectorized::IColumn* column, bool* valid) {
    // write
    vectorized::ColumnNullable* nullable_column = nullptr;
    vectorized::IColumn* column_ptr = nullptr;
    if (slot_desc->is_nullable()) {
        nullable_column = assert_cast<vectorized::ColumnNullable*>(column);
        column_ptr = &nullable_column->get_nested_column();
    }
    // TODO: if the vexpr can support another 'slot_desc type' than 'TYPE_VARCHAR',
    // we need use a function to support these types to insert data in columns.
    ColumnString* column_string = assert_cast<ColumnString*>(column_ptr);
    switch (value.type()) {
    case simdjson::ondemand::json_type::null: {
        if (column->is_nullable()) {
            // insert_default already push 1 to null_map
            nullable_column->insert_default();
        } else {
            RETURN_IF_ERROR(_append_error_msg(
                    nullptr, "Json value is null, but the column `{}` is not nullable.",
                    slot_desc->col_name(), valid));
            return Status::OK();
        }
        break;
    }
    case simdjson::ondemand::json_type::boolean: {
        nullable_column->get_null_map_data().push_back(0);
        if (value.get_bool()) {
            column_string->insert_data("1", 1);
        } else {
            column_string->insert_data("0", 1);
        }
        break;
    }
    default: {
        if (value.type() == simdjson::ondemand::json_type::string) {
            uint8_t* unescape_buffer =
                    reinterpret_cast<uint8_t*>(&_simdjson_ondemand_unscape_padding_buffer[0]);
            std::string_view unescaped_value =
                    _ondemand_json_parser->unescape(value.get_raw_json_string(), unescape_buffer);
            nullable_column->get_null_map_data().push_back(0);
            column_string->insert_data(unescaped_value.data(), unescaped_value.length());
            break;
        }
        auto value_str = simdjson::to_json_string(value).value();
        nullable_column->get_null_map_data().push_back(0);
        column_string->insert_data(value_str.data(), value_str.length());
    }
    }
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_append_error_msg(simdjson::ondemand::object* obj, std::string error_msg,
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
            [&]() -> std::string {
                if (!obj) {
                    return "";
                }
                std::string_view str_view;
                (void)!obj->raw_json().get(str_view);
                return std::string(str_view.data(), str_view.size());
            },
            [&]() -> std::string { return err_msg; }, _scanner_eof));

    _counter->num_rows_filtered++;
    if (valid != nullptr) {
        // current row is invalid
        *valid = false;
    }
    return Status::OK();
}

Status NewJsonReader::_simdjson_parse_json(bool* is_empty_row, bool* eof) {
    size_t size = 0;
    RETURN_IF_ERROR(_simdjson_parse_json_doc(&size, eof));

    // read all data, then return
    if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    if (!_parsed_jsonpaths.empty() && _strip_outer_array) {
        _total_rows = _json_value.count_elements().value();
        _next_row = 0;

        if (_total_rows == 0) {
            // meet an empty json array.
            *is_empty_row = true;
        }
    }
    return Status::OK();
}
Status NewJsonReader::_simdjson_parse_json_doc(size_t* size, bool* eof) {
    // read a whole message
    SCOPED_TIMER(_file_read_timer);
    const uint8_t* json_str = nullptr;
    std::unique_ptr<uint8_t[]> json_str_ptr;
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, size, eof, _io_ctx));
    } else {
        size_t length = 0;
        RETURN_IF_ERROR(_read_one_message(&json_str_ptr, &length));
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
    if (*size + simdjson::SIMDJSON_PADDING > _padded_size) {
        // For efficiency reasons, simdjson requires a string with a few bytes (simdjson::SIMDJSON_PADDING) at the end.
        // Hence, a re-allocation is needed if the space is not enough.
        _simdjson_ondemand_padding_buffer.resize(*size + simdjson::SIMDJSON_PADDING);
        _simdjson_ondemand_unscape_padding_buffer.resize(*size + simdjson::SIMDJSON_PADDING);
        _padded_size = *size + simdjson::SIMDJSON_PADDING;
    }
    // trim BOM since simdjson does not handle UTF-8 Unicode (with BOM)
    if (*size >= 3 && static_cast<char>(json_str[0]) == '\xEF' &&
        static_cast<char>(json_str[1]) == '\xBB' && static_cast<char>(json_str[2]) == '\xBF') {
        // skip the first three BOM bytes
        json_str += 3;
        *size -= 3;
    }
    memcpy(&_simdjson_ondemand_padding_buffer.front(), json_str, *size);
    _original_doc_size = *size;
    auto error =
            _ondemand_json_parser
                    ->iterate(std::string_view(_simdjson_ondemand_padding_buffer.data(), *size),
                              _padded_size)
                    .get(_original_json_doc);
    auto return_quality_error = [&](fmt::memory_buffer& error_msg,
                                    const std::string& doc_info) -> Status {
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return doc_info; },
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
    };
    if (error != simdjson::error_code::SUCCESS) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       error, simdjson::error_message(error));
        return return_quality_error(error_msg, std::string((char*)json_str, *size));
    }
    auto type_res = _original_json_doc.type();
    if (type_res.error() != simdjson::error_code::SUCCESS) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       type_res.error(), simdjson::error_message(type_res.error()));
        return return_quality_error(error_msg, std::string((char*)json_str, *size));
    }
    simdjson::ondemand::json_type type = type_res.value();
    if (type != simdjson::ondemand::json_type::object &&
        type != simdjson::ondemand::json_type::array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Not an json object or json array");
        return return_quality_error(error_msg, std::string((char*)json_str, *size));
    }
    if (_parsed_json_root.size() != 0 && type == simdjson::ondemand::json_type::object) {
        try {
            // set json root
            // if it is an array at top level, then we should iterate the entire array in
            // ::_simdjson_handle_flat_array_complex_json
            simdjson::ondemand::object object = _original_json_doc;
            Status st = JsonFunctions::extract_from_object(object, _parsed_json_root, &_json_value);
            if (!st.ok()) {
                fmt::memory_buffer error_msg;
                fmt::format_to(error_msg, "{}", st.to_string());
                return return_quality_error(error_msg, std::string((char*)json_str, *size));
            }
        } catch (simdjson::simdjson_error& e) {
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Encounter error while extract_from_object, error: {}",
                           e.what());
            return return_quality_error(error_msg, std::string((char*)json_str, *size));
        }
    } else {
        _json_value = _original_json_doc;
    }

    if (_json_value.type() == simdjson::ondemand::json_type::array && !_strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is array-object, `strip_outer_array` must be TRUE.");
        return return_quality_error(error_msg, std::string((char*)json_str, *size));
    }

    if (_json_value.type() != simdjson::ondemand::json_type::array && _strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
        return return_quality_error(error_msg, std::string((char*)json_str, *size));
    }
    return Status::OK();
}

Status NewJsonReader::_simdjson_write_columns_by_jsonpath(
        simdjson::ondemand::object* value, const std::vector<SlotDescriptor*>& slot_descs,
        Block& block, bool* valid) {
    // write by jsonpath
    bool has_valid_value = false;
    for (size_t i = 0; i < slot_descs.size(); i++) {
        auto slot_desc = slot_descs[i];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto* column_ptr = block.get_by_position(i).column->assume_mutable().get();
        simdjson::ondemand::value json_value;
        Status st;
        if (i < _parsed_jsonpaths.size()) {
            st = JsonFunctions::extract_from_object(*value, _parsed_jsonpaths[i], &json_value);
            if (!st.ok() && !st.is<DATA_QUALITY_ERROR>()) {
                return st;
            }
        }
        if (i >= _parsed_jsonpaths.size() || st.is<DATA_QUALITY_ERROR>()) {
            // not match in jsondata, filling with default value
            RETURN_IF_ERROR(_fill_missing_column(slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        } else {
            RETURN_IF_ERROR(
                    _simdjson_write_data_to_column(json_value, slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
            has_valid_value = true;
        }
    }
    if (!has_valid_value) {
        // there is no valid value in json line but has filled with default value before
        // so remove this line in block
        for (int i = 0; i < block.columns(); ++i) {
            auto column = block.get_by_position(i).column->assume_mutable();
            column->pop_back(1);
        }
        RETURN_IF_ERROR(
                _append_error_msg(value, "All fields is null, this is a invalid row.", "", valid));
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_get_column_default_value(
        const std::vector<SlotDescriptor*>& slot_descs,
        const std::unordered_map<std::string, vectorized::VExprContextSPtr>&
                col_default_value_ctx) {
    for (auto slot_desc : slot_descs) {
        auto it = col_default_value_ctx.find(slot_desc->col_name());
        if (it != col_default_value_ctx.end() && it->second != nullptr) {
            auto& ctx = it->second;
            // NULL_LITERAL means no valid value of current column
            if (ctx->root()->node_type() == TExprNodeType::type::NULL_LITERAL) {
                continue;
            }
            // empty block to save default value of slot_desc->col_name()
            Block block;
            // If block is empty, some functions will produce no result. So we insert a column with
            // single value here.
            block.insert({ColumnUInt8::create(1), std::make_shared<DataTypeUInt8>(), ""});
            int result = -1;
            RETURN_IF_ERROR(ctx->execute(&block, &result));
            DCHECK(result != -1);
            auto column = block.get_by_position(result).column;
            DCHECK(column->size() == 1);
            _col_default_value_map.emplace(slot_desc->col_name(),
                                           column->get_data_at(0).to_string());
        }
    }
    return Status::OK();
}

Status NewJsonReader::_fill_missing_column(SlotDescriptor* slot_desc,
                                           vectorized::IColumn* column_ptr, bool* valid) {
    if (slot_desc->is_nullable()) {
        vectorized::ColumnNullable* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        column_ptr = &nullable_column->get_nested_column();
        auto col_value = _col_default_value_map.find(slot_desc->col_name());
        if (col_value == _col_default_value_map.end()) {
            nullable_column->insert_default();
        } else {
            const std::string& v_str = col_value->second;
            nullable_column->get_null_map_data().push_back(0);
            assert_cast<ColumnString*>(column_ptr)->insert_data(v_str.c_str(), v_str.size());
        }
    } else {
        RETURN_IF_ERROR(_append_error_msg(
                nullptr, "The column `{}` is not nullable, but it's not found in jsondata.",
                slot_desc->col_name(), valid));
    }

    *valid = true;
    return Status::OK();
}

} // namespace doris::vectorized
