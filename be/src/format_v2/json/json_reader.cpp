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

#include "format_v2/json/json_reader.h"

#include <rapidjson/document.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr_context.h"
#include "format/file_reader/new_plain_text_line_reader.h"
#include "format_v2/column_mapper.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/stream_load_pipe.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/decompressor.h"
#include "util/slice.h"

namespace doris::format::json {
namespace {

DataTypePtr nullable_type(DataTypePtr type) {
    return type != nullptr && type->is_nullable() ? std::move(type)
                                                  : make_nullable(std::move(type));
}

DataTypePtr json_file_type_from_slot_type(const DataTypePtr& type) {
    if (type == nullptr) {
        return nullptr;
    }

    // Text-like file readers expose CHAR/VARCHAR as STRING and let the table column mapper cast to
    // the destination slot type. JSON follows the same file-schema convention so that v2 mapping
    // behaves consistently across text formats.
    const bool is_nullable = type->is_nullable();
    const auto nested_type = remove_nullable(type);
    DataTypePtr file_type;
    switch (nested_type->get_primitive_type()) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        file_type = std::make_shared<DataTypeString>();
        break;
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        file_type = std::make_shared<DataTypeArray>(
                json_file_type_from_slot_type(array_type->get_nested_type()));
        break;
    }
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        file_type = std::make_shared<DataTypeMap>(
                json_file_type_from_slot_type(map_type->get_key_type()),
                json_file_type_from_slot_type(map_type->get_value_type()));
        break;
    }
    case TYPE_STRUCT: {
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        DataTypes file_children;
        file_children.reserve(struct_type->get_elements().size());
        for (const auto& child_type : struct_type->get_elements()) {
            file_children.push_back(json_file_type_from_slot_type(child_type));
        }
        file_type =
                std::make_shared<DataTypeStruct>(file_children, struct_type->get_element_names());
        break;
    }
    default:
        file_type = nested_type;
        break;
    }

    return is_nullable ? make_nullable(file_type) : file_type;
}

ColumnDefinition synthetic_file_child(const std::string& name, DataTypePtr type, int32_t local_id);

std::vector<ColumnDefinition> synthesize_file_children_from_type(const DataTypePtr& type) {
    std::vector<ColumnDefinition> children;
    if (type == nullptr) {
        return children;
    }
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        children.push_back(synthetic_file_child("element", array_type->get_nested_type(), 0));
        break;
    }
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        children.push_back(synthetic_file_child("key", map_type->get_key_type(), 0));
        children.push_back(synthetic_file_child("value", map_type->get_value_type(), 1));
        break;
    }
    case TYPE_STRUCT: {
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        children.reserve(struct_type->get_elements().size());
        for (size_t idx = 0; idx < struct_type->get_elements().size(); ++idx) {
            children.push_back(synthetic_file_child(struct_type->get_element_name(idx),
                                                    struct_type->get_element(idx),
                                                    cast_set<int32_t>(idx)));
        }
        break;
    }
    default:
        break;
    }
    return children;
}

ColumnDefinition synthetic_file_child(const std::string& name, DataTypePtr type, int32_t local_id) {
    ColumnDefinition child;
    child.identifier = Field::create_field<TYPE_STRING>(name);
    child.local_id = local_id;
    child.name = name;
    child.type = std::move(type);
    child.children = synthesize_file_children_from_type(child.type);
    return child;
}

std::string lower_key(std::string_view key) {
    std::string lowered(key.data(), key.size());
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), ::tolower);
    return lowered;
}

} // namespace

JsonReader::JsonReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                       std::unique_ptr<io::FileDescription>& file_description,
                       std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                       const TFileScanRangeParams* scan_params, const TFileRangeDesc& range,
                       const std::vector<SlotDescriptor*>& file_slot_descs,
                       TFileCompressType::type range_compress_type,
                       std::optional<TUniqueId> stream_load_id)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _scan_params(scan_params),
          _range(range),
          _source_file_slot_descs(file_slot_descs),
          _range_compress_type(range_compress_type),
          _stream_load_id(std::move(stream_load_id)) {}

JsonReader::~JsonReader() {
    static_cast<void>(close());
}

Status JsonReader::init(RuntimeState* state) {
    _runtime_state = state;
    if (_scan_params == nullptr) {
        return Status::InvalidArgument("JSON v2 reader requires scan params");
    }
    if (_file_description == nullptr) {
        return Status::InvalidArgument("JSON v2 reader requires file description");
    }
    if (_runtime_state == nullptr) {
        return Status::InvalidArgument("JSON v2 reader requires runtime state");
    }
    if (!_scan_params->__isset.file_attributes) {
        return Status::InvalidArgument("JSON v2 reader requires file attributes");
    }

    const auto& attributes = _scan_params->file_attributes;
    if (attributes.__isset.text_params && attributes.text_params.__isset.line_delimiter) {
        _line_delimiter = attributes.text_params.line_delimiter;
    } else {
        _line_delimiter = "\n";
    }
    _line_delimiter_length = _line_delimiter.size();
    _jsonpaths = attributes.__isset.jsonpaths ? attributes.jsonpaths : "";
    _json_root = attributes.__isset.json_root ? attributes.json_root : "";
    _read_json_by_line = attributes.__isset.read_json_by_line && attributes.read_json_by_line;
    _strip_outer_array = attributes.__isset.strip_outer_array && attributes.strip_outer_array;
    _num_as_string = attributes.__isset.num_as_string && attributes.num_as_string;
    _fuzzy_parse = attributes.__isset.fuzzy_parse && attributes.fuzzy_parse;
    _openx_json_ignore_malformed = attributes.__isset.openx_json_ignore_malformed &&
                                   attributes.openx_json_ignore_malformed;
    _is_hive_table = _range.table_format_params.table_format_type == "hive";
    _file_compress_type = _range_compress_type != TFileCompressType::UNKNOWN
                                  ? _range_compress_type
                                  : _scan_params->compress_type;

    _source_serdes = create_data_type_serdes(_source_file_slot_descs);
    _file_schema.clear();
    _file_schema.reserve(_source_file_slot_descs.size());
    // JSON has no physical footer schema. The FE file slots are therefore the authoritative schema
    // for both field names and source local ids.
    for (size_t idx = 0; idx < _source_file_slot_descs.size(); ++idx) {
        const auto* slot = _source_file_slot_descs[idx];
        DORIS_CHECK(slot != nullptr);
        ColumnDefinition field;
        field.identifier = Field::create_field<TYPE_STRING>(slot->col_name());
        field.local_id = cast_set<int32_t>(idx);
        field.name = slot->col_name();
        field.type = nullable_type(json_file_type_from_slot_type(slot->get_data_type_ptr()));
        field.children = synthesize_file_children_from_type(field.type);
        _file_schema.push_back(std::move(field));
    }
    _eof = false;
    return Status::OK();
}

Status JsonReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("JSON v2 file_schema is null");
    }
    *file_schema = _file_schema;
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> JsonReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(std::move(options));
}

Status JsonReader::open(std::shared_ptr<FileScanRequest> request) {
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    DORIS_CHECK(_request != nullptr);
    RETURN_IF_ERROR(_build_requested_columns(*_request, &_requested_columns));
    _slot_name_to_index.clear();
    _slot_name_to_index.reserve(_requested_columns.size());
    for (size_t idx = 0; idx < _requested_columns.size(); ++idx) {
        auto name = _requested_columns[idx].slot_desc->col_name();
        _slot_name_to_index.emplace(_is_hive_table ? lower_key(name) : name, idx);
    }
    _previous_positions.clear();
    _reader_range = _json_range();
    RETURN_IF_ERROR(_open_file_reader());
    RETURN_IF_ERROR(_create_decompressor());
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_create_line_reader());
    }
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());
    _json_parser = std::make_unique<simdjson::ondemand::parser>();
    _padding_buffer.resize(_padded_size);
    _reader_eof = false;
    _single_document_read = false;
    _eof = false;
    return Status::OK();
}

Status JsonReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_json_parser == nullptr || _physical_file_reader == nullptr) {
        return Status::InternalError("JSON v2 reader is not open");
    }

    const auto batch_size = _runtime_state->batch_size();
    const auto max_block_bytes = _runtime_state->preferred_block_size_bytes();
    *rows = 0;
    *eof = false;

    while (file_block->rows() < batch_size && !_reader_eof &&
           file_block->bytes() < max_block_bytes) {
        if (_read_json_by_line && _skip_first_line) {
            size_t skipped_size = 0;
            const uint8_t* skipped_line = nullptr;
            RETURN_IF_ERROR(_line_reader->read_line(&skipped_line, &skipped_size, &_reader_eof,
                                                    _io_ctx.get()));
            _skip_first_line = false;
            continue;
        }

        const size_t original_rows = file_block->rows();
        size_t size = 0;
        bool is_empty_row = false;
        Status st = Status::OK();
        try {
            st = _parse_next_json(&size, &_reader_eof);
            if (st.ok() && !_reader_eof) {
                if (size == 0) {
                    is_empty_row = true;
                } else {
                    st = _extract_json_value(size, &_reader_eof, &is_empty_row);
                }
            }
            if (st.ok() && !_reader_eof && !is_empty_row) {
                st = _append_rows_from_current_value(file_block, &is_empty_row, &_reader_eof);
            }
        } catch (simdjson::simdjson_error& e) {
            st = Status::DataQualityError("Parse json data failed. code: {}, error info: {}",
                                          e.error(), e.what());
        }
        if (!st.ok()) {
            RETURN_IF_ERROR(_handle_json_error(st, file_block, original_rows, &is_empty_row));
        }
        // An ignored or empty JSON object can produce no row. Avoid spinning forever on a document
        // that was consumed but produced no materialized value.
        if (!is_empty_row && file_block->rows() == original_rows) {
            break;
        }
    }

    *rows = file_block->rows();
    RETURN_IF_ERROR(_apply_filters(file_block, rows));
    _reader_statistics.read_rows += *rows;
    *eof = _reader_eof && *rows == 0;
    _eof = *eof;
    return Status::OK();
}

Status JsonReader::close() {
    if (_line_reader != nullptr) {
        _line_reader->close();
        _line_reader.reset();
    }
    _json_parser.reset();
    _decompressor.reset();
    _physical_file_reader.reset();
    _tracing_file_reader.reset();
    _file_reader.reset();
    _requested_columns.clear();
    _slot_name_to_index.clear();
    _previous_positions.clear();
    _cached_string_values.clear();
    return Status::OK();
}

Status JsonReader::_build_requested_columns(const FileScanRequest& request,
                                            std::vector<RequestedColumn>* columns) const {
    DORIS_CHECK(columns != nullptr);
    columns->clear();
    // FileScanRequest stores a map from file-local id to output block position. Materialization is
    // position-driven, so normalize it into a dense vector ordered by block position while keeping
    // the original source index for jsonpaths.
    std::vector<RequestedColumn> by_position(request.local_positions.size());
    for (const auto& [file_column_id, block_position] : request.local_positions) {
        if (file_column_id.value() < 0 ||
            static_cast<size_t>(file_column_id.value()) >= _source_file_slot_descs.size()) {
            return Status::InvalidArgument("JSON v2 request references unknown local column id {}",
                                           file_column_id.value());
        }
        if (block_position.value() >= by_position.size()) {
            return Status::InvalidArgument("JSON v2 request has invalid block position {}",
                                           block_position.value());
        }
        const auto source_index = cast_set<size_t>(file_column_id.value());
        RequestedColumn requested_column;
        requested_column.file_column_id = file_column_id;
        requested_column.block_position = block_position;
        requested_column.source_index = source_index;
        requested_column.slot_desc = _source_file_slot_descs[source_index];
        requested_column.serde = _source_serdes[source_index];
        by_position[block_position.value()] = std::move(requested_column);
    }
    for (size_t pos = 0; pos < by_position.size(); ++pos) {
        if (!by_position[pos].file_column_id.is_valid()) {
            return Status::InvalidArgument("JSON v2 request misses block position {}", pos);
        }
    }
    *columns = std::move(by_position);
    return Status::OK();
}

TFileRangeDesc JsonReader::_json_range() const {
    auto range = _range;
    range.__set_path(_file_description->path);
    range.__set_start_offset(_file_description->range_start_offset);
    range.__set_size(_file_description->range_size);
    if (_file_description->file_size >= 0) {
        range.__set_file_size(_file_description->file_size);
    }
    if (!_file_description->fs_name.empty()) {
        range.__set_fs_name(_file_description->fs_name);
    }
    range.__set_file_cache_admission(_file_description->file_cache_admission);
    if (_range_compress_type != TFileCompressType::UNKNOWN) {
        range.__set_compress_type(_range_compress_type);
    }
    if (_stream_load_id.has_value()) {
        range.__set_load_id(*_stream_load_id);
    }
    return range;
}

Status JsonReader::_open_file_reader() {
    _current_offset = _reader_range.start_offset;
    if (_current_offset != 0) {
        --_current_offset;
    }
    if (_scan_params->file_type == TFileType::FILE_STREAM) {
        if (!_stream_load_id.has_value()) {
            return Status::InvalidArgument("JSON v2 stream reader requires load id");
        }
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(*_stream_load_id, &_physical_file_reader,
                                                        _runtime_state, /*need_schema=*/false));
    } else {
        _file_description->mtime =
                _reader_range.__isset.modification_time ? _reader_range.modification_time : 0;
        auto reader_options = FileFactory::get_reader_options(_runtime_state->query_options(),
                                                              *_file_description);
        auto file_reader = DORIS_TRY(FileFactory::create_file_reader(
                *_system_properties, *_file_description, reader_options, _profile));
        _physical_file_reader =
                _io_ctx && _io_ctx->file_reader_stats
                        ? std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                                  _io_ctx->file_reader_stats)
                        : file_reader;
    }
    _file_reader = _physical_file_reader;
    _tracing_file_reader = _physical_file_reader;
    return Status::OK();
}

Status JsonReader::_create_decompressor() {
    return Decompressor::create_decompressor(_file_compress_type, &_decompressor);
}

Status JsonReader::_create_line_reader() {
    int64_t size = _reader_range.size;
    if (_reader_range.start_offset != 0) {
        // Start one byte earlier and discard the first partial line, matching split semantics used
        // by text readers.
        ++size;
        _skip_first_line = true;
    } else {
        _skip_first_line = false;
    }
    _line_reader = NewPlainTextLineReader::create_unique(
            _profile, _physical_file_reader, _decompressor.get(),
            std::make_shared<PlainTextLineReaderCtx>(_line_delimiter, _line_delimiter_length,
                                                     false),
            size, _current_offset);
    return Status::OK();
}

Status JsonReader::_parse_jsonpath_and_json_root() {
    _parsed_jsonpaths.clear();
    _parsed_json_root.clear();
    if (!_jsonpaths.empty()) {
        rapidjson::Document jsonpaths_doc;
        if (jsonpaths_doc.Parse(_jsonpaths.c_str(), _jsonpaths.length()).HasParseError() ||
            !jsonpaths_doc.IsArray()) {
            return Status::InvalidJsonPath("Invalid json path: {}", _jsonpaths);
        }
        for (int i = 0; i < jsonpaths_doc.Size(); ++i) {
            const rapidjson::Value& path = jsonpaths_doc[i];
            if (!path.IsString()) {
                return Status::InvalidJsonPath("Invalid json path: {}", _jsonpaths);
            }
            std::string json_path = path.GetString();
            if (json_path.size() == 1 && json_path[0] == '$') {
                json_path.insert(1, ".");
            }
            std::vector<JsonPath> parsed_paths;
            JsonFunctions::parse_json_paths(json_path, &parsed_paths);
            _parsed_jsonpaths.push_back(std::move(parsed_paths));
        }
    }
    if (!_json_root.empty()) {
        std::string json_root = _json_root;
        if (json_root.size() == 1 && json_root[0] == '$') {
            json_root.insert(1, ".");
        }
        JsonFunctions::parse_json_paths(json_root, &_parsed_json_root);
    }
    return Status::OK();
}

Status JsonReader::_read_one_document(size_t* size, bool* eof) {
    DORIS_CHECK(size != nullptr);
    DORIS_CHECK(eof != nullptr);
    *size = 0;
    *eof = false;
    if (_line_reader != nullptr) {
        const uint8_t* line = nullptr;
        RETURN_IF_ERROR(_line_reader->read_line(&line, size, eof, _io_ctx.get()));
        if (*eof) {
            return Status::OK();
        }
        _document_buffer.assign(reinterpret_cast<const char*>(line), *size);
        return Status::OK();
    }
    // Non-line mode treats the split as one JSON document. This supports a single object or an
    // array with strip_outer_array=true.
    if (_single_document_read) {
        *eof = true;
        return Status::OK();
    }
    _single_document_read = true;
    if (_scan_params->file_type == TFileType::FILE_STREAM) {
        return _read_one_document_from_pipe(size);
    }

    auto read_size = _reader_range.size;
    if (read_size <= 0 && _reader_range.__isset.file_size) {
        read_size = _reader_range.file_size - _current_offset;
    }
    if (read_size <= 0) {
        *eof = true;
        return Status::OK();
    }
    _document_buffer.resize(cast_set<size_t>(read_size));
    Slice result(_document_buffer.data(), _document_buffer.size());
    RETURN_IF_ERROR(_physical_file_reader->read_at(_current_offset, result, size, _io_ctx.get()));
    _document_buffer.resize(*size);
    if (*size == 0) {
        *eof = true;
    }
    return Status::OK();
}

Status JsonReader::_read_one_document_from_pipe(size_t* read_size) {
    auto* stream_load_pipe = dynamic_cast<io::StreamLoadPipe*>(_physical_file_reader.get());
    if (stream_load_pipe == nullptr) {
        return Status::InternalError("JSON v2 stream reader requires StreamLoadPipe");
    }
    DorisUniqueBufferPtr<uint8_t> file_buf;
    RETURN_IF_ERROR(stream_load_pipe->read_one_message(&file_buf, read_size));
    _document_buffer.assign(reinterpret_cast<const char*>(file_buf.get()), *read_size);
    if (!stream_load_pipe->is_chunked_transfer()) {
        return Status::OK();
    }

    while (true) {
        DorisUniqueBufferPtr<uint8_t> next_buf;
        size_t next_size = 0;
        RETURN_IF_ERROR(stream_load_pipe->read_one_message(&next_buf, &next_size));
        if (next_size == 0) {
            break;
        }
        _document_buffer.append(reinterpret_cast<const char*>(next_buf.get()), next_size);
        *read_size += next_size;
    }
    return Status::OK();
}

Status JsonReader::_parse_next_json(size_t* size, bool* eof) {
    RETURN_IF_ERROR(_read_one_document(size, eof));
    if (*eof || *size == 0) {
        return Status::OK();
    }
    if (*size >= 3 && static_cast<unsigned char>(_document_buffer[0]) == 0xEF &&
        static_cast<unsigned char>(_document_buffer[1]) == 0xBB &&
        static_cast<unsigned char>(_document_buffer[2]) == 0xBF) {
        _document_buffer.erase(0, 3);
        *size -= 3;
    }
    if (*size + simdjson::SIMDJSON_PADDING > _padded_size) {
        _padded_size = *size + simdjson::SIMDJSON_PADDING;
        _padding_buffer.resize(_padded_size);
    }
    // Ondemand values reference the input buffer. Keep the padded bytes in a member buffer until the
    // current document is fully materialized.
    std::memcpy(_padding_buffer.data(), _document_buffer.data(), *size);
    _original_doc_size = *size;
    const auto error =
            _json_parser->iterate(std::string_view(_padding_buffer.data(), *size), _padded_size)
                    .get(_original_json_doc);
    if (error != simdjson::error_code::SUCCESS) {
        return Status::DataQualityError(
                "Parse json data for JsonDoc failed. code: {}, error info: {}", error,
                simdjson::error_message(error));
    }
    return Status::OK();
}

Status JsonReader::_extract_json_value(size_t size, bool* eof, bool* is_empty_row) {
    DORIS_CHECK(eof != nullptr);
    DORIS_CHECK(is_empty_row != nullptr);
    *is_empty_row = false;
    if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }
    auto type_res = _original_json_doc.type();
    if (type_res.error() != simdjson::error_code::SUCCESS) {
        return Status::DataQualityError(
                "Parse json data for JsonDoc failed. code: {}, error info: {}", type_res.error(),
                simdjson::error_message(type_res.error()));
    }
    const auto type = type_res.value();
    if (type != simdjson::ondemand::json_type::object &&
        type != simdjson::ondemand::json_type::array) {
        return Status::DataQualityError("Not an json object or json array");
    }
    _parsed_from_json_root = false;
    if (!_parsed_json_root.empty() && type == simdjson::ondemand::json_type::object) {
        // In object mode json_root can be applied once here. In outer-array mode each array element
        // needs its own root extraction, which is handled while iterating the array.
        simdjson::ondemand::object object = _original_json_doc;
        Status st = JsonFunctions::extract_from_object(object, _parsed_json_root, &_json_value);
        if (!st.ok()) {
            return Status::DataQualityError("{}", st.to_string());
        }
        _parsed_from_json_root = true;
    } else {
        _json_value = _original_json_doc;
    }

    const auto value_type = _json_value.type().value();
    if (value_type == simdjson::ondemand::json_type::array && !_strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is array-object, `strip_outer_array` must be TRUE.");
    }
    if (value_type != simdjson::ondemand::json_type::array && _strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
    }
    if (!_parsed_jsonpaths.empty() && _strip_outer_array &&
        _json_value.count_elements().value() == 0) {
        *is_empty_row = true;
    }
    return Status::OK();
}

Status JsonReader::_append_rows_from_current_value(Block* block, bool* is_empty_row, bool* eof) {
    if (_parsed_jsonpaths.empty()) {
        return _append_simple_json_rows(block, is_empty_row, eof);
    }
    if (_strip_outer_array) {
        return _append_flat_array_jsonpath_rows(block, is_empty_row, eof);
    }
    return _append_nested_jsonpath_row(block, is_empty_row, eof);
}

Status JsonReader::_append_simple_json_rows(Block* block, bool* is_empty_row, bool* eof) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(is_empty_row != nullptr);
    DORIS_CHECK(eof != nullptr);
    bool valid = false;
    if (_json_value.type().value() == simdjson::ondemand::json_type::array) {
        _array = _json_value.get_array();
        if (_array.count_elements() == 0) {
            *is_empty_row = true;
            return Status::OK();
        }
        _array_iter = _array.begin();
        while (_array_iter != _array.end()) {
            simdjson::ondemand::object object_value = (*_array_iter).get_object();
            RETURN_IF_ERROR(_set_column_values_from_object(&object_value, block, &valid));
            ++_array_iter;
            if (!valid) {
                *is_empty_row = true;
                return Status::OK();
            }
        }
    } else {
        simdjson::ondemand::object object_value = _json_value.get_object();
        RETURN_IF_ERROR(_set_column_values_from_object(&object_value, block, &valid));
        if (!valid) {
            *is_empty_row = true;
            return Status::OK();
        }
    }
    *is_empty_row = false;
    return Status::OK();
}

Status JsonReader::_append_flat_array_jsonpath_rows(Block* block, bool* is_empty_row, bool* eof) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(is_empty_row != nullptr);
    DORIS_CHECK(eof != nullptr);
    const size_t original_rows = block->rows();
    bool valid = true;
    _array = _json_value.get_array();
    _array_iter = _array.begin();
    while (_array_iter != _array.end()) {
        simdjson::ondemand::object object_value = (*_array_iter).get_object();
        if (!_parsed_from_json_root && !_parsed_json_root.empty()) {
            // For strip_outer_array, json_root is evaluated against each element. Elements without
            // the requested root do not produce rows, matching the load reader behavior.
            simdjson::ondemand::value rooted_value;
            Status st = JsonFunctions::extract_from_object(object_value, _parsed_json_root,
                                                           &rooted_value);
            if (!st.ok()) {
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    ++_array_iter;
                    continue;
                }
                return st;
            }
            if (rooted_value.type().value() != simdjson::ondemand::json_type::object) {
                ++_array_iter;
                continue;
            }
            object_value = rooted_value.get_object();
        }
        RETURN_IF_ERROR(_write_columns_by_jsonpath(&object_value, block, &valid));
        ++_array_iter;
    }
    *is_empty_row = block->rows() == original_rows;
    return Status::OK();
}

Status JsonReader::_append_nested_jsonpath_row(Block* block, bool* is_empty_row, bool* eof) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(is_empty_row != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_json_value.type().value() != simdjson::ondemand::json_type::object) {
        return Status::DataQualityError("Not object item");
    }
    bool valid = true;
    simdjson::ondemand::object object_value = _json_value.get_object();
    RETURN_IF_ERROR(_write_columns_by_jsonpath(&object_value, block, &valid));
    *is_empty_row = !valid;
    return Status::OK();
}

Status JsonReader::_set_column_values_from_object(simdjson::ondemand::object* object_value,
                                                  Block* block, bool* valid) {
    DORIS_CHECK(object_value != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(valid != nullptr);
    std::vector<bool> seen_columns(block->columns(), false);
    const size_t cur_row_count = block->rows();
    bool has_valid_value = false;
    size_t key_index = 0;

    for (auto field : *object_value) {
        std::string_view key = field.unescaped_key().value();
        const size_t column_index = _column_index(key, key_index++);
        if (column_index == static_cast<size_t>(-1)) {
            continue;
        }
        if (seen_columns[column_index]) {
            if (_is_hive_table) {
                // Hive JSON keeps the last duplicate key ignoring case. The earlier value has
                // already been appended, so remove it before writing the replacement.
                _pop_back_last_inserted_value(block, column_index);
            } else {
                continue;
            }
        }
        simdjson::ondemand::value value = field.value().value();
        const auto& requested = _requested_columns[column_index];
        auto* column_ptr = block->get_by_position(column_index).column->assert_mutable().get();
        RETURN_IF_ERROR(_write_data_to_column<false>(
                value, requested.slot_desc->get_data_type_ptr(), column_ptr,
                requested.slot_desc->col_name(), requested.serde, valid));
        if (!*valid) {
            return Status::OK();
        }
        seen_columns[column_index] = true;
        has_valid_value = true;
    }

    for (size_t i = 0; i < _requested_columns.size(); ++i) {
        if (seen_columns[i]) {
            continue;
        }
        auto* column_ptr = block->get_by_position(i).column->assert_mutable().get();
        RETURN_IF_ERROR(_fill_missing_column(_requested_columns[i], column_ptr, valid));
        if (!*valid) {
            _truncate_block_to_rows(block, cur_row_count);
            return Status::OK();
        }
    }
    *valid = true;
    if (!has_valid_value) {
        return Status::OK();
    }
    return Status::OK();
}

Status JsonReader::_write_columns_by_jsonpath(simdjson::ondemand::object* object_value,
                                              Block* block, bool* valid) {
    DORIS_CHECK(object_value != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(valid != nullptr);
    bool has_valid_value = false;
    const size_t cur_row_count = block->rows();
    _cached_string_values.clear();

    for (size_t i = 0; i < _requested_columns.size(); ++i) {
        const auto& requested = _requested_columns[i];
        auto* column_ptr = block->get_by_position(i).column->assert_mutable().get();
        simdjson::ondemand::value json_value;
        Status st = Status::OK();
        if (requested.source_index < _parsed_jsonpaths.size()) {
            st = JsonFunctions::extract_from_object(
                    *object_value, _parsed_jsonpaths[requested.source_index], &json_value);
            if (!st.ok() && !st.is<ErrorCode::NOT_FOUND>()) {
                return st;
            }
        }
        if (_is_root_path_for_column(requested)) {
            // A root jsonpath means "materialize the whole current JSON document" instead of a
            // field under it. Use the original bytes so callers receive the same document text.
            if (is_column_nullable(*column_ptr)) {
                auto* nullable_column = assert_cast<ColumnNullable*>(column_ptr);
                nullable_column->get_null_map_data().push_back(0);
                auto* column_string =
                        assert_cast<ColumnString*>(nullable_column->get_nested_column_ptr().get());
                column_string->insert_data(_padding_buffer.data(), _original_doc_size);
            } else {
                auto* column_string = assert_cast<ColumnString*>(column_ptr);
                column_string->insert_data(_padding_buffer.data(), _original_doc_size);
            }
            has_valid_value = true;
        } else if (requested.source_index >= _parsed_jsonpaths.size() ||
                   st.is<ErrorCode::NOT_FOUND>()) {
            RETURN_IF_ERROR(_fill_missing_column(requested, column_ptr, valid));
            if (!*valid) {
                _truncate_block_to_rows(block, cur_row_count);
                return Status::OK();
            }
        } else {
            RETURN_IF_ERROR(_write_data_to_column<true>(
                    json_value, requested.slot_desc->get_data_type_ptr(), column_ptr,
                    requested.slot_desc->col_name(), requested.serde, valid));
            if (!*valid) {
                _truncate_block_to_rows(block, cur_row_count);
                return Status::OK();
            }
            has_valid_value = true;
        }
    }

    if (!has_valid_value) {
        // jsonpaths can legally match nothing. Roll the row back so an all-missing path set does
        // not create a synthetic row of nulls.
        _truncate_block_to_rows(block, cur_row_count);
        *valid = false;
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

template <bool use_string_cache>
Status JsonReader::_write_data_to_column(simdjson::ondemand::value& value,
                                         const DataTypePtr& type_desc, IColumn* column_ptr,
                                         const std::string& column_name,
                                         const DataTypeSerDeSPtr& serde, bool* valid) {
    ColumnNullable* nullable_column = nullptr;
    IColumn* data_column_ptr = column_ptr;
    DataTypeSerDeSPtr data_serde = serde;
    const auto value_type = value.type().value();

    if (is_column_nullable(*column_ptr)) {
        nullable_column = assert_cast<ColumnNullable*>(column_ptr);
        data_column_ptr = nullable_column->get_nested_column().get_ptr().get();
        if (type_desc->is_nullable()) {
            data_serde = serde->get_nested_serdes()[0];
        }
        if (value_type == simdjson::ondemand::json_type::null) {
            nullable_column->insert_default();
            *valid = true;
            return Status::OK();
        }
    } else if (value_type == simdjson::ondemand::json_type::null) {
        return Status::DataQualityError("Json value is null, but the column `{}` is not nullable.",
                                        column_name);
    }

    const auto primitive_type = type_desc->get_primitive_type();
    if (!is_complex_type(primitive_type)) {
        if (value_type == simdjson::ondemand::json_type::string) {
            std::string_view value_string;
            if constexpr (use_string_cache) {
                const auto cache_key = value.raw_json().value();
                if (_cached_string_values.contains(cache_key)) {
                    value_string = _cached_string_values[cache_key];
                } else {
                    value_string = value.get_string();
                    _cached_string_values.emplace(cache_key, value_string);
                }
            } else {
                value_string = value.get_string();
            }
            Slice slice {value_string.data(), value_string.size()};
            RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                       _serde_options));
        } else if (value_type == simdjson::ondemand::json_type::boolean) {
            const char* str_value = value.get_bool() ? "1" : "0";
            Slice slice {str_value, 1};
            RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                       _serde_options));
        } else {
            std::string_view json_str = simdjson::to_json_string(value);
            Slice slice {json_str.data(), json_str.size()};
            RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                       _serde_options));
        }
    } else if (primitive_type == TYPE_STRUCT) {
        if (value_type != simdjson::ondemand::json_type::object) {
            return Status::DataQualityError(
                    "Json value isn't object, but the column `{}` is struct.", column_name);
        }
        const auto* type_struct =
                assert_cast<const DataTypeStruct*>(remove_nullable(type_desc).get());
        auto* struct_column_ptr = assert_cast<ColumnStruct*>(data_column_ptr);
        const auto sub_serdes = data_serde->get_nested_serdes();
        std::map<std::string, size_t> sub_col_name_to_idx;
        for (size_t sub_col_idx = 0; sub_col_idx < type_struct->get_elements().size();
             ++sub_col_idx) {
            sub_col_name_to_idx.emplace(lower_key(type_struct->get_element_name(sub_col_idx)),
                                        sub_col_idx);
        }
        std::vector<bool> has_value(type_struct->get_elements().size(), false);
        simdjson::ondemand::object struct_value = value.get_object();
        for (auto sub : struct_value) {
            const auto sub_key = lower_key(sub.unescaped_key().value());
            const auto it = sub_col_name_to_idx.find(sub_key);
            if (it == sub_col_name_to_idx.end()) {
                continue;
            }
            const auto sub_column_idx = it->second;
            auto sub_column_ptr = struct_column_ptr->get_column(sub_column_idx).get_ptr();
            if (has_value[sub_column_idx]) {
                // Struct fields follow Hive-style duplicate handling: the last matching nested key
                // wins. Remove the earlier nested value before appending the new one.
                sub_column_ptr->pop_back(1);
            }
            has_value[sub_column_idx] = true;
            auto sub_value = sub.value().value();
            RETURN_IF_ERROR(_write_data_to_column<use_string_cache>(
                    sub_value, type_struct->get_element(sub_column_idx), sub_column_ptr.get(),
                    column_name + "." + sub_key, sub_serdes[sub_column_idx], valid));
        }
        for (size_t sub_col_idx = 0; sub_col_idx < type_struct->get_elements().size();
             ++sub_col_idx) {
            if (has_value[sub_col_idx]) {
                continue;
            }
            auto sub_column_ptr = struct_column_ptr->get_column(sub_col_idx).get_ptr();
            if (!is_column_nullable(*sub_column_ptr)) {
                return Status::DataQualityError(
                        "Json file structColumn miss field {} and this column isn't nullable.",
                        column_name + "." + type_struct->get_element_name(sub_col_idx));
            }
            sub_column_ptr->insert_default();
        }
    } else if (primitive_type == TYPE_MAP) {
        if (value_type != simdjson::ondemand::json_type::object) {
            return Status::DataQualityError("Json value isn't object, but the column `{}` is map.",
                                            column_name);
        }
        const auto* map_type = assert_cast<const DataTypeMap*>(remove_nullable(type_desc).get());
        auto* map_column_ptr = assert_cast<ColumnMap*>(data_column_ptr);
        const auto sub_serdes = data_serde->get_nested_serdes();
        size_t field_count = 0;
        simdjson::ondemand::object object_value = value.get_object();
        for (auto member_value : object_value) {
            auto* key_column = map_column_ptr->get_keys_ptr()->assert_mutable()->get_ptr().get();
            auto key_serde = sub_serdes[0];
            if (is_column_nullable(*key_column)) {
                auto* nullable_key = assert_cast<ColumnNullable*>(key_column);
                nullable_key->get_null_map_data().push_back(0);
                key_column = nullable_key->get_nested_column().get_ptr().get();
                if (map_type->get_key_type()->is_nullable()) {
                    key_serde = key_serde->get_nested_serdes()[0];
                }
            }
            std::string_view key_view = member_value.unescaped_key().value();
            Slice key_slice(key_view.data(), key_view.size());
            RETURN_IF_ERROR(key_serde->deserialize_one_cell_from_json(*key_column, key_slice,
                                                                      _serde_options));
            simdjson::ondemand::value field_value = member_value.value().value();
            RETURN_IF_ERROR(_write_data_to_column<use_string_cache>(
                    field_value, map_type->get_value_type(),
                    map_column_ptr->get_values_ptr()->assert_mutable()->get_ptr().get(),
                    column_name + ".value", sub_serdes[1], valid));
            ++field_count;
        }
        auto& offsets = map_column_ptr->get_offsets();
        offsets.emplace_back(offsets.back() + field_count);
    } else if (primitive_type == TYPE_ARRAY) {
        if (value_type != simdjson::ondemand::json_type::array) {
            return Status::DataQualityError("Json value isn't array, but the column `{}` is array.",
                                            column_name);
        }
        const auto* array_type =
                assert_cast<const DataTypeArray*>(remove_nullable(type_desc).get());
        auto* array_column_ptr = assert_cast<ColumnArray*>(data_column_ptr);
        const auto sub_serdes = data_serde->get_nested_serdes();
        size_t field_count = 0;
        simdjson::ondemand::array array_value = value.get_array();
        for (simdjson::ondemand::value sub_value : array_value) {
            RETURN_IF_ERROR(_write_data_to_column<use_string_cache>(
                    sub_value, array_type->get_nested_type(),
                    array_column_ptr->get_data().get_ptr().get(), column_name + ".element",
                    sub_serdes[0], valid));
            ++field_count;
        }
        auto& offsets = array_column_ptr->get_offsets();
        offsets.emplace_back(offsets.back() + field_count);
    } else {
        return Status::InternalError("Not support JSON value to complex column");
    }

    if (nullable_column && value_type != simdjson::ondemand::json_type::null) {
        nullable_column->get_null_map_data().push_back(0);
    }
    *valid = true;
    return Status::OK();
}

Status JsonReader::_fill_missing_column(const RequestedColumn& column, IColumn* column_ptr,
                                        bool* valid) {
    if (column.slot_desc->is_nullable()) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column_ptr);
        nullable_column->insert_default();
        *valid = true;
        return Status::OK();
    }
    return Status::DataQualityError(
            "The column `{}` is not nullable, but it's not found in jsondata.",
            column.slot_desc->col_name());
}

Status JsonReader::_handle_json_error(const Status& status, Block* block, size_t original_rows,
                                      bool* is_empty_row) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(is_empty_row != nullptr);
    // Deserialization can fail after several columns have already appended data. Always restore the
    // block to the row count before this document before either surfacing the error or skipping the
    // ignored malformed document.
    _truncate_block_to_rows(block, original_rows);
    if (_openx_json_ignore_malformed && status.is<ErrorCode::DATA_QUALITY_ERROR>()) {
        *is_empty_row = true;
        return Status::OK();
    }
    return status;
}

Status JsonReader::_apply_filters(Block* file_block, size_t* rows) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    const size_t rows_before_filter = *rows;
    size_t rows_after_delete_filter = rows_before_filter;
    if (_request != nullptr && rows_before_filter > 0 && !_request->delete_conjuncts.empty()) {
        RETURN_IF_ERROR(VExprContext::filter_block(_request->delete_conjuncts, file_block,
                                                   file_block->columns()));
        rows_after_delete_filter =
                file_block->columns() == 0 ? rows_before_filter : file_block->rows();
    }

    size_t rows_after_filter = rows_after_delete_filter;
    if (_request != nullptr && rows_after_delete_filter > 0 && !_request->conjuncts.empty()) {
        RETURN_IF_ERROR(
                VExprContext::filter_block(_request->conjuncts, file_block, file_block->columns()));
        rows_after_filter =
                file_block->columns() == 0 ? rows_after_delete_filter : file_block->rows();
        if (_io_ctx != nullptr) {
            _io_ctx->predicate_filtered_rows += rows_after_delete_filter - rows_after_filter;
        }
    }
    *rows = rows_after_filter;
    return Status::OK();
}

void JsonReader::_truncate_block_to_rows(Block* block, size_t num_rows) {
    DORIS_CHECK(block != nullptr);
    for (int i = 0; i < block->columns(); ++i) {
        auto& column_with_type = block->get_by_position(i);
        auto column = IColumn::mutate(std::move(column_with_type.column));
        if (column->size() > num_rows) {
            column->pop_back(column->size() - num_rows);
        }
        column_with_type.column = std::move(column);
    }
}

void JsonReader::_pop_back_last_inserted_value(Block* block, size_t column_index) {
    DORIS_CHECK(block != nullptr);
    auto& column = block->get_by_position(column_index).column;
    auto mutable_column = IColumn::mutate(std::move(column));
    mutable_column->pop_back(1);
    column = std::move(mutable_column);
}

size_t JsonReader::_column_index(std::string_view key, size_t key_index) {
    std::string hive_key;
    std::string_view lookup_key = key;
    if (_is_hive_table) {
        hive_key = lower_key(key);
        lookup_key = hive_key;
    }
    if (key_index < _previous_positions.size()) {
        // Most JSON lines share field order. Reuse the previous line's key-position mapping before
        // falling back to the hash table lookup.
        const auto previous = _previous_positions[key_index];
        if (previous < _requested_columns.size()) {
            const auto previous_name = _requested_columns[previous].slot_desc->col_name();
            if ((_is_hive_table ? lower_key(previous_name) : previous_name) == lookup_key) {
                return previous;
            }
        }
    }
    const auto it = _slot_name_to_index.find(std::string(lookup_key));
    if (it == _slot_name_to_index.end()) {
        return static_cast<size_t>(-1);
    }
    if (key_index >= _previous_positions.size()) {
        _previous_positions.resize(key_index + 1, static_cast<size_t>(-1));
    }
    _previous_positions[key_index] = it->second;
    return it->second;
}

bool JsonReader::_is_root_path_for_column(const RequestedColumn& column) const {
    return column.source_index < _parsed_jsonpaths.size() &&
           JsonFunctions::is_root_path(_parsed_jsonpaths[column.source_index]);
}

} // namespace doris::format::json
