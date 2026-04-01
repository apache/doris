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

#include "format/table/fileset_reader.h"

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/file_schema_descriptor.h"
#include "io/hdfs_builder.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_writer.h"

namespace doris {

namespace {
constexpr std::string_view LAST_MODIFIED_AT_FALLBACK = "1970-01-01 00:00:00.000";
constexpr std::string_view TABLE_SCAN_RECURSIVE = "table.scan.recursive";
} // namespace

// Keep the reader lightweight: FE decides the logical table root and BE only
// receives scan-time parameters plus an optional runtime profile sink.
FilesetReader::FilesetReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                             RuntimeProfile* profile,
                             const std::map<std::string, std::string>& fileset_params)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _fileset_params(fileset_params) {
    _init_profile();
}

// Build the file list once before scan so later blocks only materialize rows.
Status FilesetReader::init_reader() {
    return _build_files();
}

// Fileset exposes a fixed FILE-typed schema, so we simply mirror slot descriptors.
Status FilesetReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

// Emit one FILE row per listed object. The row payload is assembled lazily into JSONB.
Status FilesetReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_next_file_idx >= _files.size()) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }

    size_t rows = 0;
    const bool need_materialize_file = block->columns() > 0;
    JsonbWriter writer;
    ColumnString* jsonb_column = nullptr;
    ColumnUInt8* null_map_column = nullptr;
    if (need_materialize_file) {
        auto file_column_ptr = block->get_by_position(0).column->assume_mutable();
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(file_column_ptr.get())) {
            null_map_column = &assert_cast<ColumnUInt8&>(nullable_column->get_null_map_column());
            auto& file_column = assert_cast<ColumnFile&>(nullable_column->get_nested_column());
            jsonb_column = &assert_cast<ColumnString&>(file_column.get_jsonb_column());
        } else {
            auto& file_column = assert_cast<ColumnFile&>(*file_column_ptr);
            jsonb_column = &assert_cast<ColumnString&>(file_column.get_jsonb_column());
        }
    }

    const size_t batch_size = std::max<size_t>(_state->batch_size(), 1);
    for (; _next_file_idx < _files.size() && rows < batch_size; ++_next_file_idx, ++rows) {
        if (!need_materialize_file) {
            continue;
        }
        writer.reset();
        _write_file_jsonb(writer, _files[_next_file_idx]);
        jsonb_column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        if (null_map_column != nullptr) {
            null_map_column->insert_value(0);
        }
    }

    *read_rows = rows;
    *eof = (_next_file_idx >= _files.size());
    if (_fileset_profile.emitted_rows != nullptr) {
        COUNTER_UPDATE(_fileset_profile.emitted_rows, rows);
    }
    return Status::OK();
}

// Resolve the backing filesystem and enumerate scan targets according to the explicit
// `table.scan.recursive` flag rather than backend-specific listing behavior.
Status FilesetReader::_build_files() {
    auto type = DORIS_TRY(_parse_file_type(_fileset_params.at("file_type")));
    const bool recursive = _is_recursive_scan_enabled();
    io::FileSystemProperties fs_properties;
    fs_properties.system_type = type;
    fs_properties.properties = _fileset_params;
    if (type == TFileType::FILE_HDFS) {
        fs_properties.hdfs_params = parse_properties(_fileset_params);
    }

    if (_profile != nullptr) {
        _profile->add_info_string("FilesetTablePath", _fileset_params.at("table_path"));
        _profile->add_info_string("FilesetFileType", _fileset_params.at("file_type"));
        _profile->add_info_string("FilesetRecursiveScan", recursive ? "true" : "false");
        auto role_arn_it = _fileset_params.find("AWS_ROLE_ARN");
        if (role_arn_it != _fileset_params.end() && !role_arn_it->second.empty()) {
            _profile->add_info_string("FilesetRoleArn", role_arn_it->second);
        }
        auto provider_it = _fileset_params.find("AWS_CREDENTIALS_PROVIDER_TYPE");
        if (provider_it != _fileset_params.end() && !provider_it->second.empty()) {
            _profile->add_info_string("FilesetCredentialProvider", provider_it->second);
        }
    }

    io::FileDescription file_description {
            .path = _fileset_params.at("table_path"),
            .file_size = -1,
            .mtime = 0,
            .fs_name = "",
            .file_cache_admission = true,
    };
    io::FSPropertiesRef fs_ref(fs_properties);
    io::FileSystemSPtr fs = DORIS_TRY(FileFactory::create_fs(fs_ref, file_description));

    RETURN_IF_ERROR(_list_files(fs, _fileset_params.at("table_path"), recursive));
    if (_files.empty()) {
        bool exists = false;
        RETURN_IF_ERROR(fs->exists(_fileset_params.at("table_path"), &exists));
        if (!exists) {
            return Status::NotFound("fileset table path does not exist: {}", _fileset_params.at("table_path"));
        }
    }
    if (_files.empty()) {
        return Status::OK();
    }
    int64_t range_start = 0;
    int64_t range_end = -1;
    if (auto it = _fileset_params.find("range_start"); it != _fileset_params.end()) {
        range_start = std::stoll(it->second);
    }
    if (auto it = _fileset_params.find("range_end"); it != _fileset_params.end()) {
        range_end = std::stoll(it->second);
    }
    if (range_start > 0 || range_end >= 0) {
        const size_t begin = std::min<size_t>(std::max<int64_t>(range_start, 0), _files.size());
        const size_t end = range_end < 0 ? _files.size()
                                         : std::min<size_t>(range_end, _files.size());
        if (begin >= end) {
            _files.clear();
            return Status::OK();
        }
        _files = std::vector<io::FileInfo>(_files.begin() + begin, _files.begin() + end);
    }
    size_t total_bytes = 0;
    for (const auto& file : _files) {
        total_bytes += file.file_size;
    }
    if (_fileset_profile.listed_files != nullptr) {
        COUNTER_UPDATE(_fileset_profile.listed_files, _files.size());
    }
    if (_fileset_profile.listed_bytes != nullptr) {
        COUNTER_UPDATE(_fileset_profile.listed_bytes, total_bytes);
    }
    return Status::OK();
}

// Walk the logical table root one level at a time so every filesystem follows the same
// recursion semantics. Directory expansion only happens when the table flag enables it.
Status FilesetReader::_list_files(const io::FileSystemSPtr& fs, const std::string& table_path,
                                  bool recursive) {
    _files.clear();
    std::deque<std::string> pending_paths {table_path};
    while (!pending_paths.empty()) {
        std::string current_path = std::move(pending_paths.front());
        pending_paths.pop_front();

        bool exists = false;
        std::vector<io::FileInfo> listed_entries;
        // `FileSystem::list` is always a single-level listing. We keep directory
        // entries here and let fileset own the recursion policy so all backends
        // follow the same `table.scan.recursive` semantics.
        RETURN_IF_ERROR(fs->list(current_path, false, &listed_entries, &exists));
        if (!exists) {
            if (current_path == table_path) {
                return Status::NotFound("fileset table path does not exist: {}", table_path);
            }
            return Status::NotFound("fileset nested path does not exist: {}", current_path);
        }

        for (const auto& entry : listed_entries) {
            if (entry.is_file) {
                _files.push_back(entry);
                continue;
            }
            if (!recursive) {
                continue;
            }
            std::string nested_path = current_path;
            if (!nested_path.ends_with('/')) {
                nested_path.push_back('/');
            }
            nested_path.append(entry.file_name);
            if (!nested_path.ends_with('/')) {
                nested_path.push_back('/');
            }
            pending_paths.push_back(std::move(nested_path));
        }
    }
    return Status::OK();
}

// Treat missing flags as non-recursive to keep the default table scan shallow.
bool FilesetReader::_is_recursive_scan_enabled() const {
    if (auto it = _fileset_params.find(std::string(TABLE_SCAN_RECURSIVE)); it != _fileset_params.end()) {
        return it->second == "true";
    }
    return false;
}

Result<TFileType::type> FilesetReader::_parse_file_type(const std::string& file_type) {
    if (file_type == "FILE_S3") {
        return TFileType::FILE_S3;
    }
    if (file_type == "FILE_HDFS") {
        return TFileType::FILE_HDFS;
    }
    if (file_type == "FILE_LOCAL") {
        return TFileType::FILE_LOCAL;
    }
    if (file_type == "FILE_HTTP") {
        return TFileType::FILE_HTTP;
    }
    if (file_type == "FILE_BROKER") {
        return TFileType::FILE_BROKER;
    }
    return ResultError(Status::InvalidArgument("unsupported fileset file type: {}", file_type));
}

std::string FilesetReader::_build_object_uri(const std::string& table_path,
                                             const std::string& listed_name) {
    if (listed_name.find("://") != std::string::npos) {
        return listed_name;
    }
    if (listed_name.rfind(table_path, 0) == 0) {
        return listed_name;
    }
    if (listed_name.find('/') != std::string::npos) {
        size_t scheme_pos = table_path.find("://");
        if (scheme_pos == std::string::npos) {
            return listed_name;
        }
        size_t root_end = table_path.find('/', scheme_pos + 3);
        std::string root = root_end == std::string::npos ? table_path + "/" : table_path.substr(0, root_end + 1);
        return root + (!listed_name.empty() && listed_name[0] == '/' ? listed_name.substr(1) : listed_name);
    }
    return table_path + (table_path.ends_with("/") ? "" : "/") + listed_name;
}

std::string FilesetReader::_extract_file_name(std::string_view object_uri) {
    size_t pos = object_uri.find_last_of('/');
    return pos == std::string_view::npos ? std::string(object_uri) : std::string(object_uri.substr(pos + 1));
}

std::string FilesetReader::_extract_file_extension(const std::string& file_name) {
    size_t pos = file_name.find_last_of('.');
    return pos == std::string::npos ? "" : file_name.substr(pos);
}

void FilesetReader::_write_jsonb_string(JsonbWriter& writer, const std::string& value) {
    writer.writeStartString();
    writer.writeString(value.data(), cast_set<uint32_t>(value.size()));
    writer.writeEndString();
}

// Allocate fileset-specific counters only when the caller requested profiling.
void FilesetReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    static const char* fileset_profile = "FilesetProfile";
    ADD_TIMER(_profile, fileset_profile);
    _fileset_profile.listed_files =
            ADD_CHILD_COUNTER(_profile, "ListedFiles", TUnit::UNIT, fileset_profile);
    _fileset_profile.listed_bytes =
            ADD_CHILD_COUNTER(_profile, "ListedBytes", TUnit::BYTES, fileset_profile);
    _fileset_profile.emitted_rows =
            ADD_CHILD_COUNTER(_profile, "EmittedRows", TUnit::UNIT, fileset_profile);
}

// Serialize one file entry into the FILE logical type layout expected by execution.
void FilesetReader::_write_file_jsonb(JsonbWriter& writer, const io::FileInfo& file) {
    const std::string object_uri = _build_object_uri(_fileset_params.at("table_path"), file.file_name);
    const std::string file_name = _extract_file_name(object_uri);
    const std::string file_extension = _extract_file_extension(file_name);
    const auto& schema = FileSchemaDescriptor::instance();

    writer.writeStartObject();
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::OBJECT_URI));
    _write_jsonb_string(writer, object_uri);
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::FILE_NAME));
    _write_jsonb_string(writer, file_name);
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::FILE_EXTENSION));
    _write_jsonb_string(writer, file_extension);
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::SIZE));
    writer.writeInt64(file.file_size);
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::ETAG));
    writer.writeNull();
    _write_jsonb_key(writer, schema.field_name(FileSchemaDescriptor::Field::LAST_MODIFIED_AT));
    _write_jsonb_string(writer, std::string(LAST_MODIFIED_AT_FALLBACK));
    writer.writeEndObject();
}

// JsonbWriter requires explicit key boundaries, so keep that tiny helper centralized.
void FilesetReader::_write_jsonb_key(JsonbWriter& writer, std::string_view key) {
    writer.writeKey(key.data(), cast_set<uint8_t>(key.size()));
}
} // namespace doris
