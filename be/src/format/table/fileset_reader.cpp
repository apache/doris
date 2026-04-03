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

#include <fnmatch.h>

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

FilesetReader::FilesetReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile,
                             const std::map<std::string, std::string>& fileset_params)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _fileset_params(fileset_params) {
    _init_profile();
}

Status FilesetReader::init_reader() {
    return _build_files();
}

Status FilesetReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

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
    {
        SCOPED_TIMER(_fileset_profile.serialize_time);
        for (; _next_file_idx < _files.size() && rows < batch_size; ++_next_file_idx, ++rows) {
            if (!need_materialize_file) {
                continue;
            }
            writer.reset();
            _write_file_jsonb(writer, _files[_next_file_idx]);
            jsonb_column->insert_data(writer.getOutput()->getBuffer(),
                                      writer.getOutput()->getSize());
            if (null_map_column != nullptr) {
                null_map_column->insert_value(0);
            }
        }
    }

    *read_rows = rows;
    *eof = (_next_file_idx >= _files.size());
    if (_fileset_profile.emitted_rows != nullptr) {
        COUNTER_UPDATE(_fileset_profile.emitted_rows, rows);
    }
    return Status::OK();
}

Status FilesetReader::_build_files() {
    auto type = DORIS_TRY(_parse_file_type(_fileset_params.at("file_type")));
    io::FileSystemProperties fs_properties;
    fs_properties.system_type = type;
    fs_properties.properties = _fileset_params;
    if (type == TFileType::FILE_HDFS) {
        fs_properties.hdfs_params = parse_properties(_fileset_params);
    }

    if (_profile != nullptr) {
        _profile->add_info_string("FilesetTablePath", _fileset_params.at("table_path"));
        _profile->add_info_string("FilesetFileType", _fileset_params.at("file_type"));
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

    {
        SCOPED_TIMER(_fileset_profile.list_files_time);
        RETURN_IF_ERROR(_list_files(fs, _fileset_params.at("table_path")));
    }
    if (_files.empty()) {
        bool exists = false;
        RETURN_IF_ERROR(fs->exists(_fileset_params.at("table_path"), &exists));
        if (!exists) {
            return Status::NotFound("fileset table path does not exist: {}",
                                    _fileset_params.at("table_path"));
        }
    }

    int shard_id = std::stoi(_fileset_params.at("shard_id"));
    int total_shards = std::stoi(_fileset_params.at("total_shards"));
    if (total_shards > 1) {
        std::vector<io::FileInfo> shard_files;
        for (auto& file : _files) {
            // as the file type used with AI functions, and AI function is executed slowly,
            // so we scan parallelly by sharding the files based on the hash of file name to improve the performance,
            // maybe use etag when FileInfo struct is extended in the future, but currently FileInfo only has file_name that can be used for sharding.
            size_t h = std::hash<std::string> {}(file.file_name);
            if (static_cast<int>(h % total_shards) == shard_id) {
                shard_files.push_back(std::move(file));
            }
        }
        _files = std::move(shard_files);
        if (_profile != nullptr) {
            _profile->add_info_string("ShardId", std::to_string(shard_id));
            _profile->add_info_string("TotalShards", std::to_string(total_shards));
        }
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

// Lists files in the directory specified by table_path, then filters them
// against the glob pattern from fileset_params["file_pattern"].
Status FilesetReader::_list_files(const io::FileSystemSPtr& fs, const std::string& table_path) {
    _files.clear();
    bool exists = false;
    std::vector<io::FileInfo> listed_entries;
    RETURN_IF_ERROR(fs->list(table_path, true, &listed_entries, &exists));
    if (!exists) {
        return Status::NotFound("fileset table path does not exist: {}", table_path);
    }
    auto it = _fileset_params.find("file_pattern");
    DORIS_CHECK(it != _fileset_params.end());
    const std::string& pattern = it->second;
    for (auto& entry : listed_entries) {
        if (!entry.is_file) {
            continue;
        }
        // Match the filename (not full path) against the glob pattern.
        std::string name = FileSchemaDescriptor::extract_file_name(entry.file_name);
        if (fnmatch(pattern.c_str(), name.c_str(), 0) != 0) {
            continue;
        }
        _files.push_back(std::move(entry));
    }
    return Status::OK();
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
    return ResultError(Status::InvalidArgument("unsupported fileset file type: {}", file_type));
}

std::string FilesetReader::_build_uri(const std::string& table_path,
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
        std::string root = root_end == std::string::npos ? table_path + "/"
                                                         : table_path.substr(0, root_end + 1);
        return root + (!listed_name.empty() && listed_name[0] == '/' ? listed_name.substr(1)
                                                                     : listed_name);
    }
    return table_path + (table_path.ends_with("/") ? "" : "/") + listed_name;
}

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
    _fileset_profile.list_files_time = ADD_CHILD_TIMER(_profile, "ListFilesTime", fileset_profile);
    _fileset_profile.serialize_time = ADD_CHILD_TIMER(_profile, "SerializeTime", fileset_profile);
}

void FilesetReader::_write_file_jsonb(JsonbWriter& writer, const io::FileInfo& file) {
    using S = FileSchemaDescriptor;
    const std::string uri = _build_uri(_fileset_params.at("table_path"), file.file_name);
    const std::string file_name = S::extract_file_name(uri);
    auto get_param = [&](const char* key) -> std::string {
        if (auto it = _fileset_params.find(key);
            it != _fileset_params.end() && !it->second.empty()) {
            return it->second;
        }
        return {};
    };
    FileMetadata metadata {
            .uri = uri,
            .file_name = file_name,
            .content_type = S::extension_to_content_type(S::extract_file_extension(file_name)),
            .size = file.file_size,
            .region = get_param("AWS_REGION"),
            .endpoint = get_param("AWS_ENDPOINT"),
            .ak = get_param("AWS_ACCESS_KEY"),
            .sk = get_param("AWS_SECRET_KEY"),
            .role_arn = get_param("AWS_ROLE_ARN"),
            .external_id = get_param("AWS_EXTERNAL_ID"),
    };
    S::write_file_jsonb(writer, metadata);
}
} // namespace doris
