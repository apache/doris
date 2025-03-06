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

#include "paimon_reader.h"

#include <rapidjson/document.h>

#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/deletion_vector.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
PaimonReader::PaimonReader(std::unique_ptr<GenericReader> file_format_reader,
                           RuntimeProfile* profile, RuntimeState* state,
                           const TFileScanRangeParams& params, const TFileRangeDesc& range,
                           io::IOContext* io_ctx, ShardedKVCache* kv_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx),
          _kv_cache(kv_cache) {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(_profile, paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", paimon_profile);
}

/**
sql:
create table tmp3 (
    k int,
    vVV string,
    a array<int>,
    b struct<a:int,b:string>,
    c map<string,int>
) tblproperties (
    'primary-key' = 'k',
    "file.format" = "parquet"
);

schema :
{
  "version" : 2,
  "id" : 0,
  "fields" : [ {
    "id" : 0,
    "name" : "k",
    "type" : "INT NOT NULL"
  }, {
    "id" : 1,
    "name" : "vVV",
    "type" : "STRING"
  }, {
    "id" : 2,
    "name" : "a",
    "type" : {
      "type" : "ARRAY",
      "element" : "INT"
    }
  }, {
    "id" : 3,
    "name" : "b",
    "type" : {
      "type" : "ROW",
      "fields" : [ {
        "id" : 4,
        "name" : "a",
        "type" : "INT"
      }, {
        "id" : 5,
        "name" : "b",
        "type" : "STRING"
      } ]
    }
  }, {
    "id" : 6,
    "name" : "c",
    "type" : {
      "type" : "MAP",
      "key" : "STRING NOT NULL",
      "value" : "INT"
    }
  } ],
  "highestFieldId" : 6,
  "partitionKeys" : [ ],
  "primaryKeys" : [ "k" ],
  "options" : {
    "owner" : "root",
    "file.format" : "parquet"
  },
  "timeMillis" : 1741338580187
}
*/
Status PaimonReader::read_schema_file(std::map<uint64_t, std::string>& file_id_to_name) {
    file_id_to_name.clear();
    if (!_range.table_format_params.paimon_params.__isset.schema_file_path) [[unlikely]] {
        return Status::RuntimeError("miss schema file");
    }

    io::FileSystemProperties properties = {
            .system_type = _params.file_type,
            .properties = _params.properties,
            .hdfs_params = _params.hdfs_params,
            .broker_addresses {},
    };
    if (_params.__isset.broker_addresses) {
        properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                           _params.broker_addresses.end());
    }
    io::FileDescription file_description = {
            .path = _range.table_format_params.paimon_params.schema_file_path,
            .file_size = -1,
            .mtime = 0,
            .fs_name = _range.fs_name,
    };
    auto schema_file_reader = DORIS_TRY(FileFactory::create_file_reader(
            properties, file_description, io::FileReaderOptions::DEFAULT));

    size_t bytes_read = schema_file_reader->size();
    std::vector<char> buf(bytes_read);
    Slice schema_result(buf.data(), bytes_read);
    {
        SCOPED_TIMER(_paimon_profile.delete_files_read_time);
        RETURN_IF_ERROR(schema_file_reader->read_at(0, schema_result, &bytes_read, _io_ctx));
    }

    rapidjson::Document json_doc;
    if (json_doc.Parse(schema_result.data, schema_result.size).HasParseError()) {
        return Status::IOError("failed to parse json file, path:{}",
                               _range.table_format_params.paimon_params.schema_file_path);
    }

    if (!json_doc.HasMember("fields") || !json_doc["fields"].IsArray()) {
        return Status::IOError("Invalid JSON: missing or incorrect 'fields' array, path:{} ",
                               _range.table_format_params.paimon_params.schema_file_path);
    }
    const auto& fields = json_doc["fields"];
    for (const auto& field : fields.GetArray()) {
        if (field.HasMember("id") && field["id"].IsInt() && field.HasMember("name") &&
            field["name"].IsString()) {
            int id = field["id"].GetInt();
            std::string name = to_lower(field["name"].GetString());
            file_id_to_name[id] = name;
        }
    }

    return Status::OK();
}

Status PaimonReader::gen_file_col_name(
        const std::vector<std::string>& read_table_col_names,
        const std::unordered_map<uint64_t, std::string>& table_col_id_table_name_map,
        const std::unordered_map<std::string, ColumnValueRangeType>*
                table_col_name_to_value_range) {
    // It is a bit similar to iceberg. I will consider integrating it when I write hudi schema change later.
    _table_col_to_file_col.clear();
    _file_col_to_table_col.clear();

    if (!_range.table_format_params.paimon_params.__isset.schema_file_path) [[unlikely]] {
        return Status::RuntimeError("miss schema file");
    }

    Status create_status = Status::OK();
    using MapType = std::map<uint64_t, std::string>;
    const auto table_id_to_file_name = *_kv_cache->get<MapType>(
            _range.table_format_params.paimon_params.schema_file_path, [&]() -> MapType* {
                auto* file_id_to_name_ptr = new MapType();
                create_status = read_schema_file(*file_id_to_name_ptr);
                if (!create_status) {
                    delete file_id_to_name_ptr;
                    return nullptr;
                }
                return file_id_to_name_ptr;
            });
    RETURN_IF_ERROR(create_status);

    for (auto [table_col_id, file_col_name] : table_id_to_file_name) {
        if (table_col_id_table_name_map.find(table_col_id) == table_col_id_table_name_map.end()) {
            continue;
        }
        auto& table_col_name = table_col_id_table_name_map.at(table_col_id);

        _table_col_to_file_col.emplace(table_col_name, file_col_name);
        _file_col_to_table_col.emplace(file_col_name, table_col_name);
        if (table_col_name != file_col_name) {
            _has_schema_change = true;
        }
    }

    _all_required_col_names.clear();
    _not_in_file_col_names.clear();
    for (auto name : read_table_col_names) {
        auto iter = _table_col_to_file_col.find(name);
        if (iter == _table_col_to_file_col.end()) {
            auto name_low = to_lower(name);
            _all_required_col_names.emplace_back(name_low);

            _table_col_to_file_col.emplace(name, name_low);
            _file_col_to_table_col.emplace(name_low, name);
            if (name != name_low) {
                _has_schema_change = true;
            }
        } else {
            _all_required_col_names.emplace_back(iter->second);
        }
    }

    for (auto& it : *table_col_name_to_value_range) {
        auto iter = _table_col_to_file_col.find(it.first);
        if (iter == _table_col_to_file_col.end()) {
            _new_colname_to_value_range.emplace(it.first, it.second);
        } else {
            _new_colname_to_value_range.emplace(iter->second, it.second);
        }
    }
    return Status::OK();
}

Status PaimonReader::init_row_filters() {
    const auto& table_desc = _range.table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    // set push down agg type to NONE because we can not do count push down opt
    // if there are delete files.
    if (!_range.table_format_params.paimon_params.__isset.row_count) {
        _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    }

    const auto& deletion_file = table_desc.deletion_file;
    io::FileSystemProperties properties = {
            .system_type = _params.file_type,
            .properties = _params.properties,
            .hdfs_params = _params.hdfs_params,
            .broker_addresses {},
    };
    if (_range.__isset.file_type) {
        // for compatibility
        properties.system_type = _range.file_type;
    }
    if (_params.__isset.broker_addresses) {
        properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                           _params.broker_addresses.end());
    }

    io::FileDescription file_description = {
            .path = deletion_file.path,
            .file_size = -1,
            .mtime = 0,
            .fs_name = _range.fs_name,
    };

    // TODO: cache the file in local
    auto delete_file_reader = DORIS_TRY(FileFactory::create_file_reader(
            properties, file_description, io::FileReaderOptions::DEFAULT));
    // the reason of adding 4: https://github.com/apache/paimon/issues/3313
    size_t bytes_read = deletion_file.length + 4;
    // TODO: better way to alloc memeory
    std::vector<char> buf(bytes_read);
    Slice result(buf.data(), bytes_read);
    {
        SCOPED_TIMER(_paimon_profile.delete_files_read_time);
        RETURN_IF_ERROR(
                delete_file_reader->read_at(deletion_file.offset, result, &bytes_read, _io_ctx));
    }
    if (bytes_read != deletion_file.length + 4) {
        return Status::IOError(
                "failed to read deletion vector, deletion file path: {}, offset: {}, expect "
                "length: {}, real "
                "length: {}",
                deletion_file.path, deletion_file.offset, deletion_file.length + 4, bytes_read);
    }
    auto deletion_vector = DORIS_TRY(DeletionVector::deserialize(result.data, result.size));
    if (!deletion_vector.is_empty()) {
        for (auto i = deletion_vector.minimum(); i <= deletion_vector.maximum(); i++) {
            if (deletion_vector.is_delete(i)) {
                _delete_rows.push_back(i);
            }
        }
        COUNTER_UPDATE(_paimon_profile.num_delete_rows, _delete_rows.size());
        set_delete_rows();
    }
    return Status::OK();
}

Status PaimonReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _table_col_to_file_col.find(col.name);
            if (iter != _table_col_to_file_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }

    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));

    if (_has_schema_change) {
        for (int i = 0; i < block->columns(); i++) {
            ColumnWithTypeAndName& col = block->get_by_position(i);
            auto iter = _file_col_to_table_col.find(col.name);
            if (iter != _file_col_to_table_col.end()) {
                col.name = iter->second;
            }
        }
        block->initialize_index_by_name();
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
