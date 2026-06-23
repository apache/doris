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

#include "format_v2/table/paimon_reader.h"

#include <cstring>
#include <string>

#include "format/table/deletion_vector_reader.h"
#include "format_v2/table/schema_history_util.h"

namespace doris::format::paimon {

Status PaimonReader::prepare_split(const format::SplitReadOptions& options) {
    _split_schema_id = -1;
    const auto& paimon_params = options.current_range.table_format_params.paimon_params;
    if (paimon_params.__isset.schema_id) {
        _split_schema_id = paimon_params.schema_id;
    }
    return format::TableReader::prepare_split(options);
}

format::TableColumnMappingMode PaimonReader::mapping_mode() const {
    return format::can_map_by_history_schema(_scan_params, _split_schema_id)
                   ? format::TableColumnMappingMode::BY_FIELD_ID
                   : format::TableColumnMappingMode::BY_NAME;
}

Status PaimonReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (mapping_mode() != format::TableColumnMappingMode::BY_FIELD_ID) {
        return Status::OK();
    }
    return format::annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema);
}

Status PaimonReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                 DeleteFileDesc* desc, bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    const auto& table_desc = t_desc.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }
    const auto& deletion_file = table_desc.deletion_file;

    const std::string key_prefix = "paimon_dv:";
    desc->key.resize(key_prefix.size() + deletion_file.path.size() + sizeof(deletion_file.offset));
    char* key_data = desc->key.data();
    memcpy(key_data, key_prefix.data(), key_prefix.size());
    key_data += key_prefix.size();
    memcpy(key_data, deletion_file.path.data(), deletion_file.path.size());
    key_data += deletion_file.path.size();
    memcpy(key_data, &deletion_file.offset, sizeof(deletion_file.offset));
    desc->path = deletion_file.path;
    desc->start_offset = deletion_file.offset;
    desc->size = deletion_file.length + 4;
    desc->file_size = -1;
    desc->format = DeleteFileDesc::Format::PAIMON;
    *has_delete_file = true;
    return Status::OK();
}

} // namespace doris::format::paimon
