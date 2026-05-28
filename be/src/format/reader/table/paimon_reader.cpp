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

#include "format/reader/table/paimon_reader.h"

#include <cstring>
#include <string>

#include "format/table/deletion_vector_reader.h"

namespace doris::paimon {

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

} // namespace doris::paimon
