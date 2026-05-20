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

#include "format/table/deletion_vector_reader.h"

namespace doris::paimon {

bool PaimonReader::_parse_delete_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc& desc) {
    const auto& table_desc = t_desc.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return false;
    }
    const auto& deletion_file = table_desc.deletion_file;

    desc.key.resize(deletion_file.path.size() + sizeof(deletion_file.offset));
    memcpy(desc.key.data(), deletion_file.path.data(), deletion_file.path.size());
    memcpy(desc.key.data() + deletion_file.path.size(), &deletion_file.offset,
           sizeof(deletion_file.offset));
    desc.path = deletion_file.path;
    desc.start_offset = deletion_file.offset;
    desc.size = deletion_file.length + 4;
    desc.file_size = -1;
    return true;
}

} // namespace doris::paimon
