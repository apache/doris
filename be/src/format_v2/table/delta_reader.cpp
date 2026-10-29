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

#include "format_v2/table/delta_reader.h"

#include "format/table/deletion_vector_reader.h"
#include "format/table/delta_deletion_vector_reader.h"

namespace doris::format::delta {

Status DeltaTableReader::_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc,
                                                     DeleteFileDesc* desc, bool* has_delete_file) {
    DORIS_CHECK(desc != nullptr);
    DORIS_CHECK(has_delete_file != nullptr);
    *has_delete_file = false;
    if (!t_desc.__isset.delta_params) {
        return Status::OK();
    }
    const auto& delta = t_desc.delta_params;
    if (!delta.__isset.storage_type || !delta.__isset.path_or_inline_dv ||
        !delta.__isset.size_in_bytes || !delta.__isset.cardinality) {
        return Status::DataQualityError("Delta deletion vector descriptor misses required fields");
    }
    if (delta.storage_type != "i" && delta.storage_type != "p" && delta.storage_type != "u") {
        return Status::DataQualityError("Unsupported Delta deletion vector storage type: {}",
                                        delta.storage_type);
    }
    if (delta.size_in_bytes < 0 || delta.cardinality < 0) {
        return Status::DataQualityError("Invalid Delta deletion vector size/cardinality");
    }

    desc->format = DeleteFileDesc::Format::DELTA;
    desc->cardinality = delta.cardinality;
    const std::string table_path = delta.__isset.table_path ? delta.table_path : "";
    desc->key = "delta_dv_" + delta.storage_type + "#" + table_path + "#" +
                delta.path_or_inline_dv + "#" +
                std::to_string(delta.__isset.offset ? delta.offset : 0) + "#" +
                std::to_string(delta.size_in_bytes) + "#" + std::to_string(delta.cardinality);
    if (delta.storage_type == "i") {
        desc->path = delta.path_or_inline_dv;
        desc->size = 0;
        desc->start_offset = 0;
        *has_delete_file = true;
        return Status::OK();
    }
    if (!delta.__isset.offset) {
        return Status::DataQualityError("Delta on-disk deletion vector descriptor misses offset");
    }
    RETURN_IF_ERROR(resolve_delta_deletion_vector_path(delta.storage_type, delta.path_or_inline_dv,
                                                       table_path, &desc->path));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(validate_delta_deletion_vector_read_range(delta.offset, delta.size_in_bytes,
                                                              bytes_read));
    desc->size = bytes_read;
    desc->start_offset = delta.offset;
    desc->file_size = -1;
    *has_delete_file = true;
    return Status::OK();
}

Status DeltaTableReader::_load_deletion_vector(const TTableFormatFileDesc& t_desc,
                                               const DeleteFileDesc& desc,
                                               DeletionVector* deletion_vector) {
    const auto& delta = t_desc.delta_params;
    if (delta.storage_type == "i") {
        SCOPED_TIMER(_profile.parse_delete_file_time);
        return decode_delta_inline_deletion_vector(delta.path_or_inline_dv, delta.size_in_bytes,
                                                   delta.cardinality, deletion_vector);
    }
    return TableReader::_load_deletion_vector(t_desc, desc, deletion_vector);
}

Status DeltaTableReader::_decode_deletion_vector(const char* buf, size_t buffer_size,
                                                 const DeleteFileDesc& desc,
                                                 DeletionVector* deletion_vector) {
    return decode_delta_deletion_vector_buffer(buf, buffer_size, desc.cardinality, deletion_vector);
}

} // namespace doris::format::delta
