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

#include "format_v2/blob/paimon_blob_column_builder.h"

#include <limits>
#include <string>

#include "core/data_type/data_type_file.h"

namespace doris::format::paimon {

PaimonBlobColumnBuilder::PaimonBlobColumnBuilder(const DataTypePtr& result_column_type,
                                                 PaimonBlobOutputMode output_mode,
                                                 int64_t max_inline_size, size_t row_count)
        : _output_mode(output_mode),
          _max_inline_size(max_inline_size),
          _result_nullable(result_column_type->is_nullable()),
          _result_column(result_column_type->create_column()),
          _file_value(Field::create_field<TYPE_FILE>(File(DataTypeFile::FIELD_COUNT))) {
    _result_column->reserve(row_count);
}

Status PaimonBlobColumnBuilder::append_null() {
    if (!_result_nullable) {
        return Status::Corruption("NULL in required Paimon BLOB value");
    }
    _result_column->insert_default();
    return Status::OK();
}

Status PaimonBlobColumnBuilder::_check_inline_size(int64_t payload_length) const {
    if (payload_length < 0) {
        return Status::Corruption("Negative Paimon BLOB payload length");
    }
    if (payload_length > _max_inline_size) {
        return Status::InvalidArgument(
                "Paimon BLOB inline size {} bytes exceeds paimon_blob_max_size={} bytes. "
                "Increase paimon_blob_max_size or use blob-as-descriptor=true",
                payload_length, _max_inline_size);
    }
    if (payload_length > std::numeric_limits<int32_t>::max()) {
        return Status::NotSupported("BLOB payload exceeds FILE inline VARBINARY capacity");
    }
    return Status::OK();
}

Status PaimonBlobColumnBuilder::_append_inline(int64_t payload_length) {
    if (_payload_buffer.size() != static_cast<size_t>(payload_length)) {
        return Status::Corruption("Paimon BLOB payload size does not match its reference");
    }
    auto& file_fields = _file_value.get<TYPE_FILE>();
    file_fields[DataTypeFile::INLINE_FIELD_INDEX] = Field::create_field<TYPE_VARBINARY>(
            StringView(_payload_buffer.data(), static_cast<uint32_t>(_payload_buffer.size())));
    // insert() copies borrowed payload bytes before the next read reuses the buffer.
    _result_column->insert(_file_value);
    return Status::OK();
}

void PaimonBlobColumnBuilder::_append_descriptor(const PaimonBlobReference& reference) {
    auto& file_fields = _file_value.get<TYPE_FILE>();
    file_fields[DataTypeFile::URI_FIELD_INDEX] =
            Field::create_field<TYPE_STRING>(std::string(reference.uri));
    file_fields[DataTypeFile::OFFSET_FIELD_INDEX] =
            Field::create_field<TYPE_BIGINT>(reference.offset);
    file_fields[DataTypeFile::SIZE_FIELD_INDEX] =
            Field::create_field<TYPE_BIGINT>(reference.length);
    _result_column->insert(_file_value);
}

} // namespace doris::format::paimon
