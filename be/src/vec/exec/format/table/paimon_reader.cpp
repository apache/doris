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

#include "common/status.h"
#include "util/deletion_vector.h"

namespace doris::vectorized {
PaimonReader::PaimonReader(std::unique_ptr<GenericReader> file_format_reader,
                           const TFileScanRangeParams& params)
        : TableFormatReader(std::move(file_format_reader)), _params(params) {}

Status PaimonReader::init_row_filters(const TFileRangeDesc& range) {
    const auto& table_desc = range.table_format_params.paimon_params;
    if (!table_desc.__isset.delete_file) {
        return Status::OK();
    }

    const auto& deletion_file = table_desc.delete_file;
    io::FileSystemProperties properties = {
            .system_type = _params.file_type,
            .properties = _params.properties,
            .hdfs_params = _params.hdfs_params,
    };
    if (range.__isset.file_type) {
        // for compatibility
        properties.system_type = range.file_type;
    }
    if (_params.__isset.broker_addresses) {
        properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                           _params.broker_addresses.end());
    }

    io::FileDescription file_description = {
            .path = deletion_file.path,
            .file_size = -1,
            .fs_name = range.fs_name,
            .mtime = 0,
    };

    // TODO: cache the file in local
    auto delete_file_reader = DORIS_TRY(FileFactory::create_file_reader(
            properties, file_description, io::FileReaderOptions::DEFAULT));
    Slice result;
    size_t bytes_read = deletion_file.length;
    RETURN_IF_ERROR(delete_file_reader->read_at(deletion_file.offset, result, &bytes_read));
    if (bytes_read != deletion_file.length) {
        return Status::IOError(
                "failed to read deletion vector, deletion file path: {}, offset: {}, expect "
                "length: {}, real "
                "length: {}",
                deletion_file.path, deletion_file.offset, deletion_file.length, bytes_read);
    }
    auto deletion_vector = DeletionVector::deserialize(result.data, result.size);
    if (!deletion_vector.is_empty()) {
        for (auto i = deletion_vector.minimum(); i <= deletion_vector.maximum(); i++) {
            if (deletion_vector.is_delete(i)) {
                _paimon_delete_rows.push_back(i);
            }
        }
        set_delete_rows();
    }
    return Status::OK();
}
} // namespace doris::vectorized