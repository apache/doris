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

#include <vector>

#include "common/status.h"
#include "util/deletion_vector.h"

namespace doris::vectorized {
PaimonReader::PaimonReader(std::unique_ptr<GenericReader> file_format_reader,
                           RuntimeProfile* profile, RuntimeState* state,
                           const TFileScanRangeParams& params)
        : TableFormatReader(std::move(file_format_reader)),
          _profile(profile),
          _state(state),
          _params(params) {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(_profile, paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(_profile, "DeleteFileReadTime", paimon_profile);
}

Status PaimonReader::init_row_filters(const TFileRangeDesc& range, io::IOContext* io_ctx) {
    const auto& table_desc = range.table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    // set push down agg type to NONE because we can not do count push down opt
    // if there are delete files.
    _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);
    const auto& deletion_file = table_desc.deletion_file;
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
            .mtime = 0,
            .fs_name = range.fs_name,
    };

    // TODO: cache the file in local
    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(_state, file_description);
    std::shared_ptr<io::FileSystem> file_system;
    io::FileReaderSPtr delete_file_reader;
    RETURN_IF_ERROR(FileFactory::create_file_reader(properties, file_description, reader_options,
                                                    &file_system, &delete_file_reader));
    // the reason of adding 4: https://github.com/apache/paimon/issues/3313
    size_t bytes_read = deletion_file.length + 4;
    // TODO: better way to alloc memeory
    std::vector<char> buf(bytes_read);
    Slice result(buf.data(), bytes_read);
    {
        SCOPED_TIMER(_paimon_profile.delete_files_read_time);
        RETURN_IF_ERROR(
                delete_file_reader->read_at(deletion_file.offset, result, &bytes_read, io_ctx));
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
} // namespace doris::vectorized
