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

#include "deletion_vector_reader.h"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "util/block_compression.h"

namespace doris {
namespace vectorized {
Status DeletionVectorReader::open() {
    if (_is_opened) [[unlikely]] {
        return Status::OK();
    }

    _init_system_properties();
    _init_file_description();
    RETURN_IF_ERROR(_create_file_reader());

    _file_size = _file_reader->size();
    _is_opened = true;
    return Status::OK();
}

Status DeletionVectorReader::read_at(size_t offset, Slice result) {
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop read.");
    }
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(offset, result, &bytes_read, _io_ctx));
    if (bytes_read != result.size) [[unlikely]] {
        return Status::IOError("Failed to read fully at offset {}, expected {}, got {}", offset,
                               result.size, bytes_read);
    }
    return Status::OK();
}

Status DeletionVectorReader::_create_file_reader() {
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop read.");
    }

    _file_description.mtime = _range.__isset.modification_time ? _range.modification_time : 0;
    io::FileReaderOptions reader_options =
            FileFactory::get_reader_options(_state, _file_description);
    _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
            _profile, _system_properties, _file_description, reader_options,
            io::DelegateReader::AccessMode::RANDOM, _io_ctx));
    return Status::OK();
}

void DeletionVectorReader::_init_file_description() {
    _file_description.path = _range.path;
    _file_description.file_size = _range.__isset.file_size ? _range.file_size : -1;
    if (_range.__isset.fs_name) {
        _file_description.fs_name = _range.fs_name;
    }
}

void DeletionVectorReader::_init_system_properties() {
    if (_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _range.file_type;
    } else {
        _system_properties.system_type = _params.file_type;
    }
    _system_properties.properties = _params.properties;
    _system_properties.hdfs_params = _params.hdfs_params;
    if (_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                                   _params.broker_addresses.end());
    }
}

} // namespace vectorized
} // namespace doris