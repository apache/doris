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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format_v2/deletion_vector.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/profile_collector.h"
#include "util/slice.h"

namespace doris::format {

struct DeleteFileDesc {
    enum class Format {
        PAIMON,
        ICEBERG,
    };

    std::string key = "";
    std::string path = "";
    std::string fs_name = "";
    int64_t start_offset = 0;
    int64_t size = 0;
    int64_t file_size = -1;
    int64_t modification_time = 0;
    Format format = Format::PAIMON;
};

std::string build_iceberg_deletion_vector_cache_key(const std::string& data_file_path,
                                                    const TIcebergDeleteFileDesc& delete_file);

Status decode_iceberg_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                             DeletionVector* rows_to_delete);

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file);

Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector);

class DeletionVectorReader {
public:
    DeletionVectorReader(RuntimeState* state, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const DeleteFileDesc& desc,
                         io::IOContext* io_ctx)
            : _state(state),
              _profile(profile),
              _params(params),
              _desc(desc),
              _parent_io_ctx(io_ctx) {
        _init_io_context();
    }
    ~DeletionVectorReader();

    DeletionVectorReader(const DeletionVectorReader&) = delete;
    DeletionVectorReader& operator=(const DeletionVectorReader&) = delete;

    Status open();
    Status read_at(size_t offset, Slice result);

    const io::FileCacheStatistics& file_cache_statistics() const { return _file_cache_stats; }

private:
    void _init_system_properties();
    void _init_file_description();
    void _init_io_context();
    void _merge_io_statistics();
    Status _create_file_reader();

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _params;
    DeleteFileDesc _desc;
    io::IOContext* _parent_io_ctx = nullptr;
    io::IOContext _reader_io_ctx;
    io::IOContext* _io_ctx = nullptr;
    io::FileCacheStatistics _file_cache_stats;
    io::FileReaderStats _file_reader_stats;
    bool _statistics_merged = false;

    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    io::FileReaderSPtr _file_reader;
    int64_t _file_size = 0;
    bool _is_opened = false;
};

} // namespace doris::format
