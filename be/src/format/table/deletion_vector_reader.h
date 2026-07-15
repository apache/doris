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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format/table/deletion_vector.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "util/profile_collector.h"
#include "util/slice.h"

namespace io {
struct IOContext;
} // namespace io

namespace doris {
inline constexpr int64_t MAX_DELETION_VECTOR_BYTES = 1L << 30;

Status validate_iceberg_deletion_vector_read_range(int64_t offset, int64_t size,
                                                   size_t& bytes_read);

Status validate_paimon_deletion_vector_read_range(int64_t offset, int64_t length,
                                                  size_t& bytes_read);

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

class DeletionVectorReader {
    ENABLE_FACTORY_CREATOR(DeletionVectorReader);

public:
    DeletionVectorReader(RuntimeState* state, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         io::IOContext* io_ctx)
            : _state(state), _profile(profile), _params(params), _parent_io_ctx(io_ctx) {
        _desc = DeleteFileDesc {
                .key = "",
                .path = range.path,
                .fs_name = range.__isset.fs_name ? range.fs_name : "",
                .start_offset = range.start_offset,
                .size = range.size,
                .file_size = range.__isset.file_size ? range.file_size : -1,
                .modification_time = range.__isset.modification_time ? range.modification_time : 0};
        _init_io_context();
    }
    DeletionVectorReader(RuntimeState* state, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const DeleteFileDesc& desc,
                         io::IOContext* io_ctx)
            : _state(state), _profile(profile), _params(params), _parent_io_ctx(io_ctx) {
        _desc = desc;
        _init_io_context();
    }
    ~DeletionVectorReader();
    DeletionVectorReader(const DeletionVectorReader&) = delete;
    DeletionVectorReader& operator=(const DeletionVectorReader&) = delete;
    Status open();
    Status read_at(size_t offset, Slice result);

    // These are deliberately exposed separately from the decoded-DV cache counters. Local I/O is
    // a hit in the disk-backed File Cache; remote I/O is a File Cache miss. A decoded-DV cache hit
    // does not create a DeletionVectorReader and therefore changes neither value.
    const io::FileCacheStatistics& file_cache_statistics() const { return _file_cache_stats; }

private:
    void _init_system_properties();
    void _init_file_description();
    void _init_io_context();
    void _merge_io_statistics();
    Status _create_file_reader();

private:
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    DeleteFileDesc _desc;
    const TFileScanRangeParams& _params;
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
} // namespace doris
