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

#include <gen_cpp/PlanNodes_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "format/table/deletion_vector.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"

namespace doris {

class FileMetaCache;
class RuntimeProfile;
class RuntimeState;

struct IcebergDeleteFileIOContext {
    explicit IcebergDeleteFileIOContext(RuntimeState* state);

    io::FileCacheStatistics file_cache_stats;
    io::FileReaderStats file_reader_stats;
    io::IOContext io_ctx;
};

struct IcebergDeleteFileReaderOptions {
    RuntimeState* state = nullptr;
    RuntimeProfile* profile = nullptr;
    const TFileScanRangeParams* scan_params = nullptr;
    io::IOContext* io_ctx = nullptr;
    FileMetaCache* meta_cache = nullptr;
    const std::string* fs_name = nullptr;
    // Optional per-DV attribution. The reader still merges these values into io_ctx so the
    // scanner-wide File Cache profile remains complete.
    io::FileCacheStatistics* deletion_vector_file_cache_stats = nullptr;
    size_t batch_size = 102400;
};

class IcebergPositionDeleteVisitor {
public:
    virtual ~IcebergPositionDeleteVisitor() = default;
    virtual Status visit(const std::string& file_path, int64_t pos) = 0;
};

TFileScanRangeParams build_iceberg_delete_scan_range_params(
        const std::map<std::string, std::string>& hadoop_conf, TFileType::type file_type,
        const std::vector<TNetworkAddress>& broker_addresses);

TFileRangeDesc build_iceberg_delete_file_range(const std::string& path);

bool is_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file);

std::string build_iceberg_deletion_vector_cache_key(const std::string& data_file_path,
                                                    const TIcebergDeleteFileDesc& delete_file);

Status decode_iceberg_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                             DeletionVector* rows_to_delete);

Status read_iceberg_position_delete_file(const TIcebergDeleteFileDesc& delete_file,
                                         const IcebergDeleteFileReaderOptions& options,
                                         IcebergPositionDeleteVisitor* visitor);

Status read_iceberg_deletion_vector(const TIcebergDeleteFileDesc& delete_file,
                                    const IcebergDeleteFileReaderOptions& options,
                                    DeletionVector* rows_to_delete);

} // namespace doris
