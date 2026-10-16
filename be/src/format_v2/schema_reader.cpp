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

#include "format_v2/schema_reader.h"

#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"

namespace doris::format {

Status fetch_file_schema(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         const std::shared_ptr<io::IOContext>& io_ctx, RuntimeProfile* profile,
                         std::vector<ColumnDefinition>* schema) {
    auto properties = std::make_shared<io::FileSystemProperties>();
    // Schema RPCs carry the filesystem kind on the range, not necessarily in scan params.
    properties->system_type = range.__isset.file_type ? range.file_type : params.file_type;
    properties->properties = params.properties;
    properties->hdfs_params = params.hdfs_params;
    properties->broker_addresses = params.broker_addresses;
    auto file = std::make_unique<io::FileDescription>();
    file->path = range.path;
    file->file_size = range.__isset.file_size ? range.file_size : -1;
    file->mtime = range.__isset.modification_time ? range.modification_time : 0;
    file->fs_name = range.fs_name;
    if (range.__isset.file_cache_admission) {
        file->file_cache_admission = range.file_cache_admission;
    }
    const bool timestamp_tz =
            params.__isset.enable_mapping_timestamp_tz && params.enable_mapping_timestamp_tz;
    const bool varbinary =
            params.__isset.enable_mapping_varbinary && params.enable_mapping_varbinary;
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    std::unique_ptr<FileReader> reader;
    // TVF targets must retain file precision before column mapping; a microsecond schema
    // irreversibly rounds nanos even when the subsequent row scan uses the V2 reader.
    if (params.format_type == TFileFormatType::FORMAT_PARQUET) {
        reader = std::make_unique<parquet::ParquetReader>(properties, file, io_ctx, profile,
                                                          std::nullopt, timestamp_tz, varbinary);
    } else if (params.format_type == TFileFormatType::FORMAT_ORC) {
        // ORC binary mapping is fixed by its schema reader, without a separate mapping option.
        reader = std::make_unique<orc::OrcReader>(properties, file, io_ctx, profile, std::nullopt,
                                                  timestamp_tz);
    } else {
        return Status::NotSupported("V2 schema inference requires Parquet or ORC");
    }
    RETURN_IF_ERROR(reader->init(&state));
    RETURN_IF_ERROR(reader->get_schema(schema));
    return reader->close();
}

} // namespace doris::format
