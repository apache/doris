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

#include "format_v2/delimited_text/csv_reader.h"
#include "format_v2/delimited_text/text_reader.h"
#include "format_v2/json/json_reader.h"
#include "format_v2/orc/orc_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"

namespace doris::format {

Status fetch_file_schema(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         const std::shared_ptr<io::IOContext>& io_ctx, RuntimeProfile* profile,
                         std::vector<ColumnDefinition>* schema) {
    RuntimeProfile fallback_profile("FetchFileSchema");
    // Native schema callers can omit a profile, but text line readers always register counters.
    // Keep a profile alive until the reader and any asynchronous prefetch work are destroyed.
    if (profile == nullptr) {
        profile = &fallback_profile;
    }
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
    file->range_start_offset = range.start_offset;
    file->range_size = range.__isset.size ? range.size : -1;
    file->fs_name = range.fs_name;
    if (range.__isset.file_cache_admission) {
        file->file_cache_admission = range.file_cache_admission;
    }
    const bool timestamp_tz =
            params.__isset.enable_mapping_timestamp_tz && params.enable_mapping_timestamp_tz;
    const bool varbinary =
            params.__isset.enable_mapping_varbinary && params.enable_mapping_varbinary;
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    // Text and JSON need scan attributes but no planned columns: their schema is discovered
    // before FE can construct slots. Range filesystem metadata also applies to pipe detection.
    auto scan_params = params;
    scan_params.__set_file_type(properties->system_type);
    const std::vector<SlotDescriptor*> slots;
    const auto compression =
            range.__isset.compress_type ? range.compress_type : TFileCompressType::UNKNOWN;
    const auto load_id =
            range.__isset.load_id ? std::optional<TUniqueId>(range.load_id) : std::nullopt;
    if (params.format_type == TFileFormatType::FORMAT_JSON) {
        json::JsonReader json_reader(properties, file, io_ctx, profile, &scan_params, range, slots,
                                     compression, load_id);
        RETURN_IF_ERROR(json_reader.infer_schema(&state, schema));
        return json_reader.close();
    }
    std::unique_ptr<DelimitedTextReader> text_reader;
    switch (params.format_type) {
    case TFileFormatType::FORMAT_TEXT:
        text_reader = std::make_unique<text::TextReader>(properties, file, io_ctx, profile,
                                                         &scan_params, slots, compression, load_id);
        break;
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        text_reader = std::make_unique<csv::CsvReader>(properties, file, io_ctx, profile,
                                                       &scan_params, slots, compression, load_id);
        break;
    default:
        break;
    }
    if (text_reader != nullptr) {
        RETURN_IF_ERROR(text_reader->infer_schema(&state, schema));
        return text_reader->close();
    }
    std::unique_ptr<FileReader> reader;
    // TVF targets must retain file precision before column mapping; a microsecond schema
    // irreversibly rounds nanos even when the subsequent row scan uses the V2 reader.
    if (params.format_type == TFileFormatType::FORMAT_PARQUET) {
        reader = std::make_unique<parquet::ParquetReader>(properties, file, io_ctx, profile,
                                                          std::nullopt, timestamp_tz, varbinary);
    } else if (params.format_type == TFileFormatType::FORMAT_ORC) {
        // Preserve the UUID override while ordinary ORC BINARY always maps to VARBINARY.
        reader = std::make_unique<orc::OrcReader>(properties, file, io_ctx, profile, std::nullopt,
                                                  timestamp_tz, varbinary);
    } else {
        return Status::NotSupported("Unsupported format {} for V2 schema inference",
                                    params.format_type);
    }
    RETURN_IF_ERROR(reader->init(&state));
    RETURN_IF_ERROR(reader->get_schema(schema));
    return reader->close();
}

} // namespace doris::format
