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

#include "vec/exec/format/table/fluss_reader.h"

#include "common/logging.h"
#include "common/status.h"
#include "runtime/runtime_state.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

FlussReader::FlussReader(std::unique_ptr<GenericReader> file_format_reader,
                         RuntimeProfile* profile, RuntimeState* state,
                         const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         io::IOContext* io_ctx, FileMetaCache* meta_cache)
        : TableFormatReader(std::move(file_format_reader), state, profile, params, range, io_ctx,
                            meta_cache) {
    // Log tier information for debugging
    if (range.__isset.table_format_params && 
        range.table_format_params.__isset.fluss_params) {
        const auto& fluss_params = range.table_format_params.fluss_params;
        LOG(INFO) << "FlussReader initialized for table: " 
                  << fluss_params.database_name << "." << fluss_params.table_name
                  << ", bucket: " << fluss_params.bucket_id
                  << ", format: " << fluss_params.file_format
                  << ", lake_snapshot_id: " << fluss_params.lake_snapshot_id
                  << ", lake_files: " << (fluss_params.__isset.lake_file_paths ? 
                                          fluss_params.lake_file_paths.size() : 0);
    }
}

Status FlussReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    // For MVP, we read from lake tier (Parquet/ORC files) using the underlying reader.
    // The FE has already determined which files to read based on the LakeSnapshot.
    // Future phases will add support for LOG_ONLY and HYBRID tiers via JNI bridge.
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized

