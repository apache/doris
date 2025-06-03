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

#include "exec/schema_scanner/materialized_schema_table_writer.h"

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"

namespace doris {
#include "common/compile_check_begin.h"

Status MaterializedSchemaTableWriter::write(const vectorized::Block& block, size_t& written_bytes, const std::string& file_path) {
    std::unique_ptr<doris::io::FileWriter> file_writer;
    // storage_root/materialized_schema_table/table_name/begin_timestamp-end_timestamp
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file_path, &file_writer));
    Defer defer_file_writer {[&]() {
        RETURN_IF_ERROR(file_writer->close());
        file_writer.reset();
    }};

    written_bytes = 0;
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    Status status;
    std::string buff;
    int64_t buff_size {0};

    if (block.rows() > 0) {
        {
            PBlock pblock;
            status = block.serialize(
                    BeExecVersionManager::get_newest_version(), &pblock, &uncompressed_bytes,
                    &compressed_bytes,
                    segment_v2::CompressionTypePB::ZSTD); // ZSTD for better compression ratio
            RETURN_IF_ERROR(status);
            if (!pblock.SerializeToString(&buff)) {
                return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                        "serialize data error. [path={}]", file_path);
            }
            buff_size = buff.size();
        }
        if (dir_->reach_capacity_limit(buff_size)) {
            return Status::Error<ErrorCode::DISK_REACH_CAPACITY_LIMIT>(
                    "data total size exceed limit, path: {}, size limit: {}, data size: {}",
                    dir_->path(),
                    PrettyPrinter::print_bytes(dir_->get_disk_limit_bytes()),
                    PrettyPrinter::print_bytes(dir_->get_usage_bytes()));
        }

        {
            Defer defer {[&]() {
                if (status.ok()) {
                    dir_->update_usage_bytes(buff_size);
                    written_bytes += buff_size;
                    total_written_bytes_ += buff_size;
                    ++written_blocks_;
                }
            }};
            {
                status = file_writer->append(buff);
                RETURN_IF_ERROR(status);
            }
        }
    }
    return status;
}

}