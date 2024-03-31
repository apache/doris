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

#include "vec/spill/spill_writer.h"

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::vectorized {
Status SpillWriter::open() {
    if (file_writer_) {
        return Status::OK();
    }
    return io::global_local_filesystem()->create_file(file_path_, &file_writer_);
}

SpillWriter::~SpillWriter() {
    if (!closed_) {
        (void)Status::Error<ErrorCode::INTERNAL_ERROR>("spill writer not closed correctly");
    }
}

Status SpillWriter::close() {
    if (closed_ || !file_writer_) {
        return Status::OK();
    }
    closed_ = true;

    meta_.append((const char*)&max_sub_block_size_, sizeof(max_sub_block_size_));
    meta_.append((const char*)&written_blocks_, sizeof(written_blocks_));

    // meta: block1 offset, block2 offset, ..., blockn offset, max_sub_block_size, n
    {
        SCOPED_TIMER(write_timer_);
        RETURN_IF_ERROR(file_writer_->append(meta_));
    }

    total_written_bytes_ += meta_.size();
    COUNTER_UPDATE(write_bytes_counter_, meta_.size());

    ExecEnv::GetInstance()->spill_stream_mgr()->update_usage(data_dir_->path(), meta_.size());

    RETURN_IF_ERROR(file_writer_->close());

    file_writer_.reset();
    return Status::OK();
}

Status SpillWriter::write(const Block& block, size_t& written_bytes) {
    written_bytes = 0;
    DCHECK(file_writer_);
    auto rows = block.rows();
    // file format: block1, block2, ..., blockn, meta
    if (rows <= batch_size_) {
        return _write_internal(block, written_bytes);
    } else {
        auto tmp_block = block.clone_empty();
        const auto& src_data = block.get_columns_with_type_and_name();

        for (size_t row_idx = 0; row_idx < rows;) {
            tmp_block.clear_column_data();

            auto& dst_data = tmp_block.get_columns_with_type_and_name();

            size_t block_rows = std::min(rows - row_idx, batch_size_);
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx) {
                    dst_data[col_idx].column->assume_mutable()->insert_range_from(
                            *src_data[col_idx].column, row_idx, block_rows);
                }
            });

            RETURN_IF_ERROR(_write_internal(tmp_block, written_bytes));

            row_idx += block_rows;
        }
        return Status::OK();
    }
}

Status SpillWriter::_write_internal(const Block& block, size_t& written_bytes) {
    size_t uncompressed_bytes = 0, compressed_bytes = 0;

    Status status;
    std::string buff;

    if (block.rows() > 0) {
        {
            PBlock pblock;
            SCOPED_TIMER(serialize_timer_);
            status = block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                     &uncompressed_bytes, &compressed_bytes,
                                     segment_v2::CompressionTypePB::LZ4);
            RETURN_IF_ERROR(status);
            if (!pblock.SerializeToString(&buff)) {
                return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                        "serialize spill data error. [path={}]", file_path_);
            }
        }
        auto* spill_stream_mgr = ExecEnv::GetInstance()->spill_stream_mgr();
        auto splled_data_size = spill_stream_mgr->spilled_data_size(data_dir_->path());
        if (spill_stream_mgr->reach_capacity_limit(splled_data_size, buff.size())) {
            return Status::Error<ErrorCode::DISK_REACH_CAPACITY_LIMIT>(
                    "spill data total size exceed limit, path: {}, size limit: {}, spill data "
                    "size: {}",
                    data_dir_->path(), PrettyPrinter::print_bytes(config::spill_storage_limit),
                    PrettyPrinter::print_bytes(
                            spill_stream_mgr->spilled_data_size(data_dir_->path())));
        }

        {
            Defer defer {[&]() {
                if (status.ok()) {
                    spill_stream_mgr->update_usage(data_dir_->path(), buff.size());
                }
            }};
            {
                SCOPED_TIMER(write_timer_);
                status = file_writer_->append(buff);
                RETURN_IF_ERROR(status);
            }
        }
    }

    auto buff_size = buff.size();
    written_bytes += buff_size;
    max_sub_block_size_ = std::max(max_sub_block_size_, buff_size);

    meta_.append((const char*)&total_written_bytes_, sizeof(size_t));
    COUNTER_UPDATE(write_bytes_counter_, buff_size);
    COUNTER_UPDATE(write_block_counter_, 1);
    total_written_bytes_ += buff_size;
    ++written_blocks_;

    return Status::OK();
}

} // namespace doris::vectorized