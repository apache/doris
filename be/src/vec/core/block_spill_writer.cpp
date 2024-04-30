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

#include "vec/core/block_spill_writer.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <unistd.h>

#include <algorithm>

#include "agent/be_exec_version_manager.h"
#include "io/file_factory.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris {
namespace vectorized {
void BlockSpillWriter::_init_profile() {
    write_bytes_counter_ = ADD_COUNTER(profile_, "WriteBytes", TUnit::BYTES);
    write_timer_ = ADD_TIMER(profile_, "WriteTime");
    serialize_timer_ = ADD_TIMER(profile_, "SerializeTime");
    write_blocks_num_ = ADD_COUNTER(profile_, "WriteBlockNum", TUnit::UNIT);
}

Status BlockSpillWriter::open() {
    file_writer_ = DORIS_TRY(FileFactory::create_file_writer(
            TFileType::FILE_LOCAL, ExecEnv::GetInstance(), {}, {}, file_path_,
            {
                    .write_file_cache = false,
                    .sync_file_data = false,
            }));
    is_open_ = true;
    return Status::OK();
}

Status BlockSpillWriter::close() {
    if (!is_open_) {
        return Status::OK();
    }

    is_open_ = false;

    tmp_block_.clear_column_data();

    meta_.append((const char*)&max_sub_block_size_, sizeof(max_sub_block_size_));
    meta_.append((const char*)&written_blocks_, sizeof(written_blocks_));

    Status status;
    // meta: block1 offset, block2 offset, ..., blockn offset, n
    {
        SCOPED_TIMER(write_timer_);
        status = file_writer_->append(meta_);
    }
    if (!status.ok()) {
        unlink(file_path_.c_str());
        return status;
    }

    RETURN_IF_ERROR(file_writer_->close());
    file_writer_.reset();
    return Status::OK();
}

Status BlockSpillWriter::write(const Block& block) {
    auto rows = block.rows();
    // file format: block1, block2, ..., blockn, meta
    if (rows <= batch_size_) {
        return _write_internal(block);
    } else {
        if (is_first_write_) {
            is_first_write_ = false;
            tmp_block_ = block.clone_empty();
        }

        const auto& src_data = block.get_columns_with_type_and_name();

        for (size_t row_idx = 0; row_idx < rows;) {
            tmp_block_.clear_column_data();

            auto& dst_data = tmp_block_.get_columns_with_type_and_name();

            size_t block_rows = std::min(rows - row_idx, batch_size_);
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx) {
                    dst_data[col_idx].column->assume_mutable()->insert_range_from(
                            *src_data[col_idx].column, row_idx, block_rows);
                }
            });

            RETURN_IF_ERROR(_write_internal(tmp_block_));

            row_idx += block_rows;
        }
        return Status::OK();
    }
}
Status BlockSpillWriter::_write_internal(const Block& block) {
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    size_t written_bytes = 0;

    Status status;
    std::string buff;

    if (block.rows() > 0) {
        PBlock pblock;
        {
            SCOPED_TIMER(serialize_timer_);
            status = block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                     &uncompressed_bytes, &compressed_bytes,
                                     segment_v2::CompressionTypePB::NO_COMPRESSION);
            if (!status.ok()) {
                unlink(file_path_.c_str());
                return status;
            }
            pblock.SerializeToString(&buff);
        }

        {
            SCOPED_TIMER(write_timer_);
            status = file_writer_->append(buff);
            written_bytes = buff.size();
        }

        if (!status.ok()) {
            unlink(file_path_.c_str());
            return status;
        }
    }

    max_sub_block_size_ = std::max(max_sub_block_size_, written_bytes);

    meta_.append((const char*)&total_written_bytes_, sizeof(size_t));
    COUNTER_UPDATE(write_bytes_counter_, written_bytes);
    COUNTER_UPDATE(write_blocks_num_, 1);
    total_written_bytes_ += written_bytes;
    ++written_blocks_;

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
