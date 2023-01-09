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

#include "agent/be_exec_version_manager.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace vectorized {
Status BlockSpillWriter::open() {
    RETURN_IF_ERROR(FileFactory::create_file_writer(TFileType::FILE_LOCAL, ExecEnv::GetInstance(),
                                                    {}, {}, file_path_, 0, file_writer_));

    return file_writer_->open();
}

Status BlockSpillWriter::close() {
    if (!file_writer_) {
        return Status::OK();
    }

    tmp_block_.clear_column_data();

    meta_.append((const char*)&written_blocks_, sizeof(written_blocks_));

    size_t written_bytes;
    // meta: block1 offset, block2 offset, ..., blockn offset, n
    RETURN_IF_ERROR(
            file_writer_->write((const uint8_t*)meta_.data(), meta_.size(), &written_bytes));

    file_writer_.reset();
    return Status::OK();
}

Status BlockSpillWriter::write(const Block& block) {
    auto rows = block.rows();

    // file format: block1, block2, ..., blockn, meta
    if (rows < batch_size_) {
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
            for (size_t col_idx = 0; col_idx < block.columns(); ++col_idx) {
                dst_data[col_idx].column->assume_mutable()->insert_range_from(
                        *src_data[col_idx].column, row_idx, block_rows);
            }

            RETURN_IF_ERROR(_write_internal(tmp_block_));

            row_idx += block_rows;
        }
        return Status::OK();
    }
}
Status BlockSpillWriter::_write_internal(const Block& block) {
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    size_t written_bytes = 0;
    PBlock pblock;
    RETURN_IF_ERROR(block.serialize(BeExecVersionManager::get_newest_version(), &pblock,
                                    &uncompressed_bytes, &compressed_bytes,
                                    segment_v2::CompressionTypePB::LZ4));
    std::string buff;
    pblock.SerializeToString(&buff);
    RETURN_IF_ERROR(file_writer_->write((const uint8_t*)buff.data(), buff.size(), &written_bytes));

    meta_.append((const char*)&total_written_bytes_, sizeof(size_t));
    total_written_bytes_ += written_bytes;
    ++written_blocks_;

    return Status::OK();
}

} // namespace vectorized
} // namespace doris