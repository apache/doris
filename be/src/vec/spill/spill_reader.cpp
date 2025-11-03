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

#include "vec/spill/spill_reader.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/cast_set.h"
#include "common/exception.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream_manager.h"
namespace doris {
#include "common/compile_check_begin.h"
namespace io {
class FileSystem;
} // namespace io

namespace vectorized {
Status SpillReader::open() {
    if (file_reader_) {
        return Status::OK();
    }

    SCOPED_TIMER(_read_file_timer);

    COUNTER_UPDATE(_read_file_count, 1);

    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(file_path_, &file_reader_));

    size_t file_size = file_reader_->size();
    DCHECK(file_size >= 16); // max_sub_block_size, block count

    Slice result((char*)&block_count_, sizeof(size_t));

    size_t total_read_bytes = 0;
    // read block count
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader_->read_at(file_size - sizeof(size_t), result, &bytes_read));
    DCHECK(bytes_read == 8); // max_sub_block_size, block count
    total_read_bytes += bytes_read;
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(bytes_read);
    }

    // read max sub block size
    bytes_read = 0;
    result.data = (char*)&max_sub_block_size_;
    RETURN_IF_ERROR(file_reader_->read_at(file_size - sizeof(size_t) * 2, result, &bytes_read));
    DCHECK(bytes_read == 8); // max_sub_block_size, block count
    total_read_bytes += bytes_read;
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(bytes_read);
    }

    size_t buff_size = std::max(block_count_ * sizeof(size_t), max_sub_block_size_);
    read_buff_.reserve(buff_size);

    // read block start offsets
    size_t read_offset = file_size - (block_count_ + 2) * sizeof(size_t);
    result.data = read_buff_.data();
    result.size = block_count_ * sizeof(size_t);

    RETURN_IF_ERROR(file_reader_->read_at(read_offset, result, &bytes_read));
    DCHECK(bytes_read == block_count_ * sizeof(size_t));
    total_read_bytes += bytes_read;
    COUNTER_UPDATE(_read_file_size, total_read_bytes);
    ExecEnv::GetInstance()->spill_stream_mgr()->update_spill_read_bytes(total_read_bytes);
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_read_bytes_from_local_storage(bytes_read);
    }

    block_start_offsets_.resize(block_count_ + 1);
    for (size_t i = 0; i < block_count_; ++i) {
        block_start_offsets_[i] = *(size_t*)(result.data + i * sizeof(size_t));
    }
    block_start_offsets_[block_count_] = file_size - (block_count_ + 2) * sizeof(size_t);

    return Status::OK();
}

void SpillReader::seek(size_t block_index) {
    DCHECK_LT(block_index, block_count_);
    read_block_index_ = block_index;
}

Status SpillReader::read(Block* block, bool* eos) {
    DCHECK(file_reader_);
    block->clear_column_data();

    if (read_block_index_ >= block_count_) {
        *eos = true;
        return Status::OK();
    }

    size_t bytes_to_read =
            block_start_offsets_[read_block_index_ + 1] - block_start_offsets_[read_block_index_];

    if (bytes_to_read == 0) {
        ++read_block_index_;
        return Status::OK();
    }

    Slice result(read_buff_.data(), bytes_to_read);
    size_t bytes_read = 0;
    {
        SCOPED_TIMER(_read_file_timer);
        RETURN_IF_ERROR(file_reader_->read_at(block_start_offsets_[read_block_index_], result,
                                              &bytes_read));
    }
    DCHECK(bytes_read == bytes_to_read);

    if (bytes_read > 0) {
        COUNTER_UPDATE(_read_file_size, bytes_read);
        ExecEnv::GetInstance()->spill_stream_mgr()->update_spill_read_bytes(bytes_read);
        if (_resource_ctx) {
            _resource_ctx->io_context()->update_spill_read_bytes_from_local_storage(bytes_read);
        }
        COUNTER_UPDATE(_read_block_count, 1);
        {
            SCOPED_TIMER(_deserialize_timer);
            if (!pb_block_.ParseFromArray(result.data, cast_set<int>(result.size))) {
                return Status::InternalError("Failed to read spilled block");
            }
            RETURN_IF_ERROR(block->deserialize(pb_block_));
        }
        COUNTER_UPDATE(_read_block_data_size, block->bytes());
        COUNTER_UPDATE(_read_rows_count, block->rows());
    } else {
        block->clear_column_data();
    }

    ++read_block_index_;

    return Status::OK();
}

Status SpillReader::close() {
    if (!file_reader_) {
        return Status::OK();
    }
    (void)file_reader_->close();
    file_reader_.reset();
    return Status::OK();
}

} // namespace vectorized
} // namespace doris