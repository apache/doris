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

#include "vec/core/block_spill_reader.h"

#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>

#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "vec/core/block.h"

namespace doris {
namespace io {
class FileSystem;
} // namespace io

namespace vectorized {
void BlockSpillReader::_init_profile() {
    read_time_ = ADD_TIMER(profile_, "ReadTime");
    deserialize_time_ = ADD_TIMER(profile_, "DeserializeTime");
    read_bytes_ = ADD_COUNTER(profile_, "ReadBytes", TUnit::BYTES);
    read_block_num_ = ADD_COUNTER(profile_, "ReadBlockNum", TUnit::UNIT);
}

Status BlockSpillReader::open() {
    std::shared_ptr<io::FileSystem> file_system;
    io::FileSystemProperties system_properties;
    system_properties.system_type = TFileType::FILE_LOCAL;

    io::FileDescription file_description;
    file_description.path = file_path_;
    io::FileReaderOptions reader_options = io::FileReaderOptions::DEFAULT;
    RETURN_IF_ERROR(FileFactory::create_file_reader(system_properties, file_description,
                                                    reader_options, &file_system, &file_reader_));

    size_t file_size = file_reader_->size();

    Slice result((char*)&block_count_, sizeof(size_t));

    // read block count
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader_->read_at(file_size - sizeof(size_t), result, &bytes_read));

    // read max sub block size
    result.data = (char*)&max_sub_block_size_;
    RETURN_IF_ERROR(file_reader_->read_at(file_size - sizeof(size_t) * 2, result, &bytes_read));

    size_t buff_size = std::max(block_count_ * sizeof(size_t), max_sub_block_size_);
    read_buff_.reset(new char[buff_size]);

    // read block start offsets
    size_t read_offset = file_size - (block_count_ + 2) * sizeof(size_t);
    result.data = read_buff_.get();
    result.size = block_count_ * sizeof(size_t);

    RETURN_IF_ERROR(file_reader_->read_at(read_offset, result, &bytes_read));
    DCHECK(bytes_read == block_count_ * sizeof(size_t));

    block_start_offsets_.resize(block_count_ + 1);
    for (size_t i = 0; i < block_count_; ++i) {
        block_start_offsets_[i] = *(size_t*)(result.data + i * sizeof(size_t));
    }
    block_start_offsets_[block_count_] = file_size - (block_count_ + 2) * sizeof(size_t);

    return Status::OK();
}

void BlockSpillReader::seek(size_t block_index) {
    DCHECK(file_reader_ != nullptr);
    DCHECK_LT(block_index, block_count_);
    read_block_index_ = block_index;
}

// The returned block is owned by BlockSpillReader and is
// destroyed when reading next block.
Status BlockSpillReader::read(Block* block, bool* eos) {
    DCHECK(file_reader_);
    block->clear();

    if (read_block_index_ >= block_count_) {
        *eos = true;
        return Status::OK();
    }

    size_t bytes_to_read =
            block_start_offsets_[read_block_index_ + 1] - block_start_offsets_[read_block_index_];

    if (bytes_to_read == 0) {
        ++read_block_index_;
        COUNTER_UPDATE(read_block_num_, 1);
        return Status::OK();
    }
    Slice result(read_buff_.get(), bytes_to_read);

    size_t bytes_read = 0;

    {
        SCOPED_TIMER(read_time_);
        RETURN_IF_ERROR(file_reader_->read_at(block_start_offsets_[read_block_index_], result,
                                              &bytes_read));
    }
    DCHECK(bytes_read == bytes_to_read);
    COUNTER_UPDATE(read_bytes_, bytes_read);
    COUNTER_UPDATE(read_block_num_, 1);

    if (bytes_read > 0) {
        PBlock pb_block;
        BlockUPtr new_block = nullptr;
        {
            SCOPED_TIMER(deserialize_time_);
            if (!pb_block.ParseFromArray(result.data, result.size)) {
                return Status::InternalError("Failed to read spilled block");
            }
            new_block = Block::create_unique();
            RETURN_IF_ERROR(new_block->deserialize(pb_block));
        }
        block->swap(*new_block);
    } else {
        block->clear_column_data();
    }

    ++read_block_index_;

    return Status::OK();
}

Status BlockSpillReader::close() {
    if (!file_reader_) {
        return Status::OK();
    }
    ExecEnv::GetInstance()->block_spill_mgr()->remove(stream_id_);
    file_reader_.reset();
    if (delete_after_read_) {
        io::global_local_filesystem()->delete_file(file_path_);
    }
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
