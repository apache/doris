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

#include "exec/spill/spill_file_reader.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "exec/spill/spill_file_manager.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"
#include "util/slice.h"
namespace doris {
#include "common/compile_check_begin.h"
namespace io {
class FileSystem;
} // namespace io

SpillFileReader::SpillFileReader(RuntimeState* state, RuntimeProfile* profile,
                                 std::string spill_dir, size_t part_count)
        : _spill_dir(std::move(spill_dir)),
          _part_count(part_count),
          _resource_ctx(state->get_query_ctx()->resource_ctx()) {
    // Internalize counter setup
    RuntimeProfile* custom_profile = profile->get_child("CustomCounters");
    DCHECK(custom_profile != nullptr);
    _read_file_timer = custom_profile->get_counter("SpillReadFileTime");
    _deserialize_timer = custom_profile->get_counter("SpillReadDerializeBlockTime");
    _read_block_count = custom_profile->get_counter("SpillReadBlockCount");
    _read_block_data_size = custom_profile->get_counter("SpillReadBlockBytes");
    _read_file_size = custom_profile->get_counter("SpillReadFileBytes");
    _read_rows_count = custom_profile->get_counter("SpillReadRows");
    _read_file_count = custom_profile->get_counter("SpillReadFileCount");
}

Status SpillFileReader::open() {
    if (_is_open || _part_count == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_open_part(0));
    _is_open = true;
    return Status::OK();
}

Status SpillFileReader::_open_part(size_t part_index) {
    _close_current_part();

    _current_part_index = part_index;
    _part_opened = true;
    std::string part_path = _spill_dir + "/" + std::to_string(part_index);

    SCOPED_TIMER(_read_file_timer);
    COUNTER_UPDATE(_read_file_count, 1);
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(part_path, &_file_reader));

    size_t file_size = _file_reader->size();
    DCHECK(file_size >= 16); // max_sub_block_size + block count

    Slice result((char*)&_part_block_count, sizeof(size_t));

    // read block count
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(file_size - sizeof(size_t), result, &bytes_read));
    DCHECK(bytes_read == 8);

    // read max sub block size
    bytes_read = 0;
    result.data = (char*)&_part_max_sub_block_size;
    RETURN_IF_ERROR(_file_reader->read_at(file_size - sizeof(size_t) * 2, result, &bytes_read));
    DCHECK(bytes_read == 8);

    // The buffer is used for two purposes:
    // 1. Reading the block start offsets array (needs _part_block_count * sizeof(size_t) bytes)
    // 2. Reading a single block's serialized data (needs up to _part_max_sub_block_size bytes)
    // We must ensure the buffer is large enough for either case, so take the maximum.
    size_t buff_size = std::max(_part_block_count * sizeof(size_t), _part_max_sub_block_size);
    if (buff_size > _read_buff.size()) {
        _read_buff.reserve(buff_size);
    }

    // Read the block start offsets array from the end of the file.
    // The file layout (from end backwards) is:
    //   [block count (size_t)]
    //   [max sub block size (size_t)]
    //   [block start offsets array (_part_block_count * size_t)]
    // So the offsets array starts at:
    //   file_size - (_part_block_count + 2) * sizeof(size_t)
    size_t read_offset = file_size - (_part_block_count + 2) * sizeof(size_t);
    result.data = _read_buff.data();
    result.size = _part_block_count * sizeof(size_t);

    RETURN_IF_ERROR(_file_reader->read_at(read_offset, result, &bytes_read));
    DCHECK(bytes_read == _part_block_count * sizeof(size_t));

    _block_start_offsets.resize(_part_block_count + 1);
    for (size_t i = 0; i < _part_block_count; ++i) {
        _block_start_offsets[i] = *(size_t*)(result.data + i * sizeof(size_t));
    }
    _block_start_offsets[_part_block_count] = file_size - (_part_block_count + 2) * sizeof(size_t);

    _part_read_block_index = 0;
    return Status::OK();
}

void SpillFileReader::_close_current_part() {
    if (_file_reader) {
        (void)_file_reader->close();
        _file_reader.reset();
    }
    _part_block_count = 0;
    _part_read_block_index = 0;
    _part_max_sub_block_size = 0;
    _block_start_offsets.clear();
    _part_opened = false;
}

Status SpillFileReader::read(Block* block, bool* eos) {
    DBUG_EXECUTE_IF("fault_inject::spill_file::read_next_block", {
        return Status::InternalError("fault_inject spill_file read_next_block failed");
    });
    block->clear_column_data();

    if (_part_count == 0) {
        *eos = true;
        return Status::OK();
    }

    // Advance to next part if current part is exhausted
    while (_part_read_block_index >= _part_block_count) {
        size_t next_part = _part_opened ? _current_part_index + 1 : 0;
        if (next_part >= _part_count) {
            *eos = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(_open_part(next_part));
    }

    size_t bytes_to_read = _block_start_offsets[_part_read_block_index + 1] -
                           _block_start_offsets[_part_read_block_index];

    if (bytes_to_read == 0) {
        ++_part_read_block_index;
        *eos = false;
        return Status::OK();
    }

    Slice result(_read_buff.data(), bytes_to_read);
    size_t bytes_read = 0;
    {
        SCOPED_TIMER(_read_file_timer);
        RETURN_IF_ERROR(_file_reader->read_at(_block_start_offsets[_part_read_block_index], result,
                                              &bytes_read));
    }
    DCHECK(bytes_read == bytes_to_read);

    if (bytes_read > 0) {
        COUNTER_UPDATE(_read_file_size, bytes_read);
        ExecEnv::GetInstance()->spill_file_mgr()->update_spill_read_bytes(bytes_read);
        if (_resource_ctx) {
            _resource_ctx->io_context()->update_spill_read_bytes_from_local_storage(bytes_read);
        }
        COUNTER_UPDATE(_read_block_count, 1);
        {
            SCOPED_TIMER(_deserialize_timer);
            if (!_pb_block.ParseFromArray(result.data, cast_set<int>(result.size))) {
                return Status::InternalError("Failed to read spilled block");
            }
            size_t uncompressed_size = 0;
            int64_t uncompressed_time = 0;
            RETURN_IF_ERROR(block->deserialize(_pb_block, &uncompressed_size, &uncompressed_time));
        }
        COUNTER_UPDATE(_read_block_data_size, block->bytes());
        COUNTER_UPDATE(_read_rows_count, block->rows());
    } else {
        block->clear_column_data();
    }

    ++_part_read_block_index;
    *eos = false;
    return Status::OK();
}

Status SpillFileReader::seek(size_t block_index) {
    return _seek_to_block(block_index);
}

Status SpillFileReader::_seek_to_block(size_t block_index) {
    if (_part_count == 0) {
        return Status::OK();
    }

    size_t remaining = block_index;
    for (size_t part_index = 0; part_index < _part_count; ++part_index) {
        RETURN_IF_ERROR(_open_part(part_index));
        if (remaining < _part_block_count) {
            _part_read_block_index = remaining;
            return Status::OK();
        }
        remaining -= _part_block_count;
    }

    // block_index is out of range: position reader at EOS.
    RETURN_IF_ERROR(_open_part(_part_count - 1));
    _part_read_block_index = _part_block_count;
    return Status::OK();
}

Status SpillFileReader::close() {
    _close_current_part();
    _is_open = false;
    return Status::OK();
}

} // namespace doris
