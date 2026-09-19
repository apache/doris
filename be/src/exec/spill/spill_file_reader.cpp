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
#include <cstring>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "exec/spill/spill_file_manager.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"
#include "util/slice.h"
namespace doris {
namespace io {
class FileSystem;
} // namespace io

SpillFileReader::SpillFileReader(RuntimeState* state, RuntimeProfile* profile,
                                 SpillDataDir* data_dir, std::string spill_dir,
                                 std::vector<int64_t> part_sizes)
        : _data_dir(data_dir),
          _spill_dir(std::move(spill_dir)),
          _part_sizes(std::move(part_sizes)),
          _part_count(_part_sizes.size()),
          _is_remote(data_dir != nullptr && data_dir->is_remote()),
          _resource_ctx(state->get_query_ctx()->resource_ctx()) {
    // Internalize counter setup. The counters themselves are registered by the owning
    // operator (SpillReadCounters::init), so look them up by the shared name constants:
    // a literal that drifts from the constant silently yields a null counter, which
    // turns every SCOPED_TIMER/COUNTER_UPDATE on it into a no-op.
    RuntimeProfile* custom_profile = profile->get_child(profile::CUSTOM_COUNTERS);
    DCHECK(custom_profile != nullptr);
    auto get_counter = [&](const char* name) {
        auto* counter = custom_profile->get_counter(name);
        DCHECK(counter != nullptr) << "spill read counter is not registered: " << name;
        return counter;
    };
    _read_file_timer = get_counter(profile::SPILL_READ_FILE_TIME);
    _deserialize_timer = get_counter(profile::SPILL_READ_DESERIALIZE_BLOCK_TIME);
    _read_block_count = get_counter(profile::SPILL_READ_BLOCK_COUNT);
    _read_block_data_size = get_counter(profile::SPILL_READ_BLOCK_BYTES);
    _read_file_size = get_counter(profile::SPILL_READ_FILE_BYTES);
    _read_rows_count = get_counter(profile::SPILL_READ_ROWS);
    _read_file_count = get_counter(profile::SPILL_READ_FILE_COUNT);
    // Optional: older profiles may not register it.
    _remote_read_requests = custom_profile->get_counter(profile::SPILL_REMOTE_READ_REQUESTS);
    if (_is_remote) {
        _coalesce_bytes = static_cast<size_t>(std::max<int64_t>(
                0,
                std::min(config::spill_s3_read_coalesce_bytes, state->spill_buffer_size_bytes())));
    }
}

void SpillFileReader::_record_read(size_t bytes_read) {
    COUNTER_UPDATE(_read_file_size, bytes_read);
    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_read_bytes(bytes_read);
    if (_is_remote) {
        // One read_at() is exactly one GET request on object storage.
        if (_remote_read_requests != nullptr) {
            COUNTER_UPDATE(_remote_read_requests, 1);
        }
        if (_resource_ctx) {
            _resource_ctx->io_context()->update_spill_read_bytes_from_remote_storage(bytes_read);
            _resource_ctx->io_context()->update_spill_remote_read_requests(1);
        }
        ExecEnv::GetInstance()->spill_file_mgr()->update_spill_remote_read(bytes_read, 1);
    } else if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_read_bytes_from_local_storage(bytes_read);
    }
}

Status SpillFileReader::open() {
    if (_is_open || _part_count == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_open_part(0, true));
    _is_open = true;
    return Status::OK();
}

Status SpillFileReader::_open_part(size_t part_index, bool fetch_small_part) {
    _close_current_part();

    _current_part_index = part_index;
    _part_opened = true;
    std::string part_path = _spill_dir + "/" + std::to_string(part_index);

    COUNTER_UPDATE(_read_file_count, 1);
    auto fs = _data_dir != nullptr ? _data_dir->fs() : io::global_local_filesystem();
    if (fs == nullptr) {
        return Status::InternalError("spill store {} is not ready", _data_dir->path());
    }
    io::FileReaderOptions opts;
    opts.cache_type = io::FileCachePolicy::NO_CACHE;
    // The writer recorded the part size; on object storage this saves a HEAD request.
    opts.file_size = _part_sizes[part_index];
    {
        SCOPED_TIMER(_read_file_timer);
        RETURN_IF_ERROR(fs->open_file(part_path, &_file_reader, &opts));
    }
    RETURN_IF_ERROR(_read_footer(_file_reader->size(), fetch_small_part));
    _part_read_block_index = 0;
    return Status::OK();
}

Status SpillFileReader::_read_footer(size_t file_size, bool fetch_small_part) {
    // The part layout (from the end backwards) is:
    //   [block count (size_t)]
    //   [max sub block size (size_t)]
    //   [block start offsets array (_part_block_count * size_t)]
    //   [serialized blocks]
    constexpr size_t kFooterTailBytes = 2 * sizeof(size_t);
    // Enough for the offsets of a few thousand blocks, so the footer usually takes one GET.
    constexpr size_t kRemoteFooterProbeBytes = 64 * 1024;
    if (file_size < kFooterTailBytes) {
        return Status::InternalError("spill part {} is too small: {} bytes",
                                     _file_reader->path().native(), file_size);
    }

    // Without coalescing (local disk) the footer is read exactly. Otherwise a larger tail is
    // read in one request, or the whole part when it fits in one coalesced read.
    size_t probe_size = kFooterTailBytes;
    if (_coalesce_bytes > 0) {
        probe_size = fetch_small_part && file_size <= _coalesce_bytes
                             ? file_size
                             : std::min(file_size, kRemoteFooterProbeBytes);
    }
    _ensure_read_buff(probe_size);
    RETURN_IF_ERROR(_read_exact(file_size - probe_size, _read_buff.data(), probe_size));

    const char* tail = _read_buff.data() + probe_size - kFooterTailBytes;
    memcpy(&_part_max_sub_block_size, tail, sizeof(size_t));
    memcpy(&_part_block_count, tail + sizeof(size_t), sizeof(size_t));
    if (_part_block_count > (file_size - kFooterTailBytes) / sizeof(size_t)) {
        return Status::InternalError("spill part {} of {} bytes has a corrupted block count {}",
                                     _file_reader->path().native(), file_size, _part_block_count);
    }
    const size_t footer_size = kFooterTailBytes + _part_block_count * sizeof(size_t);
    const size_t footer_start = file_size - footer_size;

    // The buffer holds [covered_begin, file_size). Fetch the head of the offsets array if the
    // probe did not reach it.
    size_t covered_begin = file_size - probe_size;
    if (footer_size > probe_size) {
        _ensure_read_buff(footer_size);
        memmove(_read_buff.data() + (footer_size - probe_size), _read_buff.data(), probe_size);
        RETURN_IF_ERROR(_read_exact(footer_start, _read_buff.data(), footer_size - probe_size));
        covered_begin = footer_start;
    }

    const char* offsets = _read_buff.data() + (footer_start - covered_begin);
    _block_start_offsets.resize(_part_block_count + 1);
    for (size_t i = 0; i < _part_block_count; ++i) {
        memcpy(&_block_start_offsets[i], offsets + i * sizeof(size_t), sizeof(size_t));
    }
    _block_start_offsets[_part_block_count] = footer_start;
    for (size_t i = 0; i < _part_block_count; ++i) {
        if (_block_start_offsets[i] > _block_start_offsets[i + 1]) {
            return Status::InternalError(
                    "spill part {} has a corrupted offset {} of block {}, next offset {}",
                    _file_reader->path().native(), _block_start_offsets[i], i,
                    _block_start_offsets[i + 1]);
        }
    }

    // Blocks the footer read already brought in are served from the buffer.
    _window_begin = covered_begin;
    _window_end = footer_start;
    return Status::OK();
}

Status SpillFileReader::_read_exact(size_t offset, char* data, size_t len) {
    size_t bytes_read = 0;
    {
        SCOPED_TIMER(_read_file_timer);
        RETURN_IF_ERROR(_file_reader->read_at(offset, Slice(data, len), &bytes_read));
    }
    if (bytes_read != len) {
        return Status::InternalError("short read of spill part {} at offset {}: {} of {} bytes",
                                     _file_reader->path().native(), offset, bytes_read, len);
    }
    _record_read(bytes_read);
    return Status::OK();
}

Status SpillFileReader::_block_slice(size_t index, Slice* out) {
    const size_t begin = _block_start_offsets[index];
    const size_t end = _block_start_offsets[index + 1];
    if (begin < _window_begin || end > _window_end) {
        // Take this block and the following ones as long as the read stays within
        // _coalesce_bytes. A block larger than that is still read whole.
        size_t last = index + 1;
        while (last < _part_block_count &&
               _block_start_offsets[last + 1] - begin <= _coalesce_bytes) {
            ++last;
        }
        const size_t window_end = _block_start_offsets[last];
        // A failed read may leave the buffer partly overwritten.
        _window_begin = _window_end = 0;
        _ensure_read_buff(window_end - begin);
        RETURN_IF_ERROR(_read_exact(begin, _read_buff.data(), window_end - begin));
        _window_begin = begin;
        _window_end = window_end;
    }
    *out = Slice(_read_buff.data() + (begin - _window_begin), end - begin);
    return Status::OK();
}

void SpillFileReader::_ensure_read_buff(size_t size) {
    if (_read_buff.size() < size) {
        _read_buff.resize(size);
    }
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
    _window_begin = 0;
    _window_end = 0;
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
        RETURN_IF_ERROR(_open_part(next_part, true));
    }

    if (_block_start_offsets[_part_read_block_index + 1] ==
        _block_start_offsets[_part_read_block_index]) {
        ++_part_read_block_index;
        *eos = false;
        return Status::OK();
    }

    Slice result;
    RETURN_IF_ERROR(_block_slice(_part_read_block_index, &result));
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

    // Skipped parts only need their footers; the current part keeps its buffered blocks.
    auto position_at_part = [&](size_t part_index) -> Status {
        if (_part_opened && _current_part_index == part_index) {
            return Status::OK();
        }
        return _open_part(part_index, false);
    };

    size_t remaining = block_index;
    for (size_t part_index = 0; part_index < _part_count; ++part_index) {
        RETURN_IF_ERROR(position_at_part(part_index));
        if (remaining < _part_block_count) {
            _part_read_block_index = remaining;
            return Status::OK();
        }
        remaining -= _part_block_count;
    }

    // block_index is out of range: position reader at EOS.
    RETURN_IF_ERROR(position_at_part(_part_count - 1));
    _part_read_block_index = _part_block_count;
    return Status::OK();
}

Status SpillFileReader::close() {
    _close_current_part();
    PaddedPODArray<char>().swap(_read_buff);
    _is_open = false;
    return Status::OK();
}

} // namespace doris
