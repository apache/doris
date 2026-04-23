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

#include "exec/spill/spill_file_writer.h"

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"

namespace doris {

SpillFileWriter::SpillFileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                                 RuntimeProfile* profile, SpillDataDir* data_dir,
                                 const std::string& spill_dir)
        : _spill_file_wptr(spill_file),
          _data_dir(data_dir),
          _spill_dir(spill_dir),
          _max_part_size(config::spill_file_part_size_bytes),
          _resource_ctx(state->get_query_ctx()->resource_ctx()) {
    // Common counters
    RuntimeProfile* common_profile = profile->get_child("CommonCounters");
    DCHECK(common_profile != nullptr);
    _memory_used_counter = common_profile->get_counter("MemoryUsage");

    // Register this writer as the active writer for the SpillFile.
    spill_file->_active_writer = this;

    // Custom (spill-specific) counters
    RuntimeProfile* custom_profile = profile->get_child("CustomCounters");
    _write_file_timer = custom_profile->get_counter("SpillWriteFileTime");
    _serialize_timer = custom_profile->get_counter("SpillWriteSerializeBlockTime");
    _write_block_counter = custom_profile->get_counter("SpillWriteBlockCount");
    _write_block_bytes_counter = custom_profile->get_counter("SpillWriteBlockBytes");
    _write_file_total_size = custom_profile->get_counter("SpillWriteFileBytes");
    _write_file_current_size = custom_profile->get_counter("SpillWriteFileCurrentBytes");
    _write_rows_counter = custom_profile->get_counter("SpillWriteRows");
    _total_file_count = custom_profile->get_counter("SpillWriteFileTotalCount");
}

SpillFileWriter::~SpillFileWriter() {
    if (_closed) {
        return;
    }
    Status st = close();
    if (!st.ok()) {
        LOG(WARNING) << "SpillFileWriter::~SpillFileWriter() failed: " << st.to_string()
                     << ", spill_dir=" << _spill_dir;
    }
}

Status SpillFileWriter::_open_next_part() {
    _current_part_path = _spill_dir + "/" + std::to_string(_current_part_index);
    // Create the spill directory lazily on first part
    if (_current_part_index == 0) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_spill_dir));
    }
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(_current_part_path, &_file_writer));
    COUNTER_UPDATE(_total_file_count, 1);
    return Status::OK();
}

Status SpillFileWriter::_close_current_part(const std::shared_ptr<SpillFile>& spill_file) {
    if (!_file_writer) {
        return Status::OK();
    }

    // Write footer: block offsets + max_sub_block_size + block_count
    _part_meta.append((const char*)&_part_max_sub_block_size, sizeof(_part_max_sub_block_size));
    _part_meta.append((const char*)&_part_written_blocks, sizeof(_part_written_blocks));

    {
        SCOPED_TIMER(_write_file_timer);
        RETURN_IF_ERROR(_file_writer->append(_part_meta));
    }

    int64_t meta_size = _part_meta.size();
    _part_written_bytes += meta_size;
    _total_written_bytes += meta_size;
    COUNTER_UPDATE(_write_file_total_size, meta_size);
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(meta_size);
    }
    if (_write_file_current_size) {
        COUNTER_UPDATE(_write_file_current_size, meta_size);
    }
    _data_dir->update_spill_data_usage(meta_size);
    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_write_bytes(meta_size);
    // Incrementally update SpillFile's accounting so gc() can always
    // decrement the correct amount, even if close() is never called.
    if (spill_file) {
        spill_file->update_written_bytes(meta_size);
    }

    RETURN_IF_ERROR(_file_writer->close());
    _file_writer.reset();

    // Advance to next part
    ++_current_part_index;
    ++_total_parts;
    if (spill_file) {
        spill_file->increment_part_count();
    }
    _part_written_blocks = 0;
    _part_written_bytes = 0;
    _part_max_sub_block_size = 0;
    _part_meta.clear();

    return Status::OK();
}

Status SpillFileWriter::_rotate_if_needed(const std::shared_ptr<SpillFile>& spill_file) {
    if (_file_writer && _part_written_bytes >= _max_part_size) {
        RETURN_IF_ERROR(_close_current_part(spill_file));
    }
    return Status::OK();
}

Status SpillFileWriter::write_block(RuntimeState* state, const Block& block) {
    DCHECK(!_closed);

    // Lock the SpillFile to ensure it is still alive. If it has already been
    // destroyed (gc'd), we must not write any more data because the disk
    // accounting would be out of sync.
    auto spill_file = _spill_file_wptr.lock();
    if (!spill_file) {
        return Status::Error<INTERNAL_ERROR>(
                "SpillFile has been destroyed, cannot write more data, spill_dir={}", _spill_dir);
    }

    // Lazily open the first part
    if (!_file_writer) {
        RETURN_IF_ERROR(_open_next_part());
    }

    DBUG_EXECUTE_IF("fault_inject::spill_file::spill_block", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_file spill_block failed");
    });

    auto rows = block.rows();
    COUNTER_UPDATE(_write_rows_counter, rows);
    COUNTER_UPDATE(_write_block_bytes_counter, block.bytes());

    RETURN_IF_ERROR(_write_internal(block, spill_file));

    // Auto-rotate if current part is full
    return _rotate_if_needed(spill_file);
}

Status SpillFileWriter::close() {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;

    DBUG_EXECUTE_IF("fault_inject::spill_file::spill_eof", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_file spill_eof failed");
    });

    auto spill_file = _spill_file_wptr.lock();
    RETURN_IF_ERROR(_close_current_part(spill_file));

    if (spill_file) {
        if (spill_file->_active_writer != this) {
            return Status::Error<INTERNAL_ERROR>(
                    "SpillFileWriter close() called but not registered as active writer, possible "
                    "double close or logic error");
        }
        spill_file->finish_writing();
    }

    return Status::OK();
}

Status SpillFileWriter::_write_internal(const Block& block,
                                        const std::shared_ptr<SpillFile>& spill_file) {
    size_t uncompressed_bytes = 0, compressed_bytes = 0;

    Status status;
    std::string buff;
    int64_t buff_size {0};

    if (block.rows() > 0) {
        {
            PBlock pblock;
            SCOPED_TIMER(_serialize_timer);
            int64_t compressed_time = 0;
            status = block.serialize(
                    BeExecVersionManager::get_newest_version(), &pblock, &uncompressed_bytes,
                    &compressed_bytes, &compressed_time,
                    segment_v2::CompressionTypePB::ZSTD); // ZSTD for better compression ratio
            RETURN_IF_ERROR(status);
            int64_t pblock_mem = pblock.ByteSizeLong();
            COUNTER_UPDATE(_memory_used_counter, pblock_mem);
            Defer defer {[&]() { COUNTER_UPDATE(_memory_used_counter, -pblock_mem); }};
            if (!pblock.SerializeToString(&buff)) {
                return Status::Error<ErrorCode::SERIALIZE_PROTOBUF_ERROR>(
                        "serialize spill data error. [path={}]", _current_part_path);
            }
            buff_size = buff.size();
            COUNTER_UPDATE(_memory_used_counter, buff_size);
            Defer defer2 {[&]() { COUNTER_UPDATE(_memory_used_counter, -buff_size); }};
        }
        if (_data_dir->reach_capacity_limit(buff_size)) {
            return Status::Error<ErrorCode::DISK_REACH_CAPACITY_LIMIT>(
                    "spill data total size exceed limit, path: {}, size limit: {}, spill data "
                    "size: {}",
                    _data_dir->path(),
                    PrettyPrinter::print_bytes(_data_dir->get_spill_data_limit()),
                    PrettyPrinter::print_bytes(_data_dir->get_spill_data_bytes()));
        }

        {
            Defer defer {[&]() {
                if (status.ok()) {
                    _data_dir->update_spill_data_usage(buff_size);
                    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_write_bytes(buff_size);

                    _part_max_sub_block_size =
                            std::max(_part_max_sub_block_size, (size_t)buff_size);

                    _part_meta.append((const char*)&_part_written_bytes, sizeof(size_t));
                    COUNTER_UPDATE(_write_file_total_size, buff_size);
                    if (_resource_ctx) {
                        _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(
                                buff_size);
                    }
                    if (_write_file_current_size) {
                        COUNTER_UPDATE(_write_file_current_size, buff_size);
                    }
                    COUNTER_UPDATE(_write_block_counter, 1);
                    _part_written_bytes += buff_size;
                    _total_written_bytes += buff_size;
                    ++_part_written_blocks;
                    // Incrementally update SpillFile so gc() can always
                    // decrement the correct amount from _data_dir.
                    spill_file->update_written_bytes(buff_size);
                }
            }};
            {
                SCOPED_TIMER(_write_file_timer);
                status = _file_writer->append(buff);
                RETURN_IF_ERROR(status);
            }
        }
    }

    return status;
}

} // namespace doris