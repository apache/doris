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

#include <algorithm>
#include <chrono>
#include <thread>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"

namespace doris {
#include "common/compile_check_begin.h"

SpillFileWriter::SpillFileWriter(const std::shared_ptr<SpillFile>& spill_file, RuntimeState* state,
                                 RuntimeProfile* profile, SpillDataDir* data_dir,
                                 io::FileSystemSPtr fs, std::string spill_dir)
        : _spill_file_wptr(spill_file),
          _data_dir(data_dir),
          _fs(std::move(fs)),
          _spill_dir(std::move(spill_dir)),
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
    _remote_upload_bytes = custom_profile->get_counter("SpillRemoteUploadBytes");
    _remote_upload_timer = custom_profile->get_counter("SpillRemoteUploadTime");
    _remote_upload_wait_timer = custom_profile->get_counter("SpillRemoteUploadWaitTime");
}

SpillFileWriter::~SpillFileWriter() {
    if (!_closed) {
        Status st = close();
        if (!st.ok()) {
            LOG(WARNING) << "SpillFileWriter::~SpillFileWriter() failed: " << st.to_string()
                         << ", spill_dir=" << _spill_dir;
        }
    }
    // A failed close() leaves this writer registered; the SpillFile must not reach it anymore.
    if (auto spill_file = _spill_file_wptr.lock();
        spill_file && spill_file->_active_writer == this) {
        spill_file->_active_writer = nullptr;
    }
}

void SpillFileWriter::_discard(SpillFile* spill_file) {
    DCHECK_EQ(spill_file->_active_writer, this);
    LOG(INFO) << "[spill][discard] " << spill_file->_log_context()
              << ", reason=file_gc_before_close, status=OK";
    spill_file->_active_writer = nullptr;
    _closed = true;
    if (!_file_writer) {
        return;
    }
    // Drop the part without a footer or close(). Destruction waits for in-flight uploads
    // without publishing the object, before the SpillFile deletes its prefix. Incomplete
    // multipart uploads are left to the bucket lifecycle rule.
    std::unique_ptr<io::FileWriter> writer = std::move(_file_writer);
    if (_data_dir->is_remote()) {
        _account_remote_part(/*closed=*/false);
    }
    writer.reset();
    _advance_part();
}

Status SpillFileWriter::_open_next_part(const std::shared_ptr<SpillFile>& spill_file) {
    auto fs = _fs;
    if (fs == nullptr) {
        return Status::InternalError("spill store {} is not ready", _data_dir->path());
    }
    _current_part_path = _spill_dir + "/" + std::to_string(_current_part_index);
    // Create the spill directory lazily on first part (a no-op on object storage)
    if (_current_part_index == 0) {
        RETURN_IF_ERROR(fs->create_directory(_spill_dir));
        if (spill_file) {
            spill_file->_dir_created = true;
        }
    }

    io::FileWriterOptions opts;
    // Spill data is read back once, sequentially; keep it out of the file cache. All three
    // fields matter: adaptive writes go to the cache even with write_file_cache=false.
    opts.write_file_cache = false;
    opts.allow_adaptive_file_cache_write = false;
    opts.approximate_bytes_to_write = 0;
    RETURN_IF_ERROR(fs->create_file(_current_part_path, &_file_writer, &opts));
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

    int64_t meta_size = _part_meta.size();
    // The footer must always be written so that the part can be closed; account it
    // without checking the capacity limit.
    Status status = _data_dir->try_reserve(meta_size, /*force=*/true);
    if (status.ok()) {
        SCOPED_TIMER(_write_file_timer);
        status = _append_to_part(Slice(_part_meta));
        if (!status.ok()) {
            _data_dir->release(meta_size);
        }
    }

    if (status.ok()) {
        _part_written_bytes += meta_size;
        COUNTER_UPDATE(_write_file_total_size, meta_size);
        if (_resource_ctx) {
            if (_data_dir->is_remote()) {
                _resource_ctx->io_context()->update_spill_write_bytes_to_remote_storage(meta_size);
            } else {
                _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(meta_size);
            }
        }
        if (_write_file_current_size) {
            COUNTER_UPDATE(_write_file_current_size, meta_size);
        }
        ExecEnv::GetInstance()->spill_file_mgr()->update_spill_write_bytes(meta_size);
        // Incrementally update SpillFile's accounting so gc() can always
        // decrement the correct amount, even if close() is never called.
        if (spill_file) {
            spill_file->update_written_bytes(meta_size);
        }
    }

    // Close synchronously. Spilling already runs on a thread that may block on every append
    // (the pipeline execution thread running the revoke task, or the async result-writer
    // thread for Iceberg), and with the default part size the wait at a part boundary (the
    // tail of the uploads plus one CompleteMultipartUpload) is negligible against the part
    // itself, so overlapping it with the next part is not worth an async close pipeline.
    std::unique_ptr<io::FileWriter> writer = std::move(_file_writer);
    if (status.ok()) {
        MonotonicStopWatch close_watch;
        close_watch.start();
        status = writer->close();
        if (_data_dir->is_remote()) {
            _part_upload_ns += close_watch.elapsed_time();
        }
    }
    if (_data_dir->is_remote()) {
        _account_remote_part(status.ok());
    }
    if (!status.ok()) {
        // A part that failed before close() (an append failed or the query was cancelled) must
        // not be published, so it is not closed; close() itself may also fail before its
        // uploads are done. Destruction waits for every in-flight upload in both cases,
        // before the SpillFile can delete its prefix. Incomplete multipart uploads are left
        // to the bucket lifecycle rule.
        writer.reset();
    }

    if (!status.ok()) {
        LOG(WARNING) << "failed to close spill part " << _current_part_path << ": " << status;
        // A failed append or close can still leave an object in S3. The writer has drained
        // in-flight uploads above, so list the prefix after accounting a conservative upper
        // bound rather than relying on the close status to prove whether PUT committed.
        if (spill_file && _data_dir->is_remote()) {
            spill_file->account_potential_part(_part_written_bytes);
        }
    } else if (spill_file) {
        spill_file->add_part(_part_written_bytes);
    }

    _advance_part();
    return status;
}

void SpillFileWriter::_account_remote_part(bool closed) {
    // A successful close() publishes the whole part, including its footer. Failed uploads
    // can still be in flight, so their bytes are not counted as successfully uploaded.
    const int64_t uploaded_bytes = closed ? _part_written_bytes : 0;
    if (_remote_upload_bytes != nullptr) {
        COUNTER_UPDATE(_remote_upload_bytes, uploaded_bytes);
        COUNTER_UPDATE(_remote_upload_timer, _part_upload_ns);
    }
    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_remote_write(uploaded_bytes);
}

Status SpillFileWriter::_wait_for_upload_queue() {
    auto is_cancelled = [this]() {
        return _resource_ctx != nullptr && _resource_ctx->task_controller()->is_cancelled();
    };
    // A cancelled query must not start another (billed) upload.
    if (is_cancelled()) {
        return Status::Cancelled("query cancelled before uploading spill data");
    }
    auto* upload_pool = ExecEnv::GetInstance()->s3_file_upload_thread_pool();
    auto queue_full = [upload_pool]() {
        return static_cast<int64_t>(upload_pool->get_queue_size()) * config::s3_write_buffer_size >=
               config::spill_s3_max_inflight_upload_bytes;
    };
    if (!queue_full()) {
        return Status::OK();
    }
    MonotonicStopWatch wait_watch;
    wait_watch.start();
    Defer record_wait {
            [&]() { COUNTER_UPDATE(_remote_upload_wait_timer, wait_watch.elapsed_time()); }};
    // Queued uploads always finish, so the wait ends without help from other writers.
    while (queue_full()) {
        if (is_cancelled()) {
            return Status::Cancelled("query cancelled while waiting for the spill upload queue");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return Status::OK();
}

Status SpillFileWriter::_append_to_part(const Slice& data) {
    RETURN_IF_ERROR(_part_append_status);
    if (!_data_dir->is_remote()) {
        _part_append_status = _file_writer->append(data);
        return _part_append_status;
    }
    // S3FileWriter splits `data` into upload buffers and queues every full one without waiting,
    // so the queue is checked once per append: `data` is already in memory, and the wait keeps
    // the next append from producing faster than object storage takes the data.
    _part_append_status = _wait_for_upload_queue();
    RETURN_IF_ERROR(_part_append_status);
    MonotonicStopWatch append_watch;
    append_watch.start();
    _part_append_status = _file_writer->append(data);
    _part_upload_ns += append_watch.elapsed_time();
    return _part_append_status;
}

void SpillFileWriter::_advance_part() {
    ++_current_part_index;
    _part_written_blocks = 0;
    _part_written_bytes = 0;
    _part_max_sub_block_size = 0;
    _part_meta.clear();
    _part_upload_ns = 0;
    _part_append_status = Status::OK();
}

Status SpillFileWriter::_rotate_if_needed(const std::shared_ptr<SpillFile>& spill_file) {
    if (_file_writer && _part_written_bytes >= _max_part_size) {
        RETURN_IF_ERROR(_close_current_part(spill_file));
    }
    return Status::OK();
}

Status SpillFileWriter::write_block(RuntimeState* state, const Block& block) {
    DCHECK(!_closed);
    // A failed write lost data, so the file can no longer be completed.
    RETURN_IF_ERROR(_write_status);
    _write_status = _write_block(state, block);
    return _write_status;
}

Status SpillFileWriter::_write_block(RuntimeState* state, const Block& block) {
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
        if (_current_part_index == 0) {
            state->get_query_ctx()->record_spill_data_dir(_data_dir);
        }
        RETURN_IF_ERROR(_open_next_part(spill_file));
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

    auto spill_file = _spill_file_wptr.lock();
    auto status = _close_impl(spill_file);
    if (spill_file) {
        if (status.ok()) {
            LOG(INFO) << "[spill][close] " << spill_file->_log_context() << ", status=OK";
        } else {
            LOG(WARNING) << "[spill][close] " << spill_file->_log_context()
                         << ", status=" << status;
        }
    }
    return status;
}

Status SpillFileWriter::_close_impl(const std::shared_ptr<SpillFile>& spill_file) {
    if (!_write_status.ok()) {
        // A write failed (possibly in a part that was already rotated out): the current part
        // is dropped without publishing it and the file is not finished.
        if (_part_append_status.ok()) {
            _part_append_status = _write_status;
        }
        static_cast<void>(_close_current_part(spill_file));
        return _write_status;
    }
    // Accounts the statistics of the last part even when it fails.
    RETURN_IF_ERROR(_close_current_part(spill_file));

    // Injected after the last part is closed and accounted.
    DBUG_EXECUTE_IF("fault_inject::spill_file::spill_eof", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_file spill_eof failed");
    });

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
        // Capacity is checked and reserved atomically; concurrent writers cannot pass the
        // limit together.
        RETURN_IF_ERROR(_data_dir->try_reserve(buff_size));

        {
            Defer defer {[&]() {
                if (status.ok()) {
                    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_write_bytes(buff_size);

                    _part_max_sub_block_size =
                            std::max(_part_max_sub_block_size, (size_t)buff_size);

                    _part_meta.append((const char*)&_part_written_bytes, sizeof(size_t));
                    COUNTER_UPDATE(_write_file_total_size, buff_size);
                    if (_resource_ctx) {
                        if (_data_dir->is_remote()) {
                            _resource_ctx->io_context()->update_spill_write_bytes_to_remote_storage(
                                    buff_size);
                        } else {
                            _resource_ctx->io_context()->update_spill_write_bytes_to_local_storage(
                                    buff_size);
                        }
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
                } else {
                    // The bytes never reached the store; give the capacity back.
                    _data_dir->release(buff_size);
                }
            }};
            {
                SCOPED_TIMER(_write_file_timer);
                status = _append_to_part(Slice(buff));
                RETURN_IF_ERROR(status);
            }
        }
    }

    return status;
}

} // namespace doris
