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
#include "exec/spill/spill_remote_upload_budget.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/local_file_writer.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_file_writer.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile_counter_names.h"
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

    if (_data_dir->is_remote()) {
        _budget = ExecEnv::GetInstance()->spill_file_mgr()->remote_upload_budget();
    }

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
    _remote_write_requests = custom_profile->get_counter(profile::SPILL_REMOTE_WRITE_REQUESTS);
    _remote_upload_part_requests =
            custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_PART_REQUESTS);
    _remote_upload_bytes = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_BYTES);
    _remote_upload_timer = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_TIME);
    _remote_upload_wait_timer = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_WAIT_TIME);
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

Status SpillFileWriter::_open_next_part(const std::shared_ptr<SpillFile>& spill_file) {
    // Confirm parts whose upload has finished meanwhile; surfaces upload errors early.
    RETURN_IF_ERROR(_reap_closing_parts(/*block=*/false, spill_file));

    auto fs = _data_dir->fs();
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
    if (_budget != nullptr) {
        _part_stats = std::make_shared<io::RemoteWriteStats>();
        _part_ledger = std::make_shared<PartBudgetLedger>();
        opts.remote_write_stats = _part_stats;
        // Budget is taken when S3FileWriter submits a buffer (appending thread) and given
        // back when that upload finished (upload thread). Both lambdas own what they touch:
        // the budget lives as long as the SpillFileManager, the ledger is shared.
        opts.upload_submit_gate = [budget = _budget, ledger = _part_ledger,
                                   ctx = _resource_ctx](size_t bytes) -> Status {
            int64_t wait_ns = 0;
            RETURN_IF_ERROR(budget->acquire(
                    static_cast<int64_t>(bytes),
                    [&ctx]() { return ctx != nullptr && ctx->task_controller()->is_cancelled(); },
                    &wait_ns));
            ledger->acquired.fetch_add(static_cast<int64_t>(bytes));
            ledger->wait_ns.fetch_add(wait_ns);
            return Status::OK();
        };
        opts.upload_done_callback = [budget = _budget, ledger = _part_ledger](size_t bytes) {
            budget->release(static_cast<int64_t>(bytes));
            ledger->released.fetch_add(static_cast<int64_t>(bytes));
        };
    }
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
        status = _file_writer->append(_part_meta);
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

    // Issue a non-blocking close so that the upload of this part overlaps with the next
    // one. The part is confirmed later by _reap_closing_parts(), which also reconciles
    // the upload budget, so it is queued even when something above failed.
    ClosingPart part;
    part.path = _current_part_path;
    part.part_index = _current_part_index;
    part.part_bytes = _part_written_bytes;
    part.ledger = std::move(_part_ledger);
    part.stats = std::move(_part_stats);
    part.close_status = status.ok() ? _file_writer->close(/*non_block=*/true) : status;
    part.writer = std::move(_file_writer);
    _closing_parts.emplace_back(std::move(part));

    // Advance to next part
    ++_current_part_index;
    _part_written_blocks = 0;
    _part_written_bytes = 0;
    _part_max_sub_block_size = 0;
    _part_meta.clear();

    return status;
}

Status SpillFileWriter::_reap_closing_parts(bool block,
                                            const std::shared_ptr<SpillFile>& spill_file) {
    Status first_error;
    while (!_closing_parts.empty()) {
        auto& part = _closing_parts.front();
        Status st = part.close_status;
        if (st.ok()) {
            st = part.writer->try_finish_close();
            if (st.is<ErrorCode::NEED_SEND_AGAIN>()) {
                if (!block) {
                    break;
                }
                st = part.writer->close();
            } else if (st.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
                // Writers without an async close protocol (local files) finished the work
                // in close(true); close() only flips the state.
                st = part.writer->state() == io::FileWriter::State::CLOSED ? Status::OK()
                                                                           : part.writer->close();
            }
        }
        st = _finish_part(part, spill_file, st);
        _closing_parts.erase(_closing_parts.begin());
        if (!st.ok() && first_error.ok()) {
            first_error = st;
            if (!block) {
                break;
            }
        }
    }
    return first_error;
}

Status SpillFileWriter::_finish_part(ClosingPart& part,
                                     const std::shared_ptr<SpillFile>& spill_file,
                                     Status close_status) {
    MultipartUploadId upload = _multipart_upload_id(part.writer.get());
    if (!close_status.ok() && part.writer != nullptr &&
        part.writer->state() != io::FileWriter::State::CLOSED) {
        // The part never reached a final state (footer failed, or close(true) could not be
        // issued). Destroying the writer waits for every in-flight upload, so the ledger below
        // is complete afterwards. close() is not used for this: on a cancelled query it would
        // be refused by the upload gate right away and drain nothing.
        part.writer.reset();
    }

    // Budget: everything the gate took for this part but the upload callback never gave
    // back (buffers that failed before their upload started) is released here. The writer is
    // in its final state at this point, so every callback that will ever fire has fired.
    if (_budget != nullptr && part.ledger != nullptr) {
        int64_t remaining = part.ledger->acquired.load() - part.ledger->released.load();
        DCHECK_GE(remaining, 0) << "upload callback released more than acquired, part="
                                << part.path;
        if (remaining > 0) {
            _budget->release(remaining);
        }
        if (_remote_upload_wait_timer != nullptr) {
            COUNTER_UPDATE(_remote_upload_wait_timer, part.ledger->wait_ns.load());
        }
    }

    if (part.stats != nullptr) {
        int64_t requests = part.stats->total_requests();
        int64_t data_requests = part.stats->put_object_requests + part.stats->upload_part_requests;
        int64_t uploaded_bytes = part.stats->uploaded_bytes;
        if (_remote_write_requests != nullptr) {
            COUNTER_UPDATE(_remote_write_requests, requests);
            COUNTER_UPDATE(_remote_upload_part_requests, data_requests);
            COUNTER_UPDATE(_remote_upload_bytes, uploaded_bytes);
            COUNTER_UPDATE(_remote_upload_timer, part.stats->request_time_ns.load());
        }
        if (_resource_ctx) {
            _resource_ctx->io_context()->update_spill_remote_write_requests(requests);
        }
        ExecEnv::GetInstance()->spill_file_mgr()->update_spill_remote_write(uploaded_bytes,
                                                                            data_requests);
    }

    if (!close_status.ok()) {
        LOG(WARNING) << "failed to close spill part " << part.path << ": " << close_status;
        _abort_multipart_upload(upload);
        return close_status;
    }
    if (spill_file) {
        spill_file->add_part(part.part_bytes);
    }
    return Status::OK();
}

SpillFileWriter::MultipartUploadId SpillFileWriter::_multipart_upload_id(io::FileWriter* writer) {
    auto* s3_writer = dynamic_cast<io::S3FileWriter*>(writer);
    if (s3_writer == nullptr || s3_writer->upload_id().empty()) {
        return {};
    }
    return {.path = s3_writer->path().native(),
            .bucket = s3_writer->bucket(),
            .key = s3_writer->key(),
            .upload_id = s3_writer->upload_id()};
}

void SpillFileWriter::_abort_multipart_upload(const MultipartUploadId& upload) {
    if (upload.upload_id.empty()) {
        return;
    }
    auto s3_fs = std::dynamic_pointer_cast<io::S3FileSystem>(_data_dir->fs());
    if (s3_fs == nullptr) {
        return;
    }
    auto client = s3_fs->client_holder()->get();
    if (client == nullptr) {
        return;
    }
    auto resp = client->abort_multipart_upload(
            {.path = upload.path, .bucket = upload.bucket, .key = upload.key}, upload.upload_id);
    LOG(INFO) << "abort multipart upload of failed spill part " << upload.path
              << ", upload_id=" << upload.upload_id << ", status=" << resp.status.msg;
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
    Status status = _close_current_part(spill_file);
    // Always drain the closing parts so that budget and statistics are reconciled even
    // when the current part failed.
    Status reap_status = _reap_closing_parts(/*block=*/true, spill_file);
    if (status.ok()) {
        status = reap_status;
    }
    RETURN_IF_ERROR(status);

    // Injected after the drain: a failed close must never strand budget or parts.
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
        // limit together. Upload budget is not taken here: S3FileWriter asks for it when it actually submits
        // a buffer (see the upload_submit_gate in _open_next_part).
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
                status = _file_writer->append(buff);
                RETURN_IF_ERROR(status);
            }
        }
    }

    return status;
}

} // namespace doris
