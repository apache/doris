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

#include "io/fs/oss_file_writer.h"

#include <alibabacloud/oss/OssClient.h>
#include <bvar/reducer.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>

#include "common/compile_check_begin.h"
#include "common/config.h"
#include "common/status.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_bufferpool.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"

namespace doris::io {

// Existing metrics
bvar::Adder<uint64_t> oss_file_writer_total("oss_file_writer_total_num");
bvar::Adder<uint64_t> oss_bytes_written_total("oss_file_writer_bytes_written");
bvar::Adder<uint64_t> oss_file_created_total("oss_file_writer_file_created");
bvar::Adder<uint64_t> oss_file_being_written("oss_file_writer_file_being_written");

// New async metrics
bvar::Adder<int64_t> oss_file_writer_async_close_queuing("oss_file_writer_async_close_queuing");
bvar::Adder<int64_t> oss_file_writer_async_close_processing(
        "oss_file_writer_async_close_processing");

OSSFileWriter::OSSFileWriter(std::shared_ptr<OSSClientHolder> client, std::string bucket,
                             std::string key, const FileWriterOptions* opts)
        : _path(fmt::format("oss://{}/{}", bucket, key)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _client(std::move(client)),
          _buffer_size(config::s3_write_buffer_size),
          _used_by_oss_committer(opts ? opts->used_by_s3_committer : false) {
    oss_file_writer_total << 1;
    oss_file_being_written << 1;

    _completed_parts.reserve(100);

    init_cache_builder(opts, _path);
}

OSSFileWriter::~OSSFileWriter() {
    // Wait for async close task or pending part uploads to complete before destruction.
    if (_async_close_pack != nullptr) {
        std::ignore = _async_close_pack->future.get();
        _async_close_pack = nullptr;
    } else {
        _wait_until_finish("~OSSFileWriter");
    }

    // Abort multipart upload if not completed
    if (state() != State::CLOSED && !_upload_id.empty()) {
        LOG(WARNING) << "OSSFileWriter destroyed without close(), aborting multipart upload: "
                     << _path.native() << " upload_id: " << _upload_id;
        static_cast<void>(_abort_multipart_upload());
    }

    if (state() == State::OPENED && !_failed) {
        oss_bytes_written_total << _bytes_appended;
    }
    oss_file_being_written << -1;
}

Status OSSFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (state() != State::OPENED) {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    for (size_t i = 0; i < data_cnt; i++) {
        size_t data_size = data[i].size;
        const char* data_ptr = data[i].data;
        size_t pos = 0;

        while (pos < data_size) {
            if (_failed) {
                return _st;
            }

            // Create new buffer if needed
            if (!_pending_buf) {
                RETURN_IF_ERROR(_build_upload_buffer());
            }

            size_t remaining = data_size - pos;
            size_t buffer_remaining = _buffer_size - _pending_buf->get_size();
            size_t to_append = std::min(remaining, buffer_remaining);

            Slice s(data_ptr + pos, to_append);
            RETURN_IF_ERROR(_pending_buf->append_data(s));

            pos += to_append;
            _bytes_appended += to_append;

            if (_pending_buf->get_size() == _buffer_size) {
                // Create multipart upload on first buffer flush
                if (_cur_part_num == 1) {
                    RETURN_IF_ERROR(_create_multipart_upload());
                }

                _cur_part_num++;
                _countdown_event.add_count();
                RETURN_IF_ERROR(FileBuffer::submit(std::move(_pending_buf)));
                _pending_buf = nullptr;
            }
        }
    }

    return Status::OK();
}

Status OSSFileWriter::_build_upload_buffer() {
    auto builder = FileBufferBuilder();
    builder.set_type(BufferType::UPLOAD)
            .set_upload_callback([part_num = _cur_part_num, this](UploadFileBuffer& buf) {
                _upload_one_part(part_num, buf);
            })
            .set_file_offset(_bytes_appended)
            .set_sync_after_complete_task([this](auto&& s) {
                return _complete_part_task_callback(std::forward<decltype(s)>(s));
            })
            .set_is_cancelled([this]() { return _failed.load(); });

    if (cache_builder() != nullptr) {
        int64_t tablet_id = get_tablet_id(_path.native()).value_or(0);
        builder.set_allocate_file_blocks_holder([builder = *cache_builder(),
                                                 offset = _bytes_appended,
                                                 tablet_id = tablet_id]() -> FileBlocksHolderPtr {
            return builder.allocate_cache_holder(offset, config::s3_write_buffer_size, tablet_id);
        });
    }

    RETURN_IF_ERROR(builder.build(&_pending_buf));
    return Status::OK();
}

void OSSFileWriter::_upload_one_part(int64_t part_num, UploadFileBuffer& buf) {
    if (buf.is_cancelled()) {
        LOG(INFO) << "OSS file " << _path.native() << " skip part " << part_num
                  << " because previous failure";
        return;
    }

    // Debug point: Simulate upload failure
    DBUG_EXECUTE_IF("OSSFileWriter::_upload_one_part.upload_error", {
        auto fail_part = dp->param<int64_t>("fail_part_num", 0);
        if (fail_part == 0 || fail_part == part_num) {
            LOG(WARNING) << "Debug point: Simulating OSS upload failure for part " << part_num;
            buf.set_status(Status::IOError("Debug OSS upload error for part {}", part_num));
            return;
        }
    });

    // Debug point: Simulate slow upload
    DBUG_EXECUTE_IF("OSSFileWriter::_upload_one_part.slow_upload", {
        auto sleep_ms = dp->param<int>("sleep_ms", 1000);
        LOG(INFO) << "Debug point: Simulating slow OSS upload, sleeping " << sleep_ms << "ms";
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    });

    auto client = _client->get();
    if (nullptr == client) {
        buf.set_status(Status::InternalError("OSS client not initialized"));
        return;
    }

    auto stream = buf.get_stream();
    if (!stream) {
        buf.set_status(Status::InternalError("Failed to get stream from upload buffer for part {}",
                                             part_num));
        return;
    }

    AlibabaCloud::OSS::UploadPartRequest request(_bucket, _key, stream);
    request.setPartNumber(static_cast<int32_t>(part_num));
    request.setUploadId(_upload_id);
    request.setContentLength(buf.get_size());

    auto outcome = client->UploadPart(request);
    if (!outcome.isSuccess()) {
        std::string err = fmt::format("OSS UploadPart {} failed: {} - {}", part_num,
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        buf.set_status(Status::IOError(err));
        return;
    }

    oss_bytes_written_total << buf.get_size();

    AlibabaCloud::OSS::Part part(static_cast<int32_t>(part_num), outcome.result().ETag());

    {
        std::lock_guard<std::mutex> lock(_completed_lock);
        _completed_parts.push_back(part);
    }

    VLOG_DEBUG << "OSS UploadPart " << part_num << " completed: " << _path.native()
               << " size: " << buf.get_size();
}

bool OSSFileWriter::_complete_part_task_callback(Status s) {
    if (!s.ok()) {
        std::unique_lock<std::mutex> lck {_completed_lock};
        _failed = true;
        _st = std::move(s);
        LOG(WARNING) << "OSS async upload failed: " << _path.native() << " error: " << _st;
    }
    _countdown_event.signal();
    return !s.ok();
}

void OSSFileWriter::_wait_until_finish(std::string_view task_name) {
    auto timeout_duration = config::s3_file_writer_log_interval_second;
    auto msg = fmt::format("OSS file {} still has unfinished async tasks: {}", _path.native(),
                           task_name);
    timespec current_time;
    auto current_time_second = time(nullptr);
    current_time.tv_sec = current_time_second + timeout_duration;
    current_time.tv_nsec = 0;

    while (0 != _countdown_event.timed_wait(current_time)) {
        current_time.tv_sec += timeout_duration;
        LOG(WARNING) << msg;
    }
}

Status OSSFileWriter::_create_multipart_upload() {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    AlibabaCloud::OSS::InitiateMultipartUploadRequest request(_bucket, _key);
    auto outcome = client->InitiateMultipartUpload(request);

    if (!outcome.isSuccess()) {
        std::string err = fmt::format("OSS InitiateMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        return Status::IOError(err);
    }

    _upload_id = outcome.result().UploadId();
    LOG(INFO) << "OSS multipart upload initiated: " << _path.native()
              << " upload_id: " << _upload_id;

    return Status::OK();
}

Status OSSFileWriter::_complete_multipart_upload() {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    // Debug point: Simulate complete multipart failure
    DBUG_EXECUTE_IF("OSSFileWriter::_complete_multipart_upload.complete_error", {
        LOG(WARNING) << "Debug point: Simulating OSS complete multipart upload failure";
        _failed = true;
        return Status::IOError("Debug OSS complete multipart upload error for path {}",
                               _path.native());
    });

    // CRITICAL: Sort parts by part number (OSS requires ascending order)
    std::sort(_completed_parts.begin(), _completed_parts.end(),
              [](const AlibabaCloud::OSS::Part& a, const AlibabaCloud::OSS::Part& b) {
                  return a.PartNumber() < b.PartNumber();
              });

    AlibabaCloud::OSS::CompleteMultipartUploadRequest request(_bucket, _key, _completed_parts,
                                                              _upload_id);
    auto outcome = client->CompleteMultipartUpload(request);

    if (!outcome.isSuccess()) {
        _failed = true;
        std::string err = fmt::format("OSS CompleteMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        static_cast<void>(_abort_multipart_upload());
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS multipart upload completed: " << _path.native()
              << " parts: " << _completed_parts.size() << " bytes: " << _bytes_appended;

    oss_file_created_total << 1;
    return Status::OK();
}

Status OSSFileWriter::_abort_multipart_upload() {
    if (_upload_id.empty()) {
        return Status::OK();
    }

    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    AlibabaCloud::OSS::AbortMultipartUploadRequest request(_bucket, _key, _upload_id);
    auto outcome = client->AbortMultipartUpload(request);

    if (!outcome.isSuccess()) {
        std::string err = fmt::format("OSS AbortMultipartUpload failed: {} - {}",
                                      outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native() << " upload_id: " << _upload_id;
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS multipart upload aborted: " << _path.native() << " upload_id: " << _upload_id;
    _upload_id.clear();

    return Status::OK();
}

Status OSSFileWriter::_put_object(UploadFileBuffer& buf) {
    auto client = _client->get();
    if (!client) {
        return Status::InternalError("OSS client not initialized");
    }

    auto stream = buf.get_stream();
    if (!stream) {
        return Status::InternalError("Failed to get stream from upload buffer");
    }

    AlibabaCloud::OSS::PutObjectRequest request(_bucket, _key, stream);
    auto outcome = client->PutObject(request);

    if (!outcome.isSuccess()) {
        _failed = true;
        std::string err = fmt::format("OSS PutObject failed: {} - {}", outcome.error().Code(),
                                      outcome.error().Message());
        LOG(WARNING) << err << ", path: " << _path.native();
        return Status::IOError(err);
    }

    LOG(INFO) << "OSS PutObject completed: " << _path.native() << " size: " << buf.get_size();

    oss_file_created_total << 1;
    oss_bytes_written_total << buf.get_size();

    return Status::OK();
}

Status OSSFileWriter::_set_upload_to_remote_less_than_buffer_size() {
    auto* buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
    if (buf == nullptr) {
        return Status::InternalError(
                "Invalid buffer type in _set_upload_to_remote_less_than_buffer_size: expected "
                "UploadFileBuffer");
    }

    if (_used_by_oss_committer) {
        // Committer mode: use multipart (FE needs upload_id and part ETags)
        buf->set_upload_to_remote([part_num = _cur_part_num, this](UploadFileBuffer& buf) {
            _upload_one_part(part_num, buf);
        });
        DCHECK(_cur_part_num == 1);
        RETURN_IF_ERROR(_create_multipart_upload());
    } else {
        // Normal mode: use PutObject for small files
        buf->set_upload_to_remote([this](UploadFileBuffer& buf) {
            Status st = _put_object(buf);
            if (!st.ok()) {
                buf.set_status(st);
            }
        });
    }

    return Status::OK();
}

Status OSSFileWriter::close(bool non_block) {
    std::lock_guard<std::mutex> lock(_close_lock);

    if (state() == State::CLOSED) {
        return _st;
    }

    if (state() == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multiple times");
        }
        CHECK(_async_close_pack != nullptr);
        _st = _async_close_pack->future.get();
        _async_close_pack = nullptr;
        _state = State::CLOSED;
        return _st;
    }

    if (non_block) {
        _state = State::ASYNC_CLOSING;
        _async_close_pack = std::make_unique<AsyncCloseStatusPack>();
        _async_close_pack->future = _async_close_pack->promise.get_future();

        oss_file_writer_async_close_queuing << 1;

        return ExecEnv::GetInstance()->non_block_close_thread_pool()->submit_func([this]() {
            oss_file_writer_async_close_queuing << -1;
            oss_file_writer_async_close_processing << 1;
            _async_close_pack->promise.set_value(_close_impl());
            oss_file_writer_async_close_processing << -1;
        });
    }

    _st = _close_impl();
    _state = State::CLOSED;
    return _st;
}

Status OSSFileWriter::_close_impl() {
    // Wait for any pending async operations to complete
    _wait_until_finish("_close_impl");

    if (_failed) {
        if (!_upload_id.empty()) {
            static_cast<void>(_abort_multipart_upload());
        }
        return _st;
    }

    if (_bytes_appended == 0) {
        DCHECK_EQ(_cur_part_num, 1);
        // Create an empty buffer for PutObject
        RETURN_IF_ERROR(_build_upload_buffer());

        if (!_used_by_oss_committer) {
            auto* pending_buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
            DCHECK(pending_buf != nullptr);
            pending_buf->set_upload_to_remote([this](UploadFileBuffer& buf) {
                Status st = _put_object(buf);
                if (!st.ok()) {
                    buf.set_status(st);
                }
            });
        } else {
            RETURN_IF_ERROR(_create_multipart_upload());
        }
    }

    if (_cur_part_num == 1 && _pending_buf) {
        RETURN_IF_ERROR(_set_upload_to_remote_less_than_buffer_size());
    }

    if (_pending_buf && _pending_buf->get_size() > 0) {
        _countdown_event.add_count();
        Status st = FileBuffer::submit(std::move(_pending_buf));
        _pending_buf = nullptr;
        if (!st.ok()) {
            if (!_upload_id.empty()) {
                static_cast<void>(_abort_multipart_upload());
            }
            return st;
        }

        _wait_until_finish("last_part");

        if (_failed) {
            if (!_upload_id.empty()) {
                static_cast<void>(_abort_multipart_upload());
            }
            return _st;
        }
    }

    if (_cur_part_num == 1) {
        _wait_until_finish("PutObject or single part");
        return _st;
    }

    _wait_until_finish("Complete multipart");

    if (_used_by_oss_committer) {
        // OSS committer mode: FE will complete multipart upload
        oss_file_created_total << 1;
        LOG(INFO) << "OSS multipart upload parts completed (FE will finish): " << _path.native()
                  << " parts: " << _completed_parts.size() << " bytes: " << _bytes_appended
                  << " upload_id: " << _upload_id;
        return Status::OK();
    }

    // TODO: add check_after_upload like S3 (off by default via config)
    return _complete_multipart_upload();
}

#include "common/compile_check_end.h"

} // namespace doris::io
