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

#include "io/fs/s3_file_writer.h"

#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <bvar/reducer.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "util/bvar_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/s3_util.h"

namespace Aws::S3::Model {
class DeleteObjectRequest;
} // namespace Aws::S3::Model

using Aws::S3::Model::AbortMultipartUploadRequest;
using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris::io {
using namespace Aws::S3::Model;
using Aws::S3::S3Client;

bvar::Adder<uint64_t> s3_file_writer_total("s3_file_writer", "total_num");
bvar::Adder<uint64_t> s3_bytes_written_total("s3_file_writer", "bytes_written");
bvar::Adder<uint64_t> s3_file_created_total("s3_file_writer", "file_created");
bvar::Adder<uint64_t> s3_file_being_written("s3_file_writer", "file_being_written");

S3FileWriter::S3FileWriter(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                           std::string key, const FileWriterOptions* opts)
        : _path(fmt::format("s3://{}/{}", bucket, key)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _client(std::move(client)),
          _used_by_s3_committer(opts ? opts->used_by_s3_committer : false) {
    s3_file_writer_total << 1;
    s3_file_being_written << 1;

    Aws::Http::SetCompliantRfc3986Encoding(true);
    if (config::enable_file_cache && opts != nullptr && opts->write_file_cache) {
        _cache_builder = std::make_unique<FileCacheAllocatorBuilder>(FileCacheAllocatorBuilder {
                opts ? opts->is_cold_data : false, opts ? opts->file_cache_expiration : 0,
                BlockFileCache::hash(_path.filename().native()),
                FileCacheFactory::instance()->get_by_path(
                        BlockFileCache::hash(_path.filename().native()))});
    }
}

S3FileWriter::~S3FileWriter() {
    if (!closed() || _failed) {
        // if we don't abort multi part upload, the uploaded part in object
        // store will not automatically reclaim itself, it would cost more money
        static_cast<void>(_abort());
    } else {
        s3_bytes_written_total << _bytes_appended;
    }
    s3_file_being_written << -1;
}

Status S3FileWriter::_create_multi_upload_request() {
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(_bucket).WithKey(_key);
    create_request.SetContentType("application/octet-stream");
    DBUG_EXECUTE_IF("s3_file_writer::_create_multi_upload_request", {
        return Status::IOError(
                "failed to create multipart upload(bucket={}, key={}, upload_id={}): injected "
                "error",
                _bucket, _path.native(), _upload_id);
    });

    SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
    auto outcome = _client->CreateMultipartUpload(create_request);

    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return s3fs_error(outcome.GetError(),
                      fmt::format("failed to create multipart upload {} upload_id={}",
                                  _path.native(), _upload_id));
}

void S3FileWriter::_wait_until_finish(std::string_view task_name) {
    auto timeout_duration = config::s3_writer_buffer_allocation_timeout;
    auto msg = fmt::format(
            "{} multipart upload already takes {} seconds, bucket={}, key={}, upload_id={}",
            task_name, timeout_duration, _bucket, _path.native(), _upload_id);
    timespec current_time;
    // We don't need high accuracy here, so we use time(nullptr)
    // since it's the fastest way to get current time(second)
    auto current_time_second = time(nullptr);
    current_time.tv_sec = current_time_second + timeout_duration;
    current_time.tv_nsec = 0;
    // bthread::countdown_event::timed_wait() should use absolute time
    while (0 != _countdown_event.timed_wait(current_time)) {
        current_time.tv_sec += timeout_duration;
        LOG(WARNING) << msg;
    }
}

Status S3FileWriter::_abort() {
    // make all pending work early quits
    _failed = true;

    // we need to reclaim the memory
    if (_pending_buf) {
        _pending_buf = nullptr;
    }
    LOG(INFO) << "S3FileWriter::abort, path: " << _path.native();
    // upload id is empty means there was no create multi upload
    if (_upload_id.empty()) {
        return Status::OK();
    }
    _wait_until_finish("Abort");
    AbortMultipartUploadRequest request;
    request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);
    SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
    auto outcome = _client->AbortMultipartUpload(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        LOG(INFO) << "Abort multipart upload successfully"
                  << "bucket=" << _bucket << ", key=" << _path.native()
                  << ", upload_id=" << _upload_id << ", whole parts=" << _dump_completed_part();
        return Status::OK();
    }
    return s3fs_error(
            outcome.GetError(),
            fmt::format("failed to abort multipart upload {} upload_id={}, whole parts={}",
                        _path.native(), _upload_id, _dump_completed_part()));
}

Status S3FileWriter::close() {
    if (closed()) {
        _wait_until_finish("close");
        return _st;
    }

    VLOG_DEBUG << "S3FileWriter::close, path: " << _path.native();

    Defer defer {[this] { _closed = true; }};

    if (_upload_id.empty() && _pending_buf) {
        RETURN_IF_ERROR(_set_upload_to_remote_less_than_buffer_size());
    }

    if (_bytes_appended == 0) {
        // No data written, but need to create an empty file
        auto builder = FileBufferBuilder();
        builder.set_type(BufferType::UPLOAD)
                .set_upload_callback([this](UploadFileBuffer& buf) { _put_object(buf); })
                .set_sync_after_complete_task([this](Status s) {
                    bool ret = false;
                    if (!s.ok()) [[unlikely]] {
                        VLOG_NOTICE << "failed at key: " << _key << ", status: " << s.to_string();
                        std::unique_lock<std::mutex> _lck {_completed_lock};
                        _failed = true;
                        ret = true;
                        this->_st = std::move(s);
                    }
                    // After the signal, there is a scenario where the previous invocation of _wait_until_finish
                    // returns to the caller, and subsequently, the S3 file writer is destructed.
                    // This means that accessing _failed afterwards would result in a heap use after free vulnerability.
                    _countdown_event.signal();
                    return ret;
                })
                .set_is_cancelled([this]() { return _failed.load(); });
        RETURN_IF_ERROR(builder.build(&_pending_buf));
        auto* buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
        DCHECK(buf != nullptr);
        if (_used_by_s3_committer) {
            buf->set_upload_to_remote([part_num = _cur_part_num, this](UploadFileBuffer& buf) {
                _upload_one_part(part_num, buf);
            });
            DCHECK(_cur_part_num == 1);
            RETURN_IF_ERROR(_create_multi_upload_request());
        }
    }

    if (_pending_buf != nullptr) {
        _countdown_event.add_count();
        RETURN_IF_ERROR(_pending_buf->submit(std::move(_pending_buf)));
        _pending_buf = nullptr;
    }

    DBUG_EXECUTE_IF("s3_file_writer::close", {
        RETURN_IF_ERROR(_complete());
        return Status::InternalError("failed to close s3 file writer");
    });
    RETURN_IF_ERROR(_complete());

    return Status::OK();
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (closed()) [[unlikely]] {
        return Status::InternalError("append to closed file: {}", _path.native());
    }

    size_t buffer_size = config::s3_write_buffer_size;
    DBUG_EXECUTE_IF("s3_file_writer::appendv",
                    { return Status::InternalError("failed to append data"); });
    for (size_t i = 0; i < data_cnt; i++) {
        size_t data_size = data[i].get_size();
        for (size_t pos = 0, data_size_to_append = 0; pos < data_size; pos += data_size_to_append) {
            if (_failed) {
                return _st;
            }
            if (!_pending_buf) {
                auto builder = FileBufferBuilder();
                builder.set_type(BufferType::UPLOAD)
                        .set_upload_callback(
                                [part_num = _cur_part_num, this](UploadFileBuffer& buf) {
                                    _upload_one_part(part_num, buf);
                                })
                        .set_file_offset(_bytes_appended)
                        .set_sync_after_complete_task([this, part_num = _cur_part_num](Status s) {
                            bool ret = false;
                            if (!s.ok()) [[unlikely]] {
                                VLOG_NOTICE << "failed at key: " << _key << ", load part "
                                            << part_num << ", st " << s;
                                std::unique_lock<std::mutex> _lck {_completed_lock};
                                _failed = true;
                                ret = true;
                                this->_st = std::move(s);
                            }
                            // After the signal, there is a scenario where the previous invocation of _wait_until_finish
                            // returns to the caller, and subsequently, the S3 file writer is destructed.
                            // This means that accessing _failed afterwards would result in a heap use after free vulnerability.
                            _countdown_event.signal();
                            return ret;
                        })
                        .set_is_cancelled([this]() { return _failed.load(); });
                if (_cache_builder != nullptr) {
                    // We would load the data into file cache asynchronously which indicates
                    // that this instance of S3FileWriter might have been destructed when we
                    // try to do writing into file cache, so we make the lambda capture the variable
                    // we need by value to extend their lifetime
                    builder.set_allocate_file_blocks_holder(
                            [builder = *_cache_builder,
                             offset = _bytes_appended]() -> FileBlocksHolderPtr {
                                return builder.allocate_cache_holder(offset,
                                                                     config::s3_write_buffer_size);
                            });
                }
                RETURN_IF_ERROR(builder.build(&_pending_buf));
            }
            // we need to make sure all parts except the last one to be 5MB or more
            // and shouldn't be larger than buf
            data_size_to_append = std::min(data_size - pos, _pending_buf->get_file_offset() +
                                                                    buffer_size - _bytes_appended);

            // if the buffer has memory buf inside, the data would be written into memory first then S3 then file cache
            // it would be written to cache then S3 if the buffer doesn't have memory preserved
            RETURN_IF_ERROR(_pending_buf->append_data(
                    Slice {data[i].get_data() + pos, data_size_to_append}));

            // if it's the last part, it could be less than 5MB, or it must
            // satisfy that the size is larger than or euqal to 5MB
            // _complete() would handle the first situation
            if (_pending_buf->get_size() == buffer_size) {
                // only create multiple upload request when the data is more
                // than one memory buffer
                if (_cur_part_num == 1) {
                    RETURN_IF_ERROR(_create_multi_upload_request());
                }
                _cur_part_num++;
                _countdown_event.add_count();
                RETURN_IF_ERROR(_pending_buf->submit(std::move(_pending_buf)));
                _pending_buf = nullptr;
            }
            _bytes_appended += data_size_to_append;
        }
    }
    return Status::OK();
}

void S3FileWriter::_upload_one_part(int64_t part_num, UploadFileBuffer& buf) {
    if (buf.is_cancelled()) {
        return;
    }
    UploadPartRequest upload_request;
    upload_request.WithBucket(_bucket).WithKey(_key).WithPartNumber(part_num).WithUploadId(
            _upload_id);

    upload_request.SetBody(buf.get_stream());

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*buf.get_stream()));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    upload_request.SetContentLength(buf.get_size());
    upload_request.SetContentType("application/octet-stream");

    UploadPartOutcome upload_part_outcome;
    {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
        upload_part_outcome = _client->UploadPart(upload_request);
    }
    DBUG_EXECUTE_IF("s3_file_writer::_upload_one_part", {
        if (part_num > 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            auto s = Status::IOError(
                    "failed to upload part (bucket={}, key={}, part_num={}, up_load_id={}): "
                    "injected error",
                    _bucket, _path.native(), part_num, _upload_id);
            LOG_WARNING(s.to_string());
            buf.set_status(std::move(s));
            return;
        }
    });
    if (!upload_part_outcome.IsSuccess()) {
        auto s = Status::IOError(
                "failed to upload part (bucket={}, key={}, part_num={}, up_load_id={}): {}, "
                "exception {}, error code {}",
                _bucket, _path.native(), part_num, _upload_id,
                upload_part_outcome.GetError().GetMessage(),
                upload_part_outcome.GetError().GetExceptionName(),
                upload_part_outcome.GetError().GetResponseCode());
        LOG_WARNING(s.to_string());
        buf.set_status(std::move(s));
        return;
    }
    s3_bytes_written_total << buf.get_size();

    std::unique_ptr<CompletedPart> completed_part = std::make_unique<CompletedPart>();
    completed_part->SetPartNumber(part_num);
    const auto& etag = upload_part_outcome.GetResult().GetETag();
    // DCHECK(etag.empty());
    completed_part->SetETag(etag);

    std::unique_lock<std::mutex> lck {_completed_lock};
    _completed_parts.emplace_back(std::move(completed_part));
}

Status S3FileWriter::_complete() {
    if (_failed) {
        _wait_until_finish("early quit");
        return _st;
    }
    // upload id is empty means there was no multipart upload
    if (_upload_id.empty()) {
        _wait_until_finish("PutObject");
        return _st;
    }
    // Wait multipart load and finish.
    _wait_until_finish("Complete");
    DBUG_EXECUTE_IF("s3_file_writer::_complete:1", { _cur_part_num++; });
    if (!_used_by_s3_committer) { // S3 committer will complete multipart upload file on FE side.
        CompleteMultipartUploadRequest complete_request;
        complete_request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);

        if (_failed || _completed_parts.size() != _cur_part_num) {
            _st = Status::InternalError(
                    "error status {}, complete parts {}, cur part num {}, whole parts {}", _st,
                    _completed_parts.size(), _cur_part_num, _dump_completed_part());
            LOG(WARNING) << _st;
            return _st;
        }
        // make sure _completed_parts are ascending order
        std::sort(_completed_parts.begin(), _completed_parts.end(),
                  [](auto& p1, auto& p2) { return p1->GetPartNumber() < p2->GetPartNumber(); });
        DBUG_EXECUTE_IF("s3_file_writer::_complete:2",
                        { _completed_parts.back()->SetPartNumber(10 * _completed_parts.size()); });
        CompletedMultipartUpload completed_upload;
        for (size_t i = 0; i < _completed_parts.size(); i++) {
            if (_completed_parts[i]->GetPartNumber() != i + 1) [[unlikely]] {
                auto st = Status::InternalError(
                        "error status {}, part num not continous, expected num {}, actual num {}, "
                        "whole parts {}",
                        _st, i + 1, _completed_parts[i]->GetPartNumber(), _dump_completed_part());
                LOG(WARNING) << st;
                _st = st;
                return st;
            }
            completed_upload.AddParts(*_completed_parts[i]);
        }

        complete_request.WithMultipartUpload(completed_upload);

        DBUG_EXECUTE_IF("s3_file_writer::_complete:3", {
            auto s = Status::IOError(
                    "failed to create complete multi part upload (bucket={}, key={}): injected "
                    "error",
                    _bucket, _path.native());
            LOG_WARNING(s.to_string());
            return s;
        });
        SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
        auto complete_outcome = _client->CompleteMultipartUpload(complete_request);

        if (!complete_outcome.IsSuccess()) {
            _st = s3fs_error(
                    complete_outcome.GetError(),
                    fmt::format(
                            "failed to complete multi part upload {}, upload_id={}, whole parts={}",
                            _path.native(), _upload_id, _dump_completed_part()));
            LOG(WARNING) << _st;
            return _st;
        }
    }
    s3_file_created_total << 1;
    return Status::OK();
}

Status S3FileWriter::finalize() {
    if (closed()) [[unlikely]] {
        return Status::InternalError("finalize closed file: {}", _path.native());
    }

    DBUG_EXECUTE_IF("s3_file_writer::finalize",
                    { return Status::IOError("failed to finalize due to injected error"); });
    // submit pending buf if it's not nullptr
    // it's the last buf, we can submit it right now
    if (_pending_buf != nullptr) {
        if (_upload_id.empty()) {
            RETURN_IF_ERROR(_set_upload_to_remote_less_than_buffer_size());
        }
        _countdown_event.add_count();
        RETURN_IF_ERROR(_pending_buf->submit(std::move(_pending_buf)));
        _pending_buf = nullptr;
    }
    _wait_until_finish("finalize");
    return _st;
}

Status S3FileWriter::_set_upload_to_remote_less_than_buffer_size() {
    auto* buf = dynamic_cast<UploadFileBuffer*>(_pending_buf.get());
    DCHECK(buf != nullptr);
    if (_used_by_s3_committer) {
        // If used_by_s3_committer, we always use multi-parts uploading.
        buf->set_upload_to_remote([part_num = _cur_part_num, this](UploadFileBuffer& buf) {
            _upload_one_part(part_num, buf);
        });
        DCHECK(_cur_part_num == 1);
        RETURN_IF_ERROR(_create_multi_upload_request());
    } else {
        // if we only need to upload one file less than 5MB, we can just
        // call PutObject to reduce the network IO
        buf->set_upload_to_remote([this](UploadFileBuffer& b) { _put_object(b); });
    }
    return Status::OK();
}

void S3FileWriter::_put_object(UploadFileBuffer& buf) {
    DCHECK(!closed());
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(_bucket).WithKey(_key);
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*buf.get_stream()));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(buf.get_stream());
    request.SetContentLength(buf.get_size());
    request.SetContentType("application/octet-stream");
    DBUG_EXECUTE_IF("s3_file_writer::_put_object", {
        _st = Status::InternalError("failed to put object");
        buf.set_status(_st);
        LOG(WARNING) << _st;
        return;
    });
    SCOPED_BVAR_LATENCY(s3_bvar::s3_put_latency);
    auto response = _client->PutObject(request);
    if (!response.IsSuccess()) {
        _st = s3fs_error(response.GetError(), fmt::format("failed to put object {}, upload_id={}",
                                                          _path.native(), _upload_id));
        LOG(WARNING) << _st;
        buf.set_status(_st);
        return;
    }
    s3_file_created_total << 1;
}

std::string S3FileWriter::_dump_completed_part() const {
    std::stringstream ss;
    ss << "part_numbers:";
    for (const auto& part : _completed_parts) {
        ss << " " << part->GetPartNumber();
    }
    return ss.str();
}

} // namespace doris::io
