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
#include <fmt/core.h>
#include <glog/logging.h>

#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_write_bufferpool.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"

namespace Aws {
namespace S3 {
namespace Model {
class DeleteObjectRequest;
} // namespace Model
} // namespace S3
} // namespace Aws

using Aws::S3::Model::AbortMultipartUploadRequest;
using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris {
namespace io {
using namespace Aws::S3::Model;
using Aws::S3::S3Client;

S3FileWriter::S3FileWriter(Path path, std::shared_ptr<S3Client> client, const S3Conf& s3_conf,
                           FileSystemSPtr fs)
        : FileWriter(Path(s3_conf.endpoint) / s3_conf.bucket / path, std::move(fs)),
          _bucket(s3_conf.bucket),
          _key(std::move(path)),
          _upload_cost_ms(std::make_unique<int64_t>()),
          _client(std::move(client)) {}

S3FileWriter::~S3FileWriter() {
    if (_opened) {
        close();
    }
    CHECK(!_opened || _closed) << "open: " << _opened << ", closed: " << _closed;
    // in case there are task which might run after this object is destroyed
    _wait_until_finish("dtor");
}

Status S3FileWriter::_create_multi_upload_request() {
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(_bucket).WithKey(_key);
    create_request.SetContentType("application/octet-stream");

    auto outcome = _client->CreateMultipartUpload(create_request);

    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return Status::IOError("failed to create multipart upload(bucket={}, key={}, upload_id={}): {}",
                           _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage());
}

void S3FileWriter::_wait_until_finish(std::string task_name) {
    auto msg =
            fmt::format("{} multipart upload already takes 5 min, bucket={}, key={}, upload_id={}",
                        std::move(task_name), _bucket, _path.native(), _upload_id);
    while (!_wait.wait()) {
        LOG(WARNING) << msg;
    }
}

Status S3FileWriter::abort() {
    _failed = true;
    if (_closed || !_opened) {
        _wait_until_finish("Abort");
        return Status::OK();
    }
    // we need to reclaim the memory
    if (_pending_buf) {
        _pending_buf->on_finished();
        _pending_buf = nullptr;
    }
    // upload id is empty means there was no create multi upload
    if (_upload_id.empty()) {
        return Status::OK();
    }
    VLOG_DEBUG << "S3FileWriter::abort, path: " << _path.native();
    _wait_until_finish("Abort");
    _closed = true;
    AbortMultipartUploadRequest request;
    request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);
    auto outcome = _client->AbortMultipartUpload(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        LOG(INFO) << "Abort multipart upload successfully"
                  << "bucket=" << _bucket << ", key=" << _path.native()
                  << ", upload_id=" << _upload_id;
        return Status::OK();
    }
    return Status::IOError("failed to abort multipart upload(bucket={}, key={}, upload_id={}): {}",
                           _bucket, _path.native(), _upload_id, outcome.GetError().GetMessage());
}

Status S3FileWriter::close() {
    if (_closed || _failed) {
        _wait_until_finish("close");
        return _st;
    }
    VLOG_DEBUG << "S3FileWriter::close, path: " << _path.native();
    if (_pending_buf != nullptr) {
        if (_upload_id.empty()) {
            _pending_buf->set_upload_remote_callback(
                    [this, buf = _pending_buf]() { _put_object(*buf); });
        }
        _wait.add();
        _pending_buf->submit();
        _pending_buf = nullptr;
    }
    RETURN_IF_ERROR(_complete());
    _closed = true;

    return Status::OK();
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    // lazy open
    if (!_opened) {
        VLOG_DEBUG << "S3FileWriter::open, path: " << _path.native();
        _closed = false;
        _opened = true;
    }
    DCHECK(!_closed);
    size_t buffer_size = config::s3_write_buffer_size;
    SCOPED_RAW_TIMER(_upload_cost_ms.get());
    for (size_t i = 0; i < data_cnt; i++) {
        size_t data_size = data[i].get_size();
        for (size_t pos = 0, data_size_to_append = 0; pos < data_size; pos += data_size_to_append) {
            if (_failed) {
                return _st;
            }
            if (!_pending_buf) {
                _pending_buf = S3FileBufferPool::GetInstance()->allocate();
                // capture part num by value along with the value of the shared ptr
                _pending_buf->set_upload_remote_callback(
                        [part_num = _cur_part_num, this, cur_buf = _pending_buf]() {
                            _upload_one_part(part_num, *cur_buf);
                        });
                _pending_buf->set_file_offset(_bytes_appended);
                // later we might need to wait all prior tasks to be finished
                _pending_buf->set_finish_upload([this]() { _wait.done(); });
                _pending_buf->set_is_cancel([this]() { return _failed.load(); });
                _pending_buf->set_on_failed([this, part_num = _cur_part_num](Status st) {
                    VLOG_NOTICE << "failed at key: " << _key << ", load part " << part_num
                                << ", st " << st.to_string();
                    std::unique_lock<std::mutex> _lck {_completed_lock};
                    this->_st = std::move(st);
                    _failed = true;
                });
            }
            // we need to make sure all parts except the last one to be 5MB or more
            // and shouldn't be larger than buf
            data_size_to_append = std::min(data_size - pos, _pending_buf->get_file_offset() +
                                                                    buffer_size - _bytes_appended);

            // if the buffer has memory buf inside, the data would be written into memory first then S3 then file cache
            // it would be written to cache then S3 if the buffer doesn't have memory preserved
            _pending_buf->append_data(Slice {data[i].get_data() + pos, data_size_to_append});

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
                _wait.add();
                _pending_buf->submit();
                _pending_buf = nullptr;
            }
            _bytes_appended += data_size_to_append;
        }
    }
    return Status::OK();
}

void S3FileWriter::_upload_one_part(int64_t part_num, S3FileBuffer& buf) {
    if (buf._is_cancelled()) {
        return;
    }
    UploadPartRequest upload_request;
    upload_request.WithBucket(_bucket).WithKey(_key).WithPartNumber(part_num).WithUploadId(
            _upload_id);

    auto _stream_ptr = buf.get_stream();

    upload_request.SetBody(buf.get_stream());

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*_stream_ptr));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    upload_request.SetContentLength(buf.get_size());
    upload_request.SetContentType("application/octet-stream");

    auto upload_part_callable = _client->UploadPartCallable(upload_request);

    UploadPartOutcome upload_part_outcome = upload_part_callable.get();
    if (!upload_part_outcome.IsSuccess()) {
        auto s = Status::IOError(
                "failed to upload part (bucket={}, key={}, part_num={}, up_load_id={}): {}",
                _bucket, _path.native(), part_num, _upload_id,
                upload_part_outcome.GetError().GetMessage());
        LOG_WARNING(s.to_string());
        buf._on_failed(s);
        return;
    }

    std::unique_ptr<CompletedPart> completed_part = std::make_unique<CompletedPart>();
    completed_part->SetPartNumber(part_num);
    auto etag = upload_part_outcome.GetResult().GetETag();
    // DCHECK(etag.empty());
    completed_part->SetETag(etag);

    std::unique_lock<std::mutex> lck {_completed_lock};
    _completed_parts.emplace_back(std::move(completed_part));
    _bytes_written += buf.get_size();
}

Status S3FileWriter::_complete() {
    SCOPED_RAW_TIMER(_upload_cost_ms.get());
    if (_failed) {
        _wait_until_finish("early quit");
        return _st;
    }
    // upload id is empty means there was no multipart upload
    if (_upload_id.empty()) {
        _wait_until_finish("PutObject");
        return _st;
    }
    CompleteMultipartUploadRequest complete_request;
    complete_request.WithBucket(_bucket).WithKey(_key).WithUploadId(_upload_id);

    _wait_until_finish("Complete");
    // make sure _completed_parts are ascending order
    std::sort(_completed_parts.begin(), _completed_parts.end(),
              [](auto& p1, auto& p2) { return p1->GetPartNumber() < p2->GetPartNumber(); });
    CompletedMultipartUpload completed_upload;
    for (auto& part : _completed_parts) {
        completed_upload.AddParts(*part);
    }

    complete_request.WithMultipartUpload(completed_upload);

    auto compute_outcome = _client->CompleteMultipartUpload(complete_request);

    if (!compute_outcome.IsSuccess()) {
        auto s = Status::IOError(
                "failed to create complete multi part upload (bucket={}, key={}): {}", _bucket,
                _path.native(), compute_outcome.GetError().GetMessage());
        LOG_WARNING(s.to_string());
        return s;
    }
    return Status::OK();
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    // submit pending buf if it's not nullptr
    // it's the last buf, we can submit it right now
    if (_pending_buf != nullptr) {
        // if we only need to upload one file less than 5MB, we can just
        // call PutObject to reduce the network IO
        if (_upload_id.empty()) {
            _pending_buf->set_upload_remote_callback(
                    [this, buf = _pending_buf]() { _put_object(*buf); });
        }
        _wait.add();
        _pending_buf->submit();
        _pending_buf = nullptr;
    }
    _wait_until_finish("finalize");
    return _st;
}

void S3FileWriter::_put_object(S3FileBuffer& buf) {
    DCHECK(!_closed && _opened) << "closed " << _closed << " opened " << _opened;
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(_bucket).WithKey(_key);
    request.SetBody(buf.get_stream());
    request.SetContentLength(buf.get_size());
    request.SetContentType("application/octet-stream");
    auto response = _client->PutObject(request);
    if (!response.IsSuccess()) {
        _st = Status::InternalError("Error: [{}:{}, responseCode:{}]",
                                    response.GetError().GetExceptionName(),
                                    response.GetError().GetMessage(),
                                    static_cast<int>(response.GetError().GetResponseCode()));
    }
}

} // namespace io
} // namespace doris
