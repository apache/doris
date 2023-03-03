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

#include <aws/core/Aws.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <fmt/core.h>
#include <sys/uio.h>

#include <cerrno>

#include "common/compiler_util.h"
#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "util/doris_metrics.h"

using Aws::S3::Model::AbortMultipartUploadRequest;
using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::DeleteObjectRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris {
namespace io {

// max size of each part when uploading: 5MB
static const int MAX_SIZE_EACH_PART = 5 * 1024 * 1024;
static const char* STREAM_TAG = "S3FileWriter";

S3FileWriter::S3FileWriter(Path path, std::shared_ptr<Aws::S3::S3Client> client,
                           const S3Conf& s3_conf)
        : FileWriter(std::move(path)), _client(client), _s3_conf(s3_conf) {
    DorisMetrics::instance()->s3_file_open_writing->increment(1);
    DorisMetrics::instance()->s3_file_writer_total->increment(1);
}

S3FileWriter::~S3FileWriter() {
    if (!_closed) {
        WARN_IF_ERROR(abort(), fmt::format("Cannot abort {}", _path.native()));
    }
}

Status S3FileWriter::close() {
    return _close();
}

Status S3FileWriter::abort() {
    AbortMultipartUploadRequest request;
    request.WithBucket(_s3_conf.bucket).WithKey(_path.native()).WithUploadId(_upload_id);
    auto outcome = _client->AbortMultipartUpload(request);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        LOG(INFO) << "Abort multipart upload successfully. endpoint=" << _s3_conf.endpoint
                  << ", bucket=" << _s3_conf.bucket << ", key=" << _path.native()
                  << ", upload_id=" << _upload_id;
        return Status::OK();
    }
    return Status::IOError(
            "failed to abort multipart upload(endpoint={}, bucket={}, key={}, upload_id={}): {}",
            _s3_conf.endpoint, _s3_conf.bucket, _path.native(), _upload_id,
            outcome.GetError().GetMessage());
}

Status S3FileWriter::_open() {
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(_s3_conf.bucket).WithKey(_path.native());
    create_request.SetContentType("text/plain");

    _reset_stream();
    auto outcome = _client->CreateMultipartUpload(create_request);

    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        LOG(INFO) << "create multi part upload successfully (endpoint=" << _s3_conf.endpoint
                  << ", bucket=" << _s3_conf.bucket << ", key=" << _path.native()
                  << ") upload_id: " << _upload_id;
        return Status::OK();
    }
    return Status::IOError(
            "failed to create multi part upload (endpoint={}, bucket={}, key={}): {}",
            _s3_conf.endpoint, _s3_conf.bucket, _path.native(), outcome.GetError().GetMessage());
}

Status S3FileWriter::append(const Slice& data) {
    Status st = appendv(&data, 1);
    if (st.ok()) {
        DorisMetrics::instance()->s3_bytes_written_total->increment(data.size);
    }
    return st;
}

Status S3FileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    if (!_is_open) {
        RETURN_IF_ERROR(_open());
        _is_open = true;
    }

    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        _stream_ptr->write(result.data, result.size);
        _bytes_appended += result.size;
        auto start_pos = _stream_ptr->tellg();
        _stream_ptr->seekg(0LL, _stream_ptr->end);
        _stream_ptr->seekg(start_pos);
    }
    if (_stream_ptr->str().size() >= MAX_SIZE_EACH_PART) {
        RETURN_IF_ERROR(_upload_part());
    }
    return Status::OK();
}

Status S3FileWriter::_upload_part() {
    if (_stream_ptr->str().size() == 0) {
        return Status::OK();
    }
    ++_cur_part_num;

    UploadPartRequest upload_request;
    upload_request.WithBucket(_s3_conf.bucket)
            .WithKey(_path.native())
            .WithPartNumber(_cur_part_num)
            .WithUploadId(_upload_id);

    upload_request.SetBody(_stream_ptr);

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*_stream_ptr));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    auto start_pos = _stream_ptr->tellg();
    _stream_ptr->seekg(0LL, _stream_ptr->end);
    upload_request.SetContentLength(static_cast<long>(_stream_ptr->tellg()));
    _stream_ptr->seekg(start_pos);

    auto upload_part_callable = _client->UploadPartCallable(upload_request);

    UploadPartOutcome upload_part_outcome = upload_part_callable.get();
    _reset_stream();
    if (!upload_part_outcome.IsSuccess()) {
        LOG(ERROR) << "failed to upload part (endpoint=" << _s3_conf.endpoint
                   << ", bucket=" << _s3_conf.bucket << ", key=" << _path.native()
                   << ", part_num=" << _cur_part_num
                   << ") Error msg: " << upload_part_outcome.GetError().GetMessage();
        return Status::IOError("failed to upload part.");
    }

    std::shared_ptr<CompletedPart> completed_part = std::make_shared<CompletedPart>();
    completed_part->SetPartNumber(_cur_part_num);
    auto etag = upload_part_outcome.GetResult().GetETag();
    if (etag.empty()) {
        LOG(ERROR) << "upload part success but etag is empty (endpoint=" << _s3_conf.endpoint
                   << ", bucket=" << _s3_conf.bucket << ", key=" << _path.native()
                   << ", part_num=" << _cur_part_num << ")";
        return Status::IOError("upload part success but etag is empty.");
    }
    completed_part->SetETag(etag);
    _completed_parts.emplace_back(completed_part);
    return Status::OK();
}

void S3FileWriter::_reset_stream() {
    _stream_ptr = Aws::MakeShared<Aws::StringStream>(STREAM_TAG, "");
}

Status S3FileWriter::finalize() {
    DCHECK(!_closed);
    if (_is_open) {
        _close();
    }
    return Status::OK();
}

Status S3FileWriter::_close() {
    if (_closed) {
        return Status::OK();
    }
    if (_is_open) {
        RETURN_IF_ERROR(_upload_part());

        CompleteMultipartUploadRequest complete_request;
        complete_request.WithBucket(_s3_conf.bucket)
                .WithKey(_path.native())
                .WithUploadId(_upload_id);

        CompletedMultipartUpload completed_upload;
        for (std::shared_ptr<CompletedPart> part : _completed_parts) {
            completed_upload.AddParts(*part);
        }

        complete_request.WithMultipartUpload(completed_upload);

        auto compute_outcome = _client->CompleteMultipartUpload(complete_request);

        if (!compute_outcome.IsSuccess()) {
            return Status::IOError(
                    "failed to create multi part upload (endpoint={}, bucket={}, key={}): {}",
                    _s3_conf.endpoint, _s3_conf.bucket, _path.native(),
                    compute_outcome.GetError().GetMessage());
        }
        _is_open = false;
    }
    _closed = true;

    DorisMetrics::instance()->s3_file_open_writing->increment(-1);
    DorisMetrics::instance()->s3_file_created_total->increment(1);
    DorisMetrics::instance()->s3_bytes_written_total->increment(_bytes_appended);

    LOG(INFO) << "complete multi part upload successfully (endpoint=" << _s3_conf.endpoint
              << ", bucket=" << _s3_conf.bucket << ", key=" << _path.native()
              << ") upload_id: " << _upload_id;
    return Status::OK();
}

Status S3FileWriter::write_at(size_t offset, const Slice& data) {
    return Status::NotSupported("not support");
}

} // namespace io
} // namespace doris
