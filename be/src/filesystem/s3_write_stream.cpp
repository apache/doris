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

#include "filesystem/s3_write_stream.h"

#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "common/status.h"
#include "filesystem/s3_common.h"

namespace doris {

S3WriteStream::S3WriteStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                             std::string key, size_t part_upload_threshold,
                             size_t singlepart_threshold)
        : _client(std::move(client)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _part_upload_threshold(part_upload_threshold),
          _singlepart_threshold(singlepart_threshold) {}

S3WriteStream::~S3WriteStream() {
    close();
}

Status S3WriteStream::create_multipart_upload() {
    Aws::S3::Model::CreateMultipartUploadRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);

    // If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    auto outcome = _client->CreateMultipartUpload(req);
    if (outcome.IsSuccess()) {
        _upload_id = outcome.GetResult().GetUploadId();
        return Status::OK();
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3WriteStream::complete_multipart_upload() {
    Aws::S3::Model::CompleteMultipartUploadRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);
    req.SetUploadId(_upload_id);

    Aws::S3::Model::CompletedMultipartUpload completed_upload;
    for (size_t i = 0; i < _part_tags.size(); ++i) {
        Aws::S3::Model::CompletedPart part;
        completed_upload.AddParts(part.WithETag(_part_tags[i]).WithPartNumber(i + 1));
    }
    req.SetMultipartUpload(std::move(completed_upload));

    auto outcome = _client->CompleteMultipartUpload(req);
    if (outcome.IsSuccess()) {
        return Status::OK();
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3WriteStream::upload_part(const char* from, size_t put_n) {
    Aws::S3::Model::UploadPartRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);
    req.SetPartNumber(_part_tags.size() + 1);
    req.SetUploadId(_upload_id);
    req.SetContentLength(put_n);

    // If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    // TODO(cyx): async upload

    req.SetBody(std::make_shared<StringViewStream>(from, put_n));

    auto outcome = _client->UploadPart(req);
    if (outcome.IsSuccess()) {
        _part_tags.push_back(outcome.GetResult().GetETag());
        return Status::OK();
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3WriteStream::upload_object() {
    Aws::S3::Model::PutObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);
    req.SetContentLength(_buffer.size());
    req.SetBody(std::make_shared<StringViewStream>(_buffer.data(), _buffer.size()));

    // If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
    req.SetContentType("binary/octet-stream");

    auto outcome = _client->PutObject(req);
    if (outcome.IsSuccess()) {
        return Status::OK();
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3WriteStream::write(const char* from, size_t put_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    // If buffer is empty and `put_n` > threshold, do not copy data into buffer.
    if (_buffer.empty()) {
        if (_upload_id.empty() && put_n > _singlepart_threshold) {
            RETURN_IF_ERROR(create_multipart_upload());
        }
        if (!_upload_id.empty() && put_n > _part_upload_threshold) {
            // TODO(cyx): If data amount is too large, we should upload it in parallel.
            RETURN_IF_ERROR(upload_part(from, put_n));
        }
    }
    _buffer.append(from, put_n);
    if (_upload_id.empty() && _buffer.size() > _singlepart_threshold) {
        RETURN_IF_ERROR(create_multipart_upload());
    }
    if (!_upload_id.empty() && _buffer.size() > _part_upload_threshold) {
        // TODO(cyx): If data amount is too large, we should upload it in parallel.
        RETURN_IF_ERROR(upload_part(_buffer.data(), _buffer.size()));
        _buffer.clear();
    }
    return Status::OK();
}

Status S3WriteStream::sync() {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (!_buffer.empty()) {
        if (_upload_id.empty()) {
            RETURN_IF_ERROR(create_multipart_upload());
        }
        RETURN_IF_ERROR(upload_part(_buffer.data(), _buffer.size()));
    }
    return Status::OK();
}

Status S3WriteStream::close() {
    if (closed()) {
        return Status::OK();
    }
    if (!_buffer.empty()) {
        if (_upload_id.empty()) {
            RETURN_IF_ERROR(upload_object());
        } else {
            RETURN_IF_ERROR(upload_part(_buffer.data(), _buffer.size()));
            RETURN_IF_ERROR(complete_multipart_upload());
        }
    }
    _buffer.clear();
    _client = nullptr;
    _closed = true;
    return Status::OK();
}

} // namespace doris
