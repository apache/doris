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

#include "filesystem/s3_read_stream.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "filesystem/s3_common.h"

namespace doris {

S3ReadStream::S3ReadStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                           std::string key, size_t offset, size_t read_until_position)
        : _client(std::move(client)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _offset(offset),
          _read_until_position(read_until_position) {}

S3ReadStream::~S3ReadStream() {
    close();
}

Status S3ReadStream::read_at(size_t position, char* to, size_t req_n, size_t* read_n) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _read_until_position) {
        return Status::IOError("Position exceeds range");
    }
    req_n = std::min(req_n, _read_until_position - position);
    if (req_n == 0) {
        *read_n = 0;
        return Status::OK();
    }
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);
    req.SetRange(fmt::format("bytes={}-{}", position, position + req_n - 1));
    req.SetResponseStreamFactory(AwsWriteableStreamFactory(to, req_n));

    auto outcome = _client->GetObject(req);
    if (outcome.IsSuccess()) {
        *read_n = outcome.GetResult().GetContentLength();
        return Status::OK();
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3ReadStream::read(char* to, size_t req_n, size_t* read_n) {
    RETURN_IF_ERROR(read_at(_offset, to, req_n, read_n));
    _offset += *read_n;
    return Status::OK();
}

Status S3ReadStream::seek(size_t position) {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    if (position > _read_until_position) {
        return Status::IOError("Position exceeds range");
    }
    _offset = position;
    return Status::OK();
}

Status S3ReadStream::tell(size_t* position) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *position = _offset;
    return Status::OK();
}

Status S3ReadStream::available(size_t* n_bytes) const {
    if (closed()) {
        return Status::IOError("Operation on closed stream");
    }
    *n_bytes = _read_until_position - _offset;
    return Status::OK();
}

Status S3ReadStream::close() {
    _client = nullptr;
    _closed = true;
    return Status::OK();
}

} // namespace doris
