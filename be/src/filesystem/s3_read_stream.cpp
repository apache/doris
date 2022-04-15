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

namespace doris {

S3ReadStream::S3ReadStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                           std::string key, size_t offset, size_t read_until_position,
                           size_t buffer_size, int max_single_read_retries)
        : _client(std::move(client)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _offset(offset),
          _read_until_position(read_until_position),
          _buffer_size(buffer_size),
          _max_single_read_retries(max_single_read_retries) {
    _buffer = buffer_size ? new char[buffer_size] : nullptr;
}

S3ReadStream::~S3ReadStream() {
    close();
    delete[] _buffer;
}

Status S3ReadStream::read_retry(char* to, size_t req_n, size_t* read_n) {
    Aws::S3::Model::GetObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(_key);
    req.SetRange(fmt::format("bytes={}-{}", _offset, _offset + req_n - 1));

    Aws::S3::Model::GetObjectOutcome outcome;
    for (int attempt = 0; attempt < _max_single_read_retries; ++attempt) {
        outcome = _client->GetObject(req);
        if (outcome.IsSuccess()) {
            outcome.GetResult().GetBody().read(to, req_n);
            *read_n = outcome.GetResult().GetContentLength();
            return Status::OK();
        }
        // TODO(cyx): sleepForMilliseconds backoff
    }
    return Status::IOError(outcome.GetError().GetMessage());
}

Status S3ReadStream::read(char* to, size_t req_n, size_t* read_n) {
    if (_offset == _read_until_position) {
        *read_n = 0;
        return Status::OK();
    }
    // Beyond buffer range
    if (_offset >= _buffer_end || _offset < _buffer_begin) {
        // Request length is larger than the capacity of buffer,
        // do not copy data into buffer.
        if (req_n > _buffer_size) {
            RETURN_IF_ERROR(read_retry(to, req_n, read_n));
            _offset += *read_n;
            return Status::OK();
        }
        RETURN_IF_ERROR(fill());
    }

    size_t copied = std::min(_buffer_end - _offset, req_n);
    memcpy(to, _buffer + _offset - _buffer_begin, copied);

    _offset += copied;
    *read_n = copied;

    req_n -= copied;
    if (req_n > 0) {
        size_t read_n1 = 0;
        RETURN_IF_ERROR(read(to + copied, req_n, &read_n1));
        *read_n += read_n1;
    }
    return Status::OK();
}

Status S3ReadStream::seek(int64_t position) {
    if (position > _read_until_position) {
        return Status::IOError("Position exceeds range");
    }
    _offset = position;
    return Status::OK();
}

Status S3ReadStream::tell(int64_t* position) {
    *position = _offset;
    return Status::OK();
}

Status S3ReadStream::fill() {
    size_t read_n = 0;
    RETURN_IF_ERROR(read_retry(_buffer, _buffer_size, &read_n));
    _buffer_begin = _offset;
    _buffer_end = _offset + read_n;
    return Status::OK();
}

Status S3ReadStream::close() {
    delete[] _buffer;
    _buffer = nullptr;
    return Status::OK();
}

} // namespace doris
