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

#pragma once

#include <aws/s3/model/GetObjectResult.h>

#include <memory>

#include "filesystem/read_stream.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris {

class S3ReadStream : public ReadStream {
public:
    S3ReadStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string key,
                 size_t offset, size_t read_until_position, size_t buffer_size,
                 int max_single_read_retries);
    ~S3ReadStream() override;

    Status read(char* to, size_t req_n, size_t* read_n) override;

    Status seek(int64_t position) override;

    Status tell(int64_t* position) override;

    Status close() override;

private:
    // Fill the buffer.
    Status fill();

    Status read_retry(char* to, size_t req_n, size_t* read_n);

private:
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _bucket;
    std::string _key;

    size_t _offset = 0;
    size_t _read_until_position = 0;

    char* _buffer;
    size_t _buffer_size;
    // Buffered begin offset relative to file.
    size_t _buffer_begin = 0;
    // Buffered end offset relative to file.
    size_t _buffer_end = 0;

    int _max_single_read_retries;
};

} // namespace doris
