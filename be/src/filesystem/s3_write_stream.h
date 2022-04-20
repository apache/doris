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

#include "filesystem/write_stream.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris {

// If total written bytes < `singlepart_threshold`, write is performed using `upload_object`.
// In another case multipart upload is used:
// Data is divided on chunks with size > `part_upload_threshold`. Last chunk can be less than this threshold.
// Each chunk is written as a part to S3.
class S3WriteStream final : public WriteStream {
public:
    S3WriteStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string key,
                  size_t part_upload_threshold, size_t singlepart_threshold);
    ~S3WriteStream() override;

    Status write(const char* from, size_t put_n) override;

    Status sync() override;

    Status close() override;

    bool closed() const override { return _closed; }

private:
    Status create_multipart_upload();

    Status complete_multipart_upload();

    Status upload_part(const char* from, size_t put_n);

    Status upload_object();

private:
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _bucket;
    std::string _key;

    std::string _upload_id;
    std::vector<std::string> _part_tags;

    size_t _part_upload_threshold;
    // `_singlepart_threshold` >= `_part_upload_threshold`
    size_t _singlepart_threshold;

    std::string _buffer;
    bool _closed = false;
};

} // namespace doris
