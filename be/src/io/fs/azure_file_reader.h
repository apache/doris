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

#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/s3_file_system.h"
namespace doris::io {
struct IOContext;

class AzureFileReader final : public FileReader {
public:
    static Result<FileReaderSPtr> create(std::shared_ptr<const S3ClientHolder> client,
                                         std::string bucket, std::string key, int64_t file_size);

    AzureFileReader(std::shared_ptr<const S3ClientHolder> client, std::string bucket,
                    std::string key, size_t file_size);

    ~AzureFileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    size_t _file_size;
    bool _closed;
    Path _path;
};
} // namespace doris::io