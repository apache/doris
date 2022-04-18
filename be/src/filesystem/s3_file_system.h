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

#include <aws/core/client/RetryStrategy.h>

#include <memory>

#include "filesystem/file_system.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris {

class S3FileSystem final : public FileSystem {
public:
    S3FileSystem(std::string root_path, std::string bucket,
                 const std::map<std::string, std::string>& properties,
                 std::shared_ptr<Aws::Client::RetryStrategy> retry_strategy = nullptr);
    ~S3FileSystem() override;

    Status exists(const std::string& path, bool* res) const override;

    Status is_file(const std::string& path, bool* res) const override;

    Status is_directory(const std::string& path, bool* res) const override;

    Status list(const std::string& path, std::vector<FileStat>* files) override;

    Status delete_directory(const std::string& path) override;

    Status delete_file(const std::string& path) override;

    Status create_directory(const std::string& path) override;

    Status read_file(const std::string& path, IOContext io_context,
                     std::unique_ptr<ReadStream>* stream) const override;

    Status write_file(const std::string& path, IOContext io_context,
                      std::unique_ptr<WriteStream>* stream) override;

private:
    Status object_exists(const std::string& key, bool* res) const;

    // If the object does not exist, return ok
    Status delete_object(const std::string& key);

private:
    std::string _root_path;
    std::string _bucket;

    std::shared_ptr<Aws::S3::S3Client> _client;
};

} // namespace doris
