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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

// Records recycler delete requests without touching a blacklisted storage vault.
class SkipDeleteAccessor final : public StorageVaultAccessor {
public:
    SkipDeleteAccessor(std::string instance_id, std::string resource_id, std::string vault_name);

    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;
    int delete_directory(const std::string& dir_path) override;
    int delete_all(int64_t expiration_time = 0) override;
    int delete_files(const std::vector<std::string>& paths) override;
    int delete_file(const std::string& path) override;
    int abort_multipart_upload(const std::string& path, const std::string& upload_id) override;

    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;
    int list_all(std::unique_ptr<ListIterator>* res) override;
    int put_file(const std::string& path, const std::string& content) override;
    int exists(const std::string& path) override;

private:
    void record_skip(const char* operation, const std::string& path) const;

    std::string instance_id_;
    std::string resource_id_;
    std::string vault_name_;
};

} // namespace doris::cloud
