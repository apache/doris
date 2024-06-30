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

#include <cstdint>
#include <memory>

#include "recycler/s3_obj_client.h"
#include "recycler/storage_vault_accessor.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris::cloud {
class ObjectStoreInfoPB;

enum class S3RateLimitType;
extern int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst,
                                 size_t limit);

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;

    enum Provider : uint8_t {
        S3,
        GCS,
        AZURE,
    };

    Provider provider;

    static std::optional<S3Conf> from_obj_store_info(const ObjectStoreInfoPB& obj_info);
};

class S3Accessor : public StorageVaultAccessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor() override;

    // returns 0 for success otherwise error
    static int create(S3Conf conf, std::shared_ptr<S3Accessor>* accessor);

    // returns 0 for success otherwise error
    int init();

    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;

    int delete_directory(const std::string& dir_path) override;

    int delete_all(int64_t expiration_time = 0) override;

    int delete_files(const std::vector<std::string>& paths) override;

    int delete_file(const std::string& path) override;

    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;

    int list_all(std::unique_ptr<ListIterator>* res) override;

    int put_file(const std::string& path, const std::string& content) override;

    int exists(const std::string& path) override;

    // Get the objects' expiration time on the conf.bucket
    // returns 0 for success otherwise error
    int get_life_cycle(int64_t* expiration_days);

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    int check_versioning();

protected:
    int list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res);

    virtual int delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time = 0);

    std::string get_key(const std::string& relative_path) const;
    std::string to_uri(const std::string& relative_path) const;

    S3Conf conf_;
    std::shared_ptr<ObjStorageClient> obj_client_;
};

class GcsAccessor final : public S3Accessor {
public:
    explicit GcsAccessor(S3Conf conf) : S3Accessor(std::move(conf)) {}
    ~GcsAccessor() override = default;

    int delete_files(const std::vector<std::string>& paths) override;

private:
    int delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time = 0) override;
};

} // namespace doris::cloud
