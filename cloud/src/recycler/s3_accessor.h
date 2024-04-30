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

#include <memory>

#include "recycler/obj_store_accessor.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris::cloud {

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
};

class S3Accessor : public ObjStoreAccessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor() override;

    const std::string& path() const override { return path_; }

    const std::shared_ptr<Aws::S3::S3Client>& s3_client() const { return s3_client_; }

    const S3Conf& conf() const { return conf_; }

    // returns 0 for success otherwise error
    int init() override;

    // returns 0 for success, returns 1 for http FORBIDDEN error, negative for other errors
    int delete_objects_by_prefix(const std::string& relative_path) override;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override;

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override;

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override;

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<ObjectMeta>* ObjectMeta) override;

    // return 0 if object exists, 1 if object is not found, negative for error
    int exist(const std::string& relative_path) override;

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    virtual int delete_expired_objects(const std::string& relative_path, int64_t expired_time);

    // returns 0 for success otherwise error
    virtual int get_bucket_lifecycle(int64_t* expiration_days);

    // returns 0 for enabling bucket versioning, otherwise error
    virtual int check_bucket_versioning();

private:
    std::string get_key(const std::string& relative_path) const;
    // return empty string if the input key does not start with the prefix of S3 conf
    std::string get_relative_path(const std::string& key) const;

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    S3Conf conf_;
    std::string path_;
};

class GcsAccessor final : public S3Accessor {
public:
    explicit GcsAccessor(S3Conf conf) : S3Accessor(std::move(conf)) {}
    ~GcsAccessor() override = default;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override;
};

} // namespace doris::cloud
