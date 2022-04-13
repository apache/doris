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

#include "filesystem/s3_file_system.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <filesystem>

#include "util/s3_util.h"

namespace fs = std::filesystem;

namespace doris {

S3FileSystem::S3FileSystem(std::string root_path, std::string bucket,
                           const std::map<std::string, std::string>& properties)
        : _root_path(std::move(root_path)),
          _bucket(std::move(bucket)),
          _client(ClientFactory::instance().create(properties)) {}

S3FileSystem::~S3FileSystem() = default;

Status S3FileSystem::exists(const std::string& path, bool* res) const {
    auto key = fs::path(_root_path) / path;
    return object_exists(key, res);
}

Status S3FileSystem::is_file(const std::string& path, bool* res) const {
    auto key = fs::path(_root_path) / path;
    return object_exists(key, res);
}

Status S3FileSystem::is_directory(const std::string& path, bool* res) const {
    return Status::NotSupported("S3FileSystem::is_directory");
}

Status S3FileSystem::list(const std::string& path, std::vector<FileStat>* files) {
    return Status::NotSupported("S3FileSystem::list");
}

Status S3FileSystem::delete_directory(const std::string& path) {
    return Status::OK();
}

Status S3FileSystem::delete_file(const std::string& path) {
    auto key = fs::path(_root_path) / path;
    return delete_object(key);
}

Status S3FileSystem::create_directory(const std::string& path) {
    return Status::OK();
}

Status S3FileSystem::object_exists(const std::string& key, bool* res) const {
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(key);

    auto outcome = _client->HeadObject(req);
    if (outcome.IsSuccess()) {
        *res = true;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        *res = false;
    } else {
        return Status::IOError(outcome.GetError().GetMessage());
    }
    return Status::OK();
}

Status S3FileSystem::delete_object(const std::string& key) {
    Aws::S3::Model::DeleteObjectRequest req;
    req.SetBucket(_bucket);
    req.SetKey(key);

    auto outcome = _client->DeleteObject(req);
    if (!outcome.IsSuccess()) {
        return Status::IOError(outcome.GetError().GetMessage());
    }
    return Status::OK();
}

} // namespace doris
