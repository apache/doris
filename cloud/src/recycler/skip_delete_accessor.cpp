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

#include "recycler/skip_delete_accessor.h"

#include <utility>

#include "common/bvars.h"
#include "common/logging.h"

namespace doris::cloud {

SkipDeleteAccessor::SkipDeleteAccessor(std::string instance_id, std::string resource_id,
                                       std::string vault_name)
        : StorageVaultAccessor(AccessorType::S3),
          instance_id_(std::move(instance_id)),
          resource_id_(std::move(resource_id)),
          vault_name_(std::move(vault_name)) {
    uri_ = "skip-delete://" + resource_id_;
}

void SkipDeleteAccessor::record_skip(const char* operation, const std::string& path) const {
    LOG_INFO("skip deleting from recycler blacklisted storage vault")
            .tag("instance_id", instance_id_)
            .tag("resource_id", resource_id_)
            .tag("vault_name", vault_name_)
            .tag("operation", operation)
            .tag("path", path);
}

int SkipDeleteAccessor::delete_prefix(const std::string& path_prefix, int64_t) {
    record_skip("delete_prefix", path_prefix);
    return 0;
}

int SkipDeleteAccessor::delete_directory(const std::string& dir_path) {
    record_skip("delete_directory", dir_path);
    return 0;
}

int SkipDeleteAccessor::delete_all(int64_t) {
    record_skip("delete_all", "*");
    return 0;
}

int SkipDeleteAccessor::delete_files(const std::vector<std::string>& paths) {
    record_skip("delete_files", paths.empty() ? "" : paths.front());
    return 0;
}

int SkipDeleteAccessor::delete_file(const std::string& path) {
    record_skip("delete_file", path);
    return 0;
}

int SkipDeleteAccessor::abort_multipart_upload(const std::string& path, const std::string&) {
    record_skip("abort_multipart_upload", path);
    return 0;
}

int SkipDeleteAccessor::list_directory(const std::string&, std::unique_ptr<ListIterator>*) {
    return -1;
}

int SkipDeleteAccessor::list_all(std::unique_ptr<ListIterator>*) {
    return -1;
}

int SkipDeleteAccessor::put_file(const std::string&, const std::string&) {
    return -1;
}

int SkipDeleteAccessor::exists(const std::string&) {
    return -1;
}

} // namespace doris::cloud
