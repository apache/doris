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

#ifdef USE_HADOOP_HDFS
#include <hadoop_hdfs/hdfs.h> // IWYU pragma: export
#else
#include <hdfs/hdfs.h> // IWYU pragma: export
#endif

#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

class HdfsVaultInfo;

using HdfsSPtr = std::shared_ptr<struct hdfs_internal>;

class HdfsAccessor final : public StorageVaultAccessor {
public:
    explicit HdfsAccessor(const HdfsVaultInfo& info);

    ~HdfsAccessor() override;

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

private:
    int delete_directory_impl(const std::string& dir_path);

    // Convert relative path to hdfs path
    std::string to_fs_path(const std::string& path);

    // Convert relative path to uri
    std::string to_uri(const std::string& path);

    const HdfsVaultInfo& info_; // Only use when init

    HdfsSPtr fs_;

    std::string prefix_; // Either be empty or start with '/' if length > 1
    // format: fs_name/prefix/
    // e.g.: hdfs://localhost:8020/test_prefix/
    std::string uri_;
};

} // namespace doris::cloud
