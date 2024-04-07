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

#include "recycler/obj_store_accessor.h"

namespace doris::cloud {

class HdfsVaultInfo;

class HdfsAccessor final : public ObjStoreAccessor {
public:
    explicit HdfsAccessor(const HdfsVaultInfo& info);

    ~HdfsAccessor() override;

    const std::string& path() const override { return uri_; }

    // returns 0 for success otherwise error
    int init() override;

    // returns 0 for success, negative for other errors
    int delete_objects_by_prefix(const std::string& relative_path) override;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override;

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override;

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override;

    // returns 0 for success otherwise error
    // Notice: list directory in hdfs has no recursive semantics
    int list(const std::string& relative_path, std::vector<ObjectMeta>* ObjectMeta) override;

    // return 0 if object exists, 1 if object is not found, negative for error
    int exist(const std::string& relative_path) override;

private:
    std::string fs_path(const std::string& relative_path);

    std::string uri(const std::string& relative_path);

    const HdfsVaultInfo& info_; // Only use when init

    hdfsFS fs_ = nullptr;

    std::string prefix_; // Either be empty or start with '/' if length > 1
    std::string uri_;
};

} // namespace doris::cloud
