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

#include "common/status.h"

namespace doris {

struct FileStat {
    std::string name;
    std::string md5;
    int64_t size;
};

class StorageBackend {
public:
    virtual Status download(const std::string& remote, const std::string& local) = 0;
    virtual Status direct_download(const std::string& remote, std::string* content) = 0;
    virtual Status upload(const std::string& local, const std::string& remote) = 0;
    virtual Status upload_with_checksum(const std::string& local, const std::string& remote,
                                        const std::string& checksum) = 0;
    virtual Status list(const std::string& remote_path, bool contain_md5,
                        bool recursion, std::map<std::string, FileStat>* files) = 0;
    virtual Status rename(const std::string& orig_name, const std::string& new_name) = 0;
    virtual Status rename_dir(const std::string& orig_name, const std::string& new_name) = 0;
    virtual Status direct_upload(const std::string& remote, const std::string& content) = 0;
    virtual Status copy(const std::string& src, const std::string& dst) = 0;
    virtual Status copy_dir(const std::string& src, const std::string& dst) = 0;
    virtual Status rm(const std::string& remote) = 0;
    virtual Status rmdir(const std::string& remote) = 0;
    virtual Status mkdir(const std::string& path) = 0;
    virtual Status mkdirs(const std::string& path) = 0;
    virtual Status exist(const std::string& path) = 0;
    virtual Status exist_dir(const std::string& path) = 0;

    virtual ~StorageBackend() = default;
};
} // end namespace doris
