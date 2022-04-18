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

#include "util/storage_backend.h"

namespace doris {

class ExecEnv;
class TNetworkAddress;

class BrokerStorageBackend : public StorageBackend {
public:
    BrokerStorageBackend(ExecEnv* env, const TNetworkAddress& broker_addr,
                         const std::map<std::string, std::string>& broker_prop);
    ~BrokerStorageBackend() {}
    Status download(const std::string& remote, const std::string& local) override;
    Status direct_download(const std::string& remote, std::string* content) override;
    Status upload(const std::string& local, const std::string& remote) override;
    Status upload_with_checksum(const std::string& local, const std::string& remote,
                                const std::string& checksum) override;
    Status rename(const std::string& orig_name, const std::string& new_name) override;
    Status rename_dir(const std::string& orig_name, const std::string& new_name) override;
    Status list(const std::string& remote_path, bool contain_md5, bool recursion,
                std::map<std::string, FileStat>* files) override;
    Status direct_upload(const std::string& remote, const std::string& content) override;
    Status rm(const std::string& remote) override;
    Status rmdir(const std::string& remote) override;
    Status copy(const std::string& src, const std::string& dst) override;
    Status copy_dir(const std::string& src, const std::string& dst) override;
    Status mkdir(const std::string& path) override;
    Status mkdirs(const std::string& path) override;
    Status exist(const std::string& path) override;
    Status exist_dir(const std::string& path) override;

private:
    ExecEnv* _env;
    const TNetworkAddress& _broker_addr;
    const std::map<std::string, std::string>& _broker_prop;
};
} // end namespace doris
