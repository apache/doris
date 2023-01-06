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

#include "io/fs/remote_file_system.h"
#include "runtime/client_cache.h"
namespace doris {

namespace io {
class BrokerFileSystem final : public RemoteFileSystem {
public:
    BrokerFileSystem(const TNetworkAddress& broker_addr,
                     const std::map<std::string, std::string>& broker_prop, size_t file_size);

    ~BrokerFileSystem() override = default;

    Status create_file(const Path& /*path*/, FileWriterPtr* /*writer*/) override {
        return Status::NotSupported("Currently not support to create file through broker.");
    }

    Status open_file(const Path& path, FileReaderSPtr* reader) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    // Delete all files under path.
    Status delete_directory(const Path& path) override;

    Status link_file(const Path& /*src*/, const Path& /*dest*/) override {
        return Status::NotSupported("Not supported link file through broker.");
    }

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& /*local_path*/, const Path& /*dest_path*/) override {
        return Status::NotSupported("Currently not support to upload file to HDFS");
    }

    Status batch_upload(const std::vector<Path>& /*local_paths*/,
                        const std::vector<Path>& /*dest_paths*/) override {
        return Status::NotSupported("Currently not support to batch upload file to HDFS");
    }

    Status connect() override;

    Status get_client(std::shared_ptr<BrokerServiceConnection>* client) const;

private:
    const TNetworkAddress& _broker_addr;
    const std::map<std::string, std::string>& _broker_prop;
    size_t _file_size;

    std::shared_ptr<BrokerServiceConnection> _client;
};
} // namespace io
} // namespace doris
