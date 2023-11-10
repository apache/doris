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

#include <stdint.h>

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "runtime/client_cache.h"

namespace doris {
class TNetworkAddress;

namespace io {
struct FileInfo;

class BrokerFileSystem final : public RemoteFileSystem {
public:
    static Status create(const TNetworkAddress& broker_addr,
                         const std::map<std::string, std::string>& broker_prop,
                         std::shared_ptr<BrokerFileSystem>* fs);

    ~BrokerFileSystem() override = default;

    Status read_file(const TBrokerFD& fd, size_t offset, size_t bytes_req, std::string* data) const;

    Status close_file(const TBrokerFD& fd) const;

protected:
    Status connect_impl() override;
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                              const FileReaderOptions& opts) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;
    Status rename_dir_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status direct_upload_impl(const Path& remote_file, const std::string& content) override;
    Status upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                     const std::string& checksum) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;
    Status direct_download_impl(const Path& remote_file, std::string* content) override;

private:
    BrokerFileSystem(const TNetworkAddress& broker_addr,
                     const std::map<std::string, std::string>& broker_prop);
    std::string error_msg(const std::string& err) const;

    const TNetworkAddress& _broker_addr;
    const std::map<std::string, std::string>& _broker_prop;

    std::unique_ptr<BrokerServiceConnection> _connection;
};
} // namespace io
} // namespace doris
