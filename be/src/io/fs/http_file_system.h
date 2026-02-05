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

#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "http/http_client.h"
#include "http_file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"

namespace doris::io {
class HttpFileSystem final : public RemoteFileSystem {
public:
    static Result<std::shared_ptr<HttpFileSystem>> create(
            std::string id, const std::string& uri,
            const std::map<std::string, std::string>& properties = {});
    ~HttpFileSystem() override = default;

protected:
    Status file_size_impl(const Path& file, int64_t* file_size) const override;

    Status exists_impl(const Path& path, bool* res) const override;

    Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                              const FileReaderOptions& opts) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override {
        return Status::NotSupported("not supported");
    }

    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override {
        return Status::NotSupported("not supported");
    }

    Status upload_impl(const Path& local_file, const Path& remote_file) override {
        return Status::NotSupported("not supported");
    }

    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override {
        return Status::NotSupported("not suported");
    }

    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override {
        return Status::NotSupported("not supported");
    }

    Status delete_file_impl(const Path& file) override {
        return Status::NotSupported("not supported");
    }

    Status batch_delete_impl(const std::vector<Path>& files) override {
        return Status::NotSupported("not supported");
    }

    Status delete_directory_impl(const Path& dir) override {
        return Status::NotSupported("not supported");
    }

    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override {
        return Status::NotSupported("not supported");
    }

    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override {
        return Status::NotSupported("not supported");
    }

    Status rename_impl(const Path& orig_name, const Path& new_name) override {
        return Status::NotSupported("not supported");
    }

private:
    HttpFileSystem(Path&& root_path, std::string id, std::map<std::string, std::string> properties);
    Status _init(const std::string& url);

    std::string _url;
    std::map<std::string, std::string> _properties;
};
} // namespace doris::io
