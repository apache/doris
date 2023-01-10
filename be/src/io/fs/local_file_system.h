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

#include "io/fs/file_system.h"
#include "util/file_cache.h"

namespace doris {
namespace io {

class LocalFileSystem final : public FileSystem {
public:
    LocalFileSystem(Path root_path, ResourceId resource_id = ResourceId());
    ~LocalFileSystem() override;

    Status create_file(const Path& path, FileWriterPtr* writer) override;

    Status open_file(const Path& path, const FileReaderOptions& reader_options,
                     FileReaderSPtr* reader) override {
        return open_file(path, reader);
    }

    Status open_file(const Path& path, FileReaderSPtr* reader) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    Status delete_directory(const Path& path) override;

    Status link_file(const Path& src, const Path& dest) override;

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

private:
    Path absolute_path(const Path& path) const;
};

const FileSystemSPtr& global_local_filesystem();

} // namespace io
} // namespace doris
