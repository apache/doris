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

namespace doris {
namespace io {

class LocalFileSystem final : public FileSystem {
public:
    static std::shared_ptr<LocalFileSystem> create(Path path, std::string id = "");
    ~LocalFileSystem() override;

    /// hard link dest file to src file
    Status link_file(const Path& src, const Path& dest);

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer) override;
    Status open_file_impl(const Path& file, const FileReaderOptions& reader_options,
                          FileReaderSPtr* reader) override;
    Status create_directory_impl(const Path& dir) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, size_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;
    Status rename_dir_impl(const Path& orig_name, const Path& new_name) override;
    Status link_file_impl(const Path& src, const Path& dest);

private:
    LocalFileSystem(Path&& root_path, std::string&& id = "");
};

const std::shared_ptr<LocalFileSystem>& global_local_filesystem();

} // namespace io
} // namespace doris
