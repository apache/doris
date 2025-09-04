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

#include <butil/macros.h>
#include <gen_cpp/olap_file.pb.h>

#include "enterprise/encryption_common.h"
#include "io/fs/file_system.h"

namespace doris::io {

class EncryptedFileSystem final : public FileSystem {
public:
    EncryptedFileSystem(FileSystemSPtr inner, EncryptionAlgorithmPB algorithm)
            : FileSystem(FileSystem::TMP_FS_ID, FileSystemType::LOCAL),
              _fs_inner(std::move(inner)),
              _algorithm(algorithm) {}

protected:
    static std::shared_ptr<EncryptedFileSystem> create(std::shared_ptr<FileSystem> inner);

    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts = nullptr) override;

    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts = nullptr) override;

    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;

    Status delete_file_impl(const Path& file) override;

    Status delete_directory_impl(const Path& dir) override;

    Status batch_delete_impl(const std::vector<Path>& files) override;

    Status exists_impl(const Path& path, bool* res) const override;

    Status file_size_impl(const Path& file, int64_t* file_size) const override;

    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;

    Status rename_impl(const Path& orig_name, const Path& new_name) override;

    Status absolute_path(const Path& path, Path& abs_path) const override;

private:
    std::shared_ptr<FileSystem> _fs_inner;
    EncryptionAlgorithmPB _algorithm;
};

} // namespace doris::io
