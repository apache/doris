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
#include <time.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"

namespace doris {
namespace io {

class LocalFileSystem final : public FileSystem {
public:
    static std::shared_ptr<LocalFileSystem> create(Path path, std::string id = "");
    ~LocalFileSystem() override;

    /// hard link dest file to src file
    Status link_file(const Path& src, const Path& dest);

    // Canonicalize 'path' by applying the following conversions:
    // - Converts a relative path into an absolute one using the cwd.
    // - Converts '.' and '..' references.
    // - Resolves all symbolic links.
    //
    // All directory entries in 'path' must exist on the filesystem.
    Status canonicalize(const Path& path, std::string* real_path);
    // Check if the given path is directory
    Status is_directory(const Path& path, bool* res);
    // Calc md5sum of given file
    Status md5sum(const Path& file, std::string* md5sum);
    // iterate the given dir and execute cb on each entry
    Status iterate_directory(const std::string& dir,
                             const std::function<bool(const FileInfo&)>& cb);
    // Return the mtime of given file
    Status mtime(const Path& file, time_t* m_time);
    // remove dir if eixsts and create a new one
    Status delete_and_create_directory(const Path& dir);
    // return disk available space where the given path is.
    Status get_space_info(const Path& path, size_t* capacity, size_t* available);
    // copy src dir to dest dir, recursivly
    Status copy_dirs(const Path& src, const Path& dest);
    // return true if parent path contain sub path
    static bool contain_path(const Path& parent, const Path& sub);
    // delete dir or file
    Status delete_directory_or_file(const Path& path);

    // read local file and save content to "content"
    Status read_file_to_string(const Path& file, std::string* content);

    Status canonicalize_local_file(const std::string& dir, const std::string& file_path,
                                   std::string* full_path);

    // glob list the files match the path pattern.
    // the result will be saved in "res", in absolute path with file size.
    // "safe" means the path will be concat with the path prefix config::user_files_secure_path,
    // so that it can not list any files outside the config::user_files_secure_path
    Status safe_glob(const std::string& path, std::vector<FileInfo>* res);
    Status directory_size(const Path& dir_path, size_t* dir_size);

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* opts) override;
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
    Status link_file_impl(const Path& src, const Path& dest);
    Status md5sum_impl(const Path& file, std::string* md5sum);
    Status iterate_directory_impl(const std::string& dir,
                                  const std::function<bool(const FileInfo&)>& cb);
    Status mtime_impl(const Path& file, time_t* m_time);
    Status delete_and_create_directory_impl(const Path& dir);
    Status get_space_info_impl(const Path& path, size_t* capacity, size_t* available);
    Status copy_dirs_impl(const Path& src, const Path& dest);
    Status delete_directory_or_file_impl(const Path& path);

private:
    // a wrapper for glob(), return file list in "res"
    Status _glob(const std::string& pattern, std::vector<std::string>* res);
    LocalFileSystem(Path&& root_path, std::string&& id = "");
};

const std::shared_ptr<LocalFileSystem>& global_local_filesystem();

} // namespace io
} // namespace doris
