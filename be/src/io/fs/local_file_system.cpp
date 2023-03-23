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

#include "io/fs/local_file_system.h"

#include "io/fs/file_system.h"
#include "io/fs/fs_utils.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "util/async_io.h"

namespace doris {
namespace io {

std::shared_ptr<LocalFileSystem> LocalFileSystem::create(Path path, std::string id) {
    return std::shared_ptr<LocalFileSystem>(new LocalFileSystem(std::move(path), std::move(id)));
}

LocalFileSystem::LocalFileSystem(Path&& root_path, std::string&& id)
        : FileSystem(std::move(root_path), std::move(id), FileSystemType::LOCAL) {}

LocalFileSystem::~LocalFileSystem() = default;

Status LocalFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer) {
    int fd = ::open(file.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    if (-1 == fd) {
        return Status::IOError("failed to open {}: {}", file.native(), errno_to_str());
    }
    *writer = std::make_unique<LocalFileWriter>(
            std::move(file), fd, std::static_pointer_cast<LocalFileSystem>(shared_from_this()));
    return Status::OK();
}

Status LocalFileSystem::open_file_impl(const Path& file,
                                       const FileReaderOptions& /*reader_options*/,
                                       FileReaderSPtr* reader) {
    size_t fsize = 0;
    RETURN_IF_ERROR(file_size_impl(file, &fsize));
    int fd = -1;
    RETRY_ON_EINTR(fd, open(file.c_str(), O_RDONLY));
    if (fd < 0) {
        return Status::IOError("failed to open {}: {}", file.native(), errno_to_str());
    }
    *reader = std::make_shared<LocalFileReader>(
            std::move(file), fsize, fd,
            std::static_pointer_cast<LocalFileSystem>(shared_from_this()));
    return Status::OK();
}

Status LocalFileSystem::create_directory_impl(const Path& dir) {
    if (std::filesystem::exists(dir)) {
        return Status::IOError("failed to create {}, already exists", dir.native());
    }
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("failed to create {}: {}", dir.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_file_impl(const Path& file) {
    if (!std::filesystem::exists(file)) {
        return Status::OK();
    }
    if (!std::filesystem::is_regular_file(file)) {
        return Status::IOError("failed to delete {}, not a file", file.native());
    }
    std::error_code ec;
    std::filesystem::remove(file, ec);
    if (ec) {
        return Status::IOError("failed to delete {}: {}", file.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory_impl(const Path& dir) {
    if (!std::filesystem::exists(dir)) {
        return Status::OK();
    }
    if (!std::filesystem::is_directory(dir)) {
        return Status::IOError("failed to delete {}, not a directory", dir.native());
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (ec) {
        return Status::IOError("failed to delete {}: {}", dir.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status LocalFileSystem::exists_impl(const Path& path, bool* res) const {
    *res = std::filesystem::exists(path);
    return Status::OK();
}

Status LocalFileSystem::file_size_impl(const Path& file, size_t* file_size) const {
    std::error_code ec;
    *file_size = std::filesystem::file_size(file, ec);
    if (ec) {
        return Status::IOError("failed to get file size {}: {}", file.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                  bool* exists) {
    if (!std::filesystem::exists(dir)) {
        *exists = false;
        return Status::OK();
    }
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (only_file && !entry.is_regular_file()) {
            continue;
        }
        FileInfo file_info;
        file_info.file_name = entry.path().filename();
        file_info.file_size = entry.file_size();
        file_info.is_file = entry.is_regular_file();
        files->push_back(std::move(file_info));
    }
    if (ec) {
        return Status::IOError("failed to list {}: {}", dir.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    std::error_code ec;
    std::filesystem::rename(orig_name, new_name, ec);
    if (ec) {
        return Status::IOError("failed to rename {} to {}: {}", orig_name.native(),
                               new_name.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::rename_dir_impl(const Path& orig_name, const Path& new_name) {
    return rename_impl(orig_name, new_name);
}

Status LocalFileSystem::link_file(const Path& src, const Path& dest) {
    auto src_file = absolute_path(src);
    auto dest_file = absolute_path(dest);
    if (bthread_self() == 0) {
        return link_file_impl(src_file, dest_file);
    }
    Status s;
    auto task = [&] { s = link_file_impl(src_file, dest_file); };
    AsyncIO::run_task(task, _type);
    return s;
}

Status LocalFileSystem::link_file_impl(const Path& src, const Path& dest) {
    if (::link(src.c_str(), dest.c_str()) != 0) {
        return Status::IOError("failed to create hard link from {} to {}: {}", src.native(),
                               dest.native(), errno_to_str());
    }
    return Status::OK();
}

static std::shared_ptr<LocalFileSystem> local_fs = io::LocalFileSystem::create("");

const std::shared_ptr<LocalFileSystem>& global_local_filesystem() {
    return local_fs;
}

} // namespace io
} // namespace doris
