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

#include <fcntl.h>
#include <fmt/format.h>
#include <glob.h>
#include <glog/logging.h>
#include <openssl/md5.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iomanip>
#include <istream>
#include <system_error>
#include <utility>

#include "common/exception.h"
#include "cpp/sync_point.h"
#include "gutil/macros.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "olap/data_dir.h"
#include "runtime/thread_context.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::io {

std::filesystem::perms LocalFileSystem::PERMS_OWNER_RW =
        std::filesystem::perms::owner_read | std::filesystem::perms::owner_write;

LocalFileSystem::LocalFileSystem() : FileSystem(FileSystem::TMP_FS_ID, FileSystemType::LOCAL) {}

LocalFileSystem::~LocalFileSystem() = default;

Status LocalFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                         const FileWriterOptions* opts) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileSystem::create_file_impl",
                                      Status::IOError("inject io error"));
    int fd = ::open(file.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    DBUG_EXECUTE_IF("LocalFileSystem.create_file_impl.open_file_failed", {
        // spare '.testfile' to make bad disk checker happy
        if (file.filename().compare(kTestFilePath)) {
            ::close(fd);
            fd = -1;
        }
    });
    if (-1 == fd) {
        return localfs_error(errno, fmt::format("failed to create file {}", file.native()));
    }
    bool sync_data = opts != nullptr ? opts->sync_file_data : true;
    *writer = std::make_unique<LocalFileWriter>(file, fd, sync_data);
    return Status::OK();
}

Status LocalFileSystem::open_file_impl(const Path& file, FileReaderSPtr* reader,
                                       const FileReaderOptions* opts) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileSystem::open_file_impl",
                                      Status::IOError("inject io error"));
    int64_t fsize = opts ? opts->file_size : -1;
    if (fsize < 0) {
        RETURN_IF_ERROR(file_size_impl(file, &fsize));
    }
    int fd = -1;
    RETRY_ON_EINTR(fd, open(file.c_str(), O_RDONLY));
    if (fd < 0) {
        return localfs_error(errno, fmt::format("failed to open {}", file.native()));
    }
    *reader = std::make_shared<LocalFileReader>(file, fsize, fd);
    return Status::OK();
}

Status LocalFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(dir, &exists));
    if (exists && failed_if_exists) {
        return Status::AlreadyExist("failed to create {}, already exists", dir.native());
    }
    if (!exists) {
        std::error_code ec;
        std::filesystem::create_directories(dir, ec);
        if (ec) {
            return localfs_error(ec, fmt::format("failed to create {}", dir.native()));
        }
    }
    return Status::OK();
}

Status LocalFileSystem::delete_file_impl(const Path& file) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(file, &exists));
    if (!exists) {
        return Status::OK();
    }
    if (!std::filesystem::is_regular_file(file)) {
        return Status::InternalError("failed to delete {}, not a file", file.native());
    }
    std::error_code ec;
    std::filesystem::remove(file, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to delete {}", file.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory_impl(const Path& dir) {
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(dir, &exists));
    if (!exists) {
        return Status::OK();
    }
    if (!std::filesystem::is_directory(dir)) {
        return Status::InternalError("failed to delete {}, not a directory", dir.native());
    }
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to delete {}", dir.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::delete_directory_or_file(const Path& path) {
    FILESYSTEM_M(delete_directory_or_file_impl(path));
}

Status LocalFileSystem::delete_directory_or_file_impl(const Path& path) {
    bool is_dir;
    RETURN_IF_ERROR(is_directory(path, &is_dir));
    if (is_dir) {
        return delete_directory_impl(path);
    } else {
        return delete_file_impl(path);
    }
}

Status LocalFileSystem::batch_delete_impl(const std::vector<Path>& files) {
    for (auto& file : files) {
        RETURN_IF_ERROR(delete_file_impl(file));
    }
    return Status::OK();
}

Status LocalFileSystem::exists_impl(const Path& path, bool* res) const {
    std::error_code ec;
    *res = std::filesystem::exists(path, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to check exists {}", path.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    std::error_code ec;
    *file_size = std::filesystem::file_size(file, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to get file size {}", file.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::directory_size(const Path& dir_path, size_t* dir_size) {
    *dir_size = 0;
    if (std::filesystem::exists(dir_path) && std::filesystem::is_directory(dir_path)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir_path)) {
            if (std::filesystem::is_regular_file(entry)) {
                *dir_size += std::filesystem::file_size(entry);
            }
        }
        return Status::OK();
    }
    // TODO(plat1ko): Use error code according to std::error_code
    return Status::InternalError("faile to get dir size {}", dir_path.native());
}

Status LocalFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                  bool* exists) {
    RETURN_IF_ERROR(exists_impl(dir, exists));
    if (!exists) {
        return Status::OK();
    }
    std::error_code ec;
    try {
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (only_file && !entry.is_regular_file()) {
                continue;
            }
            FileInfo file_info;
            file_info.file_name = entry.path().filename();
            file_info.is_file = entry.is_regular_file(ec);
            if (ec) {
                break;
            }
            if (file_info.is_file) {
                file_info.file_size = entry.file_size(ec);
                if (ec) {
                    break;
                }
            }
            files->push_back(std::move(file_info));
        }
    } catch (const std::filesystem::filesystem_error& e) {
        // although `directory_iterator(dir, ec)` does not throw an exception,
        // it may throw an exception during iterator++, so we need to catch the exception here
        return localfs_error(e.code(), fmt::format("failed to list {}, error message: {}",
                                                   dir.native(), e.what()));
    }
    if (ec) {
        return localfs_error(ec, fmt::format("failed to list {}", dir.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::rename_impl(const Path& orig_name, const Path& new_name) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("LocalFileSystem::rename",
                                      Status::IOError("inject io error"));
    std::error_code ec;
    std::filesystem::rename(orig_name, new_name, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to rename {} to {}", orig_name.native(),
                                             new_name.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::link_file(const Path& src, const Path& dest) {
    FILESYSTEM_M(link_file_impl(src, dest));
}

Status LocalFileSystem::link_file_impl(const Path& src, const Path& dest) {
    if (::link(src.c_str(), dest.c_str()) != 0) {
        return localfs_error(errno, fmt::format("failed to create hard link from {} to {}",
                                                src.native(), dest.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::canonicalize(const Path& path, std::string* real_path) {
    std::error_code ec;
    Path res = std::filesystem::canonical(path, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to canonicalize {}", path.native()));
    }
    *real_path = res.string();
    return Status::OK();
}

Status LocalFileSystem::is_directory(const Path& path, bool* res) {
    std::error_code ec;
    *res = std::filesystem::is_directory(path, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to canonicalize {}", path.native()));
    }
    return Status::OK();
}

Status LocalFileSystem::md5sum(const Path& file, std::string* md5sum) {
    FILESYSTEM_M(md5sum_impl(file, md5sum));
}

Status LocalFileSystem::md5sum_impl(const Path& file, std::string* md5sum) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return localfs_error(errno,
                             fmt::format("failed to open file for md5sum {}", file.native()));
    }

    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
        std::string err = errno_to_str();
        close(fd);
        return localfs_error(errno, fmt::format("failed to stat file {}", file.native()));
    }
    size_t file_len = statbuf.st_size;
    void* buf = mmap(nullptr, file_len, PROT_READ, MAP_SHARED, fd, 0);

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)buf, file_len, result);
    munmap(buf, file_len);

    std::stringstream ss;
    for (int32_t i = 0; i < MD5_DIGEST_LENGTH; i++) {
        ss << std::setfill('0') << std::setw(2) << std::hex << (int)result[i];
    }
    ss >> *md5sum;

    close(fd);
    return Status::OK();
}

Status LocalFileSystem::iterate_directory(const std::string& dir,
                                          const std::function<bool(const FileInfo& file)>& cb) {
    FILESYSTEM_M(iterate_directory_impl(dir, cb));
}

Status LocalFileSystem::iterate_directory_impl(
        const std::string& dir, const std::function<bool(const FileInfo& file)>& cb) {
    bool exists = true;
    std::vector<FileInfo> files;
    RETURN_IF_ERROR(list_impl(dir, false, &files, &exists));
    for (auto& file : files) {
        if (!cb(file)) {
            break;
        }
    }
    return Status::OK();
}

Status LocalFileSystem::get_space_info(const Path& dir, size_t* capacity, size_t* available) {
    FILESYSTEM_M(get_space_info_impl(dir, capacity, available));
}

Status LocalFileSystem::get_space_info_impl(const Path& path, size_t* capacity, size_t* available) {
    std::error_code ec;
    std::filesystem::space_info info = std::filesystem::space(path, ec);
    if (ec) {
        return localfs_error(
                ec, fmt::format("failed to get available space for path {}", path.native()));
    }
    *capacity = info.capacity;
    *available = info.available;
    return Status::OK();
}

Status LocalFileSystem::copy_path(const Path& src, const Path& dest) {
    FILESYSTEM_M(copy_path_impl(src, dest));
}

Status LocalFileSystem::copy_path_impl(const Path& src, const Path& dest) {
    std::error_code ec;
    std::filesystem::copy(src, dest, std::filesystem::copy_options::recursive, ec);
    if (ec) {
        return localfs_error(
                ec, fmt::format("failed to copy from {} to {}", src.native(), dest.native()));
    }
    return Status::OK();
}

bool LocalFileSystem::contain_path(const Path& parent_, const Path& sub_) {
    Path parent = parent_.lexically_normal();
    Path sub = sub_.lexically_normal();
    if (parent == sub) {
        return true;
    }

    if (parent.filename() == ".") {
        parent.remove_filename();
    }

    // We're also not interested in the file's name.
    if (sub.has_filename()) {
        sub.remove_filename();
    }
    // If dir has more components than file, then file can't possibly reside in dir.
    auto dir_len = std::distance(parent.begin(), parent.end());
    auto file_len = std::distance(sub.begin(), sub.end());
    if (dir_len > file_len) {
        return false;
    }
    auto p_it = parent.begin();
    auto s_it = sub.begin();
    for (; p_it != parent.end() && !p_it->string().empty(); ++p_it, ++s_it) {
        if (!(*p_it == *s_it)) {
            return false;
        }
    }
    return true;
}

const std::shared_ptr<LocalFileSystem>& global_local_filesystem() {
    static std::shared_ptr<LocalFileSystem> local_fs(new LocalFileSystem());
    return local_fs;
}

Status LocalFileSystem::canonicalize_local_file(const std::string& dir,
                                                const std::string& file_path,
                                                std::string* full_path) {
    const std::string absolute_path = dir + "/" + file_path;
    std::string canonical_path;
    RETURN_IF_ERROR(canonicalize(absolute_path, &canonical_path));
    if (!contain_path(dir, canonical_path)) {
        return Status::InvalidArgument("file path is not allowed: {}", canonical_path);
    }

    *full_path = canonical_path;
    return Status::OK();
}

Status LocalFileSystem::safe_glob(const std::string& path, std::vector<FileInfo>* res) {
    if (path.find("..") != std::string::npos) {
        return Status::InvalidArgument("can not contain '..' in path");
    }
    std::string full_path = config::user_files_secure_path + "/" + path;
    std::vector<std::string> files;
    RETURN_IF_ERROR(_glob(full_path, &files));
    for (auto& file : files) {
        FileInfo fi;
        fi.is_file = true;
        RETURN_IF_ERROR(canonicalize_local_file("", file, &(fi.file_name)));
        RETURN_IF_ERROR(file_size_impl(fi.file_name, &(fi.file_size)));
        res->push_back(std::move(fi));
    }
    return Status::OK();
}

Status LocalFileSystem::_glob(const std::string& pattern, std::vector<std::string>* res) {
    glob_t glob_result;
    memset(&glob_result, 0, sizeof(glob_result));

    int rc = glob(pattern.c_str(), GLOB_TILDE, NULL, &glob_result);
    if (rc != 0) {
        globfree(&glob_result);
        return Status::InternalError("failed to glob {}: {}", pattern, glob_err_to_str(rc));
    }

    for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
        res->push_back(std::string(glob_result.gl_pathv[i]));
    }

    globfree(&glob_result);
    return Status::OK();
}

Status LocalFileSystem::permission(const Path& file, std::filesystem::perms prms) {
    FILESYSTEM_M(permission_impl(file, prms));
}

Status LocalFileSystem::permission_impl(const Path& file, std::filesystem::perms prms) {
    std::error_code ec;
    std::filesystem::permissions(file, prms, ec);
    if (ec) {
        return localfs_error(ec, fmt::format("failed to change file permission {}", file.native()));
    }
    return Status::OK();
}

} // namespace doris::io
