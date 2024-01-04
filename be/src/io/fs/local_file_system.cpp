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

#include "gutil/macros.h"
#include "io/fs/err_utils.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_reader.h"
#include "io/fs/local_file_writer.h"
#include "runtime/thread_context.h"
#include "util/async_io.h" // IWYU pragma: keep
#include "util/defer_op.h"

namespace doris {
namespace io {
class FileReaderOptions;

std::shared_ptr<LocalFileSystem> LocalFileSystem::create(Path path, std::string id) {
    return std::shared_ptr<LocalFileSystem>(new LocalFileSystem(std::move(path), std::move(id)));
}

LocalFileSystem::LocalFileSystem(Path&& root_path, std::string&& id)
        : FileSystem(std::move(root_path), std::move(id), FileSystemType::LOCAL) {}

LocalFileSystem::~LocalFileSystem() = default;

Status LocalFileSystem::create_file_impl(const Path& file, FileWriterPtr* writer,
                                         const FileWriterOptions* opts) {
    int fd = ::open(file.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    if (-1 == fd) {
        return Status::IOError("failed to open {}: {}", file.native(), errno_to_str());
    }
    bool sync_data = opts != nullptr ? opts->sync_file_data : true;
    *writer = std::make_unique<LocalFileWriter>(
            std::move(file), fd, std::static_pointer_cast<LocalFileSystem>(shared_from_this()),
            sync_data);
    return Status::OK();
}

Status LocalFileSystem::open_file_impl(const FileDescription& file_desc, const Path& abs_path,
                                       const FileReaderOptions& /*reader_options*/,
                                       FileReaderSPtr* reader) {
    int64_t fsize = file_desc.file_size;
    if (fsize <= 0) {
        RETURN_IF_ERROR(file_size_impl(abs_path, &fsize));
    }
    int fd = -1;
    RETRY_ON_EINTR(fd, open(abs_path.c_str(), O_RDONLY));
    if (fd < 0) {
        return Status::IOError("failed to open {}: {}", abs_path.native(), errno_to_str());
    }
    *reader = std::make_shared<LocalFileReader>(
            std::move(abs_path), fsize, fd,
            std::static_pointer_cast<LocalFileSystem>(shared_from_this()));
    return Status::OK();
}

Status LocalFileSystem::create_directory_impl(const Path& dir, bool failed_if_exists) {
    if (failed_if_exists) {
        bool exists = true;
        RETURN_IF_ERROR(exists_impl(dir, &exists));
        if (exists) {
            return Status::IOError("failed to create {}, already exists", dir.native());
        }
    }
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("failed to create {}: {}", dir.native(), errcode_to_str(ec));
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
    bool exists = true;
    RETURN_IF_ERROR(exists_impl(dir, &exists));
    if (!exists) {
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

Status LocalFileSystem::delete_directory_or_file(const Path& path) {
    auto the_path = absolute_path(path);
    FILESYSTEM_M(delete_directory_or_file_impl(the_path));
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
        return Status::IOError("failed to check exists {}: {}", path.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::file_size_impl(const Path& file, int64_t* file_size) const {
    std::error_code ec;
    *file_size = std::filesystem::file_size(file, ec);
    if (ec) {
        return Status::IOError("failed to get file size {}: {}", file.native(), errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                                  bool* exists) {
    RETURN_IF_ERROR(exists_impl(dir, exists));
    if (!exists) {
        return Status::OK();
    }
    std::error_code ec;
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
    FILESYSTEM_M(link_file_impl(src_file, dest_file));
}

Status LocalFileSystem::link_file_impl(const Path& src, const Path& dest) {
    if (::link(src.c_str(), dest.c_str()) != 0) {
        return Status::IOError("failed to create hard link from {} to {}: {}", src.native(),
                               dest.native(), errno_to_str());
    }
    return Status::OK();
}

Status LocalFileSystem::canonicalize(const Path& path, std::string* real_path) {
    std::error_code ec;
    Path res = std::filesystem::canonical(path, ec);
    if (ec) {
        return Status::IOError("failed to canonicalize path {}: {}", path.native(),
                               errcode_to_str(ec));
    }
    *real_path = res.string();
    return Status::OK();
}

Status LocalFileSystem::is_directory(const Path& path, bool* res) {
    auto tmp_path = absolute_path(path);
    std::error_code ec;
    *res = std::filesystem::is_directory(tmp_path, ec);
    if (ec) {
        LOG(WARNING) << fmt::format("failed to check is dir {}: {}", tmp_path.native(),
                                    errcode_to_str(ec));
        return Status::IOError("failed to check is dir {}: {}", tmp_path.native(),
                               errcode_to_str(ec));
    }
    return Status::OK();
}

Status LocalFileSystem::md5sum(const Path& file, std::string* md5sum) {
    auto path = absolute_path(file);
    FILESYSTEM_M(md5sum_impl(path, md5sum));
}

Status LocalFileSystem::md5sum_impl(const Path& file, std::string* md5sum) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("failed to open file for md5sum {}: {}", file.native(),
                               errno_to_str());
    }

    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
        std::string err = errno_to_str();
        close(fd);
        return Status::InternalError("failed to stat file {}: {}", file.native(), err);
    }
    size_t file_len = statbuf.st_size;
    CONSUME_THREAD_MEM_TRACKER(file_len);
    void* buf = mmap(nullptr, file_len, PROT_READ, MAP_SHARED, fd, 0);

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)buf, file_len, result);
    munmap(buf, file_len);
    RELEASE_THREAD_MEM_TRACKER(file_len);

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
    auto path = absolute_path(dir);
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

Status LocalFileSystem::mtime(const Path& file, time_t* m_time) {
    auto path = absolute_path(file);
    FILESYSTEM_M(mtime_impl(path, m_time));
}

Status LocalFileSystem::mtime_impl(const Path& file, time_t* m_time) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("failed to get mtime for file {}: {}", file.native(),
                               errno_to_str());
    }

    Defer defer {[&]() { close(fd); }};
    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
        return Status::IOError("failed to stat file {}: {}", file.native(), errno_to_str());
    }
    *m_time = statbuf.st_mtime;
    return Status::OK();
}

Status LocalFileSystem::delete_and_create_directory(const Path& dir) {
    auto path = absolute_path(dir);
    FILESYSTEM_M(delete_and_create_directory_impl(path));
}

Status LocalFileSystem::delete_and_create_directory_impl(const Path& dir) {
    RETURN_IF_ERROR(delete_directory_impl(dir));
    return create_directory_impl(dir);
}

Status LocalFileSystem::get_space_info(const Path& dir, size_t* capacity, size_t* available) {
    auto path = absolute_path(dir);
    FILESYSTEM_M(get_space_info_impl(path, capacity, available));
}

Status LocalFileSystem::get_space_info_impl(const Path& path, size_t* capacity, size_t* available) {
    std::error_code ec;
    std::filesystem::space_info info = std::filesystem::space(path, ec);
    if (ec) {
        return Status::IOError("failed to get available space for path {}: {}", path.native(),
                               errcode_to_str(ec));
    }
    *capacity = info.capacity;
    *available = info.available;
    return Status::OK();
}

Status LocalFileSystem::copy_path(const Path& src, const Path& dest) {
    auto src_path = absolute_path(src);
    auto dest_path = absolute_path(dest);
    FILESYSTEM_M(copy_path_impl(src_path, dest_path));
}

Status LocalFileSystem::copy_path_impl(const Path& src, const Path& dest) {
    std::error_code ec;
    std::filesystem::copy(src, dest, std::filesystem::copy_options::recursive, ec);
    if (ec) {
        return Status::IOError("failed to copy from {} to {}: {}", src.native(), dest.native(),
                               errcode_to_str(ec));
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

Status LocalFileSystem::read_file_to_string(const Path& file, std::string* content) {
    FileReaderSPtr file_reader;
    FileDescription fd;
    fd.path = file.native();
    RETURN_IF_ERROR(open_file(fd, &file_reader));
    size_t file_size = file_reader->size();
    content->resize(file_size);
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(0, {*content}, &bytes_read));
    if (bytes_read != file_size) {
        return Status::IOError("failed to read file {} to string. bytes read: {}, file size: {}",
                               file.native(), bytes_read, file_size);
    }
    return file_reader->close();
}

static std::shared_ptr<LocalFileSystem> local_fs = io::LocalFileSystem::create("");

const std::shared_ptr<LocalFileSystem>& global_local_filesystem() {
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
        return Status::IOError("failed to glob {}: {}", pattern, glob_err_to_str(rc));
    }

    for (size_t i = 0; i < glob_result.gl_pathc; ++i) {
        res->push_back(std::string(glob_result.gl_pathv[i]));
    }

    globfree(&glob_result);
    return Status::OK();
}

} // namespace io
} // namespace doris
