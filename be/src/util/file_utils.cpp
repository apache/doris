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

#include "util/file_utils.h"

#include <dirent.h>
#include <openssl/md5.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <iomanip>
#include <sstream>

#include "env/env.h"
#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "gutil/strings/substitute.h"
#include "olap/file_helper.h"
#include "util/defer_op.h"

namespace doris {

using strings::Substitute;

Status FileUtils::create_dir(const std::string& path, Env* env) {
    if (path.empty()) {
        return Status::InvalidArgument(strings::Substitute("Unknown primitive type($0)", path));
    }

    boost::filesystem::path p(path);

    std::string partial_path;
    for (boost::filesystem::path::iterator it = p.begin(); it != p.end(); ++it) {
        partial_path = partial_path + it->string() + "/";
        bool is_dir = false;

        Status s = env->is_directory(partial_path, &is_dir);

        if (s.ok()) {
            if (is_dir) {
                // It's a normal directory.
                continue;
            }

            // Maybe a file or a symlink. Let's try to follow the symlink.
            std::string real_partial_path;
            RETURN_IF_ERROR(env->canonicalize(partial_path, &real_partial_path));

            RETURN_IF_ERROR(env->is_directory(real_partial_path, &is_dir));
            if (is_dir) {
                // It's a symlink to a directory.
                continue;
            } else {
                return Status::IOError(partial_path + " exists but is not a directory");
            }
        }

        RETURN_IF_ERROR(env->create_dir_if_missing(partial_path));
    }

    return Status::OK();
}

Status FileUtils::create_dir(const std::string& dir_path) {
    return create_dir(dir_path, Env::Default());
}

Status FileUtils::remove_all(const std::string& file_path) {
    boost::filesystem::path boost_path(file_path);
    boost::system::error_code ec;
    boost::filesystem::remove_all(boost_path, ec);
    if (ec != boost::system::errc::success) {
        std::stringstream ss;
        ss << "remove all(" << file_path << ") failed, because: " << ec;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status FileUtils::remove(const std::string& path, doris::Env* env) {
    bool is_dir;
    RETURN_IF_ERROR(env->is_directory(path, &is_dir));

    if (is_dir) {
        return env->delete_dir(path);
    } else {
        return env->delete_file(path);
    }
}

Status FileUtils::remove(const std::string& path) {
    return remove(path, Env::Default());
}

Status FileUtils::remove_paths(const std::vector<std::string>& paths) {
    for (const std::string& p : paths) {
        RETURN_IF_ERROR(remove(p));
    }
    return Status::OK();
}

Status FileUtils::list_files(Env* env, const std::string& dir, std::vector<std::string>* files) {
    auto cb = [files](const char* name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            files->push_back(name);
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

Status FileUtils::list_dirs_files(const std::string& path, std::set<std::string>* dirs,
                                  std::set<std::string>* files, Env* env) {
    auto cb = [path, dirs, files, env](const char* name) -> bool {
        if (is_dot_or_dotdot(name)) {
            return true;
        }

        std::string temp_path = path + "/" + name;
        bool is_dir;

        auto st = env->is_directory(temp_path, &is_dir);
        if (st.ok()) {
            if (is_dir) {
                if (dirs != nullptr) {
                    dirs->insert(name);
                }
            } else if (files != nullptr) {
                files->insert(name);
            }
        } else {
            LOG(WARNING) << "check path " << path << "is directory error: " << st.to_string();
        }

        return true;
    };

    return env->iterate_dir(path, cb);
}

Status FileUtils::get_children_count(Env* env, const std::string& dir, int64_t* count) {
    auto cb = [count](const char* name) -> bool {
        if (!is_dot_or_dotdot(name)) {
            *count += 1;
        }
        return true;
    };
    return env->iterate_dir(dir, cb);
}

bool FileUtils::is_dir(const std::string& file_path, Env* env) {
    bool ret;
    if (env->is_directory(file_path, &ret).ok()) {
        return ret;
    }

    return false;
}

bool FileUtils::is_dir(const std::string& path) {
    return is_dir(path, Env::Default());
}

// Through proc filesystem
std::string FileUtils::path_of_fd(int fd) {
    const int PATH_SIZE = 256;
    char proc_path[PATH_SIZE];
    snprintf(proc_path, PATH_SIZE, "/proc/self/fd/%d", fd);
    char path[PATH_SIZE];
    if (readlink(proc_path, path, PATH_SIZE) < 0) {
        path[0] = '\0';
    }
    return path;
}

Status FileUtils::split_paths(const char* path, std::vector<std::string>* path_vec) {
    path_vec->clear();
    *path_vec = strings::Split(path, ";", strings::SkipWhitespace());

    for (std::vector<std::string>::iterator it = path_vec->begin(); it != path_vec->end();) {
        StripWhiteSpace(&(*it));

        it->erase(it->find_last_not_of("/") + 1);
        if (it->size() == 0) {
            it = path_vec->erase(it);
        } else {
            ++it;
        }
    }

    // Check if
    std::sort(path_vec->begin(), path_vec->end());
    if (std::unique(path_vec->begin(), path_vec->end()) != path_vec->end()) {
        std::stringstream ss;
        ss << "Same path in path.[path=" << path << "]";
        return Status::InternalError(ss.str());
    }

    if (path_vec->size() == 0) {
        std::stringstream ss;
        ss << "Size of vector after split is zero.[path=" << path << "]";
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

Status FileUtils::copy_file(const std::string& src_path, const std::string& dest_path) {
    // open src file
    FileHandler src_file;
    if (src_file.open(src_path.c_str(), O_RDONLY) != OLAP_SUCCESS) {
        char errmsg[64];
        LOG(ERROR) << "open file failed: " << src_path << strerror_r(errno, errmsg, 64);
        return Status::InternalError("Internal Error");
    }
    // create dest file and overwrite existing file
    FileHandler dest_file;
    if (dest_file.open_with_mode(dest_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU) !=
        OLAP_SUCCESS) {
        char errmsg[64];
        LOG(ERROR) << "open file failed: " << dest_path << strerror_r(errno, errmsg, 64);
        return Status::InternalError("Internal Error");
    }

    const int64_t BUF_SIZE = 8192;
    char* buf = new char[BUF_SIZE];
    DeferOp free_buf(std::bind<void>(std::default_delete<char[]>(), buf));
    int64_t src_length = src_file.length();
    int64_t offset = 0;
    while (src_length > 0) {
        int64_t to_read = BUF_SIZE < src_length ? BUF_SIZE : src_length;
        if (OLAP_SUCCESS != (src_file.pread(buf, to_read, offset))) {
            return Status::InternalError("Internal Error");
        }
        if (OLAP_SUCCESS != (dest_file.pwrite(buf, to_read, offset))) {
            return Status::InternalError("Internal Error");
        }

        offset += to_read;
        src_length -= to_read;
    }
    return Status::OK();
}

Status FileUtils::md5sum(const std::string& file, std::string* md5sum) {
    int fd = open(file.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::InternalError("failed to open file");
    }

    struct stat statbuf;
    if (fstat(fd, &statbuf) < 0) {
        close(fd);
        return Status::InternalError("failed to stat file");
    }
    size_t file_len = statbuf.st_size;
    void* buf = mmap(0, file_len, PROT_READ, MAP_SHARED, fd, 0);

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

bool FileUtils::check_exist(const std::string& path) {
    return Env::Default()->path_exists(path).ok();
}

bool FileUtils::check_exist(const std::string& path, Env* env) {
    return env->path_exists(path).ok();
}

Status FileUtils::canonicalize(const std::string& path, std::string* real_path) {
    return Env::Default()->canonicalize(path, real_path);
}

} // namespace doris
