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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <dirent.h>

#include <sstream>
#include <algorithm>
#include <iomanip>

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <openssl/md5.h>

#include "olap/file_helper.h"
#include "util/defer_op.h"

namespace doris {

Status FileUtils::create_dir(const std::string& dir_path) {
    try {
        if (boost::filesystem::exists(dir_path.c_str())) {
            // No need to create one
            if (!boost::filesystem::is_directory(dir_path.c_str())) {
                std::stringstream ss;
                ss << "Path(" << dir_path << ") already exists, but not a directory.";
                return Status::InternalError(ss.str());
            }
        } else {
            if (!boost::filesystem::create_directories(dir_path.c_str())) {
                std::stringstream ss;
                ss << "make directory failed. path=" << dir_path;
                return Status::InternalError(ss.str());
            }
        }
    } catch (...) {
        std::stringstream ss;
        ss << "make directory failed. path=" << dir_path;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status FileUtils::remove_all(const std::string& file_path) {
    try {
        boost::filesystem::path boost_path(file_path);
        boost::system::error_code ec;
        boost::filesystem::remove_all(boost_path, ec);
        if (ec != boost::system::errc::success) {
            std::stringstream ss;
            ss << "remove all(" << file_path << ") failed, because: "
                << ec;
            return Status::InternalError(ss.str());
        }
    } catch (...) {
        std::stringstream ss;
        ss << "remove all(" << file_path << ") failed, because: exception";
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status FileUtils::scan_dir(
        const std::string& dir_path, std::vector<std::string>* files,
        int64_t* file_count) {

    DIR* dir = opendir(dir_path.c_str());
    if (dir == nullptr) {
        char buf[64];
        std::stringstream ss;
        ss << "opendir(" << dir_path << ") failed, because: " << strerror_r(errno, buf, 64);
        return Status::InternalError(ss.str());
    }
    DeferOp close_dir(std::bind<void>(&closedir, dir));

    int64_t count = 0;
    while (true) {
        auto result = readdir(dir);
        if (result == nullptr) {
            break;
        }
        std::string file_name = result->d_name;
        if (file_name == "." || file_name == "..") {
            continue; 
        }

        if (files != nullptr) {
            files->emplace_back(std::move(file_name));
        }
        count++; 
    }

    if (file_count != nullptr) {
        *file_count = count;
    }

    return Status::OK();
}

Status FileUtils::scan_dir(
        const std::string& dir_path,
        const std::function<bool(const std::string&, const std::string&)>& callback) {
    auto dir_closer = [] (DIR* dir) { closedir(dir); };
    std::unique_ptr<DIR, decltype(dir_closer)> dir(opendir(dir_path.c_str()), dir_closer);
    if (dir == nullptr) {
        char buf[64];
        LOG(WARNING) << "fail to open dir, dir=" << dir_path << ", errmsg=" << strerror_r(errno, buf, 64);
        return Status::InternalError("fail to opendir");
    }

    while (true) {
        auto result = readdir(dir.get());
        if (result == nullptr) {
            break;
        }
        std::string file_name = result->d_name;
        if (file_name == "." || file_name == "..") {
            continue; 
        }
        auto is_continue = callback(dir_path, file_name);
        if (!is_continue) {
            break;
        }
    }

    return Status::OK();
}

bool FileUtils::is_dir(const std::string& path) {
    struct stat path_stat;    
    if (stat(path.c_str(), &path_stat) != 0) {
        return false;
    }

    if (path_stat.st_mode & S_IFDIR) {
        return true;
    }

    return false;
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

Status FileUtils::split_pathes(const char* path, std::vector<std::string>* path_vec) {
    path_vec->clear();
    try {
        boost::split(*path_vec, path,
                     boost::is_any_of(";"),
                     boost::token_compress_on);
    } catch (...) {
        std::stringstream ss;
        ss << "Boost split path failed.[path=" << path << "]";
        return Status::InternalError(ss.str());
    }

    for (std::vector<std::string>::iterator it = path_vec->begin(); it != path_vec->end();) {
        boost::trim(*it);

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
    if (dest_file.open_with_mode(dest_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU)
        != OLAP_SUCCESS) {
        char errmsg[64];
        LOG(ERROR) << "open file failed: " << dest_path << strerror_r(errno, errmsg, 64);
        return Status::InternalError("Internal Error");
    }
    
    const int64_t BUF_SIZE = 8192;
    char *buf = new char[BUF_SIZE];
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
    MD5((unsigned char*) buf, file_len, result);
    munmap(buf, file_len); 

    std::stringstream ss;
    for (int32_t i = 0; i < MD5_DIGEST_LENGTH; i++) {
        ss << std::setfill('0') << std::setw(2) << std::hex << (int) result[i];
    }
    ss >> *md5sum;
    
    close(fd);
    return Status::OK();
}

bool FileUtils::check_exist(const std::string& path) {
    boost::system::error_code errcode;
    bool exist = boost::filesystem::exists(path, errcode);
    if (errcode != boost::system::errc::success && errcode != boost::system::errc::no_such_file_or_directory) {
        LOG(WARNING) << "error when check path:" << path << ", error code:" << errcode;
        return false;
    }
    return exist;
}

}

