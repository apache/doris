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

#include "runtime/small_file_mgr.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdint.h>
#include <stdio.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gutil/strings/split.h"
#include "http/http_client.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/doris_metrics.h"
#include "util/md5.h"
#include "util/metrics.h"
#include "util/string_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(small_file_cache_count, MetricUnit::NOUNIT);

SmallFileMgr::SmallFileMgr(ExecEnv* env, const std::string& local_path)
        : _exec_env(env), _local_path(local_path) {
    REGISTER_HOOK_METRIC(small_file_cache_count, [this]() {
        // std::lock_guard<std::mutex> l(_lock);
        return _file_cache.size();
    });
}

SmallFileMgr::~SmallFileMgr() {
    DEREGISTER_HOOK_METRIC(small_file_cache_count);
}

Status SmallFileMgr::init() {
    RETURN_IF_ERROR(_load_local_files());
    return Status::OK();
}

Status SmallFileMgr::_load_local_files() {
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(_local_path));

    auto scan_cb = [this](const io::FileInfo& file) {
        if (!file.is_file) {
            return true;
        }
        auto st = _load_single_file(_local_path, file.file_name);
        if (!st.ok()) {
            LOG(WARNING) << "load small file failed: " << st;
        }
        return true;
    };

    RETURN_IF_ERROR(io::global_local_filesystem()->iterate_directory(_local_path, scan_cb));
    return Status::OK();
}

Status SmallFileMgr::_load_single_file(const std::string& path, const std::string& file_name) {
    // file name format should be like:
    // file_id.md5
    std::vector<std::string> parts = strings::Split(file_name, ".");
    if (parts.size() != 2) {
        return Status::InternalError("Not a valid file name: {}", file_name);
    }
    int64_t file_id = std::stol(parts[0]);
    std::string md5 = parts[1];

    if (_file_cache.find(file_id) != _file_cache.end()) {
        return Status::InternalError("File with same id is already been loaded: {}", file_id);
    }

    std::string file_md5;
    RETURN_IF_ERROR(io::global_local_filesystem()->md5sum(path + "/" + file_name, &file_md5));
    if (file_md5 != md5) {
        return Status::InternalError("Invalid md5 of file: {}", file_name);
    }

    CacheEntry entry;
    entry.path = path + "/" + file_name;
    entry.md5 = file_md5;

    _file_cache.emplace(file_id, entry);
    return Status::OK();
}

Status SmallFileMgr::get_file(int64_t file_id, const std::string& md5, std::string* file_path) {
    std::unique_lock<std::mutex> l(_lock);
    // find in cache
    auto it = _file_cache.find(file_id);
    if (it != _file_cache.end()) {
        // find the cached file, check it
        CacheEntry& entry = it->second;
        Status st = _check_file(entry, md5);
        if (!st.ok()) {
            // check file failed, we should remove this cache and download it from FE again
            if (remove(entry.path.c_str()) != 0) {
                return Status::InternalError("failed to remove file: {}, err: {}", file_id,
                                             std::strerror(errno));
            }
            _file_cache.erase(it);
        } else {
            // check ok, return the path
            *file_path = entry.path;
            return Status::OK();
        }
    }

    // file not found in cache. download it from FE
    RETURN_IF_ERROR(_download_file(file_id, md5, file_path));

    return Status::OK();
}

Status SmallFileMgr::_check_file(const CacheEntry& entry, const std::string& md5) {
    bool exists;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(entry.path, &exists));
    if (!exists) {
        return Status::InternalError("file not exist: {}", entry.path);
    }
    if (!iequal(md5, entry.md5)) {
        return Status::InternalError("invalid MD5 of file: {}", entry.path);
    }
    return Status::OK();
}

Status SmallFileMgr::_download_file(int64_t file_id, const std::string& md5,
                                    std::string* file_path) {
    std::stringstream ss;
    ss << _local_path << "/" << file_id << ".tmp";
    std::string tmp_file = ss.str();
    bool should_delete = true;
    auto fp_closer = [&tmp_file, &should_delete](FILE* fp) {
        fclose(fp);
        if (should_delete) remove(tmp_file.c_str());
    };

    std::unique_ptr<FILE, decltype(fp_closer)> fp(fopen(tmp_file.c_str(), "w"), fp_closer);
    if (fp == nullptr) {
        LOG(WARNING) << "fail to open file, file=" << tmp_file;
        return Status::InternalError("fail to open file");
    }

    HttpClient client;

    std::stringstream url_ss;
    ClusterInfo* cluster_info = _exec_env->cluster_info();
    url_ss << cluster_info->master_fe_addr.hostname << ":" << cluster_info->master_fe_http_port
           << "/api/get_small_file?"
           << "file_id=" << file_id << "&token=" << cluster_info->token;

    std::string url = url_ss.str();

    LOG(INFO) << "download file from: " << url;

    RETURN_IF_ERROR(client.init(url));
    Status status;
    Md5Digest digest;
    auto download_cb = [&status, &tmp_file, &fp, &digest](const void* data, size_t length) {
        digest.update(data, length);
        auto res = fwrite(data, length, 1, fp.get());
        if (res != 1) {
            LOG(WARNING) << "fail to write data to file, file=" << tmp_file
                         << ", error=" << ferror(fp.get());
            status = Status::InternalError("fail to write data when download");
            return false;
        }
        return true;
    };
    RETURN_IF_ERROR(client.execute(download_cb));
    RETURN_IF_ERROR(status);
    digest.digest();

    if (!iequal(digest.hex(), md5)) {
        LOG(WARNING) << "file's checksum is not equal, download: " << digest.hex()
                     << ", expected: " << md5 << ", file: " << file_id;
        return Status::InternalError("download with invalid md5");
    }

    // close this file
    should_delete = false;
    fp.reset();

    // rename temporary file to library file
    std::stringstream real_ss;
    real_ss << _local_path << "/" << file_id << "." << md5;
    std::string real_file_path = real_ss.str();
    auto ret = rename(tmp_file.c_str(), real_file_path.c_str());
    if (ret != 0) {
        char buf[64];
        LOG(WARNING) << "fail to rename file from=" << tmp_file << ", to=" << real_file_path
                     << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        remove(tmp_file.c_str());
        remove(real_file_path.c_str());
        return Status::InternalError("fail to rename file");
    }

    // add to file cache
    CacheEntry entry;
    entry.path = real_file_path;
    entry.md5 = md5;
    _file_cache.emplace(file_id, entry);

    *file_path = real_file_path;

    LOG(INFO) << "finished to download file: " << file_path;
    return Status::OK();
}

} // end namespace doris
