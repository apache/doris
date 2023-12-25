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

#include "olap/wal_dirs_info.h"

#include <memory>
#include <mutex>
#include <shared_mutex>

#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/parse_util.h"

namespace doris {

std::string WalDirInfo::get_wal_dir() {
    return _wal_dir;
}

size_t WalDirInfo::get_limit() {
    std::shared_lock rlock(_lock);
    return _limit;
}

size_t WalDirInfo::get_used() {
    std::shared_lock rlock(_lock);
    return _used;
}

size_t WalDirInfo::get_pre_allocated() {
    std::shared_lock rlock(_lock);
    return _pre_allocated;
}

Status WalDirInfo::set_limit(size_t limit) {
    std::unique_lock wlock(_lock);
    _limit = limit;
    return Status::OK();
}

Status WalDirInfo::set_used(size_t used) {
    std::unique_lock wlock(_lock);
    _used = used;
    return Status::OK();
}

Status WalDirInfo::set_pre_allocated(size_t pre_allocated, bool is_add_pre_allocated) {
    std::unique_lock wlock(_lock);
    if (is_add_pre_allocated) {
        _pre_allocated += pre_allocated;
    } else {
        _pre_allocated -= pre_allocated;
    }
    return Status::OK();
}

size_t WalDirInfo::available() {
    std::unique_lock wlock(_lock);
    int64_t available = _limit - _used - _pre_allocated;
    return available > 0 ? available : 0;
}

Status WalDirInfo::update_wal_disk_info(size_t limit, size_t used, size_t pre_allocated,
                                        bool is_add_pre_allocated) {
    if (limit != static_cast<size_t>(-1)) {
        RETURN_IF_ERROR(set_limit(limit));
    } else {
        size_t available_bytes;
        size_t disk_capacity_bytes;
        RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(
                _wal_dir, &disk_capacity_bytes, &available_bytes));
        bool is_percent = true;
        int64_t wal_disk_limit = ParseUtil::parse_mem_spec(config::group_commit_wal_max_disk_limit,
                                                           -1, available_bytes, &is_percent);
        if (wal_disk_limit <= 0) {
            return Status::InternalError("Disk full! Please check your disk usage!");
        }
        size_t wal_dir_size = 0;
        RETURN_IF_ERROR(io::global_local_filesystem()->directory_size(_wal_dir, &wal_dir_size));
        RETURN_IF_ERROR(set_limit(wal_disk_limit));
    }
    if (used != static_cast<size_t>(-1)) {
        RETURN_IF_ERROR(set_used(used));
    } else {
        size_t wal_dir_size = 0;
        RETURN_IF_ERROR(io::global_local_filesystem()->directory_size(_wal_dir, &wal_dir_size));
        RETURN_IF_ERROR(set_used(wal_dir_size));
    }
    if (pre_allocated != static_cast<size_t>(-1)) {
        RETURN_IF_ERROR(set_pre_allocated(pre_allocated, is_add_pre_allocated));
    }
    return Status::OK();
}

Status WalDirsInfo::add(const std::string& wal_dir, size_t limit, size_t used,
                        size_t pre_allocated) {
    for (const auto& it : _wal_dirs_info_vec) {
        if (it->get_wal_dir() == wal_dir) {
            return Status::InternalError("wal dir {} exists!", wal_dir);
        }
    }
    std::unique_lock wlock(_lock);
    _wal_dirs_info_vec.emplace_back(
            std::make_shared<WalDirInfo>(wal_dir, limit, used, pre_allocated));
    return Status::OK();
}

std::string WalDirsInfo::get_available_random_wal_dir() {
    if (_wal_dirs_info_vec.size() == 1) {
        return (*_wal_dirs_info_vec.begin())->get_wal_dir();
    } else {
        std::vector<std::string> available_wal_dirs;
        for (const auto& wal_dir_info : _wal_dirs_info_vec) {
            if (wal_dir_info->available() > wal_dir_info->get_limit() * 0.2) {
                available_wal_dirs.emplace_back(wal_dir_info->get_wal_dir());
            }
        }
        if (available_wal_dirs.empty()) {
            return (*std::min_element(_wal_dirs_info_vec.begin(), _wal_dirs_info_vec.end(),
                                      [](const auto& info1, const auto& info2) {
                                          return info1->available() < info2->available();
                                      }))
                    ->get_wal_dir();
        } else {
            return (*std::next(_wal_dirs_info_vec.begin(), rand() % _wal_dirs_info_vec.size()))
                    ->get_wal_dir();
        }
    }
}

size_t WalDirsInfo::get_max_available_size() {
    return _wal_dirs_info_vec.size() == 1
                   ? (*_wal_dirs_info_vec.begin())->available()
                   : (*std::max_element(_wal_dirs_info_vec.begin(), _wal_dirs_info_vec.end(),
                                        [](const auto& info1, const auto& info2) {
                                            return info1->available() < info2->available();
                                        }))
                             ->available();
}

Status WalDirsInfo::update_wal_disk_info(std::string wal_dir, size_t limit, size_t used,
                                         size_t pre_allocated, bool is_add_pre_allocated) {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            return wal_dir_info->update_wal_disk_info(limit, used, pre_allocated,
                                                      is_add_pre_allocated);
        }
    }
    return Status::InternalError("Can not find wal dir in wal disks info.");
}

Status WalDirsInfo::get_wal_disk_available_size(const std::string& wal_dir,
                                                size_t* available_bytes) {
    RETURN_IF_ERROR(update_wal_disk_info(wal_dir));
    std::shared_lock l(_lock);
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            *available_bytes = wal_dir_info->available();
            return Status::OK();
        }
    }
    return Status::InternalError("can not find wal dir!");
}

} // namespace doris