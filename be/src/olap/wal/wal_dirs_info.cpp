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

#include "olap/wal/wal_dirs_info.h"

#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "util/parse_util.h"

namespace doris {

const std::string& WalDirInfo::get_wal_dir() const {
    return _wal_dir;
}

size_t WalDirInfo::get_limit() {
    std::shared_lock rlock(_lock);
    return _limit;
}

void WalDirInfo::set_limit(size_t limit) {
    std::unique_lock wlock(_lock);
    _limit = limit;
}

size_t WalDirInfo::get_used() {
    std::shared_lock rlock(_lock);
    return _used;
}

void WalDirInfo::set_used(size_t used) {
    std::unique_lock wlock(_lock);
    _used = used;
}

size_t WalDirInfo::get_estimated_wal_bytes() {
    std::shared_lock rlock(_lock);
    return _estimated_wal_bytes;
}

void WalDirInfo::set_estimated_wal_bytes(size_t increase_estimated_wal_bytes,
                                         size_t decrease_estimated_wal_bytes) {
    std::unique_lock wlock(_lock);
    _estimated_wal_bytes += increase_estimated_wal_bytes;
    _estimated_wal_bytes -= decrease_estimated_wal_bytes;
}

size_t WalDirInfo::available() {
    std::unique_lock wlock(_lock);
    int64_t available = _limit - _used - _estimated_wal_bytes;
    return available > 0 ? available : 0;
}

Status WalDirInfo::update_wal_dir_limit(size_t limit) {
    if (limit != static_cast<size_t>(-1)) {
        set_limit(limit);
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
        set_limit(wal_disk_limit);
    }
    return Status::OK();
}

Status WalDirInfo::update_wal_dir_used(size_t used) {
    if (used != static_cast<size_t>(-1)) {
        set_used(used);
    } else {
        size_t wal_dir_size = 0;
        try {
            RETURN_IF_ERROR(io::global_local_filesystem()->directory_size(_wal_dir, &wal_dir_size));
        } catch (const std::exception& e) {
            LOG(INFO) << "failed to update wal dir used info, err: {}", e.what();
            return Status::OK();
        }
        set_used(wal_dir_size);
    }
    return Status::OK();
}

void WalDirInfo::update_wal_dir_estimated_wal_bytes(size_t increase_estimated_wal_bytes,
                                                    size_t decrease_estimated_wal_bytes) {
    set_estimated_wal_bytes(increase_estimated_wal_bytes, decrease_estimated_wal_bytes);
}

std::string WalDirInfo::get_wal_dir_info_string() {
    return "[" + _wal_dir + ": limit " + std::to_string(_limit) + " Bytes, used " +
           std::to_string(_used) + " Bytes, estimated wal bytes " +
           std::to_string(_estimated_wal_bytes) + " Bytes, available " +
           std::to_string(available()) + " Bytes.]";
}

Status WalDirsInfo::add(const std::string& wal_dir, size_t limit, size_t used,
                        size_t estimated_wal_bytes) {
    for (const auto& it : _wal_dirs_info_vec) {
        if (it->get_wal_dir() == wal_dir) {
#ifdef BE_TEST
            return Status::OK();
#endif
            return Status::InternalError<false>("wal dir {} exists!", wal_dir);
        }
    }
    std::unique_lock wlock(_lock);
    _wal_dirs_info_vec.emplace_back(
            std::make_shared<WalDirInfo>(wal_dir, limit, used, estimated_wal_bytes));
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
            // if all wal dirs used space > wal dir limit * 80%, we use the max available wal dir.
            return (*std::max_element(_wal_dirs_info_vec.begin(), _wal_dirs_info_vec.end(),
                                      [](const auto& info1, const auto& info2) {
                                          return info1->available() < info2->available();
                                      }))
                    ->get_wal_dir();
        } else {
            // if there are wal dirs used space < wal dir limit * 80%, we use random wal dir.
            return (*std::next(available_wal_dirs.begin(), rand() % available_wal_dirs.size()));
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

std::string WalDirsInfo::get_wal_dirs_info_string() {
    std::string wal_dirs_info_string;
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        wal_dirs_info_string += wal_dir_info->get_wal_dir_info_string() + ";";
    }
    return wal_dirs_info_string;
}

Status WalDirsInfo::update_wal_dir_limit(const std::string& wal_dir, size_t limit) {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            return wal_dir_info->update_wal_dir_limit(limit);
        }
    }
    return Status::InternalError<false>("Can not find wal dir {} when update wal dir limit",
                                        wal_dir);
}

Status WalDirsInfo::update_all_wal_dir_limit() {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        RETURN_IF_ERROR(wal_dir_info->update_wal_dir_limit(-1));
    }
    return Status::OK();
}

Status WalDirsInfo::update_wal_dir_used(const std::string& wal_dir, size_t used) {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            return wal_dir_info->update_wal_dir_used(used);
        }
    }
    return Status::InternalError<false>("Can not find wal dir {} when update wal dir used",
                                        wal_dir);
}

Status WalDirsInfo::update_all_wal_dir_used() {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        RETURN_IF_ERROR(wal_dir_info->update_wal_dir_used(-1));
    }
    return Status::OK();
}

Status WalDirsInfo::update_wal_dir_estimated_wal_bytes(const std::string& wal_dir,
                                                       size_t increase_estimated_wal_bytes,
                                                       size_t decrease_estimated_wal_bytes) {
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            wal_dir_info->update_wal_dir_estimated_wal_bytes(increase_estimated_wal_bytes,
                                                             decrease_estimated_wal_bytes);
            return Status::OK();
        }
    }
    return Status::InternalError<false>(
            "Can not find wal dir {} when update wal dir estimated wal bytes", wal_dir);
}

Status WalDirsInfo::get_wal_dir_available_size(const std::string& wal_dir,
                                               size_t* available_bytes) {
    std::shared_lock l(_lock);
    for (const auto& wal_dir_info : _wal_dirs_info_vec) {
        if (wal_dir_info->get_wal_dir() == wal_dir) {
            *available_bytes = wal_dir_info->available();
            return Status::OK();
        }
    }
    return Status::InternalError<false>("Can not find wal dir {} when get wal dir available size",
                                        wal_dir);
}

Status WalDirsInfo::get_wal_dir_info(const std::string& wal_dir,
                                     std::shared_ptr<WalDirInfo>& wal_dir_info) {
    std::shared_lock l(_lock);
    auto it = std::find_if(_wal_dirs_info_vec.begin(), _wal_dirs_info_vec.end(),
                           [&wal_dir](auto w) { return w->get_wal_dir() == wal_dir; });
    if (it != _wal_dirs_info_vec.end()) {
        wal_dir_info = *it;
    } else {
        wal_dir_info = nullptr;
        return Status::InternalError<false>("Can not find wal dir {}", wal_dir);
    }
    return Status::OK();
}

} // namespace doris