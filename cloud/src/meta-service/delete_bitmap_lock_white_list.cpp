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

#include "delete_bitmap_lock_white_list.h"

namespace doris::cloud {

bool use_new_version_random() {
    std::mt19937 gen {std::random_device {}()};
    auto p = 0.5;
    std::bernoulli_distribution inject_fault {p};
    if (inject_fault(gen)) {
        return true;
    }
    return false;
}

DeleteBitmapLockWhiteList::DeleteBitmapLockWhiteList() {}

DeleteBitmapLockWhiteList::~DeleteBitmapLockWhiteList() {
    _update_thread_run = false;
    if (_update_thread->joinable()) {
        _update_thread->join();
    }
}

void DeleteBitmapLockWhiteList::init() {
    _update_thread = std::make_unique<std::thread>([this]() {
        while (_update_thread_run) {
            update_white_list();
            std::this_thread::sleep_for(std::chrono::seconds(
                    config::update_delete_bitmap_lock_white_list_interval_seconds));
        }
    });
}

void DeleteBitmapLockWhiteList::update_white_list() {
    std::string white_list = config::delete_bitmap_lock_version_white_list;
    if (!white_list.empty()) {
        auto white_list_vector = split(white_list, ';');
        for (auto& item : white_list_vector) {
            auto v = split(item, ':');
            if (v.size() != 2) {
                LOG(WARNING) << "failed to split config=" << item << ",white list=" << white_list;
                continue;
            }
            trim(v[0]);
            trim(v[1]);
            if (v[1] == "v1" || v[1] == "v2") {
                _lock_version_white_list[v[0]] = v[1];
            }
        }
    }
}

std::string DeleteBitmapLockWhiteList::get_delete_bitmap_lock_version(std::string instance_id) {
    std::string default_version = config::use_delete_bitmap_lock_version;
    if (config::use_delete_bitmap_lock_random_version) {
        if (use_new_version_random()) {
            return "v1";
        } else {
            return "v2";
        }
    }
    auto it = _lock_version_white_list.find(instance_id);
    if (it != _lock_version_white_list.end()) {
        return it->second;
    }
    return default_version;
}
} // namespace doris::cloud