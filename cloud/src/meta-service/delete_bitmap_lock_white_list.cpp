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

DeleteBitmapLockWhiteList::~DeleteBitmapLockWhiteList() {}

void DeleteBitmapLockWhiteList::init() {
    update_white_list(config::delete_bitmap_lock_v2_white_list);
}

void DeleteBitmapLockWhiteList::update_white_list(std::string white_list) {
    std::unique_lock<std::shared_mutex> lock(_rw_mutex);
    _delete_bitmap_lock_v2_white_list_set.clear();
    if (!white_list.empty()) {
        auto white_list_vector = split(white_list, ';');
        for (auto& item : white_list_vector) {
            if (!item.empty()) {
                trim(item);
                _delete_bitmap_lock_v2_white_list_set.insert(item);
            }
        }
    }
    _last_white_list_value = white_list;
}

std::string DeleteBitmapLockWhiteList::get_delete_bitmap_lock_version(std::string instance_id) {
    if (config::use_delete_bitmap_lock_random_version) {
        if (use_new_version_random()) {
            return "v2";
        } else {
            return "v1";
        }
    }
    std::string white_list;
    {
        std::shared_lock<std::shared_mutex> lock(*config::get_mutable_string_config_lock());
        white_list = config::delete_bitmap_lock_v2_white_list;
    }
    if (white_list == "*") {
        return "v2";
    }
    if (_last_white_list_value != white_list) {
        update_white_list(white_list);
    }
    {
        std::shared_lock<std::shared_mutex> lock(_rw_mutex);
        auto it = _delete_bitmap_lock_v2_white_list_set.find(instance_id);
        if (it != _delete_bitmap_lock_v2_white_list_set.end()) {
            return "v2";
        } else {
            return "v1";
        }
    }
}
} // namespace doris::cloud