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

#pragma once

#include <gen_cpp/olap_file.pb.h>

#include <chrono>
#include <iosfwd>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/string_util.h"

namespace doris::cloud {
enum class DeleteBitmapLockVersion { V1, V2 };

std::ostream& operator<<(std::ostream& os, DeleteBitmapLockVersion version);

class DeleteBitmapLockWhiteList {
public:
    DeleteBitmapLockWhiteList();
    ~DeleteBitmapLockWhiteList();
    void init();
    void update_white_list(std::string white_list);
    DeleteBitmapLockVersion get_delete_bitmap_lock_version(const std::string& instance_id);

    template <typename LockContext>
    DeleteBitmapLockVersion get_delete_bitmap_lock_version(const std::string& instance_id,
                                                           const LockContext& context) {
        if (context.has_delete_bitmap_lock_version()) {
            return context.delete_bitmap_lock_version() == DELETE_BITMAP_LOCK_VERSION_V2
                           ? DeleteBitmapLockVersion::V2
                           : DeleteBitmapLockVersion::V1;
        }
        return get_delete_bitmap_lock_version(instance_id);
    }

private:
    std::set<std::string> _delete_bitmap_lock_v2_white_list_set;
    std::string _last_white_list_value;
    std::shared_mutex _rw_mutex;
};

} // namespace doris::cloud
