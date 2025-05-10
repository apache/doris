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

#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/config.h"
#include "common/logging.h"
#include "common/string_util.h"

namespace doris::cloud {
class DeleteBitmapLockWhiteList;
class DeleteBitmapLockWhiteList {
public:
    DeleteBitmapLockWhiteList();
    ~DeleteBitmapLockWhiteList();
    void init();
    void update_white_list();
    std::string get_delete_bitmap_lock_version(std::string instance_id);

private:
    std::unordered_map<std::string, std::string> _lock_version_white_list;
    std::atomic_bool _update_thread_run = true;
    std::unique_ptr<std::thread> _update_thread;
};

} // namespace doris::cloud
