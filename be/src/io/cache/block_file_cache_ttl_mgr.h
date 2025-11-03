

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

#include <atomic>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_set>

#include "io/cache/file_cache_common.h"

namespace doris::io {

class BlockFileCache;
class CacheBlockMetaStore;

struct TtlInfo {
    uint64_t ttl;
    uint64_t create_time;
};

class BlockFileCacheTtlMgr {
public:
    BlockFileCacheTtlMgr(BlockFileCache* mgr, CacheBlockMetaStore* meta_store);
    ~BlockFileCacheTtlMgr();

    void register_tablet_id(int64_t tablet_id);

    // Background thread functions
    void run_backgroud_update_ttl_info_map();
    void run_backgroud_expiration_check();

private:
    FileBlocks get_file_blocks_from_tablet_id(int64_t tablet_id);

private:
    std::unordered_set<int64_t> _tablet_id_set;
    std::map<int64_t, TtlInfo> _ttl_info_map;
    BlockFileCache* _mgr;
    CacheBlockMetaStore* _meta_store;

    std::atomic<bool> _stop_background;
    std::thread _update_ttl_thread;
    std::thread _expiration_check_thread;

    std::mutex _tablet_id_mutex;
    std::mutex _ttl_info_mutex;
};

} // namespace doris::io