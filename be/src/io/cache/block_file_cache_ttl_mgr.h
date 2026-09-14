

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

#include <bvar/bvar.h>
#include <concurrentqueue.h>

#include <array>
#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"

namespace doris::io {

class BlockFileCache;
class CacheBlockMetaStore;

struct TtlInfo {
    uint64_t ttl = 0;
    uint64_t tablet_ctime = 0;
    // Cache type last applied to this tablet's blocks. The manager converts blocks only on a
    // state transition, so the applied state has to be remembered explicitly: whether an entry
    // exists cannot tell "this tablet never had a TTL" apart from "its TTL already expired and
    // the blocks were demoted", and those two need opposite handling when a new TTL arrives.
    bool blocks_are_ttl = false;

    // Whether this tablet's blocks belong in the TTL queue right now.
    bool is_ttl_active(uint64_t now) const {
        if (ttl == 0 || tablet_ctime == 0) {
            return false;
        }
        if (tablet_ctime > std::numeric_limits<uint64_t>::max() - ttl) {
            return false;
        }
        return tablet_ctime + ttl >= now;
    }
};

class BlockFileCacheTtlMgr {
public:
    BlockFileCacheTtlMgr(BlockFileCache* mgr, CacheBlockMetaStore* meta_store);
    ~BlockFileCacheTtlMgr();

    void register_tablet_id(int64_t tablet_id);
    void stop();
    void resume();

    // Background thread to update ttl_info_map
    void run_backgroud_update_ttl_info_map();
    // Background thread to find expired tablet and evict from ttl queue
    void run_backgroud_expiration_check();
    // Background thread to drain the concurrent tablet-id queue into the dedup set
    void run_background_tablet_id_flush();

private:
    FileBlocks get_file_blocks_from_tablet_id(int64_t tablet_id);

    // Drive this tablet's cached blocks to the cache type its current TTL state asks for.
    // Both background threads funnel through here and it is serialized per tablet, so they can
    // never scan the same tablet concurrently and leave the blocks in whatever type the scan
    // that happened to finish last wrote. force_demote_scan additionally scans a tablet the map
    // does not track, which is how TTL blocks restored from persisted metadata are cleaned up.
    void reconcile_tablet_blocks(int64_t tablet_id, bool force_demote_scan);

    std::mutex& transition_lock_for(int64_t tablet_id) {
        return _transition_locks[static_cast<uint64_t>(tablet_id) % kTransitionLockStripes];
    }

private:
    // Tablet ids waiting to be deduplicated + set of unique ids known to have cached data
    moodycamel::ConcurrentQueue<int64_t> _tablet_id_queue;
    std::unordered_set<int64_t> _tablet_id_set; // TODO(zhengyu): clean up old tablet ids
    std::mutex _tablet_id_mutex;

    std::map<int64_t /* tablet_id */, TtlInfo> _ttl_info_map;
    BlockFileCache* _mgr;
    CacheBlockMetaStore* _meta_store;

    std::atomic<bool> _stop_background;
    std::thread _update_ttl_thread;
    std::thread _expiration_check_thread;
    std::thread _tablet_id_flush_thread;
    std::mutex _thread_lifecycle_mutex;

    std::mutex _ttl_info_mutex;

    // Striped locks serializing block conversions per tablet. Lock order is always
    // _transition_locks[i] -> _ttl_info_mutex; _ttl_info_mutex is never held across a block scan.
    static constexpr size_t kTransitionLockStripes = 64;
    std::array<std::mutex, kTransitionLockStripes> _transition_locks;

    std::shared_ptr<bvar::Status<size_t>> _tablet_id_set_size_metrics;
};

} // namespace doris::io
