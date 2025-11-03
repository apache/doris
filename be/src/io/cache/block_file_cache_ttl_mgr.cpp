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

#include "io/cache/block_file_cache_ttl_mgr.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "common/config.h"
#include "common/logging.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_block.h"
#include "util/time.h"

namespace doris::io {

BlockFileCacheTtlMgr::BlockFileCacheTtlMgr(BlockFileCache* mgr, CacheBlockMetaStore* meta_store)
        : _mgr(mgr), _meta_store(meta_store), _stop_background(false) {
    // Start background threads
    _update_ttl_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_update_ttl_info_map, this);
    _expiration_check_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_expiration_check, this);
}

BlockFileCacheTtlMgr::~BlockFileCacheTtlMgr() {
    _stop_background.store(true, std::memory_order_release);

    if (_update_ttl_thread.joinable()) {
        _update_ttl_thread.join();
    }

    if (_expiration_check_thread.joinable()) {
        _expiration_check_thread.join();
    }
}

void BlockFileCacheTtlMgr::register_tablet_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> lock(_tablet_id_mutex);
    _tablet_id_set.insert(tablet_id);
}

FileBlocks BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id(int64_t tablet_id) {
    FileBlocks result;

    // Use meta store to get all blocks for this tablet
    auto iterator = _meta_store->range_get(tablet_id);
    if (!iterator) {
        LOG(WARNING) << "Failed to get iterator for tablet_id: " << tablet_id;
        return result;
    }

    while (iterator->valid()) {
        BlockMetaKey key = iterator->key();
        BlockMeta meta = iterator->value();

        // Get all blocks for this hash using get_blocks_by_key
        try {
            auto blocks_map = _mgr->get_blocks_by_key(key.hash);
            for (const auto& [offset, block] : blocks_map) {
                // Only add blocks that match our specific offset
                if (offset == key.offset) {
                    result.push_back(block);
                    break;
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to get file blocks for tablet_id: " << tablet_id
                         << ", hash: " << key.hash.to_string() << ", error: " << e.what();
        }

        iterator->next();
    }

    return result;
}

void BlockFileCacheTtlMgr::run_backgroud_update_ttl_info_map() {
    Thread::set_self_name("ttl_mgr_update");

    while (!_stop_background.load(std::memory_order_acquire)) {
        try {
            std::unordered_set<int64_t> tablet_ids_to_process;

            // Copy tablet IDs to process
            {
                std::lock_guard<std::mutex> lock(_tablet_id_mutex);
                tablet_ids_to_process = _tablet_id_set;
            }

            for (int64_t tablet_id : tablet_ids_to_process) {
                // TODO(zhengyu): Implement actual CloudMetaMgr or CloudTabletMgr integration
                // For now, we'll use placeholder values
                uint64_t create_time = 0;
                uint64_t ttl = 0;

                // Simulate getting tablet metadata
                // This should be replaced with actual CloudMetaMgr::get_tablet_meta call
                bool has_ttl = (tablet_id % 10 == 0); // Example condition
                if (has_ttl) {
                    create_time = UnixSeconds(); // Current time as create time
                    ttl = 3600;                  // 1 hour TTL
                }

                // Update TTL info map
                {
                    std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                    if (ttl > 0) {
                        _ttl_info_map[tablet_id] = TtlInfo {ttl, create_time};

                        // If TTL changed from 0 to non-zero, convert blocks to TTL type
                        auto old_info_it = _ttl_info_map.find(tablet_id);
                        if (old_info_it == _ttl_info_map.end() || old_info_it->second.ttl == 0) {
                            FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
                            for (auto& block : blocks) {
                                if (block->cache_type() != FileCacheType::TTL) {
                                    block->change_cache_type(FileCacheType::TTL);
                                }
                            }
                        }
                    } else {
                        // Remove from TTL map if TTL is 0
                        _ttl_info_map.erase(tablet_id);
                    }
                }
            }

            // Sleep for configured interval (use existing TTL GC interval)
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::file_cache_background_ttl_gc_interval_ms));

        } catch (const std::exception& e) {
            LOG(WARNING) << "Exception in TTL update thread: " << e.what();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

void BlockFileCacheTtlMgr::run_backgroud_expiration_check() {
    Thread::set_self_name("ttl_mgr_expire");

    while (!_stop_background.load(std::memory_order_acquire)) {
        try {
            std::unordered_map<int64_t, TtlInfo> ttl_info_copy;

            // Copy TTL info for processing
            {
                std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                ttl_info_copy = _ttl_info_map;
            }

            uint64_t current_time = UnixSeconds();

            for (const auto& [tablet_id, ttl_info] : ttl_info_copy) {
                if (ttl_info.create_time + ttl_info.ttl < current_time) {
                    // Tablet has expired, convert TTL blocks back to NORMAL type
                    FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
                    for (auto& block : blocks) {
                        if (block->cache_type() == FileCacheType::TTL) {
                            block->change_cache_type(FileCacheType::NORMAL);
                        }
                    }

                    // Remove from TTL map
                    std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                    _ttl_info_map.erase(tablet_id);
                }
            }

            // Sleep for configured interval (use existing TTL GC interval)
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::file_cache_background_ttl_gc_interval_ms));

        } catch (const std::exception& e) {
            LOG(WARNING) << "Exception in TTL expiration check thread: " << e.what();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

} // namespace doris::io