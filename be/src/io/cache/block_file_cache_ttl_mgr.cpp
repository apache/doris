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

#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_block.h"
#include "olap/base_tablet.h"
#include "runtime/exec_env.h"
#include "util/time.h"

namespace doris::io {

BlockFileCacheTtlMgr::BlockFileCacheTtlMgr(BlockFileCache* mgr, CacheBlockMetaStore* meta_store)
        : _mgr(mgr), _meta_store(meta_store), _stop_background(false) {
    _tablet_id_set_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _mgr->get_base_path().c_str(), "file_cache_ttl_mgr_tablet_id_set_size", 0);
    // Start background threads
    _update_ttl_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_update_ttl_info_map, this);
    _expiration_check_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_expiration_check, this);
    _tablet_id_flush_thread =
            std::thread(&BlockFileCacheTtlMgr::run_background_tablet_id_flush, this);
}

BlockFileCacheTtlMgr::~BlockFileCacheTtlMgr() {
    _stop_background.store(true, std::memory_order_release);

    if (_update_ttl_thread.joinable()) {
        _update_ttl_thread.join();
    }

    if (_expiration_check_thread.joinable()) {
        _expiration_check_thread.join();
    }

    if (_tablet_id_flush_thread.joinable()) {
        _tablet_id_flush_thread.join();
    }
}

void BlockFileCacheTtlMgr::register_tablet_id(int64_t tablet_id) {
    _tablet_id_queue.enqueue(tablet_id);
}

void BlockFileCacheTtlMgr::run_background_tablet_id_flush() {
    Thread::set_self_name("ttl_mgr_flush");

    static constexpr size_t kBatchSize = 1024;
    std::vector<int64_t> pending;
    pending.reserve(kBatchSize);

    auto flush_pending = [this](std::vector<int64_t>* items) {
        if (items->empty()) {
            return;
        }
        std::lock_guard<std::mutex> lock(_tablet_id_mutex);
        _tablet_id_set.insert(items->begin(), items->end());
        if (_tablet_id_set_size_metrics) {
            _tablet_id_set_size_metrics->set_value(_tablet_id_set.size());
        }
        items->clear();
    };

    auto drain_queue = [this, &pending, &flush_pending](bool* drained_flag) {
        int64_t tablet_id = 0;
        while (_tablet_id_queue.try_dequeue(tablet_id)) {
            if (drained_flag != nullptr) {
                *drained_flag = true;
            }
            pending.push_back(tablet_id);
            if (pending.size() >= kBatchSize) {
                flush_pending(&pending);
            }
        }
    };

    while (!_stop_background.load(std::memory_order_acquire)) {
        bool drained = false;
        drain_queue(&drained);
        flush_pending(&pending);

        if (!drained) {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                    config::file_cache_background_tablet_id_flush_interval_ms));
        }
    }

    // Drain remaining items before exit
    drain_queue(nullptr);
    flush_pending(&pending);
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
            {
                std::lock_guard<std::mutex> lock(_tablet_id_mutex);
                tablet_ids_to_process = _tablet_id_set;
            }

            for (int64_t tablet_id : tablet_ids_to_process) {
                if (_stop_background.load(std::memory_order_acquire)) {
                    break;
                }
                uint64_t tablet_ctime = 0;
                uint64_t ttl = 0;

                TabletMetaSharedPtr tablet_meta;
                auto meta_status = ExecEnv::get_tablet_meta(tablet_id, &tablet_meta, false);
                if (!meta_status.ok()) {
                    if (meta_status.is<ErrorCode::NOT_FOUND>()) {
                        {
                            std::lock_guard<std::mutex> lock(_tablet_id_mutex);
                            if (_tablet_id_set.erase(tablet_id) > 0 &&
                                _tablet_id_set_size_metrics) {
                                _tablet_id_set_size_metrics->set_value(_tablet_id_set.size());
                            }
                        }
                        {
                            std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                            _ttl_info_map.erase(tablet_id);
                        }
                    } else {
                        LOG(WARNING) << "Failed to get tablet meta for tablet_id: " << tablet_id
                                     << ", err: " << meta_status;
                    }
                    continue;
                }

                if (tablet_meta != nullptr) {
                    tablet_ctime = tablet_meta->creation_time();
                    int64_t ttl_seconds = tablet_meta->ttl_seconds();
                    if (ttl_seconds > 0 && tablet_ctime > 0) {
                        ttl = static_cast<uint64_t>(ttl_seconds);
                    }
                }

                // Update TTL info map
                bool need_convert_from_ttl = false;
                {
                    std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                    if (ttl > 0) {
                        auto old_info_it = _ttl_info_map.find(tablet_id);
                        bool was_zero_ttl = (old_info_it == _ttl_info_map.end() ||
                                             old_info_it->second.ttl == 0);
                        _ttl_info_map[tablet_id] = TtlInfo {ttl, tablet_ctime};

                        // If TTL changed from 0 to non-zero, convert blocks to TTL type
                        if (was_zero_ttl) {
                            FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
                            for (auto& block : blocks) {
                                if (block->cache_type() != FileCacheType::TTL) {
                                    auto change_status =
                                            block->change_cache_type(FileCacheType::TTL);
                                    if (!change_status.ok()) {
                                        LOG(WARNING) << "Failed to convert block to TTL cache_type";
                                    }
                                }
                            }
                        }
                    } else {
                        // Remove from TTL map if TTL is 0
                        _ttl_info_map.erase(tablet_id);
                        need_convert_from_ttl = true;
                    }
                }

                if (need_convert_from_ttl) {
                    FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
                    for (auto& block : blocks) {
                        if (block->cache_type() == FileCacheType::TTL) {
                            auto st = block->change_cache_type(FileCacheType::NORMAL);
                            if (!st.ok()) {
                                LOG(WARNING) << "Failed to convert block back to NORMAL cache_type";
                            }
                        }
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(
                    config::file_cache_background_ttl_info_update_interval_ms));

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
            std::map<int64_t, TtlInfo> ttl_info_copy;

            // Copy TTL info for processing
            {
                std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                ttl_info_copy = _ttl_info_map;
            }

            uint64_t current_time = UnixSeconds();

            for (const auto& [tablet_id, ttl_info] : ttl_info_copy) {
                if (ttl_info.tablet_ctime + ttl_info.ttl < current_time) {
                    // Tablet has expired, convert TTL blocks back to NORMAL type
                    FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
                    for (auto& block : blocks) {
                        if (block->cache_type() == FileCacheType::TTL) {
                            auto st = block->change_cache_type(FileCacheType::NORMAL);
                            if (!st.ok()) {
                                LOG(WARNING) << "Failed to convert block back to NORMAL cache_type";
                            }
                        }
                    }
                }
            }

            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::file_cache_background_ttl_gc_interval_ms));

        } catch (const std::exception& e) {
            LOG(WARNING) << "Exception in TTL expiration check thread: " << e.what();
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

} // namespace doris::io