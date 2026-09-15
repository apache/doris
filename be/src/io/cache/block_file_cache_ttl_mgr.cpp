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
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/file_block.h"
#include "runtime/exec_env.h"
#include "storage/tablet/base_tablet.h"
#include "util/time.h"

namespace doris::io {

BlockFileCacheTtlMgr::BlockFileCacheTtlMgr(BlockFileCache* mgr, CacheBlockMetaStore* meta_store)
        : _mgr(mgr), _meta_store(meta_store), _stop_background(false) {
    _tablet_id_set_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _mgr->get_base_path().c_str(), "file_cache_ttl_mgr_tablet_id_set_size", 0);
    _ttl_info_map_size_metrics = std::make_shared<bvar::Status<size_t>>(
            _mgr->get_base_path().c_str(), "file_cache_ttl_mgr_ttl_info_map_size", 0);
    resume();
}

BlockFileCacheTtlMgr::~BlockFileCacheTtlMgr() {
    stop();
}

void BlockFileCacheTtlMgr::stop() {
    std::lock_guard<std::mutex> lifecycle_lock(_thread_lifecycle_mutex);
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

void BlockFileCacheTtlMgr::resume() {
    std::lock_guard<std::mutex> lifecycle_lock(_thread_lifecycle_mutex);
    if (_update_ttl_thread.joinable() || _expiration_check_thread.joinable() ||
        _tablet_id_flush_thread.joinable()) {
        return;
    }

    _stop_background.store(false, std::memory_order_release);
    _update_ttl_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_update_ttl_info_map, this);
    _expiration_check_thread =
            std::thread(&BlockFileCacheTtlMgr::run_backgroud_expiration_check, this);
    _tablet_id_flush_thread =
            std::thread(&BlockFileCacheTtlMgr::run_background_tablet_id_flush, this);
}

void BlockFileCacheTtlMgr::register_tablet_id(int64_t tablet_id) {
    _tablet_id_queue.enqueue(tablet_id);
}

void BlockFileCacheTtlMgr::update_ttl_info_map_size_metrics() {
    if (_ttl_info_map_size_metrics) {
        _ttl_info_map_size_metrics->set_value(_ttl_info_map.size());
    }
}

size_t BlockFileCacheTtlMgr::tracked_tablet_num() {
    std::lock_guard<std::mutex> lock(_ttl_info_mutex);
    return _ttl_info_map.size();
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
    TEST_SYNC_POINT_CALLBACK("BlockFileCacheTtlMgr::get_file_blocks_from_tablet_id", tablet_id);

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

void BlockFileCacheTtlMgr::reconcile_tablet_blocks(int64_t tablet_id, bool force_scan) {
    // Serialize all conversions of this tablet. Whichever caller takes this lock last re-reads
    // the current state below and has the final say, so the update and expiration threads cannot
    // fight over the same blocks and strand them in the loser's cache type.
    std::lock_guard<std::mutex> transition_lock(transition_lock_for(tablet_id));

    bool want_ttl = false;
    // Unknown when the tablet is not tracked at all, which is also how a tablet looks right
    // after a restart: the blocks on disk keep their type, _ttl_info_map does not.
    std::optional<bool> blocks_are_ttl;
    bool settled = false;
    {
        // Deliberately re-read the map rather than trust what the caller saw: the expiration
        // thread picks its candidates up to a full gc interval before getting here.
        std::lock_guard<std::mutex> lock(_ttl_info_mutex);
        auto it = _ttl_info_map.find(tablet_id);
        if (it != _ttl_info_map.end()) {
            if (it->second.ttl == 0 && it->second.blocks_are_ttl == false) {
                // No TTL left and the blocks are known to be normal: nothing to track. Dropped
                // here rather than after a conversion, so that a tablet which settles without
                // needing one still stops being tracked and goes back to the cheap path where
                // it is only visited by the periodic reconcile.
                _ttl_info_map.erase(it);
                update_ttl_info_map_size_metrics();
                settled = true;
            } else {
                want_ttl = it->second.is_ttl_active(UnixSeconds());
                blocks_are_ttl = it->second.blocks_are_ttl;
            }
        }
    }

    // Nothing to apply. Note this is a state comparison, not a comparison of TTL values: a
    // tablet whose TTL is rewritten to another still-valid value keeps its blocks where they
    // are, instead of rescanning the whole tablet every time the property is touched. An
    // unknown applied state never matches, so it always costs one scan.
    if (settled || (!force_scan && blocks_are_ttl == want_ttl)) {
        return;
    }

    // Scan and convert outside _ttl_info_mutex: this walks the meta store and takes the cache
    // lock once per block, which is far too long to hold a mutex the other thread needs.
    const auto target_type = want_ttl ? FileCacheType::TTL : FileCacheType::NORMAL;
    FileBlocks blocks = get_file_blocks_from_tablet_id(tablet_id);
    size_t converted = 0;
    bool all_converted = true;
    for (auto& block : blocks) {
        if (block->cache_type() == target_type) {
            continue;
        }
        auto st = block->change_cache_type(target_type);
        if (st.ok()) {
            ++converted;
        } else {
            all_converted = false;
            LOG(WARNING) << "Failed to convert block to " << cache_type_to_string(target_type)
                         << " cache_type, tablet_id=" << tablet_id << ", err=" << st;
        }
    }
    if (converted > 0) {
        LOG(INFO) << "converted cached blocks to " << cache_type_to_string(target_type)
                  << ", tablet_id=" << tablet_id << ", block_num=" << converted
                  << ", scanned=" << blocks.size();
    }

    {
        std::lock_guard<std::mutex> lock(_ttl_info_mutex);
        auto it = _ttl_info_map.find(tablet_id);
        if (it != _ttl_info_map.end() && all_converted) {
            // Left alone when a block failed to convert, so that the mismatch is still visible
            // to the next round and gets retried instead of being recorded as done.
            it->second.blocks_are_ttl = want_ttl;
        }
    }
}

void BlockFileCacheTtlMgr::run_backgroud_update_ttl_info_map() {
    Thread::set_self_name("ttl_mgr_update");

    static constexpr uint64_t kFullReconcileIntervalRounds = 20;
    uint64_t update_round = 0;

    while (!_stop_background.load(std::memory_order_acquire)) {
        try {
            const bool need_full_reconcile = (++update_round % kFullReconcileIntervalRounds) == 0;
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
                            if (_ttl_info_map.erase(tablet_id) > 0) {
                                update_ttl_info_map_size_metrics();
                            }
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

                // Record the TTL this tablet currently has, then let reconcile_tablet_blocks()
                // decide whether that moves its blocks between the TTL and normal queues.
                bool tracked = false;
                {
                    std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                    auto it = _ttl_info_map.find(tablet_id);
                    if (ttl > 0) {
                        if (it == _ttl_info_map.end()) {
                            // blocks_are_ttl stays unknown: what the blocks already are is
                            // whatever a previous run of this BE left on disk.
                            _ttl_info_map.emplace(tablet_id, TtlInfo {ttl, tablet_ctime});
                            update_ttl_info_map_size_metrics();
                        } else {
                            // Keep blocks_are_ttl: it describes the blocks, not the tablet meta.
                            it->second.ttl = ttl;
                            it->second.tablet_ctime = tablet_ctime;
                        }
                        tracked = true;
                    } else if (it != _ttl_info_map.end()) {
                        // Hold on to the entry until the blocks are actually demoted. It carries
                        // the blocks_are_ttl bit reconcile_tablet_blocks() needs, and drops
                        // itself once the tablet has settled back to NORMAL.
                        it->second.ttl = 0;
                        tracked = true;
                    }
                }

                // An untracked tablet has no recorded state, so reconciling it scans once and
                // demotes anything restored from persisted TTL metadata. Gated on the periodic
                // round so that ordinary non-TTL tablets are not walked every time.
                if (tracked || need_full_reconcile) {
                    reconcile_tablet_blocks(tablet_id, /*force_scan=*/false);
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
            // Collect tablets whose TTL has run out.
            std::vector<int64_t> expired_tablet_ids;
            {
                std::lock_guard<std::mutex> lock(_ttl_info_mutex);
                uint64_t current_time = UnixSeconds();
                for (const auto& [tablet_id, ttl_info] : _ttl_info_map) {
                    if (ttl_info.ttl > 0 && !ttl_info.is_ttl_active(current_time)) {
                        expired_tablet_ids.push_back(tablet_id);
                    }
                }
            }

            // Swept on every round rather than once, because an expired tablet keeps producing
            // TTL blocks for as long as it is read: the read path derives the cache type from
            // ttl_seconds alone and never looks at whether the TTL has passed. Hence force_scan
            // -- the tablet-level state says the blocks were demoted already and would
            // otherwise skip the walk that collects the new ones.
            //
            // The list above is only a candidate set. reconcile_tablet_blocks() re-reads the
            // tablet's state under the per-tablet lock, so a TTL extended in between is never
            // demoted on the strength of what was observed here.
            for (int64_t tablet_id : expired_tablet_ids) {
                if (_stop_background.load(std::memory_order_acquire)) {
                    break;
                }
                reconcile_tablet_blocks(tablet_id, /*force_scan=*/true);
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
