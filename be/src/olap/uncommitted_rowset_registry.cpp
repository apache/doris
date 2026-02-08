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

#include "olap/uncommitted_rowset_registry.h"

#include <algorithm>

#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "olap/base_tablet.h"
#include "olap/calc_delete_bitmap_executor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

UncommittedRowsetRegistry::UncommittedRowsetRegistry() = default;

UncommittedRowsetRegistry::~UncommittedRowsetRegistry() {
    if (_dedup_thread_pool) {
        _dedup_thread_pool->shutdown();
    }
}

Status UncommittedRowsetRegistry::init(int dedup_threads) {
    return ThreadPoolBuilder("UncommittedDedupPool")
            .set_min_threads(1)
            .set_max_threads(dedup_threads)
            .build(&_dedup_thread_pool);
}

std::shared_ptr<std::mutex> UncommittedRowsetRegistry::_get_tablet_dedup_mutex(
        Shard& shard, int64_t tablet_id) {
    auto it = shard.tablet_dedup_mutex.find(tablet_id);
    if (it == shard.tablet_dedup_mutex.end()) {
        auto mtx = std::make_shared<std::mutex>();
        shard.tablet_dedup_mutex[tablet_id] = mtx;
        return mtx;
    }
    return it->second;
}

void UncommittedRowsetRegistry::register_rowset(std::shared_ptr<UncommittedRowsetEntry> entry) {
    int64_t tablet_id = entry->tablet_id;
    auto& shard = _get_shard(tablet_id);

    {
        std::lock_guard wlock(shard.lock);
        auto& entries = shard.entries[tablet_id];
        entries.push_back(entry);
        // Keep entries sorted by creation_time for correct dedup ordering
        std::sort(entries.begin(), entries.end(),
                  [](const auto& a, const auto& b) {
                      return a->creation_time < b->creation_time;
                  });
    }

    // For DUP_KEYS tables, no dedup needed — mark ready immediately
    if (!entry->unique_key_merge_on_write) {
        entry->dedup_ready.store(true, std::memory_order_release);
        return;
    }

    _submit_dedup_task(tablet_id, entry);
}

void UncommittedRowsetRegistry::unregister_rowset(int64_t tablet_id, int64_t transaction_id) {
    bool has_remaining_mow = false;
    {
        auto& shard = _get_shard(tablet_id);
        std::lock_guard wlock(shard.lock);

        auto it = shard.entries.find(tablet_id);
        if (it == shard.entries.end()) {
            return;
        }

        auto& entries = it->second;
        entries.erase(std::remove_if(entries.begin(), entries.end(),
                                      [transaction_id](const auto& e) {
                                          return e->transaction_id == transaction_id;
                                      }),
                      entries.end());

        if (entries.empty()) {
            shard.entries.erase(it);
            shard.tablet_dedup_mutex.erase(tablet_id);
        } else {
            // Check if remaining entries have MoW that need recomputation
            for (const auto& entry : entries) {
                if (entry->unique_key_merge_on_write) {
                    has_remaining_mow = true;
                    break;
                }
            }
        }
    }

    // Published rowset set changed — recompute bitmaps for remaining MoW entries.
    // Must be outside the lock since recomputation is async and acquires its own locks.
    if (has_remaining_mow) {
        _recompute_all_bitmaps(tablet_id);
    }
}

void UncommittedRowsetRegistry::get_ready_rowsets(
        int64_t tablet_id, std::vector<std::shared_ptr<UncommittedRowsetEntry>>* result) {
    auto& shard = _get_shard(tablet_id);
    std::shared_lock rlock(shard.lock);

    auto it = shard.entries.find(tablet_id);
    if (it == shard.entries.end()) {
        return;
    }

    for (const auto& entry : it->second) {
        if (entry->dedup_ready.load(std::memory_order_acquire)) {
            result->push_back(entry);
        }
    }
}

void UncommittedRowsetRegistry::on_compaction_completed(int64_t tablet_id) {
    auto& shard = _get_shard(tablet_id);
    bool has_mow_entries = false;

    {
        std::shared_lock rlock(shard.lock);
        auto it = shard.entries.find(tablet_id);
        if (it == shard.entries.end()) {
            return;
        }

        for (const auto& entry : it->second) {
            if (entry->unique_key_merge_on_write) {
                has_mow_entries = true;
                break;
            }
        }
    }

    // Published rowsets changed — recompute both committed and cross bitmaps
    if (has_mow_entries) {
        _recompute_all_bitmaps(tablet_id);
    }
}

void UncommittedRowsetRegistry::on_tablet_state_change(int64_t tablet_id,
                                                        TabletState new_state) {
    if (new_state == TABLET_RUNNING) {
        return; // Only clear on non-running states
    }

    auto& shard = _get_shard(tablet_id);
    std::lock_guard wlock(shard.lock);
    shard.entries.erase(tablet_id);
    shard.tablet_dedup_mutex.erase(tablet_id);
}

void UncommittedRowsetRegistry::_submit_dedup_task(
        int64_t tablet_id, std::shared_ptr<UncommittedRowsetEntry> entry) {
    if (!_dedup_thread_pool) {
        // No thread pool available, mark ready without cross-bitmap
        entry->dedup_ready.store(true, std::memory_order_release);
        return;
    }

    // Get the per-tablet dedup mutex
    std::shared_ptr<std::mutex> dedup_mutex;
    {
        auto& shard = _get_shard(tablet_id);
        std::lock_guard wlock(shard.lock);
        dedup_mutex = _get_tablet_dedup_mutex(shard, tablet_id);
    }

    // Collect earlier uncommitted rowsets for this tablet
    std::vector<RowsetSharedPtr> earlier_rowsets;
    {
        auto& shard = _get_shard(tablet_id);
        std::shared_lock rlock(shard.lock);
        auto it = shard.entries.find(tablet_id);
        if (it != shard.entries.end()) {
            for (const auto& other : it->second) {
                if (other->creation_time < entry->creation_time &&
                    other->unique_key_merge_on_write) {
                    earlier_rowsets.push_back(other->rowset);
                }
            }
        }
    }

    // If no earlier uncommitted MoW rowsets, no cross-dedup needed
    if (earlier_rowsets.empty()) {
        entry->dedup_ready.store(true, std::memory_order_release);
        return;
    }

    // Submit async task to compute cross-uncommitted delete bitmap
    auto st = _dedup_thread_pool->submit_func(
            [this, tablet_id, entry, earlier_rowsets = std::move(earlier_rowsets),
             dedup_mutex]() mutable {
                // Serialize dedup per tablet
                std::lock_guard tablet_lock(*dedup_mutex);

                // Check if entry was unregistered while waiting
                {
                    auto& shard = _get_shard(tablet_id);
                    std::shared_lock rlock(shard.lock);
                    auto it = shard.entries.find(tablet_id);
                    if (it == shard.entries.end()) {
                        return;
                    }
                    bool found = false;
                    for (const auto& e : it->second) {
                        if (e->transaction_id == entry->transaction_id) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        return;
                    }
                }

                // Get tablet for bitmap computation
                auto tablet_result =
                        ExecEnv::GetInstance()->storage_engine().get_tablet(tablet_id);
                if (!tablet_result.has_value()) {
                    LOG(WARNING) << "Failed to get tablet " << tablet_id
                                 << " for cross-uncommitted dedup, marking ready without "
                                    "cross-bitmap";
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                auto tablet = tablet_result.value();

                // Load segments from the rowset
                auto* beta_rowset = dynamic_cast<BetaRowset*>(entry->rowset.get());
                if (!beta_rowset) {
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                std::vector<segment_v2::SegmentSharedPtr> segments;
                auto load_st = beta_rowset->load_segments(&segments);
                if (!load_st.ok()) {
                    LOG(WARNING) << "Failed to load segments for cross-uncommitted dedup, "
                                    "tablet_id="
                                 << tablet_id << " txn_id=" << entry->transaction_id
                                 << ": " << load_st;
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }

                if (segments.empty()) {
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }

                // Use CalcDeleteBitmapExecutor for the actual computation
                auto* calc_executor =
                        ExecEnv::GetInstance()->storage_engine().calc_delete_bitmap_executor();
                if (!calc_executor) {
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                auto token = calc_executor->create_token();

                // Compute cross-uncommitted delete bitmap
                auto cross_bitmap = std::make_shared<DeleteBitmap>(tablet_id);
                auto calc_st = BaseTablet::calc_delete_bitmap(
                        tablet, entry->rowset, segments, earlier_rowsets, cross_bitmap,
                        DeleteBitmap::TEMP_VERSION_COMMON, token.get());
                if (calc_st.ok()) {
                    calc_st = token->wait();
                }

                if (calc_st.ok()) {
                    entry->cross_delete_bitmap = cross_bitmap;
                } else {
                    LOG(WARNING) << "Failed to compute cross-uncommitted delete bitmap, "
                                    "tablet_id="
                                 << tablet_id << " txn_id=" << entry->transaction_id
                                 << ": " << calc_st;
                }

                entry->dedup_ready.store(true, std::memory_order_release);
            });

    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit cross-uncommitted dedup task, tablet_id=" << tablet_id
                     << " txn_id=" << entry->transaction_id << ": " << st;
        entry->dedup_ready.store(true, std::memory_order_release);
    }
}

void UncommittedRowsetRegistry::_submit_full_recompute_task(
        int64_t tablet_id, std::shared_ptr<UncommittedRowsetEntry> entry) {
    if (!_dedup_thread_pool) {
        entry->dedup_ready.store(true, std::memory_order_release);
        return;
    }

    // Get the per-tablet dedup mutex
    std::shared_ptr<std::mutex> dedup_mutex;
    {
        auto& shard = _get_shard(tablet_id);
        std::lock_guard wlock(shard.lock);
        dedup_mutex = _get_tablet_dedup_mutex(shard, tablet_id);
    }

    // Collect earlier uncommitted rowsets for cross-dedup (layer 3)
    std::vector<RowsetSharedPtr> earlier_rowsets;
    {
        auto& shard = _get_shard(tablet_id);
        std::shared_lock rlock(shard.lock);
        auto it = shard.entries.find(tablet_id);
        if (it != shard.entries.end()) {
            for (const auto& other : it->second) {
                if (other->creation_time < entry->creation_time &&
                    other->unique_key_merge_on_write) {
                    earlier_rowsets.push_back(other->rowset);
                }
            }
        }
    }

    auto st = _dedup_thread_pool->submit_func(
            [this, tablet_id, entry, earlier_rowsets = std::move(earlier_rowsets),
             dedup_mutex]() mutable {
                // Serialize per tablet
                std::lock_guard tablet_lock(*dedup_mutex);

                // Check if entry was unregistered while waiting
                {
                    auto& shard = _get_shard(tablet_id);
                    std::shared_lock rlock(shard.lock);
                    auto it = shard.entries.find(tablet_id);
                    if (it == shard.entries.end()) {
                        return;
                    }
                    bool found = false;
                    for (const auto& e : it->second) {
                        if (e->transaction_id == entry->transaction_id) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        return;
                    }
                }

                auto tablet_result =
                        ExecEnv::GetInstance()->storage_engine().get_tablet(tablet_id);
                if (!tablet_result.has_value()) {
                    LOG(WARNING) << "Failed to get tablet " << tablet_id
                                 << " for full bitmap recompute, marking ready without recompute";
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                auto tablet = tablet_result.value();

                auto* beta_rowset = dynamic_cast<BetaRowset*>(entry->rowset.get());
                if (!beta_rowset) {
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                std::vector<segment_v2::SegmentSharedPtr> segments;
                auto load_st = beta_rowset->load_segments(&segments);
                if (!load_st.ok() || segments.empty()) {
                    if (!load_st.ok()) {
                        LOG(WARNING) << "Failed to load segments for bitmap recompute, tablet_id="
                                     << tablet_id << " txn_id=" << entry->transaction_id
                                     << ": " << load_st;
                    }
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }

                auto* calc_executor =
                        ExecEnv::GetInstance()->storage_engine().calc_delete_bitmap_executor();
                if (!calc_executor) {
                    entry->dedup_ready.store(true, std::memory_order_release);
                    return;
                }
                auto token = calc_executor->create_token();

                // Layer 2: Recompute committed_delete_bitmap against current published rowsets.
                // Uses pre_rowset_ids for incremental diff — only computes against
                // newly-published rowsets since last computation.
                auto committed_bitmap = std::make_shared<DeleteBitmap>(tablet_id);
                if (entry->committed_delete_bitmap) {
                    committed_bitmap->merge(*entry->committed_delete_bitmap);
                }
                auto committed_st = BaseTablet::commit_phase_update_delete_bitmap(
                        tablet, entry->rowset, entry->pre_rowset_ids, committed_bitmap, segments,
                        entry->transaction_id, token.get());
                if (committed_st.ok()) {
                    committed_st = token->wait();
                }
                if (committed_st.ok()) {
                    entry->committed_delete_bitmap = committed_bitmap;
                    // pre_rowset_ids is updated in-place by commit_phase_update_delete_bitmap
                } else {
                    LOG(WARNING) << "Failed to recompute committed delete bitmap, tablet_id="
                                 << tablet_id << " txn_id=" << entry->transaction_id
                                 << ": " << committed_st;
                }

                // Layer 3: Recompute cross-uncommitted delete bitmap
                if (!earlier_rowsets.empty()) {
                    auto cross_token = calc_executor->create_token();
                    auto cross_bitmap = std::make_shared<DeleteBitmap>(tablet_id);
                    auto cross_st = BaseTablet::calc_delete_bitmap(
                            tablet, entry->rowset, segments, earlier_rowsets, cross_bitmap,
                            DeleteBitmap::TEMP_VERSION_COMMON, cross_token.get());
                    if (cross_st.ok()) {
                        cross_st = cross_token->wait();
                    }
                    if (cross_st.ok()) {
                        entry->cross_delete_bitmap = cross_bitmap;
                    } else {
                        LOG(WARNING) << "Failed to recompute cross delete bitmap, tablet_id="
                                     << tablet_id << " txn_id=" << entry->transaction_id
                                     << ": " << cross_st;
                        entry->cross_delete_bitmap.reset();
                    }
                } else {
                    entry->cross_delete_bitmap.reset();
                }

                entry->dedup_ready.store(true, std::memory_order_release);
            });

    if (!st.ok()) {
        LOG(WARNING) << "Failed to submit full recompute task, tablet_id=" << tablet_id
                     << " txn_id=" << entry->transaction_id << ": " << st;
        entry->dedup_ready.store(true, std::memory_order_release);
    }
}

void UncommittedRowsetRegistry::_recompute_all_bitmaps(int64_t tablet_id) {
    std::vector<std::shared_ptr<UncommittedRowsetEntry>> mow_entries;

    {
        auto& shard = _get_shard(tablet_id);
        std::shared_lock rlock(shard.lock);
        auto it = shard.entries.find(tablet_id);
        if (it == shard.entries.end()) {
            return;
        }
        for (auto& entry : it->second) {
            if (entry->unique_key_merge_on_write) {
                // Invalidate: prevent queries from using stale bitmaps
                entry->dedup_ready.store(false, std::memory_order_release);
                entry->cross_delete_bitmap.reset();
                mow_entries.push_back(entry);
            }
        }
    }

    // Re-submit full recompute tasks in order (serialized via tablet dedup mutex)
    for (auto& entry : mow_entries) {
        _submit_full_recompute_task(tablet_id, entry);
    }
}

UncommittedRowsetRegistry* get_uncommitted_rowset_registry() {
    auto* env = ExecEnv::GetInstance();
    if (!env) {
        return nullptr;
    }
    if (config::is_cloud_mode()) {
        return env->storage_engine().to_cloud().uncommitted_rowset_registry();
    } else {
        return env->storage_engine().to_local().uncommitted_rowset_registry();
    }
}

} // namespace doris
