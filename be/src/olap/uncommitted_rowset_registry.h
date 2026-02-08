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
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "olap/rowset/rowset.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "util/threadpool.h"

namespace doris {

// Represents a single uncommitted rowset tracked by the registry.
// The committed_delete_bitmap is computed at commit time (vs published rowsets).
// The cross_delete_bitmap is computed asynchronously (vs earlier uncommitted rowsets).
struct UncommittedRowsetEntry {
    RowsetSharedPtr rowset;
    int64_t transaction_id;
    int64_t partition_id;
    int64_t tablet_id;
    // Delete bitmap computed at commit phase against published rowsets (layer 2)
    DeleteBitmapPtr committed_delete_bitmap;
    // Delete bitmap computed async against earlier uncommitted rowsets (layer 3)
    DeleteBitmapPtr cross_delete_bitmap;
    // Published rowset IDs already accounted for in committed_delete_bitmap.
    // Used for incremental recomputation: only diff against newly-published rowsets.
    RowsetIdUnorderedSet pre_rowset_ids;
    bool unique_key_merge_on_write;
    int64_t creation_time; // for ordering: later wins in dedup
    std::atomic<bool> dedup_ready {false}; // only serve to queries when true
};

// UncommittedRowsetRegistry tracks uncommitted rowsets across all tablets on this BE.
// It supports the READ UNCOMMITTED isolation level by making uncommitted data visible
// to queries that opt in via `SET read_uncommitted = true`.
//
// Thread safety: Uses sharded locks for the main map and per-tablet mutexes for
// serializing async dedup computation.
class UncommittedRowsetRegistry {
public:
    UncommittedRowsetRegistry();
    ~UncommittedRowsetRegistry();

    Status init(int dedup_threads);

    // === Write Path ===

    // Register an uncommitted rowset. For MoW unique key tables, this triggers
    // async dedup computation against earlier uncommitted rowsets.
    void register_rowset(std::shared_ptr<UncommittedRowsetEntry> entry);

    // Unregister on publish or rollback. Removes the entry and any associated state.
    void unregister_rowset(int64_t tablet_id, int64_t transaction_id);

    // === Read Path ===

    // Get all dedup-ready uncommitted rowsets for a tablet.
    void get_ready_rowsets(int64_t tablet_id,
                           std::vector<std::shared_ptr<UncommittedRowsetEntry>>* result);

    // === Compaction Path ===

    // Called after compaction modifies published rowsets. Invalidates cross-delete
    // bitmaps for all uncommitted rowsets on this tablet and re-computes them.
    void on_compaction_completed(int64_t tablet_id);

    // === Schema Change Path ===

    // Called when a tablet transitions to a non-running state. Clears all entries.
    void on_tablet_state_change(int64_t tablet_id, TabletState new_state);

private:
    static constexpr int SHARD_COUNT = 16;

    struct Shard {
        mutable std::shared_mutex lock;
        // tablet_id -> list of uncommitted entries (ordered by creation_time)
        std::unordered_map<int64_t, std::vector<std::shared_ptr<UncommittedRowsetEntry>>> entries;
        // Per-tablet mutex for serializing dedup computation
        std::unordered_map<int64_t, std::shared_ptr<std::mutex>> tablet_dedup_mutex;
    };

    Shard& _get_shard(int64_t tablet_id) { return _shards[tablet_id % SHARD_COUNT]; }
    const Shard& _get_shard(int64_t tablet_id) const { return _shards[tablet_id % SHARD_COUNT]; }

    // Get or create the per-tablet dedup mutex (must hold shard write lock)
    std::shared_ptr<std::mutex> _get_tablet_dedup_mutex(Shard& shard, int64_t tablet_id);

    // Submit async task to compute only cross-uncommitted delete bitmap (layer 3).
    // Used during initial registration when committed_delete_bitmap is already fresh.
    void _submit_dedup_task(int64_t tablet_id, std::shared_ptr<UncommittedRowsetEntry> entry);

    // Submit async task to recompute both committed (layer 2) and cross (layer 3) bitmaps.
    // Used when published rowset set changes (publish/compaction).
    void _submit_full_recompute_task(int64_t tablet_id,
                                     std::shared_ptr<UncommittedRowsetEntry> entry);

    // Invalidate and recompute all bitmaps for a tablet's uncommitted entries
    void _recompute_all_bitmaps(int64_t tablet_id);

    Shard _shards[SHARD_COUNT];
    std::unique_ptr<ThreadPool> _dedup_thread_pool;
};

// Helper function to get the UncommittedRowsetRegistry from the current storage engine.
// Works for both local and cloud mode. Returns nullptr if not available.
UncommittedRowsetRegistry* get_uncommitted_rowset_registry();

} // namespace doris
