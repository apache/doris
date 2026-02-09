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

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "olap/rowset/rowset.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"

namespace doris {

// Represents a single uncommitted rowset tracked by the registry.
// For MoW unique key tables, committed_delete_bitmap contains the delete bitmap
// computed at commit time against published rowsets. No cross-uncommitted dedup
// is performed — READ UNCOMMITTED may show duplicate keys across concurrent
// uncommitted transactions on the same MoW tablet.
struct UncommittedRowsetEntry {
    RowsetSharedPtr rowset;
    int64_t transaction_id;
    int64_t partition_id;
    int64_t tablet_id;
    // Delete bitmap computed at commit phase against published rowsets.
    // Already available from the normal MoW write path — no extra computation.
    DeleteBitmapPtr committed_delete_bitmap;
    bool unique_key_merge_on_write = false;
};

// UncommittedRowsetRegistry tracks uncommitted rowsets across all tablets on this BE.
// It supports the READ UNCOMMITTED isolation level by making uncommitted data visible
// to queries that opt in via `SET transaction_isolation = 'READ-UNCOMMITTED'`.
//
// Thread safety: Uses sharded read-write locks for concurrent access.
class UncommittedRowsetRegistry {
public:
    // Register an uncommitted rowset after commit.
    void register_rowset(std::shared_ptr<UncommittedRowsetEntry> entry);

    // Unregister on publish or rollback.
    void unregister_rowset(int64_t tablet_id, int64_t transaction_id);

    // Get all uncommitted rowsets for a tablet.
    void get_uncommitted_rowsets(int64_t tablet_id,
                                 std::vector<std::shared_ptr<UncommittedRowsetEntry>>* result);

    // Called when a tablet transitions to a non-running state. Clears all entries.
    void on_tablet_state_change(int64_t tablet_id, TabletState new_state);

private:
    static constexpr int SHARD_COUNT = 16;

    struct Shard {
        mutable std::shared_mutex lock;
        std::unordered_map<int64_t, std::vector<std::shared_ptr<UncommittedRowsetEntry>>> entries;
    };

    Shard& _get_shard(int64_t tablet_id) { return _shards[tablet_id % SHARD_COUNT]; }

    Shard _shards[SHARD_COUNT];
};

// Helper function to get the UncommittedRowsetRegistry from the current storage engine.
// Works for both local and cloud mode. Returns nullptr if not available.
UncommittedRowsetRegistry* get_uncommitted_rowset_registry();

} // namespace doris
