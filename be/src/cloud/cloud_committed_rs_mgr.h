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

#include <map>
#include <memory>
#include <shared_mutex>

#include "common/status.h"
#include "olap/rowset/rowset_fwd.h"
#include "util/countdown_latch.h"

namespace doris {

class Thread;

// Manages temporary rowset meta for cloud storage transactions in memory.
// This cache stores rowset meta produced during import operations after they
// are committed to MS. After the load txn was committed in MS finally, FE/BE will
// notifies the final version/visible_ts, BE can update and promote these
// temporary rowsets to the tablet meta without fetching from MS in later sync_rowsets().
class CloudCommittedRSMgr {
public:
    CloudCommittedRSMgr();
    ~CloudCommittedRSMgr();

    Status init();

    void add_committed_rowset(int64_t txn_id, int64_t tablet_id, RowsetMetaSharedPtr rowset_meta,
                              int64_t expiration_time);

    Result<std::pair<RowsetMetaSharedPtr, int64_t>> get_committed_rowset(int64_t txn_id,
                                                                         int64_t tablet_id);

    void remove_committed_rowset(int64_t txn_id, int64_t tablet_id);

    void remove_expired_committed_rowsets();

    void mark_empty_rowset(int64_t txn_id, int64_t tablet_id, int64_t txn_expiration);
    bool is_empty_rowset(int64_t txn_id, int64_t tablet_id);

private:
    void _clean_thread_callback();

    struct TxnTabletKey {
        int64_t txn_id;
        int64_t tablet_id;

        TxnTabletKey(int64_t txn_id_, int64_t tablet_id_)
                : txn_id(txn_id_), tablet_id(tablet_id_) {}

        auto operator<=>(const TxnTabletKey&) const = default;
    };

    struct CommittedRowsetValue {
        RowsetMetaSharedPtr rowset_meta;
        const int64_t expiration_time; // seconds since epoch

        CommittedRowsetValue(RowsetMetaSharedPtr rowset_meta_, int64_t expiration_time_)
                : rowset_meta(std::move(rowset_meta_)), expiration_time(expiration_time_) {}
    };

    // Map: <txn_id, tablet_id> -> <rowset_meta, expiration_time>
    std::map<TxnTabletKey, CommittedRowsetValue> _committed_rs_map;
    // Multimap for efficient expiration cleanup: expiration_time -> <txn_id, tablet_id>
    std::multimap<int64_t, TxnTabletKey> _expiration_map;
    std::map<TxnTabletKey, int64_t /* expiration_time */> _empty_rowset_markers;
    std::shared_mutex _rwlock;
    std::shared_ptr<Thread> _clean_thread;
    CountDownLatch _stop_latch;
};

} // namespace doris
