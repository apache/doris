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

#include <gen_cpp/cloud.pb.h>

namespace doris::cloud {
class Transaction;
class RangeGetIterator;

// Detached tablet stats
struct TabletStats {
    int64_t data_size = 0;
    int64_t num_rows = 0;
    int64_t num_rowsets = 0;
    int64_t num_segs = 0;
};

// Get tablet stats and detached tablet stats via `txn`. If an error occurs, `code` will be set to non OK.
// NOTE: this function returns original `TabletStatsPB` and detached tablet stats val stored in kv store,
//  MUST call `merge_tablet_stats(stats, detached_stats)` to get the real tablet stats.
void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, TabletStats& detached_stats,
                               bool snapshot = false);

// Merge `detached_stats` `stats` to `stats`.
void merge_tablet_stats(TabletStatsPB& stats, const TabletStats& detached_stats);

// Get merged tablet stats via `txn`. If an error occurs, `code` will be set to non OK.
void internal_get_tablet_stats(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               const std::string& instance_id, const TabletIndexPB& idx,
                               TabletStatsPB& stats, bool snapshot = false);

// Get detached tablet stats via `iter`, `iter.next` SHOULD be the first splitted tablet stats KV.
// Return 0 if success, otherwise error.
[[nodiscard]] int get_detached_tablet_stats(RangeGetIterator& iter, TabletStats& detached_stats);

} // namespace doris::cloud
