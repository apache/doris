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

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>

#include "common/status.h"
#include "util/stopwatch.hpp"

namespace doris {

class StorageEngine;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
class RowsetMeta;
using RowsetMetaSharedPtr = std::shared_ptr<RowsetMeta>;
class PendingRowsetGuard;

// Result of committing an ingested rowset. When commit fails, |status| preserves the
// original error so callers can log detailed diagnostics instead of a generic message.
struct IngestCommitResult {
    enum Code {
        kCommitted,    // Rowset committed successfully.
        kAlreadyExist, // Same load id already committed a different rowset; do not overwrite.
        kError,        // Commit failed with a real error.
    };

    Code code;
    Status status; // Only meaningful when code == kError.

    IngestCommitResult(Code c);
    IngestCommitResult(Code c, Status s);

    bool operator==(Code c) const;
};

// Commit an ingested rowset to the local tablet. Exposed for unit testing of the
// single-replica ingest binlog retry path.
IngestCommitResult commit_ingested_rowset(
        StorageEngine& engine, const TabletSharedPtr& local_tablet, int64_t txn_id,
        int64_t partition_id, const RowsetMetaSharedPtr& rowset_meta,
        PendingRowsetGuard pending_rs_guard, MonotonicStopWatch& watch,
        std::unordered_map<std::string_view, uint64_t>& elapsed_time_map);

// Delete files downloaded during ingest. Exposed for unit testing of the cleanup path.
Status _delete_downloaded_files(const std::vector<std::string>& files, std::string_view reason,
                                int64_t txn_id);

class TIngestBinlogRequest;
class TStatus;

// Ingest a rowset from a peer backend. Exposed for unit testing of the
// fetch_from_peer validation path.
void _ingest_binlog_from_peer(StorageEngine& engine, const TIngestBinlogRequest& request,
                              const TabletSharedPtr& local_tablet, int64_t txn_id,
                              int64_t partition_id, TStatus& tstatus);

} // namespace doris
