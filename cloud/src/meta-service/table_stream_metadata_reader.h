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

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "meta-store/meta_reader.h"

namespace doris::cloud {

class CloneChainReader;
class Transaction;

enum class TableStreamReadIntent {
    SNAPSHOT,
    CONFLICT,
};

struct TableStreamReadResult {
    MetaServiceCode code = MetaServiceCode::OK;
    std::string message;

    bool ok() const { return code == MetaServiceCode::OK; }
};

using TableStreamPartitionVersionMap =
        std::unordered_map<int64_t, std::unordered_map<int64_t, VersionPB>>;

bool is_valid_table_stream_identity(const TableStreamIdentityPB& identity);

TableStreamReadResult read_table_stream_multi_version_status(
        Transaction* txn, std::string_view instance_id, TableStreamReadIntent intent,
        MultiVersionStatus* multi_version_status);

class TableStreamMetadataReader {
public:
    TableStreamMetadataReader(Transaction* txn, std::string_view instance_id,
                              MultiVersionStatus multi_version_status,
                              CloneChainReader* clone_chain_reader);

    bool reads_from_clone_chain() const;
    bool writes_versioned_metadata() const;

    TableStreamReadResult read_recycling_streams(
            const std::vector<int64_t>& stream_ids, TableStreamReadIntent intent,
            std::unordered_set<int64_t>* recycling_stream_ids) const;

    TableStreamReadResult read_recycling_partitions(
            const std::vector<int64_t>& partition_ids, TableStreamReadIntent intent,
            std::unordered_set<int64_t>* recycling_partition_ids) const;

    // Validates partition ownership and visibility, and returns source VersionPBs. Source
    // VersionPB is always read with snapshot semantics so consumption does not conflict with
    // concurrent source-table publishes.
    TableStreamReadResult read_and_validate_partitions(
            const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
            TableStreamPartitionVersionMap* versions) const;

    TableStreamReadResult read_latest_offsets(
            const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
            TableStreamOffsetMap* offsets) const;

    TableStreamReadResult read_effective_offsets(
            const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
            TableStreamOffsetMap* offsets) const;

    // Reads only the current instance's 0x03 projection. This is used to check idempotent
    // versioned writes and must not follow the clone chain.
    TableStreamReadResult read_local_versioned_offsets(
            const std::vector<TableStreamPartitionSetPB>& bindings, TableStreamReadIntent intent,
            TableStreamOffsetMap* offsets) const;

private:
    bool snapshot(TableStreamReadIntent intent) const;

    Transaction* txn_;
    std::string instance_id_;
    MultiVersionStatus multi_version_status_;
    CloneChainReader* clone_chain_reader_;
    mutable MetaReader current_reader_;
};

} // namespace doris::cloud
