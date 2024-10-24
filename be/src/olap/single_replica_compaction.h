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

#include <vector>

#include "common/status.h"
#include "olap/compaction.h"
#include "olap/rowset/pending_rowset_helper.h"

namespace doris {

class DataDir;

//  SingleReplicaCompaction is used to fetch peer replica compaction result.
class SingleReplicaCompaction final : public CompactionMixin {
public:
    SingleReplicaCompaction(StorageEngine& engine, const TabletSharedPtr& tablet,
                            CompactionType compaction_type);
    ~SingleReplicaCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    inline CompactionType real_compact_type() const { return _compaction_type; }

protected:
    std::string_view compaction_name() const override { return "single replica compaction"; }
    ReaderType compaction_type() const override {
        return (_compaction_type == CompactionType::CUMULATIVE_COMPACTION)
                       ? ReaderType::READER_CUMULATIVE_COMPACTION
                       : ReaderType::READER_BASE_COMPACTION;
    }

private:
    Status _do_single_replica_compaction();
    Status _do_single_replica_compaction_impl();
    bool _find_rowset_to_fetch(const std::vector<Version>& peer_versions, Version* proper_version);
    Status _get_rowset_verisons_from_peer(const TReplicaInfo& addr,
                                          std::vector<Version>* peer_versions);
    Status _fetch_rowset(const TReplicaInfo& addr, const std::string& token,
                         const Version& version);
    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                          TSchemaHash schema_hash, int timeout_s, const Version& version,
                          std::string* snapshot_path);
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                           const std::string& local_path);
    Status _release_snapshot(const std::string& ip, int port, const std::string& snapshot_path);
    Status _finish_clone(const std::string& clone_dir, const Version& version);
    std::string _mask_token(const std::string& str);
    CompactionType _compaction_type;

    std::vector<PendingRowsetGuard> _pending_rs_guards;
};

} // namespace doris