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

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudIndexChangeCompaction : public CloudCompactionMixin {
public:
    CloudIndexChangeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                               int32_t schema_version, std::vector<TOlapTableIndex>& index_list,
                               std::vector<TColumn>& columns);

    ~CloudIndexChangeCompaction();

    Status prepare_compact() override;
    Status execute_compact() override;
    Status request_global_lock(bool& should_skip_err);

    void do_lease();

    bool is_finish_index_change() { return _input_rowsets.size() == 0; }

    bool is_index_change_compaction() override { return true; }

    bool is_base_compaction() const { return _compact_type == cloud::TabletCompactionJobPB::BASE; }

    Status rebuild_tablet_schema() override;

private:
    void _update_tablet_for_cumu_compaction(cloud::FinishTabletJobResponse resp,
                                            DeleteBitmapPtr output_rowset_delete_bitmap);
    void _update_tablet_for_base_compaction(cloud::FinishTabletJobResponse resp,
                                            DeleteBitmapPtr output_rowset_delete_bitmap);

protected:
    std::string_view compaction_name() const override { return "CloudIndexChangeCompaction"; }

    // if cumu rowset is modified, cumu compaction should sync rowset before execute.
    // if base rowset is modified, base compaction should sync rowset before execute.
    ReaderType compaction_type() const override {
        return is_base_compaction() ? ReaderType::READER_BASE_COMPACTION
                                    : ReaderType::READER_CUMULATIVE_COMPACTION;
    }

    Status modify_rowsets() override;

    Status garbage_collection() override;

    int32_t _schema_version;

    std::vector<TOlapTableIndex>& _index_list;

    std::vector<TColumn>& _columns;

    cloud::TabletCompactionJobPB::CompactionType _compact_type;

    int64_t _base_compaction_cnt {0};
    int64_t _cumulative_compaction_cnt {0};

    int64_t _input_segments {0};

    TabletSchemaSPtr _final_tablet_schema = nullptr;
};

}; // namespace doris
