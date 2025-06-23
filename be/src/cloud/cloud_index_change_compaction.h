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
    CloudIndexChangeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet, bool is_drop,
                               std::vector<TOlapTableIndex>& _alter_indexes);

    ~CloudIndexChangeCompaction();

    Status prepare_compact() override;
    Status execute_compact() override;
    Status request_global_lock(bool& should_skip_err);

    void do_lease();

    bool is_finish_index_change() { return _input_rowsets.size() == 0; }

    bool is_index_change_compaction() override { return true; }

    bool is_base_compaction() const { return _compact_type == cloud::TabletCompactionJobPB::BASE; }

private:
    void _update_tablet_for_cumu_compaction(cloud::FinishTabletJobResponse resp,
                                            DeleteBitmapPtr output_rowset_delete_bitmap);
    void _update_tablet_for_base_compaction(cloud::FinishTabletJobResponse resp,
                                            DeleteBitmapPtr output_rowset_delete_bitmap);

    TabletSchemaSPtr _build_output_rs_index_schema_for_drop(
            const TabletSchemaSPtr& input_rs_tablet_schema);
    TabletSchemaSPtr _build_output_rs_index_schema_for_add(
            const TabletSchemaSPtr& input_rs_tablet_schema);

protected:
    std::string_view compaction_name() const override { return "CloudIndexChangeCompaction"; }

    // if cumu rowset is modified, cumu compaction should sync rowset before execute.
    // if base rowset is modified, base compaction should sync rowset before execute.
    ReaderType compaction_type() const override {
        return is_base_compaction() ? ReaderType::READER_BASE_COMPACTION
                                    : ReaderType::READER_CUMULATIVE_COMPACTION;
    }

    TabletSchemaSPtr get_output_schema() override { return _output_schema; }

    Status modify_rowsets() override;

    Status garbage_collection() override;

    TabletSchemaSPtr _output_schema {nullptr};

    bool _is_drop {false};

    std::vector<TOlapTableIndex>& _alter_indexes;

    cloud::TabletCompactionJobPB::CompactionType _compact_type;

    int64_t _base_compaction_cnt {0};
    int64_t _cumulative_compaction_cnt {0};

    int64_t _input_segments {0};
};

}; // namespace doris
