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

class CloudFullCompaction : public CloudCompactionMixin {
public:
    CloudFullCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);

    ~CloudFullCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

protected:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudFullCompaction"; }

    Status modify_rowsets() override;
    void garbage_collection() override;

private:
    Status _cloud_full_compaction_update_delete_bitmap(int64_t initiator);
    Status _cloud_full_compaction_calc_delete_bitmap(const RowsetSharedPtr& rowset,
                                                     const int64_t& cur_version,
                                                     const DeleteBitmapPtr& delete_bitmap);

    ReaderType compaction_type() const override { return ReaderType::READER_FULL_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
