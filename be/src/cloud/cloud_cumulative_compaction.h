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

class CloudCumulativeCompaction : public CloudCompactionMixin {
public:
    CloudCumulativeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);

    ~CloudCumulativeCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

private:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudCumulativeCompaction"; }

    Status modify_rowsets() override;

    void garbage_collection() override;

    void update_cumulative_point();

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    int64_t _max_conflict_version = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
    Version _last_delete_version {-1, -1};
};

} // namespace doris
