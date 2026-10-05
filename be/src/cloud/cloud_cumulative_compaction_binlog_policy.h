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

#include "cloud/cloud_cumulative_compaction_policy.h"
#include "storage/compaction/cumulative_compaction_binlog_policy.h"

namespace doris {

class CloudBinlogCumulativeCompactionPolicy final : public CloudCumulativeCompactionPolicy {
public:
    CloudBinlogCumulativeCompactionPolicy() = default;
    ~CloudBinlogCumulativeCompactionPolicy() override = default;

    int64_t new_cumulative_point(CloudTablet* tablet, const RowsetSharedPtr& output_rowset,
                                 Version& last_delete_version,
                                 int64_t last_cumulative_point) override;

    int64_t get_compaction_level(CloudTablet* tablet,
                                 const std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr output_rowset) override;

    int64_t pick_input_rowsets(CloudTablet* tablet,
                               const std::vector<RowsetSharedPtr>& candidate_rowsets,
                               const int64_t max_compaction_score,
                               const int64_t min_compaction_score,
                               std::vector<RowsetSharedPtr>* input_rowsets,
                               Version* last_delete_version, size_t* compaction_score,
                               bool allow_delete = false) override;

    std::string name() override { return std::string(CUMULATIVE_BINLOG_POLICY); }

private:
    bool is_compaction_enough(const RowsetMetaSharedPtr& rowset_meta) const;
    void filter_new_visible_rowsets(const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                    std::vector<RowsetSharedPtr>* output_rowsets) const;
    uint32_t calc_binlog_compaction_level_score(
            CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
            int8_t level) const;
    uint32_t calc_binlog_compaction_score(CloudTablet* tablet,
                                          const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                          int8_t* compaction_level) const;
};

} // namespace doris
