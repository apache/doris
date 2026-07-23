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
#include <optional>
#include <string_view>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "storage/compaction/compaction.h"
#include "storage/compaction_task_tracker.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

class RowsetMeta;

namespace cloud {

struct SegmentGroupMergeRange {
    int64_t segment_start;
    int64_t segment_end;
    int64_t merge_way_num;
};

bool is_single_rowset_compaction_candidate(const RowsetSharedPtr& rowset);

bool should_use_single_rowset_grouped_compaction(const std::vector<RowsetSharedPtr>& input_rowsets,
                                                 const TabletSchema& tablet_schema,
                                                 std::string_view compaction_policy);

std::vector<SegmentGroupMergeRange> build_segment_group_merge_ranges(const RowsetMeta& rowset_meta,
                                                                     int64_t segment_group_size);

} // namespace cloud

class CloudCumulativeCompaction : public CloudCompactionMixin {
public:
    CloudCumulativeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);

    ~CloudCumulativeCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;
    Status request_global_lock();

    std::optional<CompactionProfileType> profile_type() const override {
        return CompactionProfileType::CUMULATIVE;
    }
    int64_t input_segments_num_value() const override { return _input_segments; }

    void do_lease();

    int64_t get_input_rowsets_bytes() const { return _input_rowsets_total_size; }
    int64_t get_input_num_rows() const { return _input_row_num; }

private:
    Status pick_rowsets_to_compact();

    Status prepare_merge_input_rowsets(MergeInputRowsetsResult* result) override;

    Status do_merge_input_rowsets(const std::vector<RowsetReaderSharedPtr>& input_rs_readers,
                                  MergeInputRowsetsResult* result) override;

    void update_output_rowset_after_build(const MergeInputRowsetsResult& result) override;

    bool should_calculate_new_cumulative_point(int64_t input_cumulative_point) const;

    std::string_view compaction_name() const override { return "CloudCumulativeCompaction"; }

    Status modify_rowsets() override;

    Status garbage_collection() override;

    void update_cumulative_point();

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    int64_t _input_segments = 0;
    int64_t _max_conflict_version = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
    Version _last_delete_version {-1, -1};
    std::optional<int64_t> _single_rowset_compaction_segment_group_size;
};

} // namespace doris
