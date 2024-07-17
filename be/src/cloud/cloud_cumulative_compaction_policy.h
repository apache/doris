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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

class Tablet;
struct Version;

class CloudCumulativeCompactionPolicy {
public:
    virtual ~CloudCumulativeCompactionPolicy() = default;

    virtual int64_t new_cumulative_point(CloudTablet* tablet, const RowsetSharedPtr& output_rowset,
                                         Version& last_delete_version,
                                         int64_t last_cumulative_point) = 0;

    virtual int64_t new_compaction_level(const std::vector<RowsetSharedPtr>& input_rowsets) = 0;

    virtual int32_t pick_input_rowsets(CloudTablet* tablet,
                                       const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                       const int64_t max_compaction_score,
                                       const int64_t min_compaction_score,
                                       std::vector<RowsetSharedPtr>* input_rowsets,
                                       Version* last_delete_version, size_t* compaction_score,
                                       bool allow_delete = false) = 0;
};

class CloudSizeBasedCumulativeCompactionPolicy : public CloudCumulativeCompactionPolicy {
public:
    CloudSizeBasedCumulativeCompactionPolicy(
            int64_t promotion_size = config::compaction_promotion_size_mbytes * 1024 * 1024,
            double promotion_ratio = config::compaction_promotion_ratio,
            int64_t promotion_min_size = config::compaction_promotion_min_size_mbytes * 1024 * 1024,
            int64_t compaction_min_size = config::compaction_min_size_mbytes * 1024 * 1024,
            int64_t promotion_version_count = config::compaction_promotion_version_count);

    ~CloudSizeBasedCumulativeCompactionPolicy() override = default;

    int64_t new_cumulative_point(CloudTablet* tablet, const RowsetSharedPtr& output_rowset,
                                 Version& last_delete_version,
                                 int64_t last_cumulative_point) override;

    int64_t new_compaction_level(const std::vector<RowsetSharedPtr>& input_rowsets) override {
        return 0;
    }

    int32_t pick_input_rowsets(CloudTablet* tablet,
                               const std::vector<RowsetSharedPtr>& candidate_rowsets,
                               const int64_t max_compaction_score,
                               const int64_t min_compaction_score,
                               std::vector<RowsetSharedPtr>* input_rowsets,
                               Version* last_delete_version, size_t* compaction_score,
                               bool allow_delete = false) override;

private:
    int64_t _level_size(const int64_t size);

    int64_t cloud_promotion_size(CloudTablet* tablet) const;

private:
    /// cumulative compaction promotion size, unit is byte.
    int64_t _promotion_size;
    /// cumulative compaction promotion ratio of base rowset total disk size.
    double _promotion_ratio;
    /// cumulative compaction promotion min size, unit is byte.
    int64_t _promotion_min_size;
    /// lower bound size to do compaction compaction.
    int64_t _compaction_min_size;
    // cumulative compaction promotion version count, only works for unique key MoW table
    int64_t _promotion_version_count;
};

class CloudTimeSeriesCumulativeCompactionPolicy : public CloudCumulativeCompactionPolicy {
public:
    CloudTimeSeriesCumulativeCompactionPolicy() = default;
    ~CloudTimeSeriesCumulativeCompactionPolicy() override = default;

    int64_t new_cumulative_point(CloudTablet* tablet, const RowsetSharedPtr& output_rowset,
                                 Version& last_delete_version,
                                 int64_t last_cumulative_point) override;

    int64_t new_compaction_level(const std::vector<RowsetSharedPtr>& input_rowsets) override;

    int32_t pick_input_rowsets(CloudTablet* tablet,
                               const std::vector<RowsetSharedPtr>& candidate_rowsets,
                               const int64_t max_compaction_score,
                               const int64_t min_compaction_score,
                               std::vector<RowsetSharedPtr>* input_rowsets,
                               Version* last_delete_version, size_t* compaction_score,
                               bool allow_delete = false) override;
};

} // namespace doris
