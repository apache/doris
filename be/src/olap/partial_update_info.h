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
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"

namespace doris {
class TabletSchema;
class PartialUpdateInfoPB;
struct RowLocation;
namespace vectorized {
class Block;
}
struct RowsetWriterContext;
struct RowsetId;

struct PartialUpdateInfo {
    void init(const TabletSchema& tablet_schema, bool partial_update,
              const std::set<std::string>& partial_update_cols, bool is_strict_mode,
              int64_t timestamp_ms, const std::string& timezone,
              const std::string& auto_increment_column, int64_t cur_max_version = -1);
    void to_pb(PartialUpdateInfoPB* partial_update_info) const;
    void from_pb(PartialUpdateInfoPB* partial_update_info);
    Status handle_non_strict_mode_not_found_error(const TabletSchema& tablet_schema);
    std::string summary() const;

private:
    void _generate_default_values_for_missing_cids(const TabletSchema& tablet_schema);

public:
    bool is_partial_update {false};
    int64_t max_version_in_flush_phase {-1};
    std::set<std::string> partial_update_input_columns;
    std::vector<uint32_t> missing_cids;
    std::vector<uint32_t> update_cids;
    // if key not exist in old rowset, use default value or null value for the unmentioned cols
    // to generate a new row, only available in non-strict mode
    bool can_insert_new_rows_in_partial_update {true};
    bool is_strict_mode {false};
    int64_t timestamp_ms {0};
    std::string timezone;
    bool is_input_columns_contains_auto_inc_column = false;
    bool is_schema_contains_auto_inc_column = false;

    // default values for missing cids
    std::vector<std::string> default_values;
};

// used in mow partial update
struct RidAndPos {
    uint32_t rid;
    // pos in block
    size_t pos;
};

class PartialUpdateReadPlan {
public:
    void prepare_to_read(const RowLocation& row_location, size_t pos);
    Status read_columns_by_plan(const TabletSchema& tablet_schema,
                                const std::vector<uint32_t> cids_to_read,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                vectorized::Block& block, std::map<uint32_t, uint32_t>* read_index,
                                const signed char* __restrict skip_map = nullptr) const;
    Status fill_missing_columns(RowsetWriterContext* rowset_ctx,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                const TabletSchema& tablet_schema, vectorized::Block& full_block,
                                const std::vector<bool>& use_default_or_null_flag,
                                bool has_default_or_nullable, const size_t& segment_start_pos,
                                const vectorized::Block* block) const;

private:
    std::map<RowsetId, std::map<uint32_t, std::vector<RidAndPos>>> plan;
};

struct PartialUpdateStats {
    int64_t num_rows_updated {0};
    int64_t num_rows_new_added {0};
    int64_t num_rows_deleted {0};
    int64_t num_rows_filtered {0};
};
} // namespace doris
