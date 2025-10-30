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
#include <gen_cpp/olap_file.pb.h>

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "vec/columns/column.h"

namespace doris {
class TabletSchema;
class PartialUpdateInfoPB;
class BitmapValue;
struct RowLocation;
namespace vectorized {
class Block;
class MutableBlock;
class IOlapColumnDataAccessor;

} // namespace vectorized
struct RowsetWriterContext;
struct RowsetId;
class BitmapValue;
namespace segment_v2 {
class VerticalSegmentWriter;
}

class SegmentCacheHandle;

struct PartialUpdateInfo {
    Status init(int64_t tablet_id, int64_t txn_id, const TabletSchema& tablet_schema,
                UniqueKeyUpdateModePB unique_key_update_mode, PartialUpdateNewRowPolicyPB policy,
                const std::set<std::string>& partial_update_cols, bool is_strict_mode,
                int64_t timestamp_ms, int32_t nano_seconds, const std::string& timezone,
                const std::string& auto_increment_column, int32_t sequence_map_col_uid = -1,
                int64_t cur_max_version = -1);
    void to_pb(PartialUpdateInfoPB* partial_update_info) const;
    void from_pb(PartialUpdateInfoPB* partial_update_info);
    Status handle_new_key(const TabletSchema& tablet_schema,
                          const std::function<std::string()>& line,
                          BitmapValue* skip_bitmap = nullptr);
    std::string summary() const;

    std::string partial_update_mode_str() const {
        switch (partial_update_mode) {
        case UniqueKeyUpdateModePB::UPSERT:
            return "upsert";
        case UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS:
            return "partial update";
        case UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS:
            return "flexible partial update";
        }
        return "";
    }
    bool is_partial_update() const { return partial_update_mode != UniqueKeyUpdateModePB::UPSERT; }
    bool is_fixed_partial_update() const {
        return partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS;
    }
    bool is_flexible_partial_update() const {
        return partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS;
    }
    UniqueKeyUpdateModePB update_mode() const { return partial_update_mode; }
    int32_t sequence_map_col_uid() const { return sequence_map_col_unqiue_id; }

private:
    void _generate_default_values_for_missing_cids(const TabletSchema& tablet_schema);

public:
    UniqueKeyUpdateModePB partial_update_mode {UniqueKeyUpdateModePB::UPSERT};
    PartialUpdateNewRowPolicyPB partial_update_new_key_policy {PartialUpdateNewRowPolicyPB::APPEND};
    int64_t max_version_in_flush_phase {-1};
    std::set<std::string> partial_update_input_columns;
    std::vector<uint32_t> missing_cids;
    std::vector<uint32_t> update_cids;
    // if key not exist in old rowset, use default value or null value for the unmentioned cols
    // to generate a new row, only available in non-strict mode
    bool can_insert_new_rows_in_partial_update {true};
    bool is_strict_mode {false};
    int64_t timestamp_ms {0};
    int32_t nano_seconds {0};
    std::string timezone;
    bool is_input_columns_contains_auto_inc_column = false;
    bool is_schema_contains_auto_inc_column = false;

    // default values for missing cids
    std::vector<std::string> default_values;

    int32_t sequence_map_col_unqiue_id {-1};
};

// used in mow partial update
struct RidAndPos {
    uint32_t rid;
    // pos in block
    size_t pos;
};

class FixedReadPlan {
public:
    bool empty() const;
    void prepare_to_read(const RowLocation& row_location, size_t pos);
    Status read_columns_by_plan(const TabletSchema& tablet_schema,
                                std::vector<uint32_t> cids_to_read,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                vectorized::Block& block, std::map<uint32_t, uint32_t>* read_index,
                                bool force_read_old_delete_signs,
                                const signed char* __restrict cur_delete_signs = nullptr) const;
    Status fill_missing_columns(RowsetWriterContext* rowset_ctx,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                const TabletSchema& tablet_schema, vectorized::Block& full_block,
                                const std::vector<bool>& use_default_or_null_flag,
                                bool has_default_or_nullable, uint32_t segment_start_pos,
                                const vectorized::Block* block) const;

private:
    std::map<RowsetId, std::map<uint32_t /* segment_id */, std::vector<RidAndPos>>> plan;
};

class FlexibleReadPlan {
public:
    FlexibleReadPlan(bool has_row_store_for_column) : use_row_store(has_row_store_for_column) {}
    void prepare_to_read(const RowLocation& row_location, size_t pos,
                         const BitmapValue& skip_bitmap);
    // for column store
    Status read_columns_by_plan(const TabletSchema& tablet_schema,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                vectorized::Block& old_value_block,
                                std::map<uint32_t, std::map<uint32_t, uint32_t>>* read_index) const;

    // for row_store
    Status read_columns_by_plan(const TabletSchema& tablet_schema,
                                const std::vector<uint32_t>& cids_to_read,
                                const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                vectorized::Block& old_value_block,
                                std::map<uint32_t, uint32_t>* read_index) const;
    Status fill_non_primary_key_columns(RowsetWriterContext* rowset_ctx,
                                        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                                        const TabletSchema& tablet_schema,
                                        vectorized::Block& full_block,
                                        const std::vector<bool>& use_default_or_null_flag,
                                        bool has_default_or_nullable, uint32_t segment_start_pos,
                                        uint32_t block_start_pos, const vectorized::Block* block,
                                        std::vector<BitmapValue>* skip_bitmaps) const;

    Status fill_non_primary_key_columns_for_column_store(
            RowsetWriterContext* rowset_ctx,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
            const TabletSchema& tablet_schema, const std::vector<uint32_t>& non_sort_key_cids,
            vectorized::Block& old_value_block, vectorized::MutableColumns& mutable_full_columns,
            const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
            uint32_t segment_start_pos, uint32_t block_start_pos, const vectorized::Block* block,
            std::vector<BitmapValue>* skip_bitmaps) const;
    Status fill_non_primary_key_columns_for_row_store(
            RowsetWriterContext* rowset_ctx,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
            const TabletSchema& tablet_schema, const std::vector<uint32_t>& non_sort_key_cids,
            vectorized::Block& old_value_block, vectorized::MutableColumns& mutable_full_columns,
            const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
            uint32_t segment_start_pos, uint32_t block_start_pos, const vectorized::Block* block,
            std::vector<BitmapValue>* skip_bitmaps) const;

private:
    bool use_row_store {false};
    // rowset_id -> segment_id -> column unique id -> mappings
    std::map<RowsetId, std::map<uint32_t, std::map<uint32_t, std::vector<RidAndPos>>>> plan;
    std::map<RowsetId, std::map<uint32_t /* segment_id */, std::vector<RidAndPos>>> row_store_plan;
};

class BlockAggregator {
public:
    ~BlockAggregator() = default;
    BlockAggregator(segment_v2::VerticalSegmentWriter& vertical_segment_writer);

    Status convert_pk_columns(vectorized::Block* block, size_t row_pos, size_t num_rows,
                              std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns);
    Status convert_seq_column(vectorized::Block* block, size_t row_pos, size_t num_rows,
                              vectorized::IOlapColumnDataAccessor*& seq_column);
    Status aggregate_for_flexible_partial_update(
            vectorized::Block* block, size_t num_rows,
            const std::vector<RowsetSharedPtr>& specified_rowsets,
            std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches);

private:
    Status aggregate_for_sequence_column(
            vectorized::Block* block, int num_rows,
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
            vectorized::IOlapColumnDataAccessor* seq_column,
            const std::vector<RowsetSharedPtr>& specified_rowsets,
            std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches);
    Status aggregate_for_insert_after_delete(
            vectorized::Block* block, size_t num_rows,
            const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns,
            const std::vector<RowsetSharedPtr>& specified_rowsets,
            std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches);
    Status filter_block(vectorized::Block* block, size_t num_rows,
                        vectorized::MutableColumnPtr filter_column, int duplicate_rows,
                        std::string col_name);

    Status fill_sequence_column(vectorized::Block* block, size_t num_rows,
                                const FixedReadPlan& read_plan,
                                std::vector<BitmapValue>& skip_bitmaps);

    void append_or_merge_row(vectorized::MutableBlock& dst_block, vectorized::Block* src_block,
                             int rid, BitmapValue& skip_bitmap, bool have_delete_sign);
    void merge_one_row(vectorized::MutableBlock& dst_block, vectorized::Block* src_block, int rid,
                       BitmapValue& skip_bitmap);
    void append_one_row(vectorized::MutableBlock& dst_block, vectorized::Block* src_block, int rid);
    void remove_last_n_rows(vectorized::MutableBlock& dst_block, int n);

    // aggregate rows with same keys in range [start, end) from block to output_block
    Status aggregate_rows(vectorized::MutableBlock& output_block, vectorized::Block* block,
                          int start, int end, std::string key,
                          std::vector<BitmapValue>* skip_bitmaps, const signed char* delete_signs,
                          vectorized::IOlapColumnDataAccessor* seq_column,
                          const std::vector<RowsetSharedPtr>& specified_rowsets,
                          std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches);

    segment_v2::VerticalSegmentWriter& _writer;
    TabletSchema& _tablet_schema;

    // used to store state when aggregating rows in block
    struct AggregateState {
        int rows {0};
        bool has_row_with_delete_sign {false};

        bool should_merge() const {
            return ((rows == 1 && !has_row_with_delete_sign) || rows == 2);
        }

        void reset() {
            rows = 0;
            has_row_with_delete_sign = false;
        }

        std::string to_string() const {
            return fmt::format("rows={}, have_delete_row={}", rows, has_row_with_delete_sign);
        }
    } _state {};
};

struct PartialUpdateStats {
    int64_t num_rows_updated {0};
    int64_t num_rows_new_added {0};
    int64_t num_rows_deleted {0};
    int64_t num_rows_filtered {0};
};
} // namespace doris
