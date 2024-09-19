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
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/version_graph.h"
#include "util/metrics.h"

namespace doris {
struct RowSetSplits;
struct RowsetWriterContext;
class RowsetWriter;
class CalcDeleteBitmapToken;
class SegmentCacheHandle;
class RowIdConversion;
struct PartialUpdateInfo;
class PartialUpdateReadPlan;

struct TabletWithVersion {
    BaseTabletSPtr tablet;
    int64_t version;
};

enum class CompactionStage { NOT_SCHEDULED, PENDING, EXECUTING };

// Base class for all tablet classes
class BaseTablet {
public:
    explicit BaseTablet(TabletMetaSharedPtr tablet_meta);
    virtual ~BaseTablet();
    BaseTablet(const BaseTablet&) = delete;
    BaseTablet& operator=(const BaseTablet&) = delete;

    TabletState tablet_state() const { return _tablet_meta->tablet_state(); }
    Status set_tablet_state(TabletState state);
    int64_t table_id() const { return _tablet_meta->table_id(); }
    int64_t index_id() const { return _tablet_meta->index_id(); }
    int64_t partition_id() const { return _tablet_meta->partition_id(); }
    int64_t tablet_id() const { return _tablet_meta->tablet_id(); }
    int32_t schema_hash() const { return _tablet_meta->schema_hash(); }
    KeysType keys_type() const { return _tablet_meta->tablet_schema()->keys_type(); }
    size_t num_key_columns() const { return _tablet_meta->tablet_schema()->num_key_columns(); }
    int64_t ttl_seconds() const { return _tablet_meta->ttl_seconds(); }
    std::mutex& get_schema_change_lock() { return _schema_change_lock; }
    bool enable_unique_key_merge_on_write() const {
#ifdef BE_TEST
        if (_tablet_meta == nullptr) {
            return false;
        }
#endif
        return _tablet_meta->enable_unique_key_merge_on_write();
    }

    // Property encapsulated in TabletMeta
    const TabletMetaSharedPtr& tablet_meta() { return _tablet_meta; }

    // FIXME(plat1ko): It is not appropriate to expose this lock
    std::shared_mutex& get_header_lock() { return _meta_lock; }

    void update_max_version_schema(const TabletSchemaSPtr& tablet_schema);

    Status update_by_least_common_schema(const TabletSchemaSPtr& update_schema);

    TabletSchemaSPtr tablet_schema() const {
        std::shared_lock rlock(_meta_lock);
        return _max_version_schema;
    }

    virtual bool exceed_version_limit(int32_t limit) = 0;

    virtual Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& context,
                                                                       bool vertical) = 0;

    virtual Status capture_consistent_rowsets_unlocked(
            const Version& spec_version, std::vector<RowsetSharedPtr>* rowsets) const = 0;

    virtual Status capture_rs_readers(const Version& spec_version,
                                      std::vector<RowSetSplits>* rs_splits,
                                      bool skip_missing_version) = 0;

    virtual size_t tablet_footprint() = 0;

    // this method just return the compaction sum on each rowset
    // note(tsy): we should unify the compaction score calculation finally
    uint32_t get_real_compaction_score() const;

    // MUST hold shared meta lock
    Status capture_rs_readers_unlocked(const Versions& version_path,
                                       std::vector<RowSetSplits>* rs_splits) const;

    // _rs_version_map and _stale_rs_version_map should be protected by _meta_lock
    // The caller must call hold _meta_lock when call this three function.
    RowsetSharedPtr get_rowset_by_version(const Version& version, bool find_is_stale = false) const;
    RowsetSharedPtr get_stale_rowset_by_version(const Version& version) const;
    RowsetSharedPtr get_rowset_with_max_version() const;

    Status get_all_rs_id(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const;
    Status get_all_rs_id_unlocked(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const;

    // Get the missed versions until the spec_version.
    Versions get_missed_versions(int64_t spec_version) const;
    Versions get_missed_versions_unlocked(int64_t spec_version) const;

    void generate_tablet_meta_copy(TabletMeta& new_tablet_meta) const;
    void generate_tablet_meta_copy_unlocked(TabletMeta& new_tablet_meta) const;

    virtual int64_t max_version_unlocked() const { return _tablet_meta->max_version().second; }

    static TabletSchemaSPtr tablet_schema_with_merged_max_schema_version(
            const std::vector<RowsetMetaSharedPtr>& rowset_metas);

    ////////////////////////////////////////////////////////////////////////////
    // begin MoW functions
    ////////////////////////////////////////////////////////////////////////////
    std::vector<RowsetSharedPtr> get_rowset_by_ids(
            const RowsetIdUnorderedSet* specified_rowset_ids);

    // Lookup a row with TupleDescriptor and fill Block
    Status lookup_row_data(const Slice& encoded_key, const RowLocation& row_location,
                           RowsetSharedPtr rowset, const TupleDescriptor* desc,
                           OlapReaderStatistics& stats, std::string& values,
                           bool write_to_cache = false);

    // Lookup the row location of `encoded_key`, the function sets `row_location` on success.
    // NOTE: the method only works in unique key model with primary key index, you will got a
    //       not supported error in other data model.
    Status lookup_row_key(const Slice& encoded_key, TabletSchema* latest_schema, bool with_seq_col,
                          const std::vector<RowsetSharedPtr>& specified_rowsets,
                          RowLocation* row_location, uint32_t version,
                          std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                          RowsetSharedPtr* rowset = nullptr, bool with_rowid = true);

    // calc delete bitmap when flush memtable, use a fake version to calc
    // For example, cur max version is 5, and we use version 6 to calc but
    // finally this rowset publish version with 8, we should make up data
    // for rowset 6-7. Also, if a compaction happens between commit_txn and
    // publish_txn, we should remove compaction input rowsets' delete_bitmap
    // and build newly generated rowset's delete_bitmap
    static Status calc_delete_bitmap(const BaseTabletSPtr& tablet, RowsetSharedPtr rowset,
                                     const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                     const std::vector<RowsetSharedPtr>& specified_rowsets,
                                     DeleteBitmapPtr delete_bitmap, int64_t version,
                                     CalcDeleteBitmapToken* token,
                                     RowsetWriter* rowset_writer = nullptr);

    Status calc_segment_delete_bitmap(RowsetSharedPtr rowset,
                                      const segment_v2::SegmentSharedPtr& seg,
                                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                                      DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                      RowsetWriter* rowset_writer);

    Status calc_delete_bitmap_between_segments(
            RowsetSharedPtr rowset, const std::vector<segment_v2::SegmentSharedPtr>& segments,
            DeleteBitmapPtr delete_bitmap);

    static Status commit_phase_update_delete_bitmap(
            const BaseTabletSPtr& tablet, const RowsetSharedPtr& rowset,
            RowsetIdUnorderedSet& pre_rowset_ids, DeleteBitmapPtr delete_bitmap,
            const std::vector<segment_v2::SegmentSharedPtr>& segments, int64_t txn_id,
            CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer = nullptr);

    static void add_sentinel_mark_to_delete_bitmap(DeleteBitmap* delete_bitmap,
                                                   const RowsetIdUnorderedSet& rowsetids);

    Status check_delete_bitmap_correctness(DeleteBitmapPtr delete_bitmap, int64_t max_version,
                                           int64_t txn_id, const RowsetIdUnorderedSet& rowset_ids,
                                           std::vector<RowsetSharedPtr>* rowsets = nullptr);

    static const signed char* get_delete_sign_column_data(vectorized::Block& block,
                                                          size_t rows_at_least = 0);

    static Status generate_default_value_block(const TabletSchema& schema,
                                               const std::vector<uint32_t>& cids,
                                               const std::vector<std::string>& default_values,
                                               const vectorized::Block& ref_block,
                                               vectorized::Block& default_value_block);

    static Status generate_new_block_for_partial_update(
            TabletSchemaSPtr rowset_schema, const PartialUpdateInfo* partial_update_info,
            const PartialUpdateReadPlan& read_plan_ori,
            const PartialUpdateReadPlan& read_plan_update,
            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
            vectorized::Block* output_block);

    // We use the TabletSchema from the caller because the TabletSchema in the rowset'meta
    // may be outdated due to schema change. Also note that the the cids should indicate the indexes
    // of the columns in the TabletSchema passed in.
    static Status fetch_value_through_row_column(RowsetSharedPtr input_rowset,
                                                 const TabletSchema& tablet_schema, uint32_t segid,
                                                 const std::vector<uint32_t>& rowids,
                                                 const std::vector<uint32_t>& cids,
                                                 vectorized::Block& block);

    static Status fetch_value_by_rowids(RowsetSharedPtr input_rowset, uint32_t segid,
                                        const std::vector<uint32_t>& rowids,
                                        const TabletColumn& tablet_column,
                                        vectorized::MutableColumnPtr& dst);

    virtual Result<std::unique_ptr<RowsetWriter>> create_transient_rowset_writer(
            const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
            int64_t txn_expiration = 0) = 0;

    static Status update_delete_bitmap(const BaseTabletSPtr& self, TabletTxnInfo* txn_info,
                                       int64_t txn_id, int64_t txn_expiration = 0);

    virtual Status save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                                      DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                                      const RowsetIdUnorderedSet& cur_rowset_ids) = 0;
    virtual CalcDeleteBitmapExecutor* calc_delete_bitmap_executor() = 0;

    void calc_compaction_output_rowset_delete_bitmap(
            const std::vector<RowsetSharedPtr>& input_rowsets,
            const RowIdConversion& rowid_conversion, uint64_t start_version, uint64_t end_version,
            std::set<RowLocation>* missed_rows,
            std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>* location_map,
            const DeleteBitmap& input_delete_bitmap, DeleteBitmap* output_rowset_delete_bitmap);

    Status check_rowid_conversion(
            RowsetSharedPtr dst_rowset,
            const std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>>&
                    location_map);

    static Status update_delete_bitmap_without_lock(
            const BaseTabletSPtr& self, const RowsetSharedPtr& rowset,
            const std::vector<RowsetSharedPtr>* specified_base_rowsets = nullptr);

    ////////////////////////////////////////////////////////////////////////////
    // end MoW functions
    ////////////////////////////////////////////////////////////////////////////

    RowsetSharedPtr get_rowset(const RowsetId& rowset_id);

    std::vector<RowsetSharedPtr> get_snapshot_rowset(bool include_stale_rowset = false) const;

    virtual void clear_cache() = 0;

    // Find the first consecutive empty rowsets. output->size() >= limit
    void calc_consecutive_empty_rowsets(std::vector<RowsetSharedPtr>* empty_rowsets,
                                        const std::vector<RowsetSharedPtr>& candidate_rowsets,
                                        int limit);

    // Return the merged schema of all rowsets
    virtual TabletSchemaSPtr merged_tablet_schema() const { return _max_version_schema; }

    void traverse_rowsets(std::function<void(const RowsetSharedPtr&)> visitor,
                          bool include_stale = false) {
        std::shared_lock rlock(_meta_lock);
        for (auto& [v, rs] : _rs_version_map) {
            visitor(rs);
        }
        if (!include_stale) return;
        for (auto& [v, rs] : _stale_rs_version_map) {
            visitor(rs);
        }
    }

    Status calc_file_crc(uint32_t* crc_value, int64_t start_version, int64_t end_version,
                         int32_t* rowset_count, int64_t* file_count);

    Status show_nested_index_file(std::string* json_meta);

protected:
    // Find the missed versions until the spec_version.
    //
    // for example:
    //     [0-4][5-5][8-8][9-9][14-14]
    // for cloud, if spec_version = 12, it will return [6-7],[10-12]
    // for local, if spec_version = 12, it will return [6, 6], [7, 7], [10, 10], [11, 11], [12, 12]
    virtual Versions calc_missed_versions(int64_t spec_version,
                                          Versions existing_versions) const = 0;

    void _print_missed_versions(const Versions& missed_versions) const;
    bool _reconstruct_version_tracker_if_necessary();

    static void _rowset_ids_difference(const RowsetIdUnorderedSet& cur,
                                       const RowsetIdUnorderedSet& pre,
                                       RowsetIdUnorderedSet* to_add, RowsetIdUnorderedSet* to_del);

    Status _capture_consistent_rowsets_unlocked(const std::vector<Version>& version_path,
                                                std::vector<RowsetSharedPtr>* rowsets) const;

    Status sort_block(vectorized::Block& in_block, vectorized::Block& output_block);

    mutable std::shared_mutex _meta_lock;
    TimestampedVersionTracker _timestamped_version_tracker;
    // After version 0.13, all newly created rowsets are saved in _rs_version_map.
    // And if rowset being compacted, the old rowsets will be saved in _stale_rs_version_map;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;
    // This variable _stale_rs_version_map is used to record these rowsets which are be compacted.
    // These _stale rowsets are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _stale_rs_version_map;
    const TabletMetaSharedPtr _tablet_meta;
    TabletSchemaSPtr _max_version_schema;

    // metrics of this tablet
    std::shared_ptr<MetricEntity> _metric_entity;

protected:
    std::mutex _schema_change_lock;

public:
    IntCounter* query_scan_bytes = nullptr;
    IntCounter* query_scan_rows = nullptr;
    IntCounter* query_scan_count = nullptr;
    IntCounter* flush_bytes = nullptr;
    IntCounter* flush_finish_count = nullptr;
    std::atomic<int64_t> published_count = 0;
    std::atomic<int64_t> read_block_count = 0;
    std::atomic<int64_t> write_count = 0;
    std::atomic<int64_t> compaction_count = 0;

    CompactionStage compaction_stage = CompactionStage::NOT_SCHEDULED;
    std::mutex sample_info_lock;
    std::vector<CompactionSampleInfo> sample_infos;
    Status last_compaction_status = Status::OK();
};

} /* namespace doris */
