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

#include "storage/mow/key_probe.h"

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "service/point_query_executor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

using namespace ErrorCode;

MowKeyProbe::MowKeyProbe(BaseTablet* tablet, TabletSchema* lookup_schema, bool has_sequence_col,
                         std::shared_ptr<MowContext> mow_context, const RowsetId& writing_rowset_id,
                         uint32_t writing_segment_id, Policy policy)
        : _tablet(tablet),
          _lookup_schema(lookup_schema),
          _has_sequence_col(has_sequence_col),
          _mow_context(std::move(mow_context)),
          _writing_rowset_id(writing_rowset_id),
          _writing_segment_id(writing_segment_id),
          _policy(policy) {}

Result<ProbeOutcome> MowKeyProbe::probe(
        const std::string& key, size_t segment_pos, bool key_has_seq_suffix, bool have_delete_sign,
        const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
        PartialUpdateStats& stats) const {
    RowLocation loc;
    // save rowset shared ptr so this rowset wouldn't delete
    RowsetSharedPtr rowset;
    auto st = _tablet->lookup_row_key(key, _lookup_schema, key_has_seq_suffix, specified_rowsets,
                                      &loc, _mow_context->max_version, segment_caches, &rowset,
                                      /*with_rowid=*/false);
    if (st.is<KEY_NOT_FOUND>()) {
        ++stats.num_rows_new_added;
        return ProbeOutcome {KeyProbeResult::NOT_FOUND, {}, nullptr, /*use_default_or_null=*/true};
    }
    if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
        LOG(WARNING) << "failed to lookup row key, error: " << st;
        return ResultError(std::move(st));
    }

    // Stored row's seq is larger, so the incoming row loses.
    bool seq_loses = st.is<KEY_ALREADY_EXISTS>();
    // A delete-signed row needs no old values -- but only without a seq col,
    // since the seq must still be read for merge-on-read compaction.
    bool delete_sign_skip = have_delete_sign && !_has_sequence_col && _policy.skip_delete_sign;
    // Flexible PU insert-after-delete: the old row was already deleted in this
    // load, so the insert counts as a brand-new row.
    bool in_load_deleted =
            _policy.skip_in_load_deleted &&
            _mow_context->delete_bitmap->contains(
                    {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id);
    // Skip reading the old row (fill defaults) in any of these cases.
    bool use_default = (seq_loses && _policy.skip_seq_loses) || delete_sign_skip || in_load_deleted;
    ProbeOutcome outcome {seq_loses ? KeyProbeResult::FOUND_NEWER : KeyProbeResult::FOUND, loc,
                          std::move(rowset), use_default};

    // Apply the delete-bitmap marks right away -- see class comment (segcompaction).
    if (seq_loses) {
        if (_policy.delete_bitmap_mode == DeleteBitmapMode::OLD_AND_NEW_ROW) {
            // although we need to mark delete current row, we still need to read missing
            // columns for this row, we need to ensure that each column is aligned
            _mow_context->delete_bitmap->add(
                    {_writing_rowset_id, _writing_segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                    cast_set<uint32_t>(segment_pos));
            ++stats.num_rows_deleted;
        }
    } else if (_policy.delete_bitmap_mode != DeleteBitmapMode::READ_ONLY) {
        _mow_context->delete_bitmap->add(
                {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id);
        ++stats.num_rows_updated;
    }
    return outcome;
}

Result<PrevSeqProbe> MowKeyProbe::probe_previous_seq_value(
        const std::string& key, const std::vector<RowsetSharedPtr>& specified_rowsets,
        std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) const {
    RowLocation loc;
    RowsetSharedPtr rowset;
    PrevSeqProbe result;
    auto st =
            _tablet->lookup_row_key(key, _lookup_schema, /*with_seq_col=*/false, specified_rowsets,
                                    &loc, _mow_context->max_version, segment_caches, &rowset,
                                    /*with_rowid=*/true, &result.encoded_seq_value);
    if (st.is<KEY_NOT_FOUND>()) {
        result.outcome = ProbeOutcome {KeyProbeResult::NOT_FOUND,
                                       {},
                                       nullptr,
                                       /*use_default_or_null=*/true};
        return result;
    }
    if (!st.ok()) {
        return ResultError(std::move(st));
    }
    result.outcome.result = KeyProbeResult::FOUND;
    result.outcome.loc = loc;
    result.outcome.rowset = std::move(rowset);
    result.outcome.use_default_or_null = false;
    return result;
}

void MowKeyProbe::maybe_invalidate_row_cache(int64_t tablet_id, const TabletSchema& schema,
                                             DataWriteType write_type, const std::string& key) {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache && schema.has_row_store_for_all_columns() &&
        write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({tablet_id, key});
    }
}

std::string encode_mow_key_invalidate_cache(
        const RowKeyEncoder& key_encoder, const std::vector<IOlapColumnDataAccessor*>& key_columns,
        const IOlapColumnDataAccessor* seq_column, size_t pos, bool row_has_seq, int64_t tablet_id,
        const TabletSchema& schema, DataWriteType write_type) {
    std::string key = key_encoder.full_encode(key_columns, pos);
    // the row cache uses the key without the seq as its key, so invalidate before the suffix
    MowKeyProbe::maybe_invalidate_row_cache(tablet_id, schema, write_type, key);
    if (row_has_seq) {
        key_encoder.append_seq_suffix(&key, seq_column, pos);
    }
    return key;
}

} // namespace doris::segment_v2
