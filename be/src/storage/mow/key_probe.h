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
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/utils.h"

namespace doris {
class BaseTablet;
class IOlapColumnDataAccessor;
class RowKeyEncoder;
class TabletSchema;
class SegmentCacheHandle;
struct MowContext;
struct PartialUpdateStats;

namespace segment_v2 {

// Result of looking up one primary key in a merge-on-write load.
enum class KeyProbeResult : uint8_t {
    // Key absent: brand-new row.
    NOT_FOUND = 0,
    // Key exists; the incoming row replaces the old one.
    FOUND = 1,
    // Stored row has a larger sequence value, so the incoming row loses.
    FOUND_NEWER = 2,
};

struct ProbeOutcome {
    KeyProbeResult result {KeyProbeResult::NOT_FOUND};
    // Only meaningful when result != NOT_FOUND; a miss leaves it default-constructed.
    RowLocation loc;
    // Keeps the rowset holding `loc` alive while the caller reads old-row columns.
    RowsetSharedPtr rowset;
    // true  -> cells not given in the input take their default/null value
    // false -> the old row at `loc` must be read into the fill plan
    bool use_default_or_null {true};
};

// The old row plus its encoded sequence value, returned by probe_previous_seq_value. The sequence
// value is empty unless outcome.result is FOUND.
struct PrevSeqProbe {
    ProbeOutcome outcome;
    std::string encoded_seq_value;
};

// IMPORTANT: probe() applies delete-bitmap marks right away (not batched), using
// TEMP_VERSION_COMMON. Segcompaction and the pre-commit checks read these TEMP marks mid-load, so
// delaying them would lose deletes.
class MowKeyProbe {
public:
    // Which rows this probe marks deleted in mow_context->delete_bitmap.
    enum class MarkDeleted : uint8_t {
        // Pure lookup: marking is somebody else's job (the row binlog history read).
        NONE = 0,
        // Mark the old row, i.e. the row already in the table that the incoming row replaces. On a
        // sequence loss nothing is marked at all: the caller of this mode drops the losing row
        // itself instead of writing it into the segment.
        OLD_ROW = 1,
        // Mark the old row, and the incoming row too when it loses on sequence: the writer has
        // already written that row into the segment, so it cannot just be dropped.
        OLD_AND_LOSING_ROW = 2,
    };

    // The use_defaults_* flags list the situations in which the probe sets
    // ProbeOutcome::use_default_or_null instead of asking for the old row's values.
    struct Policy {
        MarkDeleted mark_deleted {MarkDeleted::OLD_AND_LOSING_ROW};
        // Only without a sequence column: that one must survive for merge-on-read compaction.
        bool use_defaults_for_delete_signed {true};
        // A row that loses on sequence does not take effect, so nothing fills it.
        bool use_defaults_for_seq_loser {true};
        // Flexible partial update: the old row was already deleted earlier in this same load, so
        // the incoming row is brand new.
        bool use_defaults_for_in_load_deleted {false};
    };

    // lookup_schema goes to BaseTablet::lookup_row_key; has_sequence_col comes from the input's
    // schema and drives use_defaults_for_delete_signed; writing_rowset_id/writing_segment_id
    // identify the segment being written, used only to mark a sequence loser.
    MowKeyProbe(BaseTablet* tablet, TabletSchema* lookup_schema, bool has_sequence_col,
                std::shared_ptr<MowContext> mow_context, const RowsetId& writing_rowset_id,
                uint32_t writing_segment_id, Policy policy);

    // The partial update fill paths: mark the old row, or the incoming row when it loses on
    // sequence, and read the old row's values for the columns the input does not carry. Two cases
    // take defaults instead: a losing row (it never takes effect) and a delete-signed row without a
    // sequence column (its values are never read back). `flexible` adds the insert-after-delete
    // rule.
    static MowKeyProbe for_partial_update(BaseTablet* tablet, TabletSchema* lookup_schema,
                                          bool has_sequence_col,
                                          std::shared_ptr<MowContext> mow_context,
                                          const RowsetId& writing_rowset_id,
                                          uint32_t writing_segment_id, bool flexible) {
        return MowKeyProbe {tablet,
                            lookup_schema,
                            has_sequence_col,
                            std::move(mow_context),
                            writing_rowset_id,
                            writing_segment_id,
                            Policy {
                                    .mark_deleted = MarkDeleted::OLD_AND_LOSING_ROW,
                                    .use_defaults_for_delete_signed = true,
                                    .use_defaults_for_seq_loser = true,
                                    .use_defaults_for_in_load_deleted = flexible,
                            }};
    }

    // The row binlog history lookup: marks nothing, and keeps the old values of a sequence loser,
    // and of a delete-signed row when historical values are requested.
    static MowKeyProbe for_row_binlog(BaseTablet* tablet, TabletSchema* lookup_schema,
                                      bool has_sequence_col,
                                      std::shared_ptr<MowContext> mow_context,
                                      bool need_historical_value) {
        return MowKeyProbe {tablet,
                            lookup_schema,
                            has_sequence_col,
                            std::move(mow_context),
                            RowsetId {},
                            0,
                            Policy {
                                    .mark_deleted = MarkDeleted::NONE,
                                    .use_defaults_for_delete_signed = !need_historical_value,
                                    .use_defaults_for_seq_loser = false,
                                    .use_defaults_for_in_load_deleted = false,
                            }};
    }

    // Probe one row. `key` is the full encoded key (with seq suffix when key_has_seq_suffix).
    // `segment_pos` is the row's position in the segment being written (self-mark only). `stats`
    // counts new/updated/deleted rows, except under MarkDeleted::NONE which counts nothing; a
    // caller that doesn't track partial-update counts passes a throwaway.
    //
    // On NOT_FOUND it bumps stats.num_rows_new_added, but PartialUpdateInfo::handle_new_key() needs
    // caller-side state, so the partial update callers still run it themselves.
    Result<ProbeOutcome> probe(const std::string& key, size_t segment_pos, bool key_has_seq_suffix,
                               bool have_delete_sign,
                               const std::vector<RowsetSharedPtr>& specified_rowsets,
                               std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                               PartialUpdateStats& stats) const;

    // Lookup without the seq suffix; returns the old row plus its encoded sequence value
    // (BlockAggregator). Never touches the delete bitmap, and ignores the policy entirely.
    Result<PrevSeqProbe> probe_previous_seq_value(
            const std::string& key, const std::vector<RowsetSharedPtr>& specified_rowsets,
            std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) const;

    // Erase the row-cache entry. Erase-only: the rowset isn't visible yet, so inserting could
    // expose uncommitted data if the load fails.
    static void maybe_invalidate_row_cache(int64_t tablet_id, const TabletSchema& schema,
                                           DataWriteType write_type, const std::string& key);

private:
    BaseTablet* _tablet = nullptr;
    TabletSchema* _lookup_schema = nullptr;
    bool _has_sequence_col = false;
    std::shared_ptr<MowContext> _mow_context;
    RowsetId _writing_rowset_id;
    uint32_t _writing_segment_id = 0;
    Policy _policy;
};

std::string encode_mow_key_invalidate_cache(
        const RowKeyEncoder& key_encoder, const std::vector<IOlapColumnDataAccessor*>& key_columns,
        const IOlapColumnDataAccessor* seq_column, size_t pos, bool row_has_seq, int64_t tablet_id,
        const TabletSchema& schema, DataWriteType write_type);

} // namespace segment_v2
} // namespace doris
