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
    // The fields below are valid only when result != NOT_FOUND.
    RowLocation loc;
    // Keeps the rowset holding `loc` alive while the caller reads old-row columns.
    RowsetSharedPtr rowset;
    // true  -> cells not given in the input take default/null
    // false -> the old row at `loc` must be read into the fill plan
    bool use_default_or_null {true};
};

// The stored row plus its encoded sequence value, returned by
// probe_previous_seq_value.
struct PrevSeqProbe {
    ProbeOutcome outcome;
    std::string encoded_seq_value;
};

// IMPORTANT: probe() applies delete-bitmap marks right away (not batched),
// using TEMP_VERSION_COMMON. Segcompaction and the pre-commit checks read
// these TEMP marks mid-load, so delaying them would lose deletes.
class MowKeyProbe {
public:
    // What the probe writes into mow_context->delete_bitmap.
    enum class DeleteBitmapMode : uint8_t {
        // Pure lookup; don't touch the delete bitmap.
        READ_ONLY = 0,
        // FOUND: mark the old row only; the caller drops the losing new row itself.
        OLD_ROW = 1,
        // FOUND: mark the old row. FOUND_NEWER: also mark the losing new row
        // (the writer already wrote it, so it can't just be dropped).
        OLD_AND_NEW_ROW = 2,
    };

    struct Policy {
        DeleteBitmapMode delete_bitmap_mode {DeleteBitmapMode::OLD_AND_NEW_ROW};
        // Delete-signed row with no seq col: skip reading the old row (use
        // defaults). Binlog turns this off to keep old values for __BEFORE__*.
        bool skip_delete_sign {true};
        // Row that loses on sequence (FOUND_NEWER): skip reading the old row.
        // Binlog turns this off to read the surviving old row.
        bool skip_seq_loses {true};
        // Flexible partial update: if the old row was already deleted earlier
        // in this load, treat the incoming row as brand new.
        bool skip_in_load_deleted {false};
    };

    // lookup_schema: schema for BaseTablet::lookup_row_key (writers: load
    //   schema; binlog retriever: the tablet's latest schema).
    // has_sequence_col: does the input's schema have a seq column (drives the
    //   delete-sign skip); binlog: source schema, writers: load schema.
    // writing_rowset_id/writing_segment_id: the segment being written, for the
    //   FOUND_NEWER self-mark; used only when delete_bitmap_mode == OLD_AND_NEW_ROW.
    MowKeyProbe(BaseTablet* tablet, TabletSchema* lookup_schema, bool has_sequence_col,
                std::shared_ptr<MowContext> mow_context, const RowsetId& writing_rowset_id,
                uint32_t writing_segment_id, Policy policy);

    // Probe one row. `key` is the full encoded key (with seq suffix when
    // key_has_seq_suffix). `segment_pos` is the row's position in the segment
    // being written (self-mark only). `stats` is always written; a caller that
    // doesn't track partial-update counts passes a throwaway.
    //
    // On NOT_FOUND it bumps stats.num_rows_new_added; the caller must still
    // run PartialUpdateInfo::handle_new_key() (it needs caller-side state), so
    // that stays at the call site.
    Result<ProbeOutcome> probe(const std::string& key, size_t segment_pos, bool key_has_seq_suffix,
                               bool have_delete_sign,
                               const std::vector<RowsetSharedPtr>& specified_rowsets,
                               std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                               PartialUpdateStats& stats) const;

    // Lookup without the seq suffix; returns the stored row plus its encoded
    // sequence value (BlockAggregator). Never touches the delete bitmap.
    Result<PrevSeqProbe> probe_previous_seq_value(
            const std::string& key, const std::vector<RowsetSharedPtr>& specified_rowsets,
            std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches) const;

    // Erase the row-cache entry. Erase-only: the rowset isn't visible yet, so
    // inserting could expose uncommitted data if the load fails.
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
