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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H

#include <queue>
#include <vector>

#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/column_data.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_group.h"

namespace doris {

// Each segment group corresponds to a MergeContext, which is able to produce ordered rows.
struct AlphaMergeContext {
    std::unique_ptr<ColumnData> column_data = nullptr;

    int key_range_index = -1;

    // Read data from ColumnData for the first time.
    // ScanKey should be sought in this case.
    bool first_read_symbol = true;

    RowBlock* row_block = nullptr;

    std::unique_ptr<RowCursor> row_cursor = nullptr;

    bool is_eof = false;
};

struct AlphaMergeContextComparator {
    bool operator()(const AlphaMergeContext* x, const AlphaMergeContext* y) const;
};

class AlphaRowsetReader : public RowsetReader {
public:
    AlphaRowsetReader(int num_rows_per_row_block, AlphaRowsetSharedPtr rowset,
                      const std::shared_ptr<MemTracker>& parent_tracker = nullptr);

    ~AlphaRowsetReader() override;

    // reader init
    OLAPStatus init(RowsetReaderContext* read_context) override;

    // read next block data
    // If parent_tracker is not null, the block we get from next_block() will have the parent_tracker.
    // It's ok, because we only get ref here, the block's owner is this reader.
    OLAPStatus next_block(RowBlock** block) override;

    bool delete_flag() override;

    Version version() override;

    VersionHash version_hash() override;

    RowsetSharedPtr rowset() override;

    int64_t filtered_rows() override;

private:
    OLAPStatus _init_merge_ctxs(RowsetReaderContext* read_context);

    OLAPStatus _union_block(RowBlock** block);
    OLAPStatus _merge_block(RowBlock** block);
    OLAPStatus _pull_next_row_for_merge_rowset(RowCursor** row);
    OLAPStatus _pull_next_block(AlphaMergeContext* merge_ctx);

    // Doris will split query predicates to several scan keys
    // This function is used to fetch block when advancing
    // current scan key to next scan key.
    OLAPStatus _pull_first_block(AlphaMergeContext* merge_ctx);

    // merge by priority queue(_merge_heap)
    // this method has same function with _pull_next_row_for_merge_rowset, but using heap merge.
    // and this should replace the _pull_next_row_for_merge_rowset later.
    OLAPStatus _pull_next_row_for_merge_rowset_v2(RowCursor** row);
    // init the merge heap, this should be call before calling _pull_next_row_for_merge_rowset_v2();
    OLAPStatus _init_merge_heap();
    // update the merge ctx.
    // 1. get next row block of this ctx, if current row block is empty.
    // 2. read the current row of the row block and push it to merge heap.
    // 3. point to the next row of the row block
    OLAPStatus _update_merge_ctx_and_build_merge_heap(AlphaMergeContext* merge_ctx);

private:
    int _num_rows_per_row_block;
    AlphaRowsetSharedPtr _rowset;
    std::shared_ptr<MemTracker> _parent_tracker;
    std::string _rowset_path;
    AlphaRowsetMeta* _alpha_rowset_meta;
    const std::vector<std::shared_ptr<SegmentGroup>>& _segment_groups;

    std::vector<AlphaMergeContext> _merge_ctxs;
    std::unique_ptr<RowBlock> _read_block;
    OLAPStatus (AlphaRowsetReader::*_next_block)(RowBlock** block) = nullptr;
    RowCursor* _dst_cursor = nullptr;
    int _key_range_size;

    // In streaming ingestion, row among different segment
    // groups may overlap, and is necessary to be taken
    // into consideration deliberately.
    bool _is_segments_overlapping;

    // ordinal of ColumnData upon reading
    size_t _ordinal;

    RowsetReaderContext* _current_read_context;
    OlapReaderStatistics _owned_stats;
    OlapReaderStatistics* _stats = &_owned_stats;

    // a priority queue for merging rowsets
    std::priority_queue<AlphaMergeContext*, vector<AlphaMergeContext*>, AlphaMergeContextComparator>
            _merge_heap;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_READER_H
