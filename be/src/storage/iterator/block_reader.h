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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/iterator/tablet_block_iterator.h"
#include "storage/iterators.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {

class Block;
class RuntimeProfile;

// The row-level reader of a tablet.
//
// It reads the segments of every rowset of the read through one RowwiseIterator, the rows of
// the read: a VMergeIterator over all segments when the rowsets overlap on keys and the read
// needs key order, a VUnionIterator otherwise. One TabletBlockIterator, chosen by table model
// and read mode, turns those rows into the output Blocks. The reader itself keeps no
// per-read-mode state.
//
//   BlockReader
//    └ TabletBlockIterator (Direct / UniqueKey / SequenceMap / AggKey / Changelog / TopN)
//       └ _rows_iter: VMergeIterator or VUnionIterator over
//            every segment of a rowset whose segments overlap on keys, and
//            a VUnionIterator per rowset whose segments do not,
//          in (rowset version, segment) order
//            └ LazyInitSegmentIterator -> SegmentIterator
class BlockReader final : public TabletReader {
public:
    BlockReader();
    ~BlockReader() override;

    // Initialize BlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;

    Status next_block_with_aggregation(Block* block, bool* eof) override;

    // Rows dropped as older versions of a key, by the merge and by the fold.
    uint64_t merged_rows() const override;

    void update_profile(RuntimeProfile* profile) override;

    // Returns the configured preferred output block byte budget; 0 when adaptive is disabled.
    size_t preferred_block_size_bytes() const override {
        return config::enable_adaptive_batch_size ? _reader_context.preferred_block_size_bytes : 0;
    }

private:
    // Whether the rowsets, in version order, are not already one key-ordered sequence: some
    // rowset has overlapping segments, or two rowsets overlap on keys.
    bool _rowsets_overlap(const ReaderParams& read_params) const;
    // Build the rows of the read: `_rows_iter`, or for a TopN read `_rowset_iters`.
    Status _init_rows_iter(const ReaderParams& read_params);
    // The options the rows of the read are initialized with.
    void _init_rows_options(const ReaderParams& read_params);
    // `_rows_iter` over `children`, the segment iterators of every rowset in (rowset version,
    // segment) order: a merge when `merge`, the children one after another otherwise.
    Status _new_rows_iter(std::vector<RowwiseIteratorUPtr> children, bool merge);
    // Pick and initialize the TabletBlockIterator of this read.
    Status _init_block_iterator(const ReaderParams& read_params, bool merge_by_sequence_mapping);

    // Options the merge or union of the read is initialized with: the output Block limits and
    // the runtime state. How the rows are merged is _reader_context.merge.
    StorageReadOptions _rows_options;
    // The rows of the read; empty for a TopN read.
    RowwiseIteratorUPtr _rows_iter;
    // TopN reads only: the key-ordered rows of every rowset, in read order.
    std::vector<RowwiseIteratorUPtr> _rowset_iters;
    std::unique_ptr<TabletBlockIterator> _block_iter;
};

} // namespace doris
