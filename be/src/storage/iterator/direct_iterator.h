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
#include <vector>

#include "common/status.h"
#include "storage/iterator/block_iterator.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_fwd.h"

namespace doris {
class Block;
class RuntimeProfile;
struct OlapReaderStatistics;
struct RowsetReaderContext;

// Reads rowsets one after another in version order, and the segments of each rowset in segment
// order through RowsetReader::get_segment_iterators(), with nothing to merge or fold. Each rowset
// creates its lazy segment iterators only when the read reaches it, and its RowsetReader (which
// pins the rowset) is released as soon as its last segment reaches EOF.
class DirectIterator final : public BlockIterator {
public:
    // `rs_readers` are inited and in version order; `read_context` and `stats` outlive the
    // iterator.
    DirectIterator(std::vector<RowsetReaderSharedPtr> rs_readers, RowsetReaderContext* read_context,
                   OlapReaderStatistics* stats);

    Status next_block(Block* block, bool* eof) override;

    void update_profile(RuntimeProfile* profile) override;

private:
    // Move `_cur_segment` to the next unread segment, opening the next non-empty rowset when the
    // current rowset has no more segments; `*has_segment` is false once all rowsets are read.
    Status _advance_segment(bool* has_segment);

    std::vector<RowsetReaderSharedPtr> _rs_readers;
    size_t _next_rowset = 0;
    RowsetReaderSharedPtr _cur_rs_reader;
    std::vector<RowwiseIteratorUPtr> _segments;
    size_t _next_segment = 0;
    RowwiseIteratorUPtr _cur_segment;
    RowsetReaderContext* const _read_context;
    OlapReaderStatistics* const _stats;
};

} // namespace doris
