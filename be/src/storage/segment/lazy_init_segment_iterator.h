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

#include "core/block/block.h"
#include "storage/rowset/rowset_segment_id.h"
#include "storage/segment/common.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"

namespace doris {
class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
}; // namespace doris
namespace doris::segment_v2 {

// The iterator of one segment that opens the segment on its first init() or read, not when it
// is created, so a read that holds the iterators of many segments only pays for the ones it
// has reached. The rowset reader hands every segment of a rowset the same options object;
// `row_ranges`, when not empty, is the part of this segment the read covers.
class LazyInitSegmentIterator : public RowwiseIterator {
public:
    LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, RowsetSegmentRef segment,
                            bool should_use_cache, ReadSchemaSPtr schema,
                            std::shared_ptr<const StorageReadOptions> options,
                            RowRanges row_ranges);

    ~LazyInitSegmentIterator() override = default;

    // `opts` drives the merge or union above; the segment is read with the options of its
    // rowset reader.
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override {
        if (UNLIKELY(_need_lazy_init)) {
            RETURN_IF_ERROR(init(*_options));
            DCHECK(_inner_iterator != nullptr);
        }

        return _inner_iterator->next_batch(block);
    }

    const ReadSchema& schema() const override { return *_schema; }

    void update_profile(RuntimeProfile* profile) override {
        if (_inner_iterator != nullptr) {
            _inner_iterator->update_profile(profile);
        }
    }

private:
    bool _need_lazy_init {true};
    BetaRowsetSharedPtr _rowset;
    RowsetSegmentRef _segment {0, -1};
    bool _should_use_cache {false};
    ReadSchemaSPtr _schema = nullptr;
    std::shared_ptr<const StorageReadOptions> _options;
    RowRanges _row_ranges;
    RowwiseIteratorUPtr _inner_iterator;
};
} // namespace doris::segment_v2
