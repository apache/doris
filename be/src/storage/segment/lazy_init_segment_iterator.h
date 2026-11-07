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

#include "core/block/block.h"
#include "storage/rowset/rowset_segment_id.h"
#include "storage/segment/common.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"

namespace doris {
class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
}; // namespace doris
namespace doris::segment_v2 {

class LazyInitSegmentIterator : public RowwiseIterator {
public:
    LazyInitSegmentIterator(BetaRowsetSharedPtr rowset, RowsetSegmentRef segment,
                            bool should_use_cache, ReadSchemaSPtr schema,
                            const StorageReadOptions& opts);

    ~LazyInitSegmentIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override {
        if (UNLIKELY(_need_lazy_init)) {
            RETURN_IF_ERROR(init(_read_options));
            DCHECK(_inner_iterator != nullptr);
        }

        return _inner_iterator->next_batch(block);
    }

    const ReadSchema& schema() const override { return *_schema; }

    // The segment merge orders the rows of one key (and one sequence value) by data_id, so that
    // the segment flushed later supplies the newer row (VMergeIteratorContext::compare). Answer
    // with the id of the segment this iterator reads, the value SegmentIterator::data_id()
    // reports, whether or not init() has loaded the segment yet. Without this every segment
    // answered 0 and the tie fell to the heap order: with key 1 in segment 0 (v = 10) and in
    // segment 1 (v = 11) of one rowset, a read could keep v = 10.
    uint64_t data_id() const override { return static_cast<uint64_t>(_segment.id); }

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
    StorageReadOptions _read_options;
    RowwiseIteratorUPtr _inner_iterator;
};
} // namespace doris::segment_v2
