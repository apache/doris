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

#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "vec/core/block.h"

namespace doris::segment_v2 {

using namespace vectorized;

class LazyInitSegmentIterator : public RowwiseIterator {
public:
    LazyInitSegmentIterator(std::shared_ptr<Segment> segment, SchemaSPtr schema,
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

    const Schema& schema() const override { return *_schema; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        return _inner_iterator->current_block_row_locations(locations);
    }

    bool update_profile(RuntimeProfile* profile) override {
        if (_inner_iterator != nullptr) {
            return _inner_iterator->update_profile(profile);
        }
        return false;
    }

private:
    bool _need_lazy_init {true};
    SchemaSPtr _schema = nullptr;
    std::shared_ptr<Segment> _segment;
    StorageReadOptions _read_options;
    RowwiseIteratorUPtr _inner_iterator;
};
} // namespace doris::segment_v2