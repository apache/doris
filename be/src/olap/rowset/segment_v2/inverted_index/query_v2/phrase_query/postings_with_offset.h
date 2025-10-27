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

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TPostings>
class PostingsWithOffset : public DocSet {
public:
    PostingsWithOffset(TPostings postings, uint32_t offset)
            : _postings(std::move(postings)), _offset(offset) {}

    void postings(std::vector<uint32_t>& output) {
        _postings->positions_with_offset(_offset, output);
    }

    uint32_t advance() override { return _postings->advance(); }
    uint32_t seek(uint32_t target) override { return _postings->seek(target); }
    uint32_t doc() const override { return _postings->doc(); }
    uint32_t size_hint() const override { return _postings->size_hint(); }
    uint32_t freq() const override { return _postings->freq(); }
    uint32_t norm() const override { return _postings->norm(); }

private:
    TPostings _postings;
    uint32_t _offset = 0;
};

template <typename TPostings>
using PostingsWithOffsetPtr = std::shared_ptr<PostingsWithOffset<TPostings>>;

using PositionPostings = std::shared_ptr<SegmentPostings<TermPositionsPtr>>;
using PositionPostingsWithOffsetPtr = std::shared_ptr<PostingsWithOffset<PositionPostings>>;

} // namespace doris::segment_v2::inverted_index::query_v2