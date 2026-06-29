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

#include "snii/io/batch_range_fetcher.h"

#include <algorithm>
#include <limits>

namespace snii::io {
using doris::Status; // RETURN_IF_ERROR expands to bare Status
namespace {

doris::Status checked_end(uint64_t offset, uint64_t len, uint64_t* out) {
    if (len > std::numeric_limits<uint64_t>::max() - offset) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "batch_range_fetcher: range end overflow");
    }
    *out = offset + len;
    return doris::Status::OK();
}

doris::Status checked_size(uint64_t len, size_t* out) {
    if (len > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "batch_range_fetcher: physical range too large");
    }
    *out = static_cast<size_t>(len);
    return doris::Status::OK();
}

} // namespace

BatchRangeFetcher::BatchRangeFetcher(FileReader* reader, uint64_t coalesce_gap)
        : reader_(reader), coalesce_gap_(coalesce_gap) {}

size_t BatchRangeFetcher::add(uint64_t offset, uint64_t len) {
    reqs_.push_back(Req {offset, len});
    return reqs_.size() - 1;
}

void BatchRangeFetcher::clear() {
    reqs_.clear();
    phys_.clear();
}

doris::Status BatchRangeFetcher::fetch() {
    if (reader_ == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "batch_range_fetcher: null reader");
    phys_.clear();
    if (reqs_.empty()) return doris::Status::OK();

    std::vector<size_t> order(reqs_.size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(),
              [&](size_t a, size_t b) { return reqs_[a].offset < reqs_[b].offset; });

    // Sweep in offset order, merging requests into physical segments.
    std::vector<Range> segs;
    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    for (size_t k = 0; k < order.size(); ++k) {
        Req& r = reqs_[order[k]];
        uint64_t r_end = 0;
        RETURN_IF_ERROR(checked_end(r.offset, r.len, &r_end));
        RETURN_IF_ERROR(checked_size(r.len, &r.len_size));
        const bool disjoint = r.offset > cur_end && r.offset - cur_end > coalesce_gap_;
        if (segs.empty() || disjoint) {
            segs.push_back(Range {r.offset, 0}); // length finalized below
            cur_start = r.offset;
            cur_end = r_end;
        } else {
            cur_end = std::max(cur_end, r_end);
        }
        r.phys_idx = segs.size() - 1;
        RETURN_IF_ERROR(checked_size(r.offset - cur_start, &r.sub_offset));
        RETURN_IF_ERROR(checked_size(cur_end - cur_start, &segs.back().len));
    }

    return reader_->read_batch(segs, &phys_);
}

Slice BatchRangeFetcher::get(size_t h) const {
    const Req& r = reqs_[h];
    const std::vector<uint8_t>& buf = phys_[r.phys_idx];
    return Slice(buf.data() + r.sub_offset, r.len_size);
}

} // namespace snii::io
