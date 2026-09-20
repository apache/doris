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

#include "storage/index/query/spi/io_read_batch.h"

#include <algorithm>
#include <limits>

namespace doris::index_query {
namespace {

Status checked_end(uint64_t offset, uint64_t len, uint64_t* out) {
    if (len > std::numeric_limits<uint64_t>::max() - offset) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "batch_range_fetcher: range end overflow");
    }
    *out = offset + len;
    return Status::OK();
}

Status checked_size(uint64_t len, size_t* out) {
    if (len > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "batch_range_fetcher: physical range too large");
    }
    *out = static_cast<size_t>(len);
    return Status::OK();
}

} // namespace

IoReadBatch::IoReadBatch(IoReader* reader, uint64_t coalesce_gap, MemoryBudget* budget)
        : reader_(reader), coalesce_gap_(coalesce_gap), budget_(budget) {}

size_t IoReadBatch::add(uint64_t offset, uint64_t len) {
    reqs_.push_back(Req {.offset = offset, .len = len});
    return reqs_.size() - 1;
}

Status IoReadBatch::refresh_bounded_ranges() {
    if (bounded_requests_ == reqs_.size()) {
        return Status::OK();
    }
    bounded_ranges_.clear();
    bounded_bytes_ = 0;
    for (const Req& req : reqs_) {
        uint64_t end = 0;
        size_t len = 0;
        RETURN_IF_ERROR(checked_end(req.offset, req.len, &end));
        RETURN_IF_ERROR(checked_size(req.len, &len));
        bounded_ranges_.push_back({.offset = req.offset, .len = len});
    }
    std::ranges::sort(bounded_ranges_, {}, &IoRange::offset);
    size_t count = 0;
    for (const IoRange range : bounded_ranges_) {
        const uint64_t end = range.offset + range.len;
        if (count != 0) {
            IoRange& previous = bounded_ranges_[count - 1];
            const uint64_t previous_end = previous.offset + previous.len;
            if (range.offset <= previous_end || range.offset - previous_end <= coalesce_gap_) {
                RETURN_IF_ERROR(
                        checked_size(std::max(end, previous_end) - previous.offset, &previous.len));
                continue;
            }
        }
        bounded_ranges_[count++] = range;
    }
    bounded_ranges_.resize(count);
    for (const IoRange& range : bounded_ranges_) {
        bounded_bytes_ += range.len;
    }
    bounded_requests_ = reqs_.size();
    return Status::OK();
}

Status IoReadBatch::try_add(uint64_t offset, uint64_t len, uint64_t max_bytes, size_t max_ranges,
                            bool* accepted, size_t* handle) {
    DORIS_CHECK(accepted != nullptr);
    DORIS_CHECK(handle != nullptr);
    *accepted = false;
    uint64_t merged_end = 0;
    RETURN_IF_ERROR(checked_end(offset, len, &merged_end));
    RETURN_IF_ERROR(refresh_bounded_ranges());
    const uint64_t first_end = offset > coalesce_gap_ ? offset - coalesce_gap_ : 0;
    auto first = std::ranges::lower_bound(bounded_ranges_, first_end, {}, [](const IoRange& range) {
        return range.offset + range.len;
    });
    auto last = first;
    uint64_t merged_start = offset;
    uint64_t replaced_bytes = 0;
    while (last != bounded_ranges_.end() &&
           (last->offset <= merged_end || last->offset - merged_end <= coalesce_gap_)) {
        merged_start = std::min(merged_start, last->offset);
        merged_end = std::max(merged_end, last->offset + last->len);
        replaced_bytes += last->len;
        ++last;
    }
    const uint64_t merged_bytes = merged_end - merged_start;
    const uint64_t bytes = bounded_bytes_ - replaced_bytes + merged_bytes;
    const size_t ranges = bounded_ranges_.size() - (last - first) + 1;
    if (bytes > max_bytes || ranges > max_ranges) {
        return Status::OK();
    }
    size_t range_len = 0;
    RETURN_IF_ERROR(checked_size(merged_bytes, &range_len));
    const IoRange range {.offset = merged_start, .len = range_len};
    if (first == last) {
        bounded_ranges_.insert(first, range);
    } else {
        *first = range;
        bounded_ranges_.erase(first + 1, last);
    }
    *handle = add(offset, len);
    bounded_bytes_ = bytes;
    bounded_requests_ = reqs_.size();
    *accepted = true;
    return Status::OK();
}

void IoReadBatch::clear() {
    reqs_.clear();
    phys_.clear();
    read_memory_.reset();
    bounded_ranges_.clear();
    bounded_requests_ = 0;
    bounded_bytes_ = 0;
}

Status IoReadBatch::fetch() {
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "batch_range_fetcher: null reader");
    }
    phys_.clear();
    read_memory_.reset();
    if (reqs_.empty()) {
        return Status::OK();
    }

    std::vector<size_t> order(reqs_.size());
    for (size_t i = 0; i < order.size(); ++i) {
        order[i] = i;
    }
    std::ranges::sort(order, [&](size_t a, size_t b) { return reqs_[a].offset < reqs_[b].offset; });

    // Sweep in offset order, merging requests into physical segments.
    std::vector<IoRange> segs;
    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    for (const size_t index : order) {
        Req& r = reqs_[index];
        uint64_t r_end = 0;
        RETURN_IF_ERROR(checked_end(r.offset, r.len, &r_end));
        RETURN_IF_ERROR(checked_size(r.len, &r.len_size));
        const bool disjoint = r.offset > cur_end && r.offset - cur_end > coalesce_gap_;
        if (segs.empty() || disjoint) {
            segs.push_back(IoRange {.offset = r.offset, .len = 0}); // length finalized below
            cur_start = r.offset;
            cur_end = r_end;
        } else {
            cur_end = std::max(cur_end, r_end);
        }
        r.phys_idx = segs.size() - 1;
        RETURN_IF_ERROR(checked_size(r.offset - cur_start, &r.sub_offset));
        RETURN_IF_ERROR(checked_size(cur_end - cur_start, &segs.back().len));
    }

    if (budget_ != nullptr) {
        uint64_t bytes = 0;
        for (const IoRange& range : segs) {
            bytes += range.len;
        }
        RETURN_IF_ERROR(budget_->reserve(bytes, &read_memory_));
    }
    Status status = reader_->read_batch(segs, &phys_);
    if (!status.ok()) {
        phys_.clear();
        read_memory_.reset();
    }
    return status;
}

std::span<const uint8_t> IoReadBatch::get(size_t h) const {
    const Req& r = reqs_[h];
    const std::vector<uint8_t>& buf = phys_[r.phys_idx];
    return {buf.data() + r.sub_offset, r.len_size};
}

} // namespace doris::index_query
