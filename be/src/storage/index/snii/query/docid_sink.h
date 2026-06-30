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
#include <limits>
#include <span>
#include <vector>

#include "common/status.h"

namespace doris::snii::query {

// Bulk docid handoff for query operators. Each span is sorted ascending; callers
// that need a single vector can use VectorDocIdSink.
class DocIdSink {
public:
    virtual ~DocIdSink() = default;
    virtual Status append_sorted(std::span<const uint32_t> docids) = 0;
    virtual Status append_range(uint32_t first, uint64_t last_exclusive) = 0;

    // True iff the sink deduplicates and globally orders on its own (e.g. a Roaring
    // bitmap via addMany/addRange). For such sinks a multi-term OR can stream each
    // posting straight in -- skipping the per-term vector materialization plus the
    // K-way merge accumulator. Sinks that hand back a single globally-sorted,
    // deduplicated vector (VectorDocIdSink) keep the default false, so callers
    // materialize + merge before appending. The gate must stay conservative:
    // streaming several postings into a non-dedup sink would break that contract.
    virtual bool dedups() const { return false; }
};

class VectorDocIdSink final : public DocIdSink {
public:
    explicit VectorDocIdSink(std::vector<uint32_t>& docids) : docids_(docids) {}

    Status append_sorted(std::span<const uint32_t> docids) override {
        docids_.insert(docids_.end(), docids.begin(), docids.end());
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        if (last_exclusive <= first) {
            return Status::OK();
        }
        if (last_exclusive > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "docid_sink: range exceeds uint32 docid space");
        }
        const uint64_t count = last_exclusive - first;
        if (count > static_cast<uint64_t>(docids_.max_size() - docids_.size())) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("docid_sink: range too large");
        }
        docids_.reserve(docids_.size() + static_cast<size_t>(count));
        for (uint64_t docid = first; docid < last_exclusive; ++docid) {
            docids_.push_back(static_cast<uint32_t>(docid));
        }
        return Status::OK();
    }

private:
    std::vector<uint32_t>& docids_;
};

} // namespace doris::snii::query
