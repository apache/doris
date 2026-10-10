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
#include <roaring/roaring.hh>
#include <span>

#include "storage/index/query/docid_sink.h"

namespace doris::index_query {

class RoaringDocIdSink final : public DocIdSink {
public:
    explicit RoaringDocIdSink(roaring::Roaring& bitmap) : bitmap_(bitmap) {}

    void append(uint32_t docid) { bitmap_.add(docid); }

    Status append_sorted(std::span<const uint32_t> docids) override {
        if (!docids.empty()) {
            bitmap_.addMany(docids.size(), docids.data());
        }
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        if (last_exclusive > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "docid_sink: range exceeds uint32 docid space");
        }
        if (last_exclusive > first) {
            bitmap_.addRange(first, last_exclusive);
        }
        return Status::OK();
    }

    bool dedups() const override { return true; }

private:
    roaring::Roaring& bitmap_;
};

} // namespace doris::index_query
