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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace doris {

class EqualityDeleteHashIndex {
public:
    struct Entry {
        uint64_t hash;
        size_t row_index;
    };

    using const_iterator = std::vector<Entry>::const_iterator;

    explicit EqualityDeleteHashIndex(std::vector<uint64_t> hashes) {
        _entries.reserve(hashes.size());
        for (size_t row = 0; row < hashes.size(); ++row) {
            _entries.push_back({hashes[row], row});
        }
        // A node-based map amplifies memory for large delete files. Keep candidates contiguous;
        // the retained delete block still resolves hash collisions with full-key comparisons.
        std::ranges::sort(_entries, {}, &Entry::hash);
    }

    std::pair<const_iterator, const_iterator> equal_range(uint64_t hash) const {
        const auto first = std::ranges::lower_bound(_entries, hash, {}, &Entry::hash);
        const auto last = std::ranges::upper_bound(_entries, hash, {}, &Entry::hash);
        return {first, last};
    }

    bool empty() const { return _entries.empty(); }

    size_t memory_usage() const { return _entries.size() * sizeof(Entry); }

private:
    std::vector<Entry> _entries;
};

} // namespace doris
