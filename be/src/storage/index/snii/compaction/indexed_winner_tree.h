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

#include <boost/container/small_vector.hpp>
#include <cstddef>
#include <limits>
#include <utility>

#include "common/check.h"

namespace doris::snii::compaction {

// Complete binary winner tree over dense source ordinals. Updating one source
// touches only its leaf-to-root path. runner_up() inspects the sibling winners
// on the current winner's path without changing the tree.
template <typename Before>
class IndexedWinnerTree {
public:
    static constexpr size_t kNoSource = std::numeric_limits<size_t>::max();

    explicit IndexedWinnerTree(Before before) : before_(std::move(before)), nodes_(2, kNoSource) {}

    template <typename IsLive>
    void build(size_t source_count, IsLive&& is_live) {
        source_count_ = source_count;
        leaf_base_ = 1;
        while (leaf_base_ < source_count_) {
            DORIS_CHECK_LE(leaf_base_, std::numeric_limits<size_t>::max() / 2);
            leaf_base_ *= 2;
        }
        nodes_.assign(leaf_base_ * 2, kNoSource);
        for (size_t source = 0; source < source_count_; ++source) {
            if (is_live(source)) {
                nodes_[leaf_base_ + source] = source;
            }
        }
        for (size_t node = leaf_base_; node-- > 1;) {
            nodes_[node] = select(nodes_[node * 2], nodes_[node * 2 + 1]);
        }
    }

    bool empty() const noexcept { return nodes_[1] == kNoSource; }

    size_t winner() const {
        DCHECK(!empty());
        return nodes_[1];
    }

    size_t runner_up() const {
        const size_t current_winner = winner();
        size_t candidate = kNoSource;
        size_t node = leaf_base_ + current_winner;
        while (node > 1) {
            candidate = select(candidate, nodes_[node ^ 1]);
            node /= 2;
        }
        return candidate;
    }

    void update(size_t source, bool live) {
        DCHECK_LT(source, source_count_);
        size_t node = leaf_base_ + source;
        nodes_[node] = live ? source : kNoSource;
        while (node > 1) {
            node /= 2;
            nodes_[node] = select(nodes_[node * 2], nodes_[node * 2 + 1]);
        }
    }

private:
    size_t select(size_t candidate, size_t challenger) const {
        if (candidate == kNoSource) {
            return challenger;
        }
        if (challenger == kNoSource) {
            return candidate;
        }
        return before_(challenger, candidate) ? challenger : candidate;
    }

    Before before_;
    size_t source_count_ = 0;
    size_t leaf_base_ = 1;
    boost::container::small_vector<size_t, 2> nodes_;
};

} // namespace doris::snii::compaction
