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

#include "storage/index/inverted/query_v2/segment_postings.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// A min-heap keeps the unfinished subs ordered by document, so a step or a seek moves only the
// subs behind it.
class UnionPostings final : public Postings {
public:
    explicit UnionPostings(std::vector<SegmentPostingsPtr> subs) : _subs(std::move(subs)) {
        for (const auto& sub : _subs) {
            if (sub->doc() != TERMINATED) {
                _heap.push_back(sub.get());
            }
        }
        std::ranges::make_heap(_heap, later);
        _doc = _heap.empty() ? TERMINATED : _heap.front()->doc();
    }

    uint32_t advance() override {
        while (!_heap.empty() && _heap.front()->doc() == _doc) {
            std::ranges::pop_heap(_heap, later);
            restore_back(_heap.back()->advance());
        }
        return _doc = _heap.empty() ? TERMINATED : _heap.front()->doc();
    }

    uint32_t seek(uint32_t target) override {
        if (target <= _doc) {
            return _doc;
        }
        while (!_heap.empty() && _heap.front()->doc() < target) {
            std::ranges::pop_heap(_heap, later);
            restore_back(_heap.back()->seek(target));
        }
        return _doc = _heap.empty() ? TERMINATED : _heap.front()->doc();
    }

    uint32_t doc() const override { return _doc; }

    uint32_t size_hint() const override {
        uint32_t hint = 0;
        for (const auto& sub : _subs) {
            hint += sub->size_hint();
        }
        return hint;
    }

    uint32_t freq() const override {
        uint32_t total = 0;
        for (const auto& sub : _subs) {
            if (sub->doc() == _doc) {
                total += sub->freq();
            }
        }
        return total;
    }

    uint32_t norm() const override {
        if (_doc == TERMINATED) {
            return 1;
        }
        for (const auto& sub : _subs) {
            if (sub->doc() == _doc) {
                return sub->norm();
            }
        }
        return 1;
    }

    void append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) override {
        size_t start = output.size();
        for (auto& sub : _subs) {
            if (sub->doc() == _doc) {
                sub->append_positions_with_offset(offset, output);
            }
        }
        if (output.size() - start > 1) {
            std::sort(output.begin() + start, output.end());
        }
    }

private:
    static constexpr auto later = [](const SegmentPostings* left, const SegmentPostings* right) {
        return left->doc() > right->doc();
    };

    // Puts the sub popped to the back, now on `doc`, back into the heap, or drops it once done.
    void restore_back(uint32_t doc) {
        if (doc == TERMINATED) {
            _heap.pop_back();
        } else {
            std::ranges::push_heap(_heap, later);
        }
    }

    std::vector<SegmentPostingsPtr> _subs;
    std::vector<SegmentPostings*> _heap;
    uint32_t _doc = TERMINATED;
};

using UnionPostingsPtr = std::shared_ptr<UnionPostings>;

inline UnionPostingsPtr make_union_postings(std::vector<SegmentPostingsPtr> subs) {
    return std::make_shared<UnionPostings>(std::move(subs));
}

} // namespace doris::segment_v2::inverted_index::query_v2
