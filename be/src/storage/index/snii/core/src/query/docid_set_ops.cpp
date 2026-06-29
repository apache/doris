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

#include "snii/query/internal/docid_set_ops.h"

#include <algorithm>
#include <iterator>
#include <queue>
#include <utility>

namespace snii::query::internal {

std::vector<uint32_t> intersect_sorted(const std::vector<uint32_t>& a,
                                       const std::vector<uint32_t>& b) {
    std::vector<uint32_t> out;
    out.reserve(std::min(a.size(), b.size()));
    std::set_intersection(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(out));
    return out;
}

void union_sorted_into(std::vector<uint32_t>* acc, const std::vector<uint32_t>& next) {
    std::vector<uint32_t> merged;
    merged.reserve(acc->size() + next.size());
    std::set_union(acc->begin(), acc->end(), next.begin(), next.end(), std::back_inserter(merged));
    *acc = std::move(merged);
}

std::vector<uint32_t> union_sorted_many(const std::vector<std::vector<uint32_t>>& lists) {
    constexpr size_t kLinearFanInMax = 8;
    struct Cursor {
        uint32_t docid = 0;
        size_t list = 0;
        size_t offset = 0;
    };
    struct GreaterDocId {
        bool operator()(const Cursor& a, const Cursor& b) const { return a.docid > b.docid; }
    };

    size_t non_empty = 0;
    size_t largest = 0;
    std::priority_queue<Cursor, std::vector<Cursor>, GreaterDocId> heap;
    for (size_t i = 0; i < lists.size(); ++i) {
        if (lists[i].empty()) continue;
        ++non_empty;
        largest = std::max(largest, lists[i].size());
        heap.push(Cursor {lists[i][0], i, 0});
    }
    if (non_empty == 0) return {};
    if (non_empty == 1) {
        for (const std::vector<uint32_t>& docs : lists) {
            if (!docs.empty()) return docs;
        }
    }

    if (non_empty <= kLinearFanInMax) {
        std::vector<size_t> offsets(lists.size(), 0);
        std::vector<uint32_t> out;
        out.reserve(largest);
        bool has_last = false;
        uint32_t last = 0;
        for (;;) {
            bool found = false;
            uint32_t next = 0;
            for (size_t i = 0; i < lists.size(); ++i) {
                if (offsets[i] >= lists[i].size()) continue;
                const uint32_t docid = lists[i][offsets[i]];
                if (!found || docid < next) {
                    found = true;
                    next = docid;
                }
            }
            if (!found) break;
            if (!has_last || next != last) {
                out.push_back(next);
                last = next;
                has_last = true;
            }
            for (size_t i = 0; i < lists.size(); ++i) {
                while (offsets[i] < lists[i].size() && lists[i][offsets[i]] == next) {
                    ++offsets[i];
                }
            }
        }
        return out;
    }

    std::vector<uint32_t> out;
    out.reserve(largest);
    bool has_last = false;
    uint32_t last = 0;
    while (!heap.empty()) {
        const Cursor cur = heap.top();
        heap.pop();
        if (!has_last || cur.docid != last) {
            out.push_back(cur.docid);
            last = cur.docid;
            has_last = true;
        }
        const size_t next_offset = cur.offset + 1;
        const std::vector<uint32_t>& docs = lists[cur.list];
        if (next_offset < docs.size()) {
            heap.push(Cursor {docs[next_offset], cur.list, next_offset});
        }
    }
    return out;
}

} // namespace snii::query::internal
