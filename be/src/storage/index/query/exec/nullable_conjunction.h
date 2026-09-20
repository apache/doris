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
#include <optional>
#include <roaring/roaring.hh>
#include <span>
#include <utility>

#include "common/check.h"
#include "storage/index/query/exec/docid_buffer.h"

namespace doris::index_query {

inline constexpr uint64_t kDocIdEnd = uint64_t {1} << 32;

// DocSet supplies doc() and seek(uint32_t), using kDocIdEnd for exhaustion.
// TRUE and UNKNOWN inputs are disjoint and remain alive throughout traversal.
template <typename DocSet>
class NullableDocSet {
public:
    NullableDocSet(DocSet true_docs, const roaring::Roaring* null_rows)
            : _true_docs(std::move(true_docs)) {
        if (null_rows != nullptr) {
            _null_docs.emplace(null_rows->begin());
            _null_count = null_rows->cardinality();
            _update_null_doc();
        }
        _doc = std::min(_true_docs.doc(), _null_doc);
    }

    uint64_t doc() const { return _doc; }
    bool is_true() const { return _doc != kDocIdEnd && _doc != _null_doc; }
    uint64_t cost() const { return _true_docs.cost() + _null_count; }

    uint64_t seek(uint64_t target) {
        if (target <= _doc) {
            return _doc;
        }
        if (target >= kDocIdEnd) {
            return _doc = kDocIdEnd;
        }
        if (_null_doc < target) {
            _null_docs->equalorlarger(static_cast<uint32_t>(target));
            _update_null_doc();
        }
        // Known UNKNOWN rows need no lookup in the disjoint TRUE postings.
        if (_null_doc == target) {
            return _doc = target;
        }
        if (_true_docs.doc() < target) {
            _true_docs.seek(static_cast<uint32_t>(target));
        }
        return _doc = std::min(_true_docs.doc(), _null_doc);
    }

private:
    void _update_null_doc() { _null_doc = _null_docs->i.has_value ? **_null_docs : kDocIdEnd; }

    DocSet _true_docs;
    std::optional<roaring::Roaring::const_iterator> _null_docs;
    uint64_t _null_doc = kDocIdEnd;
    uint64_t _null_count = 0;
    uint64_t _doc = kDocIdEnd;
};

// Intersects possible rows in one pass and visits only final TRUE matches.
// Inputs support cheap forward seeks; callers order them by estimated cost.
template <typename Input, size_t Extent, typename Visitor>
Status collect_nullable_conjunction(std::span<Input, Extent> inputs, uint64_t row_limit,
                                    DocIdSink& true_sink, DocIdSink& null_sink, Visitor&& visit) {
    DORIS_CHECK(!inputs.empty());
    DORIS_CHECK_LE(row_limit, kDocIdEnd);
    DocIdBuffer true_rows(true_sink);
    DocIdBuffer null_rows(null_sink);
    uint64_t candidate = inputs.front().doc();
    while (candidate < row_limit) {
        bool all_true = true;
        size_t matched = 0;
        for (; matched < inputs.size(); ++matched) {
            const uint64_t next = inputs[matched].seek(candidate);
            if (next != candidate) {
                candidate = next;
                break;
            }
            all_true &= inputs[matched].is_true();
        }
        if (matched != inputs.size()) {
            continue;
        }
        const auto doc = static_cast<uint32_t>(candidate);
        if (all_true) {
            RETURN_IF_ERROR(true_rows.append(doc));
            visit(doc);
        } else {
            RETURN_IF_ERROR(null_rows.append(doc));
        }
        candidate = inputs.front().seek(candidate + 1);
    }
    RETURN_IF_ERROR(true_rows.flush());
    return null_rows.flush();
}

} // namespace doris::index_query
