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

#include <climits>
#include <cstdint>
#include <memory>

#include "roaring/roaring.hh"

namespace doris::segment_v2 {

// Read-only DISI adapter over a candidate row bitmap
// (IndexQueryContext::candidate_rows). Joining the leapfrog intersection of
// PhraseQuery, it restricts doc-list intersection and position verification to
// the candidate set (two-phase evaluation: this iterator drives the
// approximation, real term iterators keep the position semantics). It never
// joins a matcher's postings, so freq()/next_position()/norm() only satisfy
// the DISI interface with neutral values.
//
// The underlying bitmap is NOT owned and must outlive the iterator; leapfrog
// only moves forward, so advance() targets are monotonically non-decreasing.
class RoaringDocIdIterator {
public:
    explicit RoaringDocIdIterator(const roaring::Roaring* rows)
            : _rows(rows), _iter(rows->begin()) {}

    // DISI convention: an iterator starts positioned BEFORE its first doc
    // (search_by_skiplist opens with a NextDoc on the lead), so doc_id() is -1
    // until the first next_doc()/advance() moves onto a real position.
    int32_t doc_id() const {
        if (!_started) {
            return -1;
        }
        return _iter == _rows->end() ? INT_MAX : static_cast<int32_t>(*_iter);
    }

    int32_t freq() const { return 1; }

    int32_t next_doc() {
        if (!_started) {
            _started = true;
        } else if (_iter != _rows->end()) {
            ++_iter;
        }
        return doc_id();
    }

    int32_t advance(int32_t target) {
        _started = true;
        if (target > 0) {
            _iter.equalorlarger(static_cast<uint32_t>(target));
        }
        return doc_id();
    }

    int32_t doc_freq() const {
        uint64_t cardinality = _rows->cardinality();
        return cardinality > INT_MAX ? INT_MAX : static_cast<int32_t>(cardinality);
    }

    int32_t next_position() { return 0; }

    int32_t norm() const { return 1; }

private:
    const roaring::Roaring* _rows;
    roaring::Roaring::const_iterator _iter;
    bool _started = false;
};
using RoaringDocIdIterPtr = std::shared_ptr<RoaringDocIdIterator>;

} // namespace doris::segment_v2
