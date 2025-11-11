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

#include "olap/rowset/segment_v2/inverted_index/query_v2/union/simple_union.h"

#include <algorithm>

#include "olap/rowset/segment_v2/inverted_index/query_v2/segment_postings.h"

namespace doris::segment_v2::inverted_index::query_v2 {

template <typename TDocSet>
SimpleUnionPtr<TDocSet> SimpleUnion<TDocSet>::create(std::vector<TDocSet> docsets) {
    return std::make_shared<SimpleUnion<TDocSet>>(std::move(docsets));
}

template <typename TDocSet>
SimpleUnion<TDocSet>::SimpleUnion(std::vector<TDocSet> docsets)
        : _docsets(std::move(docsets)), _doc(0) {
    _docsets.erase(
            std::remove_if(_docsets.begin(), _docsets.end(),
                           [](const TDocSet& docset) { return docset->doc() == TERMINATED; }),
            _docsets.end());

    initialize_first_doc_id();
}

template <typename TDocSet>
void SimpleUnion<TDocSet>::initialize_first_doc_id() {
    uint32_t next_doc = TERMINATED;

    for (const auto& docset : _docsets) {
        next_doc = std::min(next_doc, docset->doc());
    }
    _doc = next_doc;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::advance_to_next() {
    uint32_t next_doc = TERMINATED;

    for (auto& docset : _docsets) {
        if (docset->doc() <= _doc) {
            docset->advance();
        }
        next_doc = std::min(next_doc, docset->doc());
    }
    _doc = next_doc;
    return _doc;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::advance() {
    advance_to_next();
    return _doc;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::seek(uint32_t target) {
    _doc = TERMINATED;
    for (auto& docset : _docsets) {
        if (docset->doc() < target) {
            docset->seek(target);
        }
        if (docset->doc() < _doc) {
            _doc = docset->doc();
        }
    }
    return _doc;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::doc() const {
    return _doc;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::size_hint() const {
    uint32_t max_hint = 0;
    for (const auto& docset : _docsets) {
        max_hint = std::max(max_hint, docset->size_hint());
    }
    return max_hint;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::freq() const {
    uint32_t total_freq = 0;
    for (const auto& docset : _docsets) {
        if (docset->doc() == _doc) {
            total_freq += docset->freq();
        }
    }
    return total_freq;
}

template <typename TDocSet>
uint32_t SimpleUnion<TDocSet>::norm() const {
    for (const auto& docset : _docsets) {
        if (docset->doc() == _doc) {
            return docset->norm();
        }
    }
    return 1;
}

template <typename TDocSet>
void SimpleUnion<TDocSet>::append_positions_with_offset(uint32_t offset,
                                                        std::vector<uint32_t>& output) {
    size_t initial_size = output.size();

    for (auto& docset : _docsets) {
        if (docset->doc() == _doc) {
            docset->append_positions_with_offset(offset, output);
        }
    }

    if (output.size() > initial_size) {
        std::sort(output.begin() + initial_size, output.end());
        auto last = std::unique(output.begin() + initial_size, output.end());
        output.erase(last, output.end());
    }
}

template class SimpleUnion<MockDocSetPtr>;
template class SimpleUnion<PostingsPtr>;
template class SimpleUnion<PositionPostingsPtr>;

} // namespace doris::segment_v2::inverted_index::query_v2