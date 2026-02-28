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

#include "olap/rowset/segment_v2/inverted_index/query_v2/postings/loaded_postings.h"

#include <algorithm>

namespace doris::segment_v2::inverted_index::query_v2 {

LoadedPostings::LoadedPostings(std::vector<uint32_t> doc_ids,
                               std::vector<std::vector<uint32_t>> positions)
        : _doc_ids(std::move(doc_ids)) {
    if (_doc_ids.empty()) {
        _cursor = 0;
        return;
    }

    size_t total_positions = 0;
    for (const auto& pos : positions) {
        total_positions += pos.size();
    }

    _positions.reserve(total_positions);
    _position_offsets.reserve(positions.size() + 1);

    for (const auto& pos : positions) {
        _position_offsets.push_back(static_cast<uint32_t>(_positions.size()));
        _positions.insert(_positions.end(), pos.begin(), pos.end());
    }
    _position_offsets.push_back(static_cast<uint32_t>(_positions.size()));
}

template <typename TPostings>
LoadedPostingsPtr LoadedPostings::load(TPostings& segment_postings) {
    auto loaded = std::make_shared<LoadedPostings>();

    uint32_t num_docs = segment_postings.size_hint();
    loaded->_doc_ids.reserve(num_docs);
    loaded->_position_offsets.reserve(num_docs + 1);

    while (segment_postings.doc() != TERMINATED) {
        loaded->_position_offsets.push_back(static_cast<uint32_t>(loaded->_positions.size()));
        loaded->_doc_ids.push_back(segment_postings.doc());
        segment_postings.append_positions_with_offset(0, loaded->_positions);
        segment_postings.advance();
    }
    loaded->_position_offsets.push_back(static_cast<uint32_t>(loaded->_positions.size()));
    loaded->_cursor = 0;

    return loaded;
}

uint32_t LoadedPostings::advance() {
    ++_cursor;
    if (_cursor >= _doc_ids.size()) {
        _cursor = _doc_ids.size();
        return TERMINATED;
    }
    return doc();
}

uint32_t LoadedPostings::seek(uint32_t target) {
    if (_doc_ids.empty() || _cursor >= _doc_ids.size()) {
        _cursor = _doc_ids.size();
        return TERMINATED;
    }

    if (_doc_ids[_cursor] >= target) {
        return _doc_ids[_cursor];
    }

    auto it = std::lower_bound(_doc_ids.begin() + _cursor, _doc_ids.end(), target);
    if (it == _doc_ids.end()) {
        _cursor = _doc_ids.size();
        return TERMINATED;
    }

    _cursor = static_cast<size_t>(it - _doc_ids.begin());
    return *it;
}

uint32_t LoadedPostings::doc() const {
    if (_cursor >= _doc_ids.size()) {
        return TERMINATED;
    }
    return _doc_ids[_cursor];
}

uint32_t LoadedPostings::size_hint() const {
    return static_cast<uint32_t>(_doc_ids.size());
}

uint32_t LoadedPostings::freq() const {
    if (_cursor >= _doc_ids.size()) {
        return 0;
    }
    uint32_t start = _position_offsets[_cursor];
    uint32_t end = _position_offsets[_cursor + 1];
    return end - start;
}

uint32_t LoadedPostings::norm() const {
    return 1;
}

void LoadedPostings::append_positions_with_offset(uint32_t offset, std::vector<uint32_t>& output) {
    if (_cursor >= _doc_ids.size()) {
        return;
    }

    size_t start = _position_offsets[_cursor];
    size_t end = _position_offsets[_cursor + 1];

    for (size_t i = start; i < end; ++i) {
        output.push_back(_positions[i] + offset);
    }
}

template LoadedPostingsPtr LoadedPostings::load<SegmentPostings>(SegmentPostings& segment_postings);

} // namespace doris::segment_v2::inverted_index::query_v2