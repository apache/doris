
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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/ordered_sloppy_phrase_matcher.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

OrderedSloppyPhraseMatcher::OrderedSloppyPhraseMatcher(std::vector<PostingsAndPosition> postings,
                                                       int32_t slop)
        : _allowed_slop(slop), _postings(std::move(postings)) {}

void OrderedSloppyPhraseMatcher::reset(int32_t doc) {
    for (PostingsAndPosition& posting : _postings) {
        int32_t cur_doc = visit_node(posting._postings, DocID {});
        if (cur_doc != doc) {
            std::string error_message = "docID mismatch: expected " + std::to_string(doc) +
                                        ", but got " + std::to_string(cur_doc);
            throw Exception(ErrorCode::INTERNAL_ERROR, error_message);
        }

        posting._freq = visit_node(posting._postings, Freq {});
        posting._pos = -1;
        posting._upTo = 0;
    }
}

bool OrderedSloppyPhraseMatcher::next_match() {
    PostingsAndPosition* prev_posting = _postings.data();
    while (prev_posting->_upTo < prev_posting->_freq) {
        prev_posting->_pos = visit_node(prev_posting->_postings, NextPosition {});
        prev_posting->_upTo += 1;
        if (stretch_to_order(prev_posting) && _match_width <= _allowed_slop) {
            return true;
        }
    }
    return false;
}

bool OrderedSloppyPhraseMatcher::stretch_to_order(PostingsAndPosition* prev_posting) {
    _match_width = 0;
    for (size_t i = 1; i < _postings.size(); i++) {
        PostingsAndPosition& posting = _postings[i];
        int32_t prev_pos = prev_posting->_pos - prev_posting->_offset;
        int32_t target_pos = prev_pos + posting._offset;
        if (!advance_position(posting, target_pos)) {
            return false;
        }
        int32_t curr_start = posting._pos - posting._offset;
        _match_width += (curr_start - prev_pos);
        if (_match_width > _allowed_slop) {
            return false;
        }
        prev_posting = &posting;
    }
    return true;
}

bool OrderedSloppyPhraseMatcher::advance_position(PostingsAndPosition& posting, int32_t target) {
    while (posting._pos < target) {
        if (posting._upTo == posting._freq) {
            return false;
        }
        posting._pos = visit_node(posting._postings, NextPosition {});
        posting._upTo += 1;
    }
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index