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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/exact_phrase_matcher.h"

namespace doris::segment_v2::inverted_index {

ExactPhraseMatcher::ExactPhraseMatcher(std::vector<PostingsAndPosition> postings)
        : _postings(std::move(postings)) {}

void ExactPhraseMatcher::reset(int32_t doc) {
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

bool ExactPhraseMatcher::next_match() {
    PostingsAndPosition& lead = _postings[0];
    if (lead._upTo < lead._freq) {
        lead._pos = visit_node(lead._postings, NextPosition {});
        lead._upTo += 1;
    } else {
        return false;
    }

    while (true) {
        int32_t phrasePos = lead._pos - lead._offset;

        bool advance_head = false;
        for (size_t j = 1; j < _postings.size(); ++j) {
            PostingsAndPosition& posting = _postings[j];
            int32_t expectedPos = phrasePos + posting._offset;
            // advance up to the same position as the lead
            if (!advance_position(posting, expectedPos)) {
                return false;
            }

            if (posting._pos != expectedPos) { // we advanced too far
                if (advance_position(lead, posting._pos - posting._offset + lead._offset)) {
                    advance_head = true;
                    break;
                } else {
                    return false;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return true;
    }

    return false;
}

bool ExactPhraseMatcher::advance_position(PostingsAndPosition& posting, int32_t target) {
    while (posting._pos < target) {
        if (posting._upTo == posting._freq) {
            return false;
        } else {
            posting._pos = visit_node(posting._postings, NextPosition {});
            posting._upTo += 1;
        }
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index