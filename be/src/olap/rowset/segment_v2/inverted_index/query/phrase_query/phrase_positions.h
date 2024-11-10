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

#include <CLucene/search/query/TermPositionIterator.h>

namespace doris::segment_v2::inverted_index {

class PhrasePositions {
public:
    PhrasePositions(const TermPositionIterator& postings, int32_t o, int32_t ord,
                    const std::vector<std::string>& terms) {
        _postings = postings;
        _offset = o;
        _ord = ord;
        _terms = terms;
    }

    void first_position() {
        _count = _postings.freq();
        next_position();
    }

    bool next_position() {
        if (_count-- > 0) {
            _position = _postings.nextPosition() - _offset;
            return true;
        } else {
            return false;
        }
    }

    int32_t _position = 0;
    int32_t _count = 0;
    int32_t _offset = 0;
    int32_t _ord = 0;
    TermPositionIterator _postings;
    PhrasePositions* _next = nullptr;
    uint32_t _rpt_group = -1;
    uint32_t _rpt_ind = 0;
    std::vector<std::string> _terms;
};

} // namespace doris::segment_v2::inverted_index
