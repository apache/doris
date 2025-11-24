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

#include "olap/rowset/segment_v2/inverted_index/util/docid_set_iterator.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class PostingsAndFreq {
public:
    PostingsAndFreq(DISI postings, int32_t position, std::vector<std::string> terms)
            : _postings(std::move(postings)), _position(position), _terms(std::move(terms)) {
        _n_terms = _terms.size();
        if (_n_terms > 1) {
            std::sort(_terms.begin(), _terms.end());
        }
    }

    DISI _postings;
    int32_t _position = 0;
    std::vector<std::string> _terms;
    size_t _n_terms = 0;
};

class PostingsAndPosition {
public:
    PostingsAndPosition(DISI postings, int32_t offset)
            : _postings(std::move(postings)), _offset(offset) {}

    DISI _postings;
    int32_t _offset = 0;
    int32_t _freq = 0;
    int32_t _upTo = 0;
    int32_t _pos = 0;
};

template <typename Derived>
class PhraseMatcherBase {
public:
    // Handle position information for different types of phrase queries
    inline bool matches(int32_t doc) {
        derived()->reset(doc);
        return derived()->next_match();
    }

private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index