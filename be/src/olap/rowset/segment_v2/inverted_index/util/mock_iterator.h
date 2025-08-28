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

#include <map>
#include <memory>
#include <vector>

#include "common/cast_set.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class MockIterator {
public:
    MockIterator() : _impl(std::make_shared<Impl>()) {}

    MockIterator(std::map<int32_t, std::vector<int32_t>> postings)
            : _impl(std::make_shared<Impl>(std::move(postings))) {}

    int32_t doc_id() const {
        return _impl->current_doc != _impl->postings.end() ? _impl->current_doc->first : INT_MAX;
    }

    int32_t freq() const { return cast_set<int32_t>(_impl->current_freq); }

    int32_t next_doc() {
        auto& postings = _impl->postings;
        auto& current_doc = _impl->current_doc;

        if (current_doc == postings.end()) {
            return INT_MAX;
        }

        if (++current_doc != postings.end()) {
            _impl->current_freq = current_doc->second.size();
            _impl->pos_idx = 0;
            return current_doc->first;
        }
        _impl->current_freq = 0;
        _impl->pos_idx = 0;
        return INT_MAX;
    }

    int32_t advance(int32_t target) {
        auto& postings = _impl->postings;
        auto& current_doc = _impl->current_doc;

        auto it = postings.lower_bound(target);
        if (it != postings.end()) {
            current_doc = it;
            _impl->current_freq = current_doc->second.size();
            _impl->pos_idx = 0;
            return current_doc->first;
        }
        current_doc = postings.end();
        _impl->current_freq = 0;
        _impl->pos_idx = 0;
        return INT_MAX;
    }

    int32_t doc_freq() const { return cast_set<int32_t>(_impl->postings.size()); }

    int32_t next_position() {
        auto& current_doc = _impl->current_doc;
        auto& pos_idx = _impl->pos_idx;

        if (current_doc == _impl->postings.end() || pos_idx >= current_doc->second.size()) {
            return -1;
        }
        return current_doc->second[pos_idx++];
    }

    void set_postings(std::map<int32_t, std::vector<int32_t>> postings) {
        _impl->postings = std::move(postings);
        _impl->current_doc = _impl->postings.begin();
        if (_impl->current_doc != _impl->postings.end()) {
            _impl->current_freq = _impl->current_doc->second.size();
        }
        _impl->pos_idx = 0;
    }

private:
    struct Impl {
        std::map<int32_t, std::vector<int32_t>> postings;
        std::map<int32_t, std::vector<int32_t>>::iterator current_doc;
        size_t pos_idx = 0;
        size_t current_freq = 0;

        Impl() {
            current_doc = postings.begin();
            if (current_doc != postings.end()) {
                current_freq = current_doc->second.size();
            }
        }

        Impl(std::map<int32_t, std::vector<int32_t>> postings_list)
                : postings(std::move(postings_list)) {
            current_doc = postings.begin();
            if (current_doc != postings.end()) {
                current_freq = current_doc->second.size();
            }
        }
    };

    std::shared_ptr<Impl> _impl;
};
using MockIterPtr = std::shared_ptr<MockIterator>;

} // namespace doris::segment_v2::inverted_index
#include "common/compile_check_end.h"