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

#include <memory>
#include <utility>
#include <vector>

#include "CLucene.h" // IWYU pragma: keep
#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class MatchAllDocsScorer : public Scorer {
public:
    MatchAllDocsScorer(uint32_t max_doc,
                       const std::vector<std::shared_ptr<lucene::index::IndexReader>>& readers)
            : _max_doc(max_doc), _readers(readers) {
        if (_max_doc == 0) {
            _doc = TERMINATED;
        } else {
            _doc = _next_live_doc(0);
        }
    }
    ~MatchAllDocsScorer() override = default;

    uint32_t advance() override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        uint32_t candidate = _doc + 1;
        if (candidate >= _max_doc) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        _doc = _next_live_doc(candidate);
        return _doc;
    }

    uint32_t seek(uint32_t target) override {
        if (_doc == TERMINATED) {
            return TERMINATED;
        }
        if (target <= _doc) {
            return _doc;
        }
        if (target >= _max_doc) {
            _doc = TERMINATED;
            return TERMINATED;
        }
        _doc = _next_live_doc(target);
        return _doc;
    }

    uint32_t doc() const override { return _doc; }

    uint32_t size_hint() const override { return _max_doc; }

    float score() override { return 1.0F; }

private:
    [[nodiscard]] uint32_t _next_live_doc(uint32_t start) const {
        for (uint32_t doc = start; doc < _max_doc; ++doc) {
            if (_is_live(doc)) {
                return doc;
            }
        }
        return TERMINATED;
    }

    [[nodiscard]] bool _is_live(uint32_t doc) const {
        for (const auto& reader : _readers) {
            if (reader != nullptr && reader->isDeleted(static_cast<int32_t>(doc))) {
                return false;
            }
        }
        return true;
    }

    uint32_t _max_doc;
    std::vector<std::shared_ptr<lucene::index::IndexReader>> _readers;
    uint32_t _doc = TERMINATED;
};

} // namespace doris::segment_v2::inverted_index::query_v2
