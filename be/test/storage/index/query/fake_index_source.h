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
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/postings_cursor.h"
#include "storage/index/query/term_pattern.h"

namespace doris::index_query::testing {

// The postings a test declared for one term, as one block; positions when the test gave them.
class FakePostingsCursor final : public PostingsCursor, public PositionCursor {
public:
    struct Posting {
        uint32_t doc = 0;
        std::vector<uint32_t> positions;
    };

    FakePostingsCursor(std::vector<Posting> postings, bool positions, bool scoring)
            : _postings(std::move(postings)), _positions(positions), _scoring(scoring) {
        for (const Posting& posting : _postings) {
            _docs.push_back(posting.doc);
            _freqs.push_back(std::max<uint32_t>(1, posting.positions.size()));
        }
    }

    uint32_t doc_freq() const override { return static_cast<uint32_t>(_docs.size()); }

    Status next_block(PostingsBlock* block, bool* eof) override {
        *block = {};
        *eof = _read || _docs.empty();
        if (*eof) {
            return Status::OK();
        }
        _read = true;
        block->docs = _docs;
        if (_scoring) {
            block->freqs = _freqs;
        }
        return Status::OK();
    }

    Status seek_block(uint32_t /*target*/, PostingsBlock* block, bool* eof) override {
        return next_block(block, eof);
    }

    Status shallow_seek(uint32_t /*target*/, bool* moved) override {
        *moved = false;
        return Status::OK();
    }

    BlockBound current_block_bound() const override {
        if (_docs.empty()) {
            return {};
        }
        return {.last_doc = _docs.back(), .last_doc_known = true};
    }

    Status open_positions(uint32_t ordinal, PositionCursor** out) override {
        *out = nullptr;
        if (!_positions) {
            return Status::NotSupported("this fake posting holds no positions");
        }
        _current = ordinal;
        _next_position = 0;
        *out = this;
        return Status::OK();
    }

    uint32_t frequency() const override {
        return static_cast<uint32_t>(_postings[_current].positions.size());
    }

    Status next_position(uint32_t* position, bool* available) override {
        const auto& positions = _postings[_current].positions;
        *available = _next_position < positions.size();
        if (*available) {
            *position = positions[_next_position++];
        }
        return Status::OK();
    }

    Status finish_doc() override { return Status::OK(); }

private:
    std::vector<Posting> _postings;
    std::vector<uint32_t> _docs;
    std::vector<uint32_t> _freqs;
    bool _positions;
    bool _scoring;
    bool _read = false;
    size_t _current = 0;
    size_t _next_position = 0;
};

// An index a test declares term by term: the documents of each term with their positions, the
// documents alive, and the calls the engine made.
class FakeIndexSource final : public IndexSource {
public:
    using Posting = FakePostingsCursor::Posting;

    void add(const std::string& term, const std::vector<uint32_t>& docs) {
        std::vector<Posting> postings;
        for (const uint32_t doc : docs) {
            postings.push_back({.doc = doc, .positions = {}});
        }
        add(term, std::move(postings));
    }

    void add(const std::string& term, std::vector<Posting> postings) {
        std::ranges::sort(postings, {}, &Posting::doc);
        _terms[term] = std::move(postings);
    }

    void remove(uint32_t doc) { _deleted.insert(doc); }
    void set_doc_count(uint32_t doc_count) { _doc_count = doc_count; }

    uint32_t doc_count() const override { return _doc_count; }

    Status prepare_terms(std::span<const std::string> terms) override {
        prepared.emplace_back(terms.begin(), terms.end());
        return Status::OK();
    }

    Status open_term(std::string_view term, bool positions, bool scoring,
                     std::unique_ptr<PostingsCursor>* out) override {
        opened.emplace_back(term);
        out->reset();
        const auto it = _terms.find(std::string(term));
        if (it == _terms.end()) {
            return Status::OK();
        }
        *out = std::make_unique<FakePostingsCursor>(it->second, positions, scoring);
        return Status::OK();
    }

    Status expand_terms(TermPattern& pattern, int32_t max_expansions,
                        std::vector<std::string>* out) override {
        expanded.push_back(pattern.enumeration_prefix());
        out->clear();
        if (!pattern.can_match()) {
            return Status::OK();
        }
        for (const auto& [term, postings] : _terms) {
            if (!term.starts_with(pattern.enumeration_prefix()) || !pattern.matches(term)) {
                continue;
            }
            out->push_back(term);
            if (max_expansions > 0 && out->size() == static_cast<size_t>(max_expansions)) {
                break;
            }
        }
        return Status::OK();
    }

    bool is_live(uint32_t doc) const override { return !_deleted.contains(doc); }

    // Every dictionary batch, every term opened and every pattern's enumeration prefix, in order.
    std::vector<std::vector<std::string>> prepared;
    std::vector<std::string> opened;
    std::vector<std::string> expanded;

private:
    std::map<std::string, std::vector<Posting>> _terms;
    std::set<uint32_t> _deleted;
    uint32_t _doc_count = 0;
};

} // namespace doris::index_query::testing
