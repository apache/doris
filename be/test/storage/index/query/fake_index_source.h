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

// The cursors a source has open, and the most it had open at once.
struct LiveCursors {
    size_t now = 0;
    size_t peak = 0;
};

// A one-block posting held in memory, recording the prefetches and the streamed ordinals it is
// asked for, and reporting `position_work` as its positions' decode work per document.
class FakePostingsCursor final : public PostingsCursor, public PositionCursor {
public:
    struct Posting {
        uint32_t doc = 0;
        std::vector<uint32_t> positions;
    };
    struct Prefetch {
        std::vector<uint32_t> candidates;
        bool whole = false;
        bool positions = false;
    };

    FakePostingsCursor(std::vector<Posting> postings, bool positions, bool scoring,
                       std::vector<Prefetch>* prefetches = nullptr,
                       std::vector<uint32_t> norms = {}, uint64_t position_work = 0,
                       std::vector<std::vector<uint32_t>>* streams = nullptr,
                       LiveCursors* live = nullptr)
            : _postings(std::move(postings)),
              _norms(std::move(norms)),
              _positions(positions),
              _scoring(scoring),
              _prefetches(prefetches),
              _position_work(position_work),
              _streams(streams),
              _live(live) {
        for (const Posting& posting : _postings) {
            _docs.push_back(posting.doc);
            _freqs.push_back(std::max<uint32_t>(1, posting.positions.size()));
        }
        if (_live != nullptr) {
            _live->peak = std::max(_live->peak, ++_live->now);
        }
    }

    ~FakePostingsCursor() override {
        if (_live != nullptr) {
            --_live->now;
        }
    }

    uint32_t doc_freq() const override { return static_cast<uint32_t>(_docs.size()); }

    Status prefetch(const std::vector<uint32_t>* candidates, bool positions) override {
        if (_prefetches != nullptr) {
            _prefetches->push_back(
                    {.candidates = candidates == nullptr ? std::vector<uint32_t> {} : *candidates,
                     .whole = candidates == nullptr,
                     .positions = positions});
        }
        return Status::OK();
    }

    Status rewind() override {
        _read = false;
        return Status::OK();
    }

    Status positions_per_doc(uint64_t* out) override {
        *out = _position_work;
        return Status::OK();
    }

    Status stream_positions(std::span<const uint32_t> ordinals) override {
        if (_streams != nullptr) {
            _streams->emplace_back(ordinals.begin(), ordinals.end());
        }
        return Status::OK();
    }

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
            if (!_norms.empty()) {
                block->norms = _norms;
            }
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

    Status next_position(uint32_t* position, bool* available) override {
        const auto& positions = _postings[_current].positions;
        *available = _next_position < positions.size();
        if (*available) {
            *position = positions[_next_position++];
            ++positions_read;
        }
        return Status::OK();
    }

    Status finish_doc() override { return Status::OK(); }

    size_t positions_read = 0;

private:
    std::vector<Posting> _postings;
    std::vector<uint32_t> _docs;
    std::vector<uint32_t> _freqs;
    std::vector<uint32_t> _norms;
    bool _positions;
    bool _scoring;
    std::vector<Prefetch>* _prefetches;
    uint64_t _position_work;
    std::vector<std::vector<uint32_t>>* _streams;
    LiveCursors* _live;
    bool _read = false;
    size_t _current = 0;
    size_t _next_position = 0;
};

// An in-memory source recording how the engine reads it. With `batches` it answers as a
// source that reads in rounds, so the engine takes its batched strategies.
class FakeIndexSource final : public IndexSource {
public:
    using Posting = FakePostingsCursor::Posting;
    using Prefetch = FakePostingsCursor::Prefetch;

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
    bool batches_reads() const override { return batches; }

    Status prepare_terms(std::span<const std::string> terms) override {
        prepared.emplace_back(terms.begin(), terms.end());
        return Status::OK();
    }

    Status open_term(std::string_view term, bool positions, bool scoring,
                     std::unique_ptr<PostingsCursor>* out) override {
        opened.emplace_back(term);
        *out = _cursor(term, positions, scoring);
        return Status::OK();
    }

    Status open_terms(std::span<const std::string> terms, bool positions, bool scoring,
                      std::vector<std::unique_ptr<PostingsCursor>>* out) override {
        opened_together.emplace_back(terms.begin(), terms.end());
        out->clear();
        for (const std::string& term : terms) {
            out->push_back(_cursor(term, positions, scoring));
        }
        return Status::OK();
    }

    Status doc_freq(std::string_view term, uint64_t* out) override {
        const auto it = _terms.find(std::string(term));
        *out = it == _terms.end() ? 0 : it->second.size();
        return Status::OK();
    }

    Status may_hold(std::string_view term, bool* held) override {
        *held = _terms.contains(std::string(term));
        return Status::OK();
    }

    Status encoded_norms(std::span<const uint32_t> docs, std::vector<uint32_t>* out) override {
        out->clear();
        for (const uint32_t doc : docs) {
            out->push_back(norm_of(doc));
        }
        return Status::OK();
    }

    // The encoded norm of `doc`, 1 unless set.
    uint32_t norm_of(uint32_t doc) const {
        const auto it = norms.find(doc);
        return it == norms.end() ? 1 : it->second;
    }

    Status fetch_pending() override {
        ++fetches;
        return Status::OK();
    }

    Status expand_terms(TermPattern& pattern, int32_t max_expansions,
                        std::vector<std::string>* out) override {
        expanded.push_back(pattern.enumeration_prefix());
        out->clear();
        RETURN_IF_ERROR(expand_status);
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

    bool batches = false;
    // The decode work per document every cursor reports for its positions.
    uint64_t position_work = 0;
    // What every expansion returns before it enumerates.
    Status expand_status = Status::OK();
    // The encoded norm of each document that has one.
    std::map<uint32_t, uint32_t> norms;
    // Every dictionary batch, every term opened (alone or together), every pattern's
    // enumeration prefix, every prefetch by term and the rounds fetched, in order.
    std::vector<std::vector<std::string>> prepared;
    std::vector<std::string> opened;
    std::vector<std::vector<std::string>> opened_together;
    std::vector<std::string> expanded;
    std::map<std::string, std::vector<Prefetch>> prefetches;
    // The ordinals each term's cursor was asked to stream, block by block.
    std::map<std::string, std::vector<std::vector<uint32_t>>> streams;
    size_t fetches = 0;
    LiveCursors live;

private:
    std::unique_ptr<PostingsCursor> _cursor(std::string_view term, bool positions, bool scoring) {
        const auto it = _terms.find(std::string(term));
        if (it == _terms.end()) {
            return nullptr;
        }
        std::vector<uint32_t> posting_norms;
        for (const Posting& posting : it->second) {
            posting_norms.push_back(norm_of(posting.doc));
        }
        return std::make_unique<FakePostingsCursor>(
                it->second, positions, scoring, &prefetches[std::string(term)],
                std::move(posting_norms), position_work, &streams[std::string(term)], &live);
    }

    std::map<std::string, std::vector<Posting>> _terms;
    std::set<uint32_t> _deleted;
    uint32_t _doc_count = 0;
};

} // namespace doris::index_query::testing
