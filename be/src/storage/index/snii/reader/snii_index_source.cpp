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

#include "storage/index/snii/reader/snii_index_source.h"

#include <algorithm>
#include <utility>

#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/format/phrase_bigram.h"

namespace doris::snii::reader {

namespace {

Status check_user_term(std::string_view term) {
    if (format::term_overlaps_internal_namespace(term)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII raw term overlaps an internal term namespace");
    }
    return Status::OK();
}

} // namespace

SniiIndexSource::SniiIndexSource(const LogicalIndexReader& idx, format::PrxDecodeStats* prx_stats)
        : _idx(idx), _prx_stats(prx_stats), _wave(idx.reader()) {}

uint32_t SniiIndexSource::doc_count() const {
    return static_cast<uint32_t>(_idx.stats().doc_count);
}

Status SniiIndexSource::prepare_terms(std::span<const std::string> terms) {
    std::vector<std::string> sorted;
    for (const std::string& term : terms) {
        RETURN_IF_ERROR(check_user_term(term));
        if (!_terms.contains(term)) {
            sorted.push_back(term);
        }
    }
    if (sorted.empty()) {
        return Status::OK();
    }
    std::ranges::sort(sorted);
    const auto duplicates = std::ranges::unique(sorted);
    sorted.erase(duplicates.begin(), duplicates.end());
    std::vector<LogicalIndexReader::BatchLookupResult> results;
    RETURN_IF_ERROR(_idx.lookup_batch(sorted, &results));
    for (size_t i = 0; i < sorted.size(); ++i) {
        _terms[std::move(sorted[i])] = {.hit = std::move(results[i]), .prelude = nullptr};
    }
    return Status::OK();
}

Status SniiIndexSource::_resolve(std::string_view term, Term** out) {
    auto it = _terms.find(std::string(term));
    if (it == _terms.end()) {
        Term resolved;
        RETURN_IF_ERROR(_idx.lookup(term, &resolved.hit.found, &resolved.hit.entry,
                                    &resolved.hit.frq_base, &resolved.hit.prx_base));
        it = _terms.emplace(std::string(term), std::move(resolved)).first;
    }
    *out = &it->second;
    return Status::OK();
}

Status SniiIndexSource::_open_norms(const format::NormsPodReader** out) {
    if (!_norms_opened) {
        RETURN_IF_ERROR(_idx.open_norms(&_norms));
        _norms_opened = true;
    }
    *out = &_norms;
    return Status::OK();
}

Status SniiIndexSource::_cursor(Term& term, bool positions, bool scoring, SniiReadWave* wave,
                                std::unique_ptr<SniiPostingsCursor>* out) {
    const format::NormsPodReader* norms = nullptr;
    if (scoring && _idx.has_norms()) {
        RETURN_IF_ERROR(_open_norms(&norms));
    }
    *out = std::make_unique<SniiPostingsCursor>(_idx, term.hit.entry, term.hit.frq_base,
                                                term.hit.prx_base, positions, scoring, norms, wave,
                                                _prx_stats);
    if (term.prelude != nullptr) {
        (*out)->set_prelude(term.prelude);
    }
    return Status::OK();
}

Status SniiIndexSource::open_term(std::string_view term, bool positions, bool scoring,
                                  std::unique_ptr<index_query::PostingsCursor>* out) {
    out->reset();
    if (positions && !_idx.has_positions()) {
        return Status::NotSupported("snii: the index holds no positions");
    }
    RETURN_IF_ERROR(check_user_term(term));
    Term* resolved = nullptr;
    RETURN_IF_ERROR(_resolve(term, &resolved));
    if (!resolved->hit.found) {
        return Status::OK();
    }
    std::unique_ptr<SniiPostingsCursor> cursor;
    RETURN_IF_ERROR(_cursor(*resolved, positions, scoring, /*wave=*/nullptr, &cursor));
    *out = std::move(cursor);
    return Status::OK();
}

Status SniiIndexSource::open_terms(std::span<const std::string> terms, bool positions, bool scoring,
                                   std::vector<std::unique_ptr<index_query::PostingsCursor>>* out) {
    out->clear();
    if (positions && !_idx.has_positions()) {
        return Status::NotSupported("snii: the index holds no positions");
    }
    RETURN_IF_ERROR(prepare_terms(terms));
    for (const std::string& term : terms) {
        Term* resolved = nullptr;
        RETURN_IF_ERROR(_resolve(term, &resolved));
        std::unique_ptr<SniiPostingsCursor> cursor;
        if (resolved->hit.found) {
            RETURN_IF_ERROR(_cursor(*resolved, positions, scoring, &_wave, &cursor));
            RETURN_IF_ERROR(cursor->open_prelude());
            if (cursor->prelude_pending()) {
                // Later cursors of the term start from the prelude this one reads.
                _wave.after_fetch(cursor.get(),
                                  [resolved, opened = cursor.get()](const io::BatchRangeFetcher&) {
                                      if (resolved->prelude == nullptr) {
                                          resolved->prelude = opened->prelude();
                                      }
                                      return Status::OK();
                                  });
            }
        }
        out->push_back(std::move(cursor));
    }
    return Status::OK();
}

// The norms section is read once, when a document's norm is first asked.
Status SniiIndexSource::encoded_norms(std::span<const uint32_t> docs, std::vector<uint32_t>* out) {
    out->assign(docs.size(), 1);
    if (docs.empty() || !_idx.has_norms()) {
        return Status::OK();
    }
    const format::NormsPodReader* norms = nullptr;
    RETURN_IF_ERROR(_open_norms(&norms));
    for (size_t i = 0; i < docs.size(); ++i) {
        uint8_t norm = 0;
        RETURN_IF_ERROR(norms->try_encoded_norm(docs[i], &norm));
        (*out)[i] = norm;
    }
    return Status::OK();
}

// The dictionary's bloom filter and sparse term index answer without a dictionary read.
Status SniiIndexSource::may_hold(std::string_view term, bool* held) {
    RETURN_IF_ERROR(check_user_term(term));
    return _idx.may_contain(term, held);
}

Status SniiIndexSource::expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                                     std::vector<std::string>* out) {
    out->clear();
    if (!pattern.can_match()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_check_enumeration(pattern.enumeration_prefix()));
    const std::string& required = pattern.required_text();
    return _idx.visit_prefix_terms(
            pattern.enumeration_prefix(),
            [&](LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
                if (!required.empty() && hit.term.find(required) == std::string::npos) {
                    return Status::OK();
                }
                if (!pattern.matches(hit.term)) {
                    return Status::OK();
                }
                // The dictionary answered the term here, so its open needs no lookup.
                _terms[hit.term] = {.hit = {.found = true,
                                            .entry = std::move(hit.entry),
                                            .frq_base = hit.frq_base,
                                            .prx_base = hit.prx_base},
                                    .prelude = nullptr};
                out->push_back(std::move(hit.term));
                *stop = max_expansions > 0 && out->size() == static_cast<size_t>(max_expansions);
                return Status::OK();
            });
}

// A prefix reaching the marker enumerates internal terms; an empty one does when the dictionary
// holds any.
Status SniiIndexSource::_check_enumeration(std::string_view prefix) {
    if (!prefix.empty()) {
        if (format::prefix_overlaps_internal_namespace(prefix)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "SNII raw expansion overlaps an internal term namespace");
        }
        return Status::OK();
    }
    if (!_has_internal_terms.has_value()) {
        bool found = false;
        RETURN_IF_ERROR(_idx.visit_prefix_terms(
                format::kPhraseBigramTermMarker,
                [&found](LogicalIndexReader::PrefixHit&&, bool* stop) -> Status {
                    found = true;
                    *stop = true;
                    return Status::OK();
                }));
        _has_internal_terms = found;
    }
    if (*_has_internal_terms) {
        return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "SNII raw expansion overlaps an existing internal term namespace");
    }
    return Status::OK();
}

} // namespace doris::snii::reader
