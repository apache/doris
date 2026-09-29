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

namespace doris::snii::reader {

SniiIndexSource::SniiIndexSource(const LogicalIndexReader& idx) : _idx(idx), _wave(idx.reader()) {}

uint32_t SniiIndexSource::doc_count() const {
    return static_cast<uint32_t>(_idx.stats().doc_count);
}

Status SniiIndexSource::prepare_terms(std::span<const std::string> terms) {
    std::vector<std::string> sorted;
    for (const std::string& term : terms) {
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
                                                term.hit.prx_base, positions, scoring, norms, wave);
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
    std::vector<std::pair<Term*, SniiPostingsCursor*>> opened;
    for (const std::string& term : terms) {
        Term* resolved = nullptr;
        RETURN_IF_ERROR(_resolve(term, &resolved));
        std::unique_ptr<SniiPostingsCursor> cursor;
        if (resolved->hit.found) {
            RETURN_IF_ERROR(_cursor(*resolved, positions, scoring, &_wave, &cursor));
            RETURN_IF_ERROR(cursor->open_prelude());
            opened.emplace_back(resolved, cursor.get());
        }
        out->push_back(std::move(cursor));
    }
    // The preludes of the windowed terms arrive in one round; later cursors of the same terms
    // start from them.
    RETURN_IF_ERROR(_wave.fetch());
    for (auto& [term, cursor] : opened) {
        if (term->prelude == nullptr) {
            term->prelude = cursor->prelude();
        }
    }
    return Status::OK();
}

Status SniiIndexSource::expand_terms(index_query::TermPattern& pattern, int32_t max_expansions,
                                     std::vector<std::string>* out) {
    out->clear();
    if (!pattern.can_match()) {
        return Status::OK();
    }
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

} // namespace doris::snii::reader
