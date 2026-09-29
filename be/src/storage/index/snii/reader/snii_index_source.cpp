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
#include "storage/index/snii/reader/snii_postings_cursor.h"

namespace doris::snii::reader {

SniiIndexSource::SniiIndexSource(const LogicalIndexReader& idx) : _idx(idx) {}

uint32_t SniiIndexSource::doc_count() const {
    return static_cast<uint32_t>(_idx.stats().doc_count);
}

Status SniiIndexSource::prepare_terms(std::span<const std::string> terms) {
    std::vector<std::string> sorted(terms.begin(), terms.end());
    std::ranges::sort(sorted);
    const auto duplicates = std::ranges::unique(sorted);
    sorted.erase(duplicates.begin(), duplicates.end());
    std::vector<LogicalIndexReader::BatchLookupResult> results;
    RETURN_IF_ERROR(_idx.lookup_batch(sorted, &results));
    for (size_t i = 0; i < sorted.size(); ++i) {
        _prepared[std::move(sorted[i])] = std::move(results[i]);
    }
    return Status::OK();
}

Status SniiIndexSource::_resolve(std::string_view term,
                                 LogicalIndexReader::BatchLookupResult* out) {
    if (const auto it = _prepared.find(std::string(term)); it != _prepared.end()) {
        *out = it->second;
        return Status::OK();
    }
    return _idx.lookup(term, &out->found, &out->entry, &out->frq_base, &out->prx_base);
}

Status SniiIndexSource::_open_norms(const format::NormsPodReader** out) {
    if (!_norms_opened) {
        RETURN_IF_ERROR(_idx.open_norms(&_norms));
        _norms_opened = true;
    }
    *out = &_norms;
    return Status::OK();
}

Status SniiIndexSource::open_term(std::string_view term, bool positions, bool scoring,
                                  std::unique_ptr<index_query::PostingsCursor>* out) {
    out->reset();
    if (positions && !_idx.has_positions()) {
        return Status::NotSupported("snii: the index holds no positions");
    }
    LogicalIndexReader::BatchLookupResult hit;
    RETURN_IF_ERROR(_resolve(term, &hit));
    if (!hit.found) {
        return Status::OK();
    }
    const format::NormsPodReader* norms = nullptr;
    if (scoring && _idx.has_norms()) {
        RETURN_IF_ERROR(_open_norms(&norms));
    }
    *out = std::make_unique<SniiPostingsCursor>(_idx, std::move(hit.entry), hit.frq_base,
                                                hit.prx_base, positions, scoring, norms);
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
                out->push_back(std::move(hit.term));
                *stop = max_expansions > 0 && out->size() == static_cast<size_t>(max_expansions);
                return Status::OK();
            });
}

} // namespace doris::snii::reader
