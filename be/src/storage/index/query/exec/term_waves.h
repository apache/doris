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
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/query/docid_sink.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/collect_postings.h"
#include "storage/index/query/spi/index_source.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::index_query {

// The most terms whose postings a source batching its reads reads in one round. A leaf of more
// terms reads them a wave at a time, consuming each wave and releasing its cursors before the
// next opens.
inline constexpr size_t kTermsPerWave = 32;

// Opens `terms` together and reads their whole postings in one round; a term the dictionary
// lacks has a null cursor.
inline Status read_term_wave(IndexSource& source, std::span<const std::string> terms, bool scoring,
                             std::vector<std::unique_ptr<PostingsCursor>>* cursors,
                             const std::vector<uint32_t>* candidates = nullptr) {
    RETURN_IF_ERROR(source.open_terms(terms, /*positions=*/false, scoring, cursors));
    bool reads = false;
    for (const auto& cursor : *cursors) {
        if (cursor != nullptr) {
            RETURN_IF_ERROR(cursor->prefetch(candidates, /*positions=*/false));
            reads = true;
        }
    }
    return reads ? source.fetch_pending() : Status::OK();
}

// Calls `visit(i, cursor)` for every term `terms[i]` in order, `cursor` positioned to list the
// term's whole posting (with frequencies and norms when `scoring`) and null when the dictionary
// lacks the term. A source batching its reads resolves the terms together, then opens and reads
// them a wave at a time, one round each, and releases a wave's cursors once all of them were
// visited; another opens them one at a time.
template <typename Visit>
Status visit_term_postings(IndexSource& source, std::span<const std::string> terms, bool scoring,
                           Visit&& visit, const std::vector<uint32_t>* candidates = nullptr) {
    if (!source.batches_reads()) {
        for (size_t i = 0; i < terms.size(); ++i) {
            std::unique_ptr<PostingsCursor> cursor;
            RETURN_IF_ERROR(source.open_term(terms[i], /*positions=*/false, scoring, &cursor));
            if (cursor != nullptr && candidates != nullptr) {
                RETURN_IF_ERROR(cursor->prefetch(candidates, /*positions=*/false));
            }
            RETURN_IF_ERROR(visit(i, cursor.get()));
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(source.prepare_terms(terms));
    std::vector<std::unique_ptr<PostingsCursor>> cursors;
    for (size_t begin = 0; begin < terms.size(); begin += kTermsPerWave) {
        const auto wave = terms.subspan(begin, std::min(kTermsPerWave, terms.size() - begin));
        RETURN_IF_ERROR(read_term_wave(source, wave, scoring, &cursors, candidates));
        for (size_t i = 0; i < cursors.size(); ++i) {
            RETURN_IF_ERROR(visit(begin + i, cursors[i].get()));
        }
        cursors.clear();
    }
    return Status::OK();
}

// Adds the rows any of `terms` holds to `sink`, their postings read as visit_term_postings reads
// them; `any_present`, when given, tells whether the dictionary holds one of them.
inline Status collect_term_rows(IndexSource& source, std::span<const std::string> terms,
                                DocIdSink& sink, bool* any_present = nullptr) {
    bool present = false;
    RETURN_IF_ERROR(visit_term_postings(
            source, terms, /*scoring=*/false, [&](size_t, PostingsCursor* cursor) -> Status {
                if (cursor == nullptr) {
                    return Status::OK();
                }
                present = true;
                BlockDocSet docs(*cursor);
                return collect_postings<false>(docs, nullptr, sink,
                                               [](uint32_t, uint32_t, uint32_t) {});
            }));
    if (any_present != nullptr) {
        *any_present = present;
    }
    return Status::OK();
}

} // namespace doris::index_query
