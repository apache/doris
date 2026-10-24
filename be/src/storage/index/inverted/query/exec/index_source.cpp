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

#include "storage/index/inverted/query/spi/index_source.h"

#include <algorithm>
#include <numeric>

#include "storage/index/inverted/query/exec/cursor_chained_postings.h"
#include "storage/index/inverted/query/exec/docid_sink.h"
#include "storage/index/inverted/query/exec/term_waves.h"

namespace doris::index_query {

Status IndexSource::collect_terms(std::span<const std::string> terms, DocIdSink& sink,
                                  bool* any_present) {
    bool present = false;
    RETURN_IF_ERROR(visit_term_postings(
            *this, terms, /*scoring=*/false, [&](size_t, PostingsCursor* cursor) -> Status {
                if (cursor == nullptr) {
                    return Status::OK();
                }
                present = true;
                return for_each_block(*cursor, [&sink](const PostingsBlock& block) {
                    return block.dense ? sink.append_range(block.range_begin, block.range_end)
                                       : sink.append_sorted(block.docs);
                });
            }));
    if (any_present != nullptr) {
        *any_present = present;
    }
    return Status::OK();
}

namespace {

// How many postings a scan decodes in the time a seek costs.
constexpr uint64_t kPostingsPerSeek = 32;

// Decodes every posting once and walks `docs` alongside, both ascending.
Status count_listed_docs_by_scan(PostingsCursor& cursor, std::span<const uint32_t> docs,
                                 uint64_t* out) {
    size_t next = 0;
    PostingsBlock block;
    bool eof = false;
    while (next < docs.size()) {
        RETURN_IF_ERROR(cursor.next_block(&block, &eof));
        if (eof) {
            break;
        }
        if (block.dense) {
            const auto begin = std::lower_bound(docs.begin() + next, docs.end(), block.range_begin);
            const auto end = std::lower_bound(begin, docs.end(), block.range_end);
            *out += static_cast<uint64_t>(end - begin);
            next = end - docs.begin();
            continue;
        }
        for (const uint32_t doc : block.docs) {
            while (next < docs.size() && docs[next] < doc) {
                ++next;
            }
            if (next == docs.size()) {
                break;
            }
            if (docs[next] == doc) {
                ++*out;
                ++next;
            }
        }
    }
    return Status::OK();
}

} // namespace

Status count_listed_docs(PostingsCursor& cursor, std::span<const uint32_t> docs, uint64_t* out) {
    *out = 0;
    if (docs.empty() || cursor.doc_freq() == 0) {
        return Status::OK();
    }
    // Leaping seeks once per wanted document, scanning decodes every posting: leap only over
    // documents much rarer than the postings.
    if (docs.size() * kPostingsPerSeek >= cursor.doc_freq()) {
        return count_listed_docs_by_scan(cursor, docs, out);
    }
    size_t next = 0;
    PostingsBlock block;
    bool eof = false;
    while (next < docs.size()) {
        RETURN_IF_ERROR(cursor.seek_block(docs[next], &block, &eof));
        if (eof) {
            break;
        }
        const uint64_t size = block.size();
        uint64_t ordinal = 0;
        while (ordinal < size && next < docs.size()) {
            const uint32_t listed = block.doc_at(ordinal);
            const uint32_t wanted = docs[next];
            if (listed == wanted) {
                ++*out;
                ++ordinal;
                ++next;
            } else if (listed < wanted) {
                if (block.dense) {
                    ordinal = wanted - block.range_begin;
                } else {
                    ordinal = std::lower_bound(block.docs.begin() + ordinal, block.docs.end(),
                                               wanted) -
                              block.docs.begin();
                }
            } else {
                next = std::lower_bound(docs.begin() + next, docs.end(), listed) - docs.begin();
            }
        }
    }
    return Status::OK();
}

Status IndexSource::scoring_doc_freq_among(std::string_view term, std::span<const uint32_t> docs,
                                           uint64_t* out) {
    *out = 0;
    if (docs.empty()) {
        return Status::OK();
    }
    std::unique_ptr<PostingsCursor> cursor;
    RETURN_IF_ERROR(open_term(term, /*positions=*/false, /*scoring=*/false, &cursor));
    if (cursor == nullptr) {
        return Status::OK();
    }
    return count_listed_docs(*cursor, docs, out);
}

Status IndexSource::sum_norm_lengths_of(std::span<const uint32_t> docs, uint64_t* out) {
    *out = 0;
    const std::span<const float> lengths = norm_lengths();
    std::vector<uint32_t> norms;
    RETURN_IF_ERROR(encoded_norms(docs, &norms));
    for (const uint32_t norm : norms) {
        if (norm >= lengths.size()) {
            return Status::Corruption("encoded norm {} is out of range", norm);
        }
        *out += static_cast<uint64_t>(lengths[norm]);
    }
    return Status::OK();
}

Status IndexSource::sum_norm_lengths(uint64_t* out) {
    *out = 0;
    constexpr uint32_t kBatch = 4096;
    std::vector<uint32_t> docs;
    docs.reserve(kBatch);
    const uint32_t count = doc_count();
    for (uint32_t begin = 0; begin < count; begin += std::min(kBatch, count - begin)) {
        docs.resize(std::min(kBatch, count - begin));
        std::iota(docs.begin(), docs.end(), begin);
        uint64_t batch = 0;
        RETURN_IF_ERROR(sum_norm_lengths_of(docs, &batch));
        *out += batch;
    }
    return Status::OK();
}

} // namespace doris::index_query
