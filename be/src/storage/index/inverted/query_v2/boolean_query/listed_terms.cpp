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

#include "storage/index/inverted/query_v2/boolean_query/listed_terms.h"

#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <bit>
#include <optional>
#include <span>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/query_v2/postings/listed_walk.h"
#include "storage/index/inverted/query_v2/scored_rows_scorer.h"
#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/exec/term_waves.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::segment_v2::inverted_index::query_v2 {

ListedTerms::ListedTerms(index_query::IndexSourcePtr source,
                         std::shared_ptr<roaring::Roaring> nulls)
        : _source(std::move(source)), _nulls(std::move(nulls)) {}

void ListedTerms::add(size_t clause, std::string term,
                      index_query::ScoringContextPtr<float> similarity) {
    DCHECK(_clauses.empty() || _clauses.back() < clause);
    _clauses.push_back(clause);
    _terms.push_back(std::move(term));
    _similarities.push_back(std::move(similarity));
}

bool ListedTerms::holds(size_t clause) const {
    return std::ranges::binary_search(_clauses, clause);
}

void ListedTerms::open(bool conjunctive, bool scoring) {
    if (!conjunctive) {
        return;
    }
    for (const std::string& term : _terms) {
        bool held = false;
        THROW_IF_ERROR(_source->may_hold(term, &held));
        if (!held) {
            _cursors.resize(_terms.size());
            return;
        }
    }
    // A scored conjunction reads the positions of the rows it lists.
    THROW_IF_ERROR(
            _source->open_terms(_terms, /*positions=*/scoring, /*scoring=*/false, &_cursors));
}

bool ListedTerms::has_absent_term() const {
    return std::ranges::any_of(_cursors, [](const auto& cursor) { return cursor == nullptr; });
}

uint64_t ListedTerms::cheapest_doc_freq() const {
    uint64_t cheapest = UINT64_MAX;
    for (const auto& cursor : _cursors) {
        cheapest = std::min<uint64_t>(cheapest, cursor == nullptr ? 0 : cursor->doc_freq());
    }
    return cheapest;
}

index_query::TruthSet ListedTerms::conjunction(const std::vector<uint32_t>* candidates) {
    index_query::TruthSet result;
    if (has_absent_term()) {
        return result;
    }
    std::vector<uint32_t> docs;
    THROW_IF_ERROR(index_query::chain_cursors(_cursors, candidates, &docs));
    result.true_rows.addMany(docs.size(), docs.data());
    if (_nulls != nullptr) {
        result.null_rows = *_nulls;
    }
    return result;
}

index_query::TruthSet ListedTerms::disjunction() {
    index_query::TruthSet result;
    bool any_present = false;
    index_query::RoaringDocIdSink sink(result.true_rows);
    THROW_IF_ERROR(index_query::collect_term_rows(*_source, _terms, sink, &any_present));
    if (any_present && _nulls != nullptr) {
        result.null_rows = *_nulls;
    }
    return result;
}

namespace {

// Merges a term's postings into the rows scored so far, both ascending: a row both hold sums
// the scores, a row either holds alone keeps its own.
void merge_term(index_query::BlockDocSet& docs, index_query::ScoringContext<float>& similarity,
                std::vector<uint32_t>* rows, std::vector<float>* scores) {
    // Reading the rows so far through spans lets the loop keep their bounds in registers, which
    // it cannot do through vectors a caller may hold by reference.
    const std::span<const uint32_t> old_rows(*rows);
    const std::span<const float> old_scores(*scores);
    std::vector<uint32_t> merged_rows;
    std::vector<float> merged_scores;
    merged_rows.reserve(old_rows.size() + docs.size_hint());
    merged_scores.reserve(old_rows.size() + docs.size_hint());
    size_t i = 0;
    for (; !docs.exhausted(); docs.advance()) {
        const uint32_t doc = docs.doc();
        for (; i < old_rows.size() && old_rows[i] < doc; ++i) {
            merged_rows.push_back(old_rows[i]);
            merged_scores.push_back(old_scores[i]);
        }
        float score = similarity.score(static_cast<float>(docs.freq()), docs.norm());
        if (i < old_rows.size() && old_rows[i] == doc) {
            score += old_scores[i];
            ++i;
        }
        merged_rows.push_back(doc);
        merged_scores.push_back(score);
    }
    merged_rows.insert(merged_rows.end(), old_rows.begin() + static_cast<std::ptrdiff_t>(i),
                       old_rows.end());
    merged_scores.insert(merged_scores.end(), old_scores.begin() + static_cast<std::ptrdiff_t>(i),
                         old_scores.end());
    rows->swap(merged_rows);
    scores->swap(merged_scores);
}

// The rows a disjunction holds and their scores, one slot per row of the segment: a term adds
// into the slots in one pass where merging would copy every row scored so far.
class RowSlots {
public:
    RowSlots(uint32_t doc_count, std::span<const uint32_t> rows, std::span<const float> scores)
            : _scores(doc_count, 0.0F), _held((doc_count + 63) / 64, 0) {
        for (size_t i = 0; i < rows.size(); ++i) {
            _scores[rows[i]] = scores[i];
            hold(rows[i]);
        }
    }

    void add_term(index_query::BlockDocSet& docs, index_query::ScoringContext<float>& similarity) {
        for (; !docs.exhausted(); docs.advance()) {
            const uint32_t doc = docs.doc();
            DCHECK_LT(doc, _scores.size());
            _scores[doc] += similarity.score(static_cast<float>(docs.freq()), docs.norm());
            hold(doc);
        }
    }

    // The rows held, ascending, and their scores.
    void list(std::vector<uint32_t>* rows, std::vector<float>* scores) const {
        size_t count = 0;
        for (const uint64_t word : _held) {
            count += std::popcount(word);
        }
        rows->clear();
        scores->clear();
        rows->reserve(count);
        scores->reserve(count);
        for (size_t word = 0; word < _held.size(); ++word) {
            for (uint64_t bits = _held[word]; bits != 0; bits &= bits - 1) {
                const auto row = static_cast<uint32_t>(word * 64 + std::countr_zero(bits));
                rows->push_back(row);
                scores->push_back(_scores[row]);
            }
        }
    }

private:
    void hold(uint32_t row) { _held[row / 64] |= uint64_t {1} << (row % 64); }

    std::vector<float> _scores;
    std::vector<uint64_t> _held;
};

} // namespace

// The chain lists the rows, then the terms read their positions a wave at a time, one round
// each, and score each row on the positions it holds there and the source's norm.
ScorerPtr ListedTerms::scored_conjunction() {
    if (has_absent_term()) {
        return std::make_shared<EmptyScorer>();
    }
    std::vector<uint32_t> rows;
    THROW_IF_ERROR(index_query::chain_cursors(_cursors, nullptr, &rows));
    if (rows.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    std::vector<uint32_t> norms;
    THROW_IF_ERROR(_source->encoded_norms(rows, &norms));
    std::vector<float> scores(rows.size(), 0.0F);
    for (size_t begin = 0; begin < _cursors.size(); begin += index_query::kTermsPerWave) {
        const size_t end = std::min(_cursors.size(), begin + index_query::kTermsPerWave);
        for (size_t i = begin; i < end; ++i) {
            THROW_IF_ERROR(_cursors[i]->prefetch(&rows, /*positions=*/true));
        }
        THROW_IF_ERROR(_source->fetch_pending());
        for (size_t i = begin; i < end; ++i) {
            _similarities[i]->bind_norms(_source->norm_lengths());
            THROW_IF_ERROR(_cursors[i]->rewind());
            TermWalk walk(*_cursors[i], rows);
            for (size_t row = 0; row < rows.size();) {
                THROW_IF_ERROR(walk.prepare(row, rows[row]));
                for (const size_t row_end = walk.end(); row < row_end; ++row) {
                    const auto positions = walk.positions(row);
                    const auto frequency = static_cast<float>(positions.second - positions.first);
                    scores[row] += _similarities[i]->score(frequency, norms[row]);
                }
            }
            _cursors[i].reset();
        }
    }
    return std::make_shared<ScoredRowsScorer>(std::move(rows), std::move(scores), _nulls);
}

// Every term reads its whole posting, with its frequencies and norms, a wave of terms per round;
// a row's score sums the scores of the terms holding it, in clause order. Once a term and the
// rows scored before it cover a quarter of the segment, the terms add into a slot per row.
// Sparse results switch to a map after one wave to bound repeated merging.
ScorerPtr ListedTerms::scored_disjunction() {
    bool any_present = false;
    std::vector<uint32_t> rows;
    std::vector<float> scores;
    std::optional<RowSlots> slots;
    std::optional<phmap::flat_hash_map<uint32_t, float>> sparse;
    const uint32_t doc_count = _source->doc_count();
    THROW_IF_ERROR(index_query::visit_term_postings(
            *_source, _terms, /*scoring=*/true,
            [&](size_t i, index_query::PostingsCursor* cursor) -> Status {
                if (cursor == nullptr) {
                    return Status::OK();
                }
                any_present = true;
                _similarities[i]->bind_norms(_source->norm_lengths());
                if (!slots.has_value()) {
                    const size_t held = sparse.has_value() ? sparse->size() : rows.size();
                    if (held != 0 && (held + cursor->doc_freq()) * 4 >= doc_count) {
                        if (sparse.has_value()) {
                            rows.clear();
                            scores.clear();
                            for (const auto& [row, score] : *sparse) {
                                rows.push_back(row);
                                scores.push_back(score);
                            }
                            sparse.reset();
                        }
                        slots.emplace(doc_count, rows, scores);
                    } else if (!sparse.has_value() && i >= index_query::kTermsPerWave) {
                        sparse.emplace();
                        sparse->reserve(rows.size() + cursor->doc_freq());
                        for (size_t row = 0; row < rows.size(); ++row) {
                            sparse->emplace(rows[row], scores[row]);
                        }
                    }
                }
                index_query::BlockDocSet docs(*cursor);
                if (slots.has_value()) {
                    slots->add_term(docs, *_similarities[i]);
                } else if (sparse.has_value()) {
                    for (; !docs.exhausted(); docs.advance()) {
                        (*sparse)[docs.doc()] += _similarities[i]->score(
                                static_cast<float>(docs.freq()), docs.norm());
                    }
                } else {
                    merge_term(docs, *_similarities[i], &rows, &scores);
                }
                return Status::OK();
            }));
    if (!any_present) {
        return std::make_shared<EmptyScorer>();
    }
    if (slots.has_value()) {
        slots->list(&rows, &scores);
    } else if (sparse.has_value()) {
        rows.clear();
        scores.clear();
        for (const auto& [row, score] : *sparse) {
            rows.push_back(row);
        }
        std::ranges::sort(rows);
        for (const uint32_t row : rows) {
            scores.push_back(sparse->at(row));
        }
    }
    return std::make_shared<ScoredRowsScorer>(std::move(rows), std::move(scores), _nulls);
}

} // namespace doris::segment_v2::inverted_index::query_v2
