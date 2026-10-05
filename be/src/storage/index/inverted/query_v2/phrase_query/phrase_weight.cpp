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

#include "storage/index/inverted/query_v2/phrase_query/phrase_weight.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/complete_null_bitmap.h"
#include "storage/index/inverted/query_v2/const_score_query/const_score_scorer.h"
#include "storage/index/inverted/query_v2/nullable_scorer.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_scorer.h"
#include "storage/index/inverted/query_v2/postings/listed_walk.h"
#include "storage/index/inverted/query_v2/scored_rows_scorer.h"
#include "storage/index/inverted/query_v2/segment_postings.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/exec/term_waves.h"
#include "storage/index/query/phrase/exact_phrase_matcher.h"
#include "storage/index/query/phrase/exact_phrase_stream_matcher.h"
#include "storage/index/query/phrase/phrase_verifier.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/spi/postings_cursor.h"
#include "storage/index/query/term_pattern.h"

namespace doris::segment_v2::inverted_index::query_v2 {

SlotPhraseWeight::SlotPhraseWeight(std::wstring field, index_query::PhraseQueryOptions options,
                                   index_query::ScoringContextPtr<float> similarity,
                                   bool enable_scoring, bool nullable)
        : _field(std::move(field)),
          _options(options),
          _similarity(std::move(similarity)),
          _enable_scoring(enable_scoring),
          _nullable(nullable) {}

index_query::IndexSourcePtr SlotPhraseWeight::_source(const QueryExecutionContext& ctx,
                                                      const std::string& binding_key) const {
    auto source = lookup_source(_field, ctx, binding_key);
    if (!source) {
        throw Exception(ErrorCode::NOT_FOUND, "Reader not found for field '{}'",
                        StringHelper::to_string(_field));
    }
    return source;
}

ScorerPtr SlotPhraseWeight::scorer(const QueryExecutionContext& ctx,
                                   const std::string& binding_key) {
    auto source = _source(ctx, binding_key);
    ScorerPtr scorer = _lists(*source) ? _listed_scorer(*source, _options.candidates)
                                       : _streamed_scorer(*source, ctx.segment_num_rows);
    if (_nullable) {
        auto logical_field = logical_field_or_fallback(ctx, binding_key, _field);
        return make_nullable_scorer(scorer, logical_field, ctx.null_resolver);
    }
    return scorer;
}

// Only an unscored phrase lists its rows for a conjunction: a scored one scores them itself.
bool SlotPhraseWeight::lists_rows(const QueryExecutionContext& ctx,
                                  const std::string& binding_key) const {
    auto source = lookup_source(_field, ctx, binding_key);
    return source != nullptr && !_enable_scoring && _lists(*source);
}

index_query::TruthSet SlotPhraseWeight::listed_rows(const QueryExecutionContext& ctx,
                                                    const std::string& binding_key,
                                                    const roaring::Roaring* candidates) {
    auto source = _source(ctx, binding_key);
    DORIS_CHECK(_lists(*source));
    // The scan's candidates narrow the conjunction's for the rows listed; the UNKNOWN rows
    // are the field's among the conjunction's candidates, as the streamed phrase reports them.
    const roaring::Roaring* listed = candidates;
    std::optional<roaring::Roaring> narrowed;
    if (_options.candidates != nullptr) {
        if (candidates == nullptr) {
            listed = _options.candidates;
        } else {
            narrowed = *candidates & *_options.candidates;
            listed = &*narrowed;
        }
    }
    index_query::TruthSet result;
    collect_true_rows(_listed_scorer(*source, listed), &result.true_rows);
    if (_nullable) {
        auto nulls = FieldNullBitmapFetcher::fetch(
                ctx.null_resolver, logical_field_or_fallback(ctx, binding_key, _field));
        if (nulls != nullptr) {
            result.null_rows = candidates == nullptr ? *nulls : *nulls & *candidates;
        }
    }
    return result;
}

PhraseWeight::PhraseWeight(std::wstring field, std::vector<TermInfo> term_infos,
                           index_query::PhraseQueryOptions options,
                           index_query::ScoringContextPtr<float> similarity, bool enable_scoring,
                           bool nullable)
        : SlotPhraseWeight(std::move(field), options, std::move(similarity), enable_scoring,
                           nullable),
          _term_infos(std::move(term_infos)) {}

std::vector<PhraseSlot> PhraseWeight::_slots() const {
    std::vector<PhraseSlot> slots;
    slots.reserve(_term_infos.size());
    for (const auto& term_info : _term_infos) {
        slots.push_back({.offset = static_cast<uint32_t>(term_info.position),
                         .terms = {term_info.get_single_term()}});
    }
    return slots;
}

ScorerPtr PhraseWeight::_streamed_scorer(index_query::IndexSource& source, uint32_t num_docs) {
    std::vector<std::pair<size_t, SegmentPostingsPtr>> term_postings_list;
    for (const auto& term_info : _term_infos) {
        size_t offset = term_info.position;
        auto posting = open_postings(source, term_info.get_single_term(),
                                     /*positions=*/true, _enable_scoring, _similarity);
        if (posting) {
            term_postings_list.emplace_back(offset, std::move(posting));
        } else {
            return std::make_shared<EmptyScorer>();
        }
    }
    return PhraseScorer<SegmentPostingsPtr>::create(term_postings_list, _similarity, _options,
                                                    num_docs);
}

namespace {

// The cursors of a slot's terms the index holds.
using SlotCursors = std::vector<std::unique_ptr<index_query::PostingsCursor>>;
// For each slot of several terms, the listed rows each of its terms holds.
using HeldRows = std::vector<std::vector<std::vector<uint32_t>>>;

// The phrase's shape: which distinct slot each clause reads, where it sits and what it costs,
// and the documents each distinct slot's terms hold.
struct PhraseClauses {
    std::vector<size_t> slots;
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> costs;
    std::vector<uint64_t> slot_docs;
};

// A slot's positions, merged across its terms, at the rows it holds among those it was given:
// rows ascending, the positions of rows[i] from offsets[i] up to offsets[i + 1].
struct GatheredSlot {
    std::vector<uint32_t> rows;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> positions;
};

// Whether a slot of `terms` reads them a wave at a time instead of opening them together.
bool is_waved(const std::vector<std::string>& terms) {
    return terms.size() > index_query::kTermsPerWave;
}

// The distinct slots of `phrase`; clauses matching the same terms read one slot.
std::vector<PhraseSlot> distinct_slots(std::vector<PhraseSlot> phrase, PhraseClauses* clauses) {
    std::vector<PhraseSlot> distinct;
    for (PhraseSlot& slot : phrase) {
        std::ranges::sort(slot.terms);
        slot.terms.erase(std::ranges::unique(slot.terms).begin(), slot.terms.end());
        const auto found = std::ranges::find_if(distinct, [&slot](const PhraseSlot& other) {
            return other.expand == slot.expand && other.terms == slot.terms;
        });
        clauses->slots.push_back(static_cast<size_t>(found - distinct.begin()));
        clauses->offsets.push_back(slot.offset);
        if (found == distinct.end()) {
            distinct.push_back(std::move(slot));
        }
    }
    return distinct;
}

// Whether the index may hold one of `terms`, asked without reading its dictionary.
Status may_hold_any(index_query::IndexSource& source, std::span<const std::string> terms,
                    bool* held) {
    *held = false;
    for (const std::string& term : terms) {
        RETURN_IF_ERROR(source.may_hold(term, held));
        if (*held) {
            return Status::OK();
        }
    }
    return Status::OK();
}

// Opens the terms of the slots `which` in one call, each slot keeping the cursors of its terms
// the index holds. `found` tells whether every one of them holds some.
Status open_slot_terms(index_query::IndexSource& source,
                       const std::vector<std::vector<std::string>>& terms_of,
                       std::span<const size_t> which, std::vector<SlotCursors>* opened,
                       bool* found) {
    *found = true;
    if (which.empty()) {
        return Status::OK();
    }
    std::vector<std::string> terms;
    for (const size_t slot : which) {
        terms.insert(terms.end(), terms_of[slot].begin(), terms_of[slot].end());
    }
    SlotCursors cursors;
    RETURN_IF_ERROR(source.open_terms(terms, /*positions=*/true, /*scoring=*/false, &cursors));
    auto cursor = cursors.begin();
    for (const size_t slot : which) {
        for (size_t term = 0; term < terms_of[slot].size(); ++term, ++cursor) {
            if (*cursor != nullptr) {
                (*opened)[slot].push_back(std::move(*cursor));
            }
        }
        *found = *found && !(*opened)[slot].empty();
    }
    return Status::OK();
}

// The documents each distinct slot's terms hold: summed from its cursors, or from the
// dictionary for a slot of more terms than one wave reads, whose terms then move to `waved`.
Status count_slot_docs(index_query::IndexSource& source,
                       std::vector<std::vector<std::string>>& terms_of,
                       const std::vector<SlotCursors>& opened,
                       std::vector<std::vector<std::string>>* waved, std::vector<uint64_t>* docs) {
    docs->assign(terms_of.size(), 0);
    waved->assign(terms_of.size(), {});
    for (size_t slot = 0; slot < terms_of.size(); ++slot) {
        if (!is_waved(terms_of[slot])) {
            for (const auto& slot_cursor : opened[slot]) {
                (*docs)[slot] += slot_cursor->doc_freq();
            }
            continue;
        }
        RETURN_IF_ERROR(source.prepare_terms(terms_of[slot]));
        for (const std::string& term : terms_of[slot]) {
            uint64_t term_docs = 0;
            RETURN_IF_ERROR(source.doc_freq(term, &term_docs));
            (*docs)[slot] += term_docs;
        }
        (*waved)[slot] = std::move(terms_of[slot]);
    }
    return Status::OK();
}

// Opens the phrase's slots: the exact ones first, so a missing term ends the phrase before any
// expansion runs, then the terms the others expand to. A slot of more terms than one wave reads
// opens none and keeps its terms in `waved`, for its rows to gather a wave at a time. `slots`
// stays empty when a slot holds no term.
Status open_slots(index_query::IndexSource& source, std::vector<PhraseSlot> phrase,
                  PhraseClauses* clauses, std::vector<SlotCursors>* slots,
                  std::vector<std::vector<std::string>>* waved) {
    std::vector<PhraseSlot> distinct = distinct_slots(std::move(phrase), clauses);
    std::vector<std::vector<std::string>> terms_of(distinct.size());
    std::vector<size_t> exact;
    std::vector<size_t> expanded;
    for (size_t slot = 0; slot < distinct.size(); ++slot) {
        if (distinct[slot].expand.has_value()) {
            expanded.push_back(slot);
        } else {
            exact.push_back(slot);
            terms_of[slot] = std::move(distinct[slot].terms);
        }
    }
    // A slot of terms the index surely lacks ends the phrase before its dictionary is read.
    for (const size_t slot : exact) {
        bool held = false;
        RETURN_IF_ERROR(may_hold_any(source, terms_of[slot], &held));
        if (!held) {
            return Status::OK();
        }
    }
    const auto together = [&terms_of](std::span<const size_t> which) {
        std::vector<size_t> opened;
        std::ranges::copy_if(which, std::back_inserter(opened),
                             [&terms_of](size_t slot) { return !is_waved(terms_of[slot]); });
        return opened;
    };
    std::vector<SlotCursors> opened(distinct.size());
    bool found = false;
    RETURN_IF_ERROR(open_slot_terms(source, terms_of, together(exact), &opened, &found));
    if (!found) {
        return Status::OK();
    }
    for (const size_t slot : expanded) {
        index_query::TermPattern pattern;
        RETURN_IF_ERROR(index_query::TermPattern::create(*distinct[slot].expand,
                                                         distinct[slot].terms.front(), &pattern));
        RETURN_IF_ERROR(
                source.expand_terms(pattern, distinct[slot].max_expansions, &terms_of[slot]));
        if (terms_of[slot].empty()) {
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(open_slot_terms(source, terms_of, together(expanded), &opened, &found));
    if (!found) {
        return Status::OK();
    }
    std::vector<uint64_t> docs;
    RETURN_IF_ERROR(count_slot_docs(source, terms_of, opened, waved, &docs));
    for (const size_t slot : clauses->slots) {
        clauses->costs.push_back(docs[slot]);
    }
    clauses->slot_docs = std::move(docs);
    *slots = std::move(opened);
    return Status::OK();
}

void append_union_rows(std::span<const std::vector<uint32_t>> terms, std::vector<uint32_t>* out) {
    roaring::Roaring rows;
    for (const auto& term : terms) {
        rows.addMany(term.size(), term.data());
    }
    const size_t begin = out->size();
    out->resize(begin + rows.cardinality());
    if (!rows.isEmpty()) {
        rows.toUint32Array(out->data() + begin);
    }
}

// Several terms of one slot listed as the union of their documents. The documents each term
// listed are kept, since its positions are read on its own afterwards.
class UnionChainedPostings final : public index_query::ChainedPostings {
public:
    explicit UnionChainedPostings(const SlotCursors& cursors) : _held(cursors.size()) {
        _members.reserve(cursors.size());
        for (const auto& cursor : cursors) {
            _members.emplace_back(*cursor);
        }
    }

    uint64_t doc_freq() const override {
        uint64_t sum = 0;
        for (const auto& member : _members) {
            sum += member.doc_freq();
        }
        return sum;
    }

    Status start(const std::vector<uint32_t>* candidates) override {
        for (auto& member : _members) {
            RETURN_IF_ERROR(member.start(candidates));
        }
        return Status::OK();
    }

    Status collect(std::vector<uint32_t>* out) override {
        for (size_t member = 0; member < _members.size(); ++member) {
            _held[member].clear();
            RETURN_IF_ERROR(_members[member].collect(&_held[member]));
        }
        append_union_rows(_held, out);
        return Status::OK();
    }

    // The documents term `member` listed.
    const std::vector<uint32_t>& held(size_t member) const { return _held[member]; }

private:
    std::vector<index_query::CursorChainedPostings> _members;
    std::vector<std::vector<uint32_t>> _held;
};

// Candidates this many times more than the documents of the cheapest slot to list filter its
// rows instead of seeding the chain, since listing them would cost more than the slot.
constexpr uint64_t kCandidateFilterRatio = 8;

// The rows seeding the chain of `first` among `candidates`: the candidates themselves when they
// are few next to the documents of the cheapest slot, and otherwise that slot's rows among them,
// the slot then left out of `first`.
Status seed_chain(std::vector<index_query::ChainedPostings*>* first,
                  const roaring::Roaring& candidates, std::vector<uint32_t>* seed) {
    const auto cheapest = std::ranges::min_element(
            *first, {},
            [](const index_query::ChainedPostings* postings) { return postings->doc_freq(); });
    if (candidates.cardinality() <= (*cheapest)->doc_freq() * kCandidateFilterRatio) {
        seed->resize(candidates.cardinality());
        candidates.toUint32Array(seed->data());
        return Status::OK();
    }
    RETURN_IF_ERROR((*cheapest)->start(nullptr));
    RETURN_IF_ERROR((*cheapest)->collect(seed));
    roaring::BulkContext context;
    std::erase_if(*seed, [&](uint32_t row) { return !candidates.containsBulk(context, row); });
    first->erase(cheapest);
    return Status::OK();
}

// A slot of several terms decodes each term's blocks, so it lists with the exact slots only
// when its terms hold this many times fewer documents than the rarest of them.
constexpr uint64_t kSeveralTermsListingRatio = 8;

// The rows holding a term of every opened slot, among `candidates` when given, listed as a
// chain, and for a slot of several terms the rows each of its terms holds. A slot of several
// terms that is not far rarer than every exact slot lists last, on the rows the others kept. A
// slot read in waves, which opened no cursor, is left to its own gathering.
Status chain_rows(std::span<const SlotCursors> slots, const roaring::Roaring* candidates,
                  std::vector<uint32_t>* rows, HeldRows* held) {
    std::vector<index_query::CursorChainedPostings> singles;
    std::vector<UnionChainedPostings> unions;
    singles.reserve(slots.size());
    unions.reserve(slots.size());
    std::vector<index_query::ChainedPostings*> first;
    uint64_t rarest_exact = std::numeric_limits<uint64_t>::max();
    for (const SlotCursors& cursors : slots) {
        if (cursors.empty()) {
            continue;
        }
        if (cursors.size() == 1) {
            first.push_back(&singles.emplace_back(*cursors.front()));
            rarest_exact = std::min<uint64_t>(rarest_exact, cursors.front()->doc_freq());
        } else {
            unions.emplace_back(cursors);
        }
    }
    std::vector<index_query::ChainedPostings*> later;
    for (UnionChainedPostings& postings : unions) {
        const bool rare = postings.doc_freq() * kSeveralTermsListingRatio <= rarest_exact;
        (rare ? first : later).push_back(&postings);
    }
    std::vector<uint32_t> seed;
    if (candidates != nullptr) {
        RETURN_IF_ERROR(seed_chain(&first, *candidates, &seed));
    }
    std::vector<uint32_t> kept;
    RETURN_IF_ERROR(index_query::chained_conjunction(first, candidates == nullptr ? nullptr : &seed,
                                                     &kept));
    if (later.empty()) {
        *rows = std::move(kept);
    } else {
        RETURN_IF_ERROR(index_query::chained_conjunction(later, &kept, rows));
    }
    held->assign(slots.size(), {});
    auto listed = unions.begin();
    for (size_t slot = 0; slot < slots.size(); ++slot) {
        if (slots[slot].size() <= 1) {
            continue;
        }
        for (size_t member = 0; member < slots[slot].size(); ++member) {
            auto& member_rows = (*held)[slot].emplace_back();
            std::ranges::set_intersection(listed->held(member), *rows,
                                          std::back_inserter(member_rows));
        }
        ++listed;
    }
    return Status::OK();
}

// Reads, in one round, the positions each cursor's term holds at its rows in `held`, and
// appends them to `out` with their rows.
Status append_held_positions(index_query::IndexSource& source, const SlotCursors& cursors,
                             const std::vector<std::vector<uint32_t>>& held,
                             std::vector<std::pair<uint32_t, uint32_t>>* out) {
    bool reads = false;
    for (size_t member = 0; member < cursors.size(); ++member) {
        if (!held[member].empty()) {
            RETURN_IF_ERROR(cursors[member]->prefetch(&held[member], /*positions=*/true));
            reads = true;
        }
    }
    if (!reads) {
        return Status::OK();
    }
    RETURN_IF_ERROR(source.fetch_pending());
    for (size_t member = 0; member < cursors.size(); ++member) {
        const std::vector<uint32_t>& rows = held[member];
        if (rows.empty()) {
            continue;
        }
        RETURN_IF_ERROR(cursors[member]->rewind());
        TermWalk walk(*cursors[member], rows);
        for (size_t row = 0; row < rows.size(); ++row) {
            index_query::PhrasePositionSpan positions;
            RETURN_IF_ERROR(walk.positions_of(row, rows[row], &positions));
            for (const uint32_t* position = positions.first; position != positions.second;
                 ++position) {
                out->emplace_back(rows[row], *position);
            }
        }
    }
    return Status::OK();
}

// The positions of a slot's terms, merged per row: rows ascending, each with its positions.
void build_gathered(const std::vector<std::vector<uint32_t>>& term_rows,
                    std::vector<std::pair<uint32_t, uint32_t>> held_positions, GatheredSlot* out) {
    out->rows.clear();
    append_union_rows(term_rows, &out->rows);
    std::ranges::sort(held_positions);
    held_positions.erase(std::ranges::unique(held_positions).begin(), held_positions.end());
    out->positions.clear();
    out->positions.reserve(held_positions.size());
    out->offsets.assign(out->rows.size() + 1, 0);
    size_t next = 0;
    for (size_t row = 0; row < out->rows.size(); ++row) {
        for (; next < held_positions.size() && held_positions[next].first == out->rows[row];
             ++next) {
            out->positions.push_back(held_positions[next].second);
        }
        out->offsets[row + 1] = static_cast<uint32_t>(out->positions.size());
    }
    DCHECK_EQ(next, held_positions.size());
}

// Lists, a wave of terms at a time, the rows among `domain` (every row when null) each of
// `terms` holds, into `term_rows` by term. With `held_positions`, each wave also reads its
// terms' positions at those rows, one round more, before it is released.
Status list_term_rows(index_query::IndexSource& source, std::span<const std::string> terms,
                      const std::vector<uint32_t>* domain,
                      std::vector<std::vector<uint32_t>>* term_rows,
                      std::vector<std::pair<uint32_t, uint32_t>>* held_positions) {
    term_rows->assign(terms.size(), {});
    SlotCursors cursors;
    for (size_t begin = 0; begin < terms.size(); begin += index_query::kTermsPerWave) {
        const auto wave =
                terms.subspan(begin, std::min(index_query::kTermsPerWave, terms.size() - begin));
        RETURN_IF_ERROR(
                source.open_terms(wave, held_positions != nullptr, /*scoring=*/false, &cursors));
        std::vector<index_query::CursorChainedPostings> members;
        members.reserve(cursors.size());
        for (const auto& cursor : cursors) {
            if (cursor != nullptr) {
                RETURN_IF_ERROR(members.emplace_back(*cursor).start(domain));
            }
        }
        std::vector<std::vector<uint32_t>> held;
        SlotCursors present;
        for (size_t i = 0, member = 0; i < cursors.size(); ++i) {
            if (cursors[i] == nullptr) {
                continue;
            }
            auto& rows = (*term_rows)[begin + i];
            RETURN_IF_ERROR(members[member++].collect(&rows));
            held.push_back(rows);
            present.push_back(std::move(cursors[i]));
        }
        if (held_positions != nullptr) {
            RETURN_IF_ERROR(append_held_positions(source, present, held, held_positions));
        }
        cursors.clear();
    }
    return Status::OK();
}

// Reads, a wave of terms at a time, the positions each of `terms` holds at its rows in
// `term_rows`, opening only the terms holding one.
Status read_term_positions(index_query::IndexSource& source, std::span<const std::string> terms,
                           const std::vector<std::vector<uint32_t>>& term_rows,
                           std::vector<std::pair<uint32_t, uint32_t>>* held_positions) {
    std::vector<std::string> holding;
    std::vector<std::vector<uint32_t>> held;
    for (size_t i = 0; i < terms.size(); ++i) {
        if (!term_rows[i].empty()) {
            holding.push_back(terms[i]);
            held.push_back(term_rows[i]);
        }
    }
    SlotCursors cursors;
    for (size_t begin = 0; begin < holding.size(); begin += index_query::kTermsPerWave) {
        const size_t count = std::min(index_query::kTermsPerWave, holding.size() - begin);
        RETURN_IF_ERROR(source.open_terms(std::span(holding).subspan(begin, count),
                                          /*positions=*/true, /*scoring=*/false, &cursors));
        const std::vector<std::vector<uint32_t>> wave_held(
                held.begin() + static_cast<std::ptrdiff_t>(begin),
                held.begin() + static_cast<std::ptrdiff_t>(begin + count));
        // The dictionary listed every one of these terms a moment ago.
        DORIS_CHECK(std::ranges::none_of(cursors, [](const auto& cursor) { return !cursor; }));
        RETURN_IF_ERROR(append_held_positions(source, cursors, wave_held, held_positions));
        cursors.clear();
    }
    return Status::OK();
}

// Keeps, of the rows each opened slot's term holds, those still listed.
void keep_listed(const std::vector<uint32_t>& rows, HeldRows* held) {
    for (auto& slot : *held) {
        for (auto& member : slot) {
            std::vector<uint32_t> kept;
            std::ranges::set_intersection(member, rows, std::back_inserter(kept));
            member.swap(kept);
        }
    }
}

// The rows among `domain` (or `candidates` while it is unset, or every row) that each of `terms`
// holds, docids only, by term, and their union into `domain`. Rows given in numbers far above
// the slot's documents are not listed on: the slot lists whole and keeps those among them.
Status list_rare_slot(index_query::IndexSource& source, std::span<const std::string> terms,
                      uint64_t slot_docs, const roaring::Roaring* candidates,
                      std::optional<std::vector<uint32_t>>* domain,
                      std::vector<std::vector<uint32_t>>* term_rows) {
    const uint64_t few = slot_docs * kCandidateFilterRatio;
    if (!domain->has_value() && candidates != nullptr && candidates->cardinality() <= few) {
        domain->emplace(candidates->cardinality());
        candidates->toUint32Array((*domain)->data());
    }
    const bool on_domain = domain->has_value() && (*domain)->size() <= few;
    RETURN_IF_ERROR(list_term_rows(source, terms, on_domain ? &domain->value() : nullptr, term_rows,
                                   nullptr));
    roaring::BulkContext context;
    for (auto& rows : *term_rows) {
        if (domain->has_value() && !on_domain) {
            std::erase_if(rows, [&domain](uint32_t row) {
                return !std::ranges::binary_search(**domain, row);
            });
        } else if (!domain->has_value() && candidates != nullptr) {
            std::erase_if(rows, [candidates, &context](uint32_t row) {
                return !candidates->containsBulk(context, row);
            });
        }
    }
    domain->emplace();
    append_union_rows(*term_rows, &domain->value());
    return Status::OK();
}

// Gathers the positions of the slots read in waves, in `order`, at the rows of `rows`, narrowing
// them to the rows each slot holds: a slot listed earlier reads only its terms holding a row,
// and another lists on the rows and reads their positions a wave at a time.
Status gather_slots(index_query::IndexSource& source,
                    std::span<const std::vector<std::string>> waved, std::span<const size_t> order,
                    std::vector<std::vector<std::vector<uint32_t>>>& listed,
                    std::vector<uint32_t>* rows, std::vector<GatheredSlot>* gathered) {
    for (const size_t slot : order) {
        if (rows->empty()) {
            break;
        }
        std::vector<std::vector<uint32_t>> term_rows;
        std::vector<std::pair<uint32_t, uint32_t>> held_positions;
        if (listed[slot].empty()) {
            RETURN_IF_ERROR(list_term_rows(source, waved[slot], rows, &term_rows, &held_positions));
        } else {
            term_rows = std::move(listed[slot]);
            for (auto& term : term_rows) {
                std::vector<uint32_t> kept;
                std::ranges::set_intersection(term, *rows, std::back_inserter(kept));
                term.swap(kept);
            }
            RETURN_IF_ERROR(read_term_positions(source, waved[slot], term_rows, &held_positions));
        }
        build_gathered(term_rows, std::move(held_positions), &(*gathered)[slot]);
        *rows = (*gathered)[slot].rows;
    }
    return Status::OK();
}

// The rows holding a term of every slot, among `candidates` when given, with the rows each term
// of an opened slot of several terms holds and, for a slot read in waves, its positions gathered
// at those rows. A slot read in waves that holds 8 times fewer documents than the rarest
// single-term slot, and every one of them when the phrase has no single-term slot, first lists
// its rows by term, docids only, and those rows seed the chain of the opened slots. Every slot
// read in waves then gathers its positions at the rows the chain kept, so positions are read
// only where every slot holds a term.
Status list_rows(index_query::IndexSource& source, std::span<const SlotCursors> slots,
                 std::span<const std::vector<std::string>> waved,
                 std::span<const uint64_t> slot_docs, const roaring::Roaring* candidates,
                 std::vector<uint32_t>* rows, HeldRows* held, std::vector<GatheredSlot>* gathered) {
    std::vector<size_t> order;
    uint64_t rarest_single = std::numeric_limits<uint64_t>::max();
    bool any_opened = false;
    for (size_t slot = 0; slot < slots.size(); ++slot) {
        if (!waved[slot].empty()) {
            order.push_back(slot);
            continue;
        }
        any_opened = true;
        if (slots[slot].size() == 1) {
            rarest_single = std::min(rarest_single, slot_docs[slot]);
        }
    }
    if (order.empty()) {
        return chain_rows(slots, candidates, rows, held);
    }
    std::ranges::stable_sort(order, {}, [slot_docs](size_t slot) { return slot_docs[slot]; });
    gathered->assign(slots.size(), {});
    rows->clear();
    held->assign(slots.size(), {});
    std::vector<std::vector<std::vector<uint32_t>>> listed(slots.size());
    std::optional<std::vector<uint32_t>> domain;
    for (const size_t slot : order) {
        if (slot_docs[slot] * kSeveralTermsListingRatio > rarest_single) {
            continue;
        }
        RETURN_IF_ERROR(list_rare_slot(source, waved[slot], slot_docs[slot], candidates, &domain,
                                       &listed[slot]));
        if (domain->empty()) {
            return Status::OK();
        }
    }
    if (any_opened) {
        roaring::Roaring narrowed;
        const roaring::Roaring* chain_candidates = candidates;
        if (domain.has_value()) {
            narrowed.addMany(domain->size(), domain->data());
            chain_candidates = &narrowed;
        }
        RETURN_IF_ERROR(chain_rows(slots, chain_candidates, rows, held));
    } else {
        *rows = std::move(*domain);
    }
    RETURN_IF_ERROR(gather_slots(source, waved, order, listed, rows, gathered));
    keep_listed(*rows, held);
    return Status::OK();
}

// Asks every term for the positions of the listed rows it holds, read in one round.
Status prefetch_positions(std::span<const SlotCursors> slots, const std::vector<uint32_t>& rows,
                          const HeldRows& held) {
    for (size_t slot = 0; slot < slots.size(); ++slot) {
        if (slots[slot].size() == 1) {
            RETURN_IF_ERROR(slots[slot].front()->prefetch(&rows, /*positions=*/true));
            continue;
        }
        for (size_t member = 0; member < slots[slot].size(); ++member) {
            RETURN_IF_ERROR(slots[slot][member]->prefetch(&held[slot][member], /*positions=*/true));
        }
    }
    return Status::OK();
}

// The positions a slot's terms hold at the listed rows: one term's as they are, several terms'
// merged in ascending order.
class SlotWalk {
public:
    // A slot of one term, which holds every listed row.
    SlotWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows) {
        _walks.emplace_back(cursor, rows);
    }

    // A slot of several terms, term `member` holding the rows `held[member]`.
    SlotWalk(const SlotCursors& cursors, std::span<const std::vector<uint32_t>> held)
            : _held(held), _next(held.size(), 0) {
        _walks.reserve(cursors.size());
        for (size_t member = 0; member < cursors.size(); ++member) {
            _walks.emplace_back(*cursors[member], held[member]);
        }
    }

    // A slot whose positions were gathered at every listed row.
    explicit SlotWalk(const GatheredSlot& gathered) : _gathered(&gathered) {}

    Status positions_of(size_t row, uint32_t doc, index_query::PhrasePositionSpan* span) {
        if (_gathered != nullptr) {
            return _gathered_positions(doc, span);
        }
        if (_held.empty()) {
            return _walks.front().positions_of(row, doc, span);
        }
        return _merged_positions(doc, span);
    }

private:
    Status _merged_positions(uint32_t doc, index_query::PhrasePositionSpan* span);

    // The listed rows come in ascending order, and the gathered slot holds each of them.
    Status _gathered_positions(uint32_t doc, index_query::PhrasePositionSpan* span) {
        const std::vector<uint32_t>& rows = _gathered->rows;
        while (rows[_next_gathered] < doc) {
            ++_next_gathered;
        }
        DCHECK_EQ(rows[_next_gathered], doc);
        const uint32_t* positions = _gathered->positions.data();
        *span = {positions + _gathered->offsets[_next_gathered],
                 positions + _gathered->offsets[_next_gathered + 1]};
        return Status::OK();
    }

    const GatheredSlot* _gathered = nullptr;
    size_t _next_gathered = 0;
    std::span<const std::vector<uint32_t>> _held;
    std::vector<size_t> _next;
    std::vector<TermWalk> _walks;
    std::vector<uint32_t> _merged;
};

// The positions of the terms holding `doc`, merged when more than one does.
Status SlotWalk::_merged_positions(uint32_t doc, index_query::PhrasePositionSpan* span) {
    size_t holders = 0;
    for (size_t member = 0; member < _walks.size(); ++member) {
        const std::vector<uint32_t>& held = _held[member];
        size_t& next = _next[member];
        while (next < held.size() && held[next] < doc) {
            ++next;
        }
        if (next == held.size() || held[next] != doc) {
            continue;
        }
        index_query::PhrasePositionSpan positions;
        RETURN_IF_ERROR(_walks[member].positions_of(next, doc, &positions));
        if (++holders == 1) {
            *span = positions;
            continue;
        }
        if (holders == 2) {
            _merged.assign(span->first, span->second);
        }
        _merged.insert(_merged.end(), positions.first, positions.second);
    }
    DCHECK_GT(holders, 0);
    if (holders > 1) {
        std::ranges::sort(_merged);
        _merged.erase(std::ranges::unique(_merged).begin(), _merged.end());
        *span = {_merged.data(), _merged.data() + _merged.size()};
    }
    return Status::OK();
}

// The rows the phrase matches, each verified over the positions its slots hold there, with the
// phrase's frequency in each when counting. Two distinct slots of an exact phrase check
// directly; every other shape goes through the shared verifier, which may skip a slot on a row
// it already rejected.
template <bool kCounting, typename Walk>
Status verify_rows(std::span<Walk> walks, std::span<const uint32_t> rows,
                   const PhraseClauses& clauses, const index_query::PhraseQueryOptions& options,
                   std::vector<uint32_t>* matched, std::vector<float>* frequencies) {
    if (options.slop == 0 && clauses.slots.size() == 2 && clauses.slots[0] == 0 &&
        clauses.slots[1] == 1) {
        const uint32_t delta = clauses.offsets[1] - clauses.offsets[0];
        for (size_t row = 0; row < rows.size(); ++row) {
            index_query::PhrasePositionSpan left;
            index_query::PhrasePositionSpan right;
            RETURN_IF_ERROR(walks[0].positions_of(row, rows[row], &left));
            RETURN_IF_ERROR(walks[1].positions_of(row, rows[row], &right));
            if constexpr (kCounting) {
                const uint32_t count = index_query::count_two_term_phrase(left, right, delta);
                if (count > 0) {
                    matched->push_back(rows[row]);
                    frequencies->push_back(static_cast<float>(count));
                }
            } else if (index_query::contains_two_term_phrase(left, right, delta)) {
                matched->push_back(rows[row]);
            }
        }
        return Status::OK();
    }
    index_query::PhraseVerifier verifier(clauses.slots, clauses.offsets, clauses.costs,
                                         options.slop, options.ordered);
    for (size_t row = 0; row < rows.size(); ++row) {
        const uint32_t doc = rows[row];
        const auto load = [&walks, row, doc](size_t slot, index_query::PhrasePositionSpan* span) {
            return walks[slot].positions_of(row, doc, span);
        };
        float frequency = 0.0F;
        RETURN_IF_ERROR(verifier.verify(load, kCounting, &frequency));
        if (frequency > 0.0F) {
            matched->push_back(doc);
            if constexpr (kCounting) {
                frequencies->push_back(frequency);
            }
        }
    }
    return Status::OK();
}

template <typename Walk>
Status verify_rows(std::vector<Walk>& walks, std::span<const uint32_t> rows,
                   const PhraseClauses& clauses, const index_query::PhraseQueryOptions& options,
                   std::vector<uint32_t>* matched, std::vector<float>* frequencies) {
    return frequencies != nullptr
                   ? verify_rows<true, Walk>(walks, rows, clauses, options, matched, frequencies)
                   : verify_rows<false, Walk>(walks, rows, clauses, options, matched, frequencies);
}

// An exact phrase reads its rows' positions as the match goes, each document's only as far as it
// needs, when one of its terms holds this many per document and the rows hold this many in all;
// lighter positions decode faster a block at a time.
constexpr uint64_t kStreamedPositionsPerDoc = 8;
constexpr uint64_t kStreamedPositions = 512;

// Whether an exact phrase of distinct single-term slots streams its rows' positions.
Status streams_positions(std::span<const SlotCursors> slots, std::span<const uint32_t> rows,
                         const PhraseClauses& clauses,
                         const index_query::PhraseQueryOptions& options, bool* streams) {
    *streams = false;
    if (options.slop != 0 || clauses.slots.size() != slots.size() ||
        std::ranges::adjacent_find(clauses.offsets, std::greater_equal {}) !=
                clauses.offsets.end()) {
        return Status::OK();
    }
    uint64_t heaviest = 0;
    uint64_t per_row = 0;
    for (const SlotCursors& cursors : slots) {
        uint64_t per_doc = 0;
        RETURN_IF_ERROR(cursors.front()->positions_per_doc(&per_doc));
        heaviest = std::max(heaviest, per_doc);
        per_row += per_doc;
    }
    *streams = heaviest >= kStreamedPositionsPerDoc &&
               per_row >= (kStreamedPositions + rows.size() - 1) / rows.size();
    return Status::OK();
}

// Verifies an exact phrase of distinct single-term slots on the listed rows with each row's
// positions streamed.
Status verify_streamed(std::span<const SlotCursors> slots, std::span<const uint32_t> rows,
                       const PhraseClauses& clauses, std::vector<uint32_t>* matched) {
    std::vector<StreamWalk> walks;
    walks.reserve(slots.size());
    for (const SlotCursors& cursors : slots) {
        walks.emplace_back(*cursors.front(), rows);
    }
    const std::span<StreamWalk> cursors(walks);
    index_query::validate_exact_phrase_stream_inputs(cursors, std::span(clauses.slots),
                                                     std::span(clauses.offsets));
    for (const uint32_t row : rows) {
        bool hit = false;
        RETURN_IF_ERROR(index_query::match_exact_phrase_document(
                cursors, std::span(clauses.slots), std::span(clauses.offsets), row, &hit));
        if (hit) {
            matched->push_back(row);
        }
    }
    return Status::OK();
}

// Verifies the phrase on the listed rows, the slots' cursors rewound after the chain, and
// counts the phrase's frequency per row into `frequencies` when given. Slots of one term each
// are walked directly, and a phrase with a slot of several terms merges their positions per
// row; a slot read in waves gives the positions it gathered.
Status verify_slots(std::span<const SlotCursors> slots, std::span<const GatheredSlot> gathered,
                    std::span<const uint32_t> rows, const HeldRows& held,
                    const PhraseClauses& clauses, const index_query::PhraseQueryOptions& options,
                    std::vector<uint32_t>* matched, std::vector<float>* frequencies) {
    for (const SlotCursors& cursors : slots) {
        for (const auto& cursor : cursors) {
            RETURN_IF_ERROR(cursor->rewind());
        }
    }
    if (std::ranges::all_of(slots,
                            [](const SlotCursors& cursors) { return cursors.size() == 1; })) {
        bool streams = false;
        if (frequencies == nullptr) {
            RETURN_IF_ERROR(streams_positions(slots, rows, clauses, options, &streams));
        }
        if (streams) {
            return verify_streamed(slots, rows, clauses, matched);
        }
        std::vector<TermWalk> walks;
        walks.reserve(slots.size());
        for (const SlotCursors& cursors : slots) {
            walks.emplace_back(*cursors.front(), rows);
        }
        return verify_rows(walks, rows, clauses, options, matched, frequencies);
    }
    std::vector<SlotWalk> walks;
    walks.reserve(slots.size());
    for (size_t slot = 0; slot < slots.size(); ++slot) {
        if (slots[slot].empty()) {
            walks.emplace_back(gathered[slot]);
        } else if (slots[slot].size() == 1) {
            walks.emplace_back(*slots[slot].front(), rows);
        } else {
            walks.emplace_back(slots[slot], held[slot]);
        }
    }
    return verify_rows(walks, rows, clauses, options, matched, frequencies);
}

} // namespace

ScorerPtr SlotPhraseWeight::_listed_scorer(index_query::IndexSource& source,
                                           const roaring::Roaring* candidates) {
    PhraseClauses clauses;
    std::vector<SlotCursors> slots;
    std::vector<std::vector<std::string>> waved;
    THROW_IF_ERROR(open_slots(source, _slots(), &clauses, &slots, &waved));
    if (slots.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    if (_options.candidate_rows_consumed != nullptr) {
        *_options.candidate_rows_consumed = true;
    }
    std::vector<uint32_t> rows;
    HeldRows held;
    std::vector<GatheredSlot> gathered;
    THROW_IF_ERROR(list_rows(source, slots, waved, clauses.slot_docs, candidates, &rows, &held,
                             &gathered));
    if (rows.empty()) {
        return std::make_shared<EmptyScorer>();
    }
    // The opened slots' positions at the rows, read in one round, then every row verified as the
    // slots are walked.
    THROW_IF_ERROR(prefetch_positions(slots, rows, held));
    THROW_IF_ERROR(source.fetch_pending());
    std::vector<uint32_t> matched;
    std::vector<float> frequencies;
    THROW_IF_ERROR(verify_slots(slots, gathered, rows, held, clauses, _options, &matched,
                                _enable_scoring ? &frequencies : nullptr));
    if (!_enable_scoring) {
        auto matched_rows = std::make_shared<roaring::Roaring>();
        matched_rows->addMany(matched.size(), matched.data());
        return std::make_shared<ConstScoreScorer<BitSetScorerPtr>>(
                std::make_shared<BitSetScorer>(std::move(matched_rows)));
    }
    // Each matched row is scored on its phrase frequency and the source's norm.
    DORIS_CHECK(_similarity != nullptr);
    std::vector<uint32_t> norms;
    THROW_IF_ERROR(source.encoded_norms(matched, &norms));
    _similarity->bind_norms(source.norm_lengths());
    std::vector<float> scores(matched.size());
    for (size_t i = 0; i < matched.size(); ++i) {
        scores[i] = _similarity->score(frequencies[i], norms[i]);
    }
    return std::make_shared<ScoredRowsScorer>(std::move(matched), std::move(scores), nullptr);
}

} // namespace doris::segment_v2::inverted_index::query_v2
