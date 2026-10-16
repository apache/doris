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

#include "storage/index/inverted/reader/text_index_reader.h"

#include <algorithm>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/common/single_flight.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/query/engine/all_query/all_query.h"
#include "storage/index/inverted/query/engine/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query/engine/boolean_query/listed_terms.h"
#include "storage/index/inverted/query/engine/boolean_query/operator.h"
#include "storage/index/inverted/query/engine/collect/doc_set_collector.h"
#include "storage/index/inverted/query/engine/complete_null_bitmap.h"
#include "storage/index/inverted/query/engine/empty_query.h"
#include "storage/index/inverted/query/engine/expand_query/expand_query.h"
#include "storage/index/inverted/query/engine/phrase_prefix_query/phrase_prefix_query.h"
#include "storage/index/inverted/query/engine/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query/engine/phrase_query/phrase_query.h"
#include "storage/index/inverted/query/engine/term_query/term_query.h"
#include "storage/index/inverted/query/exec/docid_set_ops.h"
#include "storage/index/inverted/query/exec/roaring_docid_sink.h"
#include "storage/index/inverted/query/exec/term_pattern.h"
#include "storage/index/inverted/query/logical/match_lowering.h"
#include "storage/index/inverted/query/scoring/bm25_similarity.h"
#include "storage/olap_common.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {

namespace logical = index_query::logical;
namespace query_v2 = inverted_index::query_v2;

namespace {

namespace logical = index_query::logical;
namespace query_v2 = inverted_index::query_v2;

query_v2::QueryPtr term_query(const IndexQueryContextPtr& context, const std::wstring& field,
                              const std::string& term) {
    return std::make_shared<query_v2::TermQuery>(context, field, term);
}

// One term is queried as itself; several form the boolean the set asks for. SEARCH counts a
// threshold before a set reaches here.
query_v2::QueryPtr term_set_query(const IndexQueryContextPtr& context, const std::wstring& field,
                                  const std::string& binding_key, const logical::TermSet& set) {
    DORIS_CHECK(set.min_should_match == 0);
    if (set.terms.size() == 1) {
        return term_query(context, field, set.terms.front());
    }
    auto builder = query_v2::create_operator_boolean_query_builder(
            set.require_all ? query_v2::OperatorType::OP_AND : query_v2::OperatorType::OP_OR);
    for (const auto& term : set.terms) {
        builder->add(term_query(context, field, term), binding_key);
    }
    return builder->build();
}

Status phrase_query(const IndexQueryContextPtr& context, const std::wstring& field,
                    const logical::Phrase& phrase, const roaring::Roaring* candidates,
                    query_v2::QueryPtr* out) {
    const bool single_terms = std::ranges::all_of(
            phrase.slots, [](const TermInfo& slot) { return slot.is_single_term(); });
    if (phrase.prefix) {
        if (!single_terms) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "a phrase prefix with several terms at one position is not supported");
        }
        *out = std::make_shared<query_v2::PhrasePrefixQuery>(context, field, phrase.slots,
                                                             candidates, phrase.suffix);
        return Status::OK();
    }
    const index_query::PhraseQueryOptions options {
            .slop = static_cast<uint32_t>(phrase.slop),
            .ordered = phrase.ordered,
            .candidates = candidates,
            .candidate_rows_consumed =
                    candidates == nullptr ? nullptr : &context->candidate_rows_consumed};
    if (single_terms) {
        *out = std::make_shared<query_v2::PhraseQuery>(context, field, phrase.slots, options);
    } else {
        *out = std::make_shared<query_v2::MultiPhraseQuery>(context, field, phrase.slots, options);
    }
    return Status::OK();
}

index_query::TermPatternKind pattern_kind(logical::ExpandKind kind) {
    switch (kind) {
    case logical::ExpandKind::kPrefix:
        return index_query::TermPatternKind::kPrefix;
    case logical::ExpandKind::kRegexp:
        return index_query::TermPatternKind::kRegexp;
    case logical::ExpandKind::kContains:
        return index_query::TermPatternKind::kContains;
    case logical::ExpandKind::kWildcard:
    default:
        return index_query::TermPatternKind::kWildcard;
    }
}

} // namespace

Status TextIndexReader::open_source(const IndexQueryContextPtr& context, const std::wstring& field,
                                    std::unique_ptr<OpenedIndex>* opened,
                                    index_query::IndexSourcePtr* source) {
    RETURN_IF_ERROR(_open_index(context, opened));
    *source = _bind_source(context, field, **opened);
    return Status::OK();
}

Status run_leaf(const IndexQueryContextPtr& context, const std::wstring& field,
                const logical::Node& leaf, const roaring::Roaring* candidates, bool scoring,
                index_query::IndexSourcePtr source, uint32_t doc_count,
                const std::shared_ptr<roaring::Roaring>& result) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_timer);
    if (const auto* expand = leaf.as<logical::Expand>(); expand != nullptr && !scoring) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
        if (doc_count == 0) {
            return Status::OK();
        }
        const auto kind = pattern_kind(expand->kind);
        index_query::TermPattern pattern;
        THROW_IF_ERROR(index_query::TermPattern::create(kind, expand->pattern, &pattern));
        THROW_IF_ERROR(query_v2::collect_expanded_rows(
                *source, pattern,
                index_query::expansion_limit(kind, index_query::max_expansions(*context)), nullptr,
                result.get()));
        return Status::OK();
    }
    std::span<const std::string> terms;
    if (const auto* term = leaf.as<logical::Term>(); term != nullptr) {
        terms = std::span(&term->term, 1);
    } else if (const auto* set = leaf.as<logical::TermSet>();
               set != nullptr && (!set->require_all || set->terms.size() == 1) &&
               set->min_should_match == 0) {
        terms = set->terms;
    }
    if (!terms.empty() && (!scoring || terms.size() > 1)) {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
        if (!scoring) {
            index_query::RoaringDocIdSink sink(*result);
            return source->collect_terms(terms, sink);
        }
        query_v2::ListedTerms listed(std::move(source), nullptr);
        for (size_t i = 0; i < terms.size(); ++i) {
            auto similarity = std::make_shared<BM25Similarity>();
            similarity->for_one_term(context, field,
                                     inverted_index::StringHelper::to_wstring(terms[i]));
            listed.add(i, terms[i], std::move(similarity));
        }
        const auto scorer = listed.scored_disjunction();
        if (context->collection_similarity != nullptr) {
            *result |= query_v2::collect_scored_rows(scorer, 0, *context->collection_similarity);
        } else {
            query_v2::collect_true_rows(scorer, result.get());
        }
        return Status::OK();
    }
    query_v2::WeightPtr weight;
    {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_init_timer);
        query_v2::QueryPtr query;
        RETURN_IF_ERROR(plan_query(leaf, context, field, "", candidates, &query));
        weight = query->weight(scoring);
    }
    SCOPED_RAW_TIMER(&context->stats->inverted_index_searcher_search_exec_timer);
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = doc_count;
    exec_ctx.field_sources.emplace(field, std::move(source));
    query_v2::collect_multi_segment_doc_set(weight, exec_ctx, "", result,
                                            context->collection_similarity, scoring);
    return Status::OK();
}

Status plan_query(const logical::Node& leaf, const IndexQueryContextPtr& context,
                  const std::wstring& field, const std::string& binding_key,
                  const roaring::Roaring* candidates, query_v2::QueryPtr* out) {
    if (const auto* term = leaf.as<logical::Term>()) {
        *out = term_query(context, field, term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        *out = term_set_query(context, field, binding_key, *set);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        return phrase_query(context, field, *phrase, candidates, out);
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        *out = std::make_shared<query_v2::ExpandQuery>(context, field, pattern_kind(expand->kind),
                                                       expand->pattern);
    } else if (leaf.as<logical::Exists>() != nullptr) {
        *out = std::make_shared<query_v2::AllQuery>(field, /*nullable=*/true);
    } else if (leaf.as<logical::Empty>() != nullptr) {
        *out = std::make_shared<query_v2::EmptyQuery>(field);
    } else {
        return Status::InternalError("leaf kind {} cannot run on a CLucene field",
                                     leaf.value.index());
    }
    return Status::OK();
}

namespace {

using QueryFlight =
        inverted_index::SingleFlight<std::pair<Status, std::shared_ptr<roaring::Roaring>>>;

QueryFlight& query_single_flight() {
    static QueryFlight flight;
    return flight;
}

// Runs `compute` once for concurrent identical queries: the first caller leads and the others
// take its bitmap. A leader's failure leaves each follower to compute for itself.
template <typename Compute>
Status run_query_single_flight(
        QueryFlight& flight, const std::string& key, std::shared_ptr<roaring::Roaring>* result,
#ifdef BE_TEST
        InvertedIndexReader::SingleFlightFollowerJoinedObserver follower_joined_observer,
        void* follower_joined_opaque,
        InvertedIndexReader::SingleFlightLeaderBeforeComputeObserver leader_before_compute_observer,
        void* leader_before_compute_opaque,
#endif
        Compute&& compute) {
    auto follower = flight.join_or_lead(key);
    if (follower.has_value()) {
#ifdef BE_TEST
        if (follower_joined_observer != nullptr) {
            follower_joined_observer(follower_joined_opaque);
        }
#endif
        auto [leader_status, leader_bitmap] = follower->get();
        if (leader_status.ok() && leader_bitmap != nullptr) {
            *result = std::move(leader_bitmap);
            return Status::OK();
        }
    }
    const bool is_leader = !follower.has_value();
#ifdef BE_TEST
    if (is_leader && leader_before_compute_observer != nullptr) {
        leader_before_compute_observer(leader_before_compute_opaque);
    }
#endif
    Status status = Status::OK();
    std::shared_ptr<roaring::Roaring> bitmap;
    {
        // Followers learn the outcome on every exit path, errors included.
        DEFER(if (is_leader) { flight.publish(key, std::make_pair(status, bitmap)); });
        status = compute(&bitmap);
    }
    RETURN_IF_ERROR(status);
    *result = std::move(bitmap);
    return Status::OK();
}

// The one exact term a COUNT_ON_INDEX scan of `leaf` can answer from the dictionary.
const std::string* single_exact_term(const logical::Node& leaf) {
    if (const auto* term = leaf.as<logical::Term>()) {
        return &term->term;
    }
    if (const auto* set = leaf.as<logical::TermSet>(); set != nullptr && set->terms.size() == 1) {
        return &set->terms.front();
    }
    const auto* phrase = leaf.as<logical::Phrase>();
    if (phrase != nullptr && phrase->slots.size() == 1 && !phrase->prefix && !phrase->suffix &&
        phrase->slots.front().is_single_term()) {
        return &phrase->slots.front().get_single_term();
    }
    return nullptr;
}

// Only a phrase of several slots reads positions row by row, so only it narrows to the scan's
// candidate rows; its partial result stays out of the cache and the flight.
bool consumes_candidates(const logical::Node& leaf) {
    const auto* phrase = leaf.as<logical::Phrase>();
    return phrase != nullptr && phrase->slots.size() > 1;
}

// The longest term of a leaf, which the ignore_above limit of a keyword index applies to.
size_t longest_term(const logical::Node& leaf) {
    size_t longest = 0;
    const auto note = [&longest](const std::string& term) {
        longest = std::max(longest, term.size());
    };
    if (const auto* term = leaf.as<logical::Term>()) {
        note(term->term);
    } else if (const auto* set = leaf.as<logical::TermSet>()) {
        std::ranges::for_each(set->terms, note);
    } else if (const auto* phrase = leaf.as<logical::Phrase>()) {
        for (const auto& slot : phrase->slots) {
            if (slot.is_single_term()) {
                note(slot.get_single_term());
            } else {
                std::ranges::for_each(slot.get_multi_terms(), note);
            }
        }
    } else if (const auto* expand = leaf.as<logical::Expand>()) {
        note(expand->pattern);
    }
    return longest;
}

} // namespace

Status TextIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    if (*iterator == nullptr) {
        *iterator = InvertedIndexIterator::create_unique();
    }
    dynamic_cast<InvertedIndexIterator*>(iterator->get())
            ->add_reader(type(), dynamic_pointer_cast<InvertedIndexReader>(shared_from_this()));
    return Status::OK();
}

Status TextIndexReader::query(const IndexQueryContextPtr& context, const std::string& column_name,
                              const Field& query_value, InvertedIndexQueryType query_type,
                              std::shared_ptr<roaring::Roaring>& bit_map,
                              const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    return _query_raw(context, column_name, query_value.get<PrimitiveType::TYPE_STRING>(),
                      query_type, bit_map, nullptr, analyzer_ctx);
}

Status TextIndexReader::query_with_null_bitmap(
        const IndexQueryContextPtr& context, const std::string& column_name,
        const Field& query_value, InvertedIndexQueryType query_type,
        std::shared_ptr<roaring::Roaring>& bit_map,
        InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
        const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    DORIS_CHECK(null_bitmap_cache_handle != nullptr);
    return _query_raw(context, column_name, query_value.get<PrimitiveType::TYPE_STRING>(),
                      query_type, bit_map, null_bitmap_cache_handle, analyzer_ctx);
}

Status TextIndexReader::query_leaf(const IndexQueryContextPtr& context,
                                   const std::string& column_name, const logical::Node& leaf,
                                   std::shared_ptr<roaring::Roaring>& bit_map,
                                   InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    const LeafRequest request {.query_type = logical::leaf_query_type(leaf),
                               .leaf = &leaf,
                               .longest_value_bytes = longest_term(leaf),
                               .text = {}};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

Status TextIndexReader::_query_raw(const IndexQueryContextPtr& context,
                                   const std::string& column_name, const std::string& value,
                                   InvertedIndexQueryType query_type,
                                   std::shared_ptr<roaring::Roaring>& bit_map,
                                   InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                                   const InvertedIndexAnalyzerCtx* analyzer_ctx) {
    const LeafRequest request {.query_type = query_type,
                               .longest_value_bytes = value.size(),
                               .text = value,
                               .analyzer_ctx = analyzer_ctx};
    return _execute(context, column_name, request, bit_map, null_bitmap_cache_handle);
}

Status TextIndexReader::_admit(const IndexQueryContextPtr& /*context*/,
                               const std::string& column_name, const LeafRequest& request,
                               Admission* /*admission*/) {
    // CLucene indexes do not persist the gram tokenizer contract needed to compile a pattern.
    if (is_gram_query(request.query_type)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "{} requires SNII storage format for column {}",
                query_type_to_string(request.query_type), column_name);
    }
    return Status::OK();
}

Status TextIndexReader::_run_gram(const IndexQueryContextPtr& /*context*/, OpenedIndex& /*index*/,
                                  const LeafRequest& request,
                                  std::shared_ptr<roaring::Roaring>* /*out*/) {
    return Status::InternalError("a text index admitted {} it cannot run",
                                 query_type_to_string(request.query_type));
}

namespace {

// A keyword index drops values longer than ignore_above: a longer query value finds nothing in
// it, and a contains match may still need such a value, so rows answer both.
Status check_keyword_request(const LeafRequest& request,
                             const std::map<std::string, std::string>& properties) {
    if (request.query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "a keyword index does not run MATCH_PHRASE_EDGE; evaluating by function");
    }
    if (int ignore_above = std::stoi(get_parser_ignore_above_value_from_properties(properties));
        std::cmp_greater(request.longest_value_bytes, ignore_above)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "query value is too long, evaluate skipped.");
    }
    return Status::OK();
}

} // namespace

// The null bitmap is read after the query and outside its timer, from the index the query opened.
Status TextIndexReader::_execute(const IndexQueryContextPtr& context,
                                 const std::string& column_name, const LeafRequest& request,
                                 std::shared_ptr<roaring::Roaring>& bit_map,
                                 InvertedIndexQueryCacheHandle* null_bitmap_cache_handle) {
    std::unique_ptr<OpenedIndex> index;
    bool count_shaped = false;
    {
        SCOPED_RAW_TIMER(&context->stats->inverted_index_query_timer);
        RETURN_IF_ERROR(
                _run_request(context, column_name, request, bit_map, &index, &count_shaped));
    }
    if (null_bitmap_cache_handle != nullptr) {
        RETURN_IF_ERROR(_read_null_bitmap(context, null_bitmap_cache_handle, index.get()));
    }
    if (count_shaped) {
        // Tells the scan the bitmap is count-shaped, so it may emit the count without iterating
        // rows. Never set on a cache hit or a shared result.
        context->count_on_index_fastpath_hit = true;
    }
    return Status::OK();
}

Status TextIndexReader::_lower(const IndexQueryContextPtr& context, const LeafRequest& request,
                               bool keyword, logical::Node* out) {
    SCOPED_RAW_TIMER(&context->stats->inverted_index_analyzer_timer);
    logical::AnalyzeValue analyze;
    const bool analyzed =
            request.analyzer_ctx != nullptr ? request.analyzer_ctx->requires_analysis() : !keyword;
    if (analyzed) {
        analyze = [&](std::string_view text, std::vector<TermInfo>* tokens) {
            return inverted_index::InvertedIndexAnalyzer::analyze(text, request.analyzer_ctx,
                                                                  _index_meta.properties(), tokens);
        };
    }
    return logical::lower_match(request.query_type, request.text, analyze, out);
}

// Keep the cache, count-only, candidate and single-flight decisions in one linear path.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status TextIndexReader::_run_request(const IndexQueryContextPtr& context,
                                     const std::string& column_name, const LeafRequest& request,
                                     std::shared_ptr<roaring::Roaring>& bit_map,
                                     std::unique_ptr<OpenedIndex>* index, bool* count_shaped) {
    const InvertedIndexQueryType query_type = request.query_type;
    // Fresh per-search reply: only the query about to run decides whether its result is
    // candidate-restricted.
    context->candidate_rows_consumed = false;
    Admission admission;
    RETURN_IF_ERROR(_admit(context, column_name, request, &admission));
    const bool keyword = type() == InvertedIndexReaderType::STRING_TYPE;
    if (keyword) {
        RETURN_IF_ERROR(check_keyword_request(request, _index_meta.properties()));
    }

    // A scoring query publishes its scores while it runs, so neither a cached bitmap nor a
    // leader's can answer it.
    const bool scoring = context->collection_similarity != nullptr &&
                         IndexReaderHelper::is_need_similarity_score(query_type, &_index_meta);
    const bool allow_result_cache = !scoring && admission.cacheable;
    InvertedIndexQueryCache::CacheKey cache_key;
    std::string index_file_key;
    if (allow_result_cache &&
        (admission.coalesce ||
         context->runtime_state->query_options().enable_inverted_index_query_cache)) {
        index_file_key = _index_file_reader->get_index_file_cache_key(&_index_meta);
        const int32_t max_expansions = index_query::max_expansions(*context);
        if (request.leaf != nullptr) {
            cache_key = {index_file_key, column_name, query_type,
                         InvertedIndexLeafSemantic {.leaf = request.leaf,
                                                    .max_expansions = max_expansions}
                                 .encode()};
        } else {
            cache_key = {index_file_key, column_name, query_type,
                         InvertedIndexRawQuerySemantic {.raw_query_bytes = request.text,
                                                        .query_type = query_type,
                                                        .max_expansions = max_expansions}};
        }
    }
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (handle_query_cache(context, cache, cache_key, &cache_handler, bit_map,
                           allow_result_cache)) {
        return Status::OK();
    }

    // A gram query runs over the index's grams; any other raw request is lowered, before the
    // index opens unless its format admits the segment first.
    const bool gram = is_gram_query(query_type);
    const bool lower = !gram && request.leaf == nullptr;
    logical::Node lowered;
    if (lower && admission.plan_before_open) {
        RETURN_IF_ERROR(_lower(context, request, keyword, &lowered));
    }
    if (Status status = _open_index(context, index, index_file_key); !status.ok()) {
        return admission.open_failed == nullptr ? status : admission.open_failed(std::move(status));
    }
    if (admission.check_open != nullptr) {
        RETURN_IF_ERROR(admission.check_open(**index));
    }
    if (lower && !admission.plan_before_open) {
        RETURN_IF_ERROR(_lower(context, request, keyword, &lowered));
    }
    const logical::Node* leaf = lower ? &lowered : request.leaf;
    if (!gram && leaf->as<logical::Empty>() != nullptr) {
        auto msg = fmt::format("token parser result is empty for query '{}'", request.text);
        if (is_match_query(query_type)) {
            LOG(WARNING) << msg;
            bit_map = std::make_shared<roaring::Roaring>();
            insert_query_cache(context, cache, cache_key, bit_map, &cache_handler,
                               allow_result_cache);
            return Status::OK();
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_NO_TERMS>(msg);
    }

    // Concurrent identical full-segment queries of a coalescing format share one run, which
    // caches what it computes.
    std::shared_ptr<roaring::Roaring> result;
    const bool coalesce = allow_result_cache && admission.coalesce;
    const auto run_coalesced = [&](const auto& run) {
        return run_query_single_flight(query_single_flight(), cache_key.encode(), &result,
#ifdef BE_TEST
                                       _single_flight_follower_joined_observer,
                                       _single_flight_follower_joined_opaque,
                                       _single_flight_leader_before_compute_observer,
                                       _single_flight_leader_before_compute_opaque,
#endif
                                       [&](std::shared_ptr<roaring::Roaring>* shared) {
                                           RETURN_IF_ERROR(run(shared));
                                           insert_query_cache(context, cache, cache_key, *shared,
                                                              &cache_handler, allow_result_cache);
                                           return Status::OK();
                                       });
    };

    if (gram) {
        const auto run_gram = [&](std::shared_ptr<roaring::Roaring>* out) {
            return _run_gram(context, **index, request, out);
        };
        if (coalesce) {
            RETURN_IF_ERROR(run_coalesced(run_gram));
        } else {
            RETURN_IF_ERROR(run_gram(&result));
            insert_query_cache(context, cache, cache_key, result, &cache_handler,
                               allow_result_cache);
        }
        bit_map = std::move(result);
        return Status::OK();
    }

    // A count-only scan of one exact term is answered from the dictionary. The cache came first,
    // since a cached row-accurate bitmap counts correctly, and the count-shaped bitmap never
    // enters the cache or the flight, which serve real row ids.
    if (context->count_on_index_fastpath) {
        if (const std::string* term = single_exact_term(*leaf); term != nullptr) {
            bool handled = false;
            std::shared_ptr<roaring::Roaring> count_bitmap;
            RETURN_IF_ERROR(
                    _count_from_df(context, column_name, **index, *term, &handled, &count_bitmap));
            if (handled) {
                bit_map = std::move(count_bitmap);
                *count_shaped = true;
                return Status::OK();
            }
        }
    }

    const bool consume_candidates =
            context->candidate_rows != nullptr && consumes_candidates(*leaf);
    if (coalesce && !consume_candidates) {
        RETURN_IF_ERROR(run_coalesced([&](std::shared_ptr<roaring::Roaring>* out) {
            return _run_leaf(context, column_name, **index, *leaf, nullptr, false, out);
        }));
    } else {
        const roaring::Roaring* candidates = consume_candidates ? context->candidate_rows : nullptr;
        RETURN_IF_ERROR(
                _run_leaf(context, column_name, **index, *leaf, candidates, scoring, &result));
        // A phrase with a slot the index lacks stops before the candidates: its result is empty
        // for the whole segment, so it may be cached.
        if (!consume_candidates || !context->candidate_rows_consumed) {
            DORIS_CHECK(!consume_candidates || result->isEmpty());
            insert_query_cache(context, cache, cache_key, result, &cache_handler,
                               allow_result_cache);
        }
    }
    DORIS_CHECK(result != nullptr);
    bit_map = std::move(result);
    return Status::OK();
}

Status TextIndexReader::_count_from_df(const IndexQueryContextPtr& context,
                                       const std::string& column_name, OpenedIndex& index,
                                       const std::string& term, bool* handled,
                                       std::shared_ptr<roaring::Roaring>* out) {
    *handled = false;
    if (_rows_of_segment == 0) {
        return Status::OK();
    }
    std::shared_ptr<roaring::Roaring> nulls;
    const auto read_nulls = [&]() -> Status {
        InvertedIndexQueryCacheHandle handle;
        RETURN_IF_ERROR(_read_null_bitmap(context, &handle, &index));
        nulls = handle.get_bitmap();
        DORIS_CHECK(nulls != nullptr);
        return Status::OK();
    };
    if (_column_is_array) {
        RETURN_IF_ERROR(read_nulls());
        if (!nulls->isEmpty()) {
            return Status::OK();
        }
    }
    uint64_t df = 0;
    uint64_t document_count = 0;
    RETURN_IF_ERROR(_term_document_frequency(column_name, index, term, &df, &document_count));
    // The fabricated ids lie inside the index's document domain, which lies inside the segment;
    // the segment's own row count is the one bound a corrupt image cannot move.
    if (df > document_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "count fast path: term df {} exceeds the index document count {}", df,
                document_count);
    }
    if (document_count > _rows_of_segment) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "count fast path: index document count {} exceeds the segment row count {}",
                document_count, _rows_of_segment);
    }
    auto result = std::make_shared<roaring::Roaring>();
    if (df > 0) {
        if (nulls == nullptr) {
            RETURN_IF_ERROR(read_nulls());
        }
        // The scan subtracts the null bitmap from every result, so the ids avoid the NULL rows and
        // the count stays df. Ids that do not fit belong to a corrupt index, which decoding
        // answers.
        if (nulls->isEmpty()) {
            result->addRange(0, df);
        } else if (!index_query::fabricate_null_disjoint_count_bitmap(df, *nulls, result.get())
                            .ok()) {
            return Status::OK();
        }
    }
    *out = std::move(result);
    *handled = true;
    return Status::OK();
}

} // namespace doris::segment_v2
