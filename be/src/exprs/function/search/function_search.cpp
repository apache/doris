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

#include "exprs/function/search/function_search.h"

#include <CLucene/config/repl_wchar.h>
#include <CLucene/debug/error.h>
#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/search/function_search_leaf.h"
#include "exprs/function/search/search_lowering.h"
#include "exprs/function/search/variant_inverted_index_search.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query/engine/all_query/all_query.h"
#include "storage/index/inverted/query/engine/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query/engine/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query/engine/boolean_query/operator.h"
#include "storage/index/inverted/query/engine/collect/doc_set_collector.h"
#include "storage/index/inverted/query/engine/collect/top_k_collector.h"
#include "storage/index/inverted/query/engine/null_masked_query.h"
#include "storage/index/inverted/query/exec/term_pattern.h"
#include "storage/index/inverted/query/logical/node.h"
#include "storage/index/inverted/reader/collection_statistics.h"
#include "storage/olap_common.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "util/thrift_util.h"

namespace doris {

namespace query_v2 = segment_v2::inverted_index::query_v2;

// Serializes the whole TSearchParam, so every field of the DSL takes part in the signature, and
// adds the limit on the terms PREFIX, WILDCARD and REGEXP clauses expand to.
std::string FunctionSearch::dsl_cache_signature(const TSearchParam& search_param,
                                                const IndexQueryContext* context) {
    ThriftSerializer ser(false, 1024);
    TSearchParam copy = search_param;
    std::string sig;
    auto st = ser.serialize(&copy, &sig);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "build_dsl_signature: Thrift serialization failed: " << st.to_string()
                     << ", caching disabled for this query";
        return "";
    }
    sig += "|max_expansions=";
    sig += std::to_string(
            index_query::max_expansions(context != nullptr ? context->runtime_state : nullptr));
    return sig;
}

// Extract segment path prefix from the first available inverted index iterator.
// All fields in the same segment share the same path prefix.
static std::string extract_segment_prefix(
        const std::unordered_map<std::string, IndexIterator*>& iterators) {
    for (const auto& [field_name, iter] : iterators) {
        auto* inv_iter = dynamic_cast<InvertedIndexIterator*>(iter);
        if (!inv_iter) continue;
        // Try fulltext reader first, then string type
        for (auto type :
             {InvertedIndexReaderType::FULLTEXT, InvertedIndexReaderType::STRING_TYPE}) {
            IndexReaderType reader_type = type;
            auto reader = inv_iter->get_reader(reader_type);
            if (!reader) continue;
            auto inv_reader = std::dynamic_pointer_cast<InvertedIndexReader>(reader);
            if (!inv_reader) continue;
            auto file_reader = inv_reader->get_index_file_reader();
            if (!file_reader) continue;
            return file_reader->get_index_path_prefix();
        }
    }
    VLOG_DEBUG << "extract_segment_prefix: no suitable inverted index reader found across "
               << iterators.size() << " iterators, caching disabled for this query";
    return "";
}

namespace {

// Keep two-valued SEARCH results separate from older SQL UNKNOWN cache entries.
InvertedIndexQueryCache::CacheKey dsl_cache_key(const std::string& seg_prefix,
                                                const std::string& signature) {
    return {seg_prefix, "__search_dsl_2vl__", InvertedIndexQueryType::SEARCH_DSL_QUERY, signature};
}

bool lookup_dsl_result(InvertedIndexQueryCache* cache, const std::string& seg_prefix,
                       const std::string& signature, InvertedIndexResultBitmap* result) {
    InvertedIndexQueryCacheHandle rows_handle;
    if (!cache->lookup(dsl_cache_key(seg_prefix, signature), &rows_handle)) {
        return false;
    }
    auto rows = rows_handle.get_bitmap();
    if (rows == nullptr) {
        return false;
    }
    // SQL bitmap composition can mutate its input. The cache owns an independent copy.
    *result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(*rows),
                                        std::make_shared<roaring::Roaring>());
    return true;
}

void insert_dsl_result(InvertedIndexQueryCache* cache, const std::string& seg_prefix,
                       const std::string& signature, const InvertedIndexResultBitmap& result) {
    InvertedIndexQueryCacheHandle rows_handle;
    cache->insert(dsl_cache_key(seg_prefix, signature),
                  std::make_shared<roaring::Roaring>(*result.get_data_bitmap()), &rows_handle);
}

bool is_nested_group_search_supported() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

namespace logical = index_query::logical;

// Lowering's view of the resolver: what index a field binds to and how that
// index analyzes values.
class SearchFieldCatalog final : public logical::FieldCatalog {
public:
    SearchFieldCatalog(FieldReaderResolver& resolver, std::shared_ptr<IndexQueryContext> context)
            : _resolver(resolver), _context(std::move(context)) {}

    Status resolve(const std::string& field, InvertedIndexQueryType query_type,
                   logical::FieldProps* out) override {
        FieldReaderBinding binding;
        RETURN_IF_ERROR(_resolver.resolve(field, query_type, &binding));
        if (!binding.is_bound()) {
            LOG(INFO) << "search: No inverted index for field '" << field
                      << "' in this segment, query_type=" << static_cast<int>(query_type)
                      << ", returning non-matching bitmap";
            *out = logical::FieldProps {};
            return Status::OK();
        }
        const bool scalar = binding.inverted_reader != nullptr &&
                            binding.inverted_reader->type() == InvertedIndexReaderType::BKD;
        const bool analyzed = !scalar && inverted_index::InvertedIndexAnalyzer::should_analyzer(
                                                 binding.index_properties);
        *out = logical::FieldProps {.bound = true,
                                    .direct_index = scalar,
                                    .analyzed = analyzed,
                                    .binding = binding.binding_key};
        return Status::OK();
    }

    Status analyze(const logical::FieldProps& props, const std::string& value,
                   std::vector<logical::Token>* out) override {
        const FieldReaderBinding* binding = _resolver.find_binding(props.binding);
        if (binding == nullptr) {
            return Status::InternalError("search: no binding '{}' to analyze with", props.binding);
        }
        int64_t unused_timer = 0;
        SCOPED_RAW_TIMER(_context != nullptr && _context->stats != nullptr
                                 ? &_context->stats->inverted_index_analyzer_timer
                                 : &unused_timer);
        InvertedIndexAnalyzerCtxSPtr analyzer_ctx;
        RETURN_IF_ERROR(_resolver.analyzer_context_for(props.binding, &analyzer_ctx));
        return inverted_index::InvertedIndexAnalyzer::analyze(value, analyzer_ctx.get(),
                                                              binding->index_properties, out);
    }

    Status normalize(const logical::FieldProps& props, const std::string& value,
                     std::string* out) override {
        const FieldReaderBinding* binding = _resolver.find_binding(props.binding);
        if (binding == nullptr) {
            return Status::InternalError("search: no binding '{}' to normalize with",
                                         props.binding);
        }
        try {
            *out = inverted_index::InvertedIndexAnalyzer::normalize(value,
                                                                    binding->index_properties);
        } catch (const CLuceneError& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "search: normalizing '{}' failed: {}", value, e.what());
        } catch (const Exception& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "search: normalizing '{}' failed: {}", value, e.what());
        }
        return Status::OK();
    }

private:
    FieldReaderResolver& _resolver;
    std::shared_ptr<IndexQueryContext> _context;
};

query_v2::Occur to_query_occur(logical::Occur occur) {
    switch (occur) {
    case logical::Occur::kShould:
        return query_v2::Occur::SHOULD;
    case logical::Occur::kMustNot:
        return query_v2::Occur::MUST_NOT;
    case logical::Occur::kMust:
    default:
        return query_v2::Occur::MUST;
    }
}

Status compile_node(const logical::Node& node, const SearchLeafContext& ctx,
                    FieldReaderResolver& resolver, query_v2::QueryPtr* out,
                    std::string* binding_key);

// Physical NULL postings can overlap a NULL ARRAY's leftover values. Exclude them
// before Boolean composition without introducing SQL UNKNOWN or changing scores.
Status mask_null_rows(const FieldReaderResolver& resolver, const std::string& field,
                      query_v2::QueryPtr* query) {
    IndexIterator* iterator = resolver.get_iterator(field);
    DORIS_CHECK(iterator != nullptr);
    const bool has_null = DORIS_TRY(iterator->has_null());
    if (!has_null) {
        return Status::OK();
    }
    InvertedIndexQueryCacheHandle handle;
    RETURN_IF_ERROR(iterator->read_null_bitmap(&handle));
    auto nulls = handle.get_bitmap();
    DORIS_CHECK(nulls != nullptr);
    if (!nulls->isEmpty()) {
        *query = std::make_shared<query_v2::NullMaskedQuery>(std::move(*query), std::move(nulls),
                                                             /*nulls_are_false=*/true);
    }
    return Status::OK();
}

using Clauses = std::vector<std::pair<logical::Occur, logical::NodePtr>>;

bool has_required_clause(const Clauses& clauses) {
    return std::ranges::any_of(
            clauses, [](const auto& clause) { return clause.first == logical::Occur::kMust; });
}

// Without scoring, optional clauses beside a required one do not change the match set.
Clauses matching_clauses(const logical::Bool& boolean, bool scoring) {
    if (scoring || boolean.op != logical::BoolOp::kOccur || boolean.min_should_match != 0 ||
        !has_required_clause(boolean.clauses)) {
        return boolean.clauses;
    }
    Clauses kept;
    std::ranges::copy_if(boolean.clauses, std::back_inserter(kept), [](const auto& clause) {
        return clause.first != logical::Occur::kShould;
    });
    return kept;
}

// AND, OR and NOT ignore the per-clause occur; OCCUR keeps it and the threshold. A Boolean
// left with one clause after dropping clauses that only score is that clause, with its binding.
Status compile_bool(const logical::Bool& boolean, const SearchLeafContext& ctx,
                    FieldReaderResolver& resolver, query_v2::QueryPtr* out,
                    std::string* binding_key) {
    const Clauses clauses = matching_clauses(boolean, ctx.scoring);
    if (clauses.size() == 1 && boolean.clauses.size() > 1) {
        return compile_node(*clauses.front().second, ctx, resolver, out, binding_key);
    }
    std::vector<query_v2::QueryPtr> queries(clauses.size());
    std::vector<std::string> keys(clauses.size());
    for (size_t i = 0; i < clauses.size(); ++i) {
        RETURN_IF_ERROR(compile_node(*clauses[i].second, ctx, resolver, &queries[i], &keys[i]));
    }
    if (boolean.op == logical::BoolOp::kOccur) {
        auto builder = query_v2::create_occur_boolean_query_builder();
        builder->set_minimum_number_should_match(boolean.min_should_match);
        for (size_t i = 0; i < clauses.size(); ++i) {
            builder->add(queries[i], to_query_occur(clauses[i].first), std::move(keys[i]));
        }
        *out = builder->build();
        return Status::OK();
    }
    query_v2::OperatorType op = query_v2::OperatorType::OP_AND;
    if (boolean.op == logical::BoolOp::kOr) {
        op = query_v2::OperatorType::OP_OR;
    } else if (boolean.op == logical::BoolOp::kNot) {
        op = query_v2::OperatorType::OP_NOT;
    }
    auto builder = query_v2::create_operator_boolean_query_builder(op);
    for (size_t i = 0; i < clauses.size(); ++i) {
        builder->add(queries[i], std::move(keys[i]));
    }
    *out = builder->build();
    return Status::OK();
}

// A threshold counts the set's terms above the field's compiler, so every format
// answers it the same way: each term is its own leaf on the field.
Status compile_term_threshold(const logical::TermSet& set, const SearchLeafContext& ctx,
                              SearchLeafCompiler& compiler, query_v2::QueryPtr* out) {
    auto builder = query_v2::create_occur_boolean_query_builder();
    builder->set_minimum_number_should_match(set.min_should_match);
    const query_v2::Occur occur = set.require_all ? query_v2::Occur::MUST : query_v2::Occur::SHOULD;
    for (const auto& term : set.terms) {
        const auto leaf = logical::make_node(logical::TermSet {
                .field = set.field, .terms = {term}, .require_all = false, .min_should_match = 0});
        query_v2::QueryPtr term_query;
        RETURN_IF_ERROR(compiler.compile(*leaf, ctx, &term_query));
        builder->add(term_query, occur, set.field.binding);
    }
    *out = builder->build();
    return Status::OK();
}

// A leaf goes to the compiler of the index it was bound to; its binding key and
// the leaf mapper follow it. A term threshold stays one leaf for the mapper.
Status compile_node(const logical::Node& node, const SearchLeafContext& ctx,
                    FieldReaderResolver& resolver, query_v2::QueryPtr* out,
                    std::string* binding_key) {
    *out = nullptr;
    if (binding_key != nullptr) {
        binding_key->clear();
    }
    if (node.as<logical::All>() != nullptr) {
        *out = std::make_shared<query_v2::AllQuery>();
        return Status::OK();
    }
    if (const auto* boolean = node.as<logical::Bool>()) {
        return compile_bool(*boolean, ctx, resolver, out, binding_key);
    }
    const logical::FieldRef* field = node.field();
    DCHECK(field != nullptr);
    if (binding_key != nullptr) {
        *binding_key = field->binding;
    }
    if (node.as<logical::Empty>() != nullptr) {
        *out = std::make_shared<query_v2::BitSetQuery>(std::make_shared<roaring::Roaring>());
        return resolver.map_leaf_query(field->name, out);
    }
    const FieldReaderBinding* binding = resolver.find_binding(field->binding);
    if (binding == nullptr || binding->leaf_compiler == nullptr) {
        return Status::InternalError("search: field '{}' has no compiler for binding '{}'",
                                     field->name, field->binding);
    }
    SearchLeafContext leaf_ctx = ctx;
    RETURN_IF_ERROR(resolver.leaf_num_rows(field->name, &leaf_ctx.num_rows));
    const auto* set = node.as<logical::TermSet>();
    const bool threshold = set != nullptr && set->min_should_match > 0;
    if (node.as<logical::Exists>() != nullptr) {
        // Exists also works for BKD and uses the NestedGroup leaf's element domain.
        auto matches = std::make_shared<roaring::Roaring>();
        matches->addRange(0, leaf_ctx.num_rows);
        *out = std::make_shared<query_v2::BitSetQuery>(std::move(matches));
    } else if (threshold) {
        RETURN_IF_ERROR(compile_term_threshold(*set, leaf_ctx, *binding->leaf_compiler, out));
    } else {
        RETURN_IF_ERROR(binding->leaf_compiler->compile(node, leaf_ctx, out));
    }
    RETURN_IF_ERROR(mask_null_rows(resolver, field->name, out));
    return resolver.map_leaf_query(field->name, out);
}

} // namespace

Status FunctionSearch::execute_impl(FunctionContext* /*context*/, Block& /*block*/,
                                    const ColumnNumbers& /*arguments*/, uint32_t /*result*/,
                                    size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

// Enhanced implementation: Handle new parameter structure (DSL + SlotReferences)
Status FunctionSearch::evaluate_inverted_index(
        const ColumnsWithTypeAndName& arguments,
        const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<IndexIterator*> iterators, uint32_t num_rows,
        const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/,
        InvertedIndexResultBitmap& bitmap_result) const {
    return Status::OK();
}

Status FunctionSearch::evaluate_inverted_index_with_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
        std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
        InvertedIndexResultBitmap& bitmap_result, bool enable_cache) const {
    return evaluate_inverted_index_with_search_param(search_param, data_type_with_names,
                                                     std::move(iterators), num_rows, bitmap_result,
                                                     enable_cache, nullptr, nullptr);
}

// A lazy leaf reads its index while the result is listed; what it throws is returned like the
// errors the evaluation returns.
Status FunctionSearch::evaluate_inverted_index_with_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
        std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
        InvertedIndexResultBitmap& bitmap_result, bool enable_cache,
        const IndexExecContext* index_exec_ctx, const TabletColumn* nested_column,
        const std::shared_ptr<IndexQueryContext>& index_query_context) const {
    // VSearchExpr enters here directly, outside IFunction::execute() and its exception boundary,
    // so this is where a failure inside the search, such as an analyzer whose first token
    // stream throws, has to become a Status.
    try {
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_evaluate_search_param(
                search_param, data_type_with_names, std::move(iterators), num_rows, bitmap_result,
                enable_cache, index_exec_ctx, nested_column, index_query_context));
    } catch (const CLuceneError& e) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>("search failed: {}",
                                                                       e.what());
    } catch (const Exception& e) {
        return e.to_status();
    }
    return Status::OK();
}

// The evaluation keeps its result cache, the tree's compile and the collection in one path.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
Status FunctionSearch::_evaluate_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
        std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
        InvertedIndexResultBitmap& bitmap_result, bool enable_cache,
        const IndexExecContext* index_exec_ctx, const TabletColumn* nested_column,
        const std::shared_ptr<IndexQueryContext>& index_query_context) const {
    const bool is_nested_query = search_param.root.clause_type == "NESTED";
    if (is_nested_query && !is_nested_group_search_supported()) {
        return Status::NotSupported(
                "NESTED query requires NestedGroup support, which is unavailable in this build");
    }

    // Track overall query time (equivalent to inverted_index_query_timer in MATCH path).
    // Must be declared before the DSL cache lookup so that cache-hit fast paths are
    // also covered by the timer.
    int64_t query_timer_dummy = 0;
    OlapReaderStatistics* outer_stats = index_query_context ? index_query_context->stats : nullptr;
    SCOPED_RAW_TIMER(outer_stats ? &outer_stats->inverted_index_query_timer : &query_timer_dummy);

    // DSL result cache only stores the two-valued match bitmap. It does not store BM25 scores,
    // so score() queries must execute scorers to populate CollectionSimilarity.
    const bool enable_scoring =
            index_query_context != nullptr && index_query_context->collection_similarity != nullptr;
    auto* dsl_cache =
            enable_cache && !enable_scoring ? InvertedIndexQueryCache::instance() : nullptr;
    std::string seg_prefix;
    std::string dsl_sig;
    bool cache_usable = false;
    if (dsl_cache) {
        seg_prefix = extract_segment_prefix(iterators);
        dsl_sig = dsl_cache_signature(search_param, index_query_context.get());
        if (!seg_prefix.empty() && !dsl_sig.empty()) {
            cache_usable = true;
            bool dsl_hit = false;
            {
                int64_t lookup_dummy = 0;
                SCOPED_RAW_TIMER(outer_stats ? &outer_stats->inverted_index_lookup_timer
                                             : &lookup_dummy);
                dsl_hit = lookup_dsl_result(dsl_cache, seg_prefix, dsl_sig, &bitmap_result);
            }
            if (dsl_hit) {
                if (outer_stats) {
                    outer_stats->inverted_index_query_cache_hit++;
                }
                return Status::OK();
            }
            if (outer_stats) {
                outer_stats->inverted_index_query_cache_miss++;
            }
        }
    }

    std::shared_ptr<IndexQueryContext> context;
    if (index_query_context) {
        context = index_query_context;
    } else {
        context = std::make_shared<IndexQueryContext>();
        context->collection_statistics = std::make_shared<CollectionStatistics>();
        context->collection_similarity = std::make_shared<CollectionSimilarity>();
    }

    const auto* effective_data_type_with_names = &data_type_with_names;

    // Pass field_bindings to resolver for variant subcolumn detection
    FieldReaderResolver resolver(*effective_data_type_with_names, iterators, context,
                                 search_param.field_bindings);

    if (is_nested_query) {
        std::shared_ptr<roaring::Roaring> row_bitmap;
        VariantNestedSearchEvaluator nested_evaluator(*this);
        RETURN_IF_ERROR(nested_evaluator.evaluate(search_param, search_param.root, context,
                                                  resolver, num_rows, index_exec_ctx, nested_column,
                                                  row_bitmap));
        bitmap_result = InvertedIndexResultBitmap(std::move(row_bitmap),
                                                  std::make_shared<roaring::Roaring>());
        bitmap_result.mask_out_null();
        return Status::OK();
    }

    // Extract default_operator from TSearchParam (default: "or")
    std::string default_operator = "or";
    if (search_param.__isset.default_operator && !search_param.default_operator.empty()) {
        default_operator = search_param.default_operator;
    }
    // Extract minimum_should_match from TSearchParam (-1 means not set)
    int32_t minimum_should_match = -1;
    if (search_param.__isset.minimum_should_match) {
        minimum_should_match = search_param.minimum_should_match;
    }

    auto* stats = context->stats;
    int64_t dummy_timer = 0;
    SCOPED_RAW_TIMER(stats ? &stats->inverted_index_searcher_search_timer : &dummy_timer);

    query_v2::QueryPtr root_query;
    std::string root_binding_key;
    {
        int64_t init_dummy = 0;
        SCOPED_RAW_TIMER(stats ? &stats->inverted_index_searcher_search_init_timer : &init_dummy);
        RETURN_IF_ERROR(build_query_recursive(search_param.root, context, resolver, &root_query,
                                              &root_binding_key, default_operator,
                                              minimum_should_match, num_rows, enable_scoring));
    }
    if (root_query == nullptr) {
        LOG(INFO) << "search: Query tree resolved to empty query, dsl:"
                  << search_param.original_dsl;
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    SearchNullBitmapResolver null_resolver;
    query_v2::QueryExecutionContext exec_ctx =
            build_variant_search_query_execution_context(num_rows, resolver, &null_resolver);

    bool is_asc = false;
    size_t top_k = 0;
    if (index_query_context) {
        is_asc = index_query_context->is_asc;
        top_k = index_query_context->query_limit;
    }

    auto weight = root_query->weight(enable_scoring);
    if (!weight) {
        LOG(WARNING) << "search: Failed to build query weight";
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
    // Keyword readers can lack BM25 length statistics. Preserve their original
    // scores and all matches for the SQL sort when pruning cannot order them.
    const bool can_prune_scores = std::none_of(
            resolver.binding_cache().begin(), resolver.binding_cache().end(),
            [](const auto& entry) {
                const auto& binding = entry.second;
                DORIS_CHECK(binding.inverted_reader != nullptr);
                return binding.inverted_reader->type() == InvertedIndexReaderType::STRING_TYPE;
            });
    const bool top_k_path = enable_scoring && !is_asc && top_k > 0 && top_k < num_rows &&
                            top_k <= query_v2::TopKCollector::kMaxK && can_prune_scores;
    exec_ctx.top_k = top_k_path;
    {
        int64_t exec_dummy = 0;
        SCOPED_RAW_TIMER(stats ? &stats->inverted_index_searcher_search_exec_timer : &exec_dummy);
        if (top_k_path) {
            bool use_wand = index_query_context->runtime_state != nullptr &&
                            index_query_context->runtime_state->query_options()
                                    .enable_inverted_index_wand_query;
            query_v2::collect_multi_segment_top_k(weight, exec_ctx, root_binding_key, top_k,
                                                  roaring,
                                                  index_query_context->collection_similarity,
                                                  use_wand, index_query_context->delete_bitmap);
        } else {
            query_v2::collect_multi_segment_doc_set(
                    weight, exec_ctx, root_binding_key, roaring,
                    index_query_context ? index_query_context->collection_similarity : nullptr,
                    enable_scoring);
        }
    }

    VLOG_DEBUG << "search: Query completed, matched " << roaring->cardinality() << " documents";

    // SEARCH never returns UNKNOWN; outer SQL composes this non-nullable match set.
    bitmap_result =
            InvertedIndexResultBitmap(std::move(roaring), std::make_shared<roaring::Roaring>());

    if (dsl_cache && cache_usable) {
        insert_dsl_result(dsl_cache, seg_prefix, dsl_sig, bitmap_result);
    }

    return Status::OK();
}

// Aligned with FE QsClauseType enum - uses enum.name() as clause_type
Status FunctionSearch::build_query_recursive(
        const TSearchClause& clause, const std::shared_ptr<IndexQueryContext>& context,
        FieldReaderResolver& resolver, inverted_index::query_v2::QueryPtr* out,
        std::string* binding_key, const std::string& default_operator, int32_t minimum_should_match,
        uint32_t num_rows, bool scoring) const {
    DCHECK(out != nullptr);
    *out = nullptr;
    if (binding_key != nullptr) {
        binding_key->clear();
    }
    SearchFieldCatalog catalog(resolver, context);
    logical::NodePtr root;
    RETURN_IF_ERROR(logical::lower_search_clause(
            clause,
            {.default_operator = default_operator, .minimum_should_match = minimum_should_match},
            catalog, &root));
    return compile_node(
            *root, SearchLeafContext {.context = context, .num_rows = num_rows, .scoring = scoring},
            resolver, out, binding_key);
}

void register_function_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSearch>();
}

} // namespace doris
