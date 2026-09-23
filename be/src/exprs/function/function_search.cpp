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

#include "exprs/function/function_search.h"

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
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/columns_with_type_and_name.h"
#include "exprs/function/search_leaf_compiler.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function/variant_inverted_index_search.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/query/logical/node.h"
#include "storage/index/query/logical/search_lowering.h"
#include "storage/olap_common.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "util/thrift_util.h"

namespace doris {

// Build canonical DSL signature for cache key.
// Serializes the entire TSearchParam via Thrift binary protocol so that
// every field (DSL, AST root, field bindings, default_operator,
// minimum_should_match, etc.) is included automatically.
static std::string build_dsl_signature(const TSearchParam& param) {
    ThriftSerializer ser(false, 1024);
    TSearchParam copy = param;
    std::string sig;
    auto st = ser.serialize(&copy, &sig);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "build_dsl_signature: Thrift serialization failed: " << st.to_string()
                     << ", caching disabled for this query";
        return "";
    }
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

static void collect_referenced_fields(const TSearchClause& clause,
                                      std::unordered_set<std::string>* fields) {
    DORIS_CHECK(fields != nullptr);
    if (clause.__isset.field_name && !clause.field_name.empty()) {
        fields->insert(clause.field_name);
    }
    for (const auto& child : clause.children) {
        collect_referenced_fields(child, fields);
    }
}

static bool referenced_fields_contain_snii_reader(
        const TSearchClause& root,
        const std::unordered_map<std::string, IndexIterator*>& iterators) {
    std::unordered_set<std::string> referenced_fields;
    collect_referenced_fields(root, &referenced_fields);
    for (const auto& field_name : referenced_fields) {
        auto iterator_it = iterators.find(field_name);
        if (iterator_it == iterators.end()) {
            continue;
        }
        auto* inv_iter = dynamic_cast<InvertedIndexIterator*>(iterator_it->second);
        if (inv_iter == nullptr) {
            continue;
        }
        for (auto type : {InvertedIndexReaderType::FULLTEXT, InvertedIndexReaderType::STRING_TYPE,
                          InvertedIndexReaderType::BKD}) {
            IndexReaderType reader_type = type;
            auto reader = inv_iter->get_reader(reader_type);
            if (reader == nullptr) {
                continue;
            }
            auto inv_reader = std::dynamic_pointer_cast<InvertedIndexReader>(reader);
            DORIS_CHECK(inv_reader != nullptr);
            auto file_reader = inv_reader->get_index_file_reader();
            DORIS_CHECK(file_reader != nullptr);
            if (file_reader->get_storage_format() == InvertedIndexStorageFormatPB::SNII) {
                return true;
            }
        }
    }
    return false;
}

namespace {

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
                      << ", returning UNKNOWN bitmap";
            *out = logical::FieldProps {};
            return Status::OK();
        }
        const bool scalar = binding.inverted_reader != nullptr &&
                            binding.inverted_reader->type() == InvertedIndexReaderType::BKD;
        const bool analyzed = !scalar && inverted_index::InvertedIndexAnalyzer::should_analyzer(
                                                 binding.index_properties);
        *out = logical::FieldProps {
                .bound = true,
                .direct_index = scalar,
                .analyzed = analyzed,
                .lowercase_patterns =
                        analyzed && get_parser_lowercase_from_properties(
                                            binding.index_properties) == INVERTED_INDEX_PARSER_TRUE,
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
        try {
            InvertedIndexAnalyzerCtxSPtr analyzer_ctx;
            RETURN_IF_ERROR(_resolver.analyzer_context_for(props.binding, &analyzer_ctx));
            auto analyzer = analyzer_ctx != nullptr ? analyzer_ctx->get_analyzer() : nullptr;
            if (analyzer_ctx != nullptr && analyzer != nullptr) {
                auto reader = inverted_index::InvertedIndexAnalyzer::create_reader(
                        analyzer_ctx->char_filter_map);
                reader->init(value.data(), static_cast<int32_t>(value.size()), true);
                *out = inverted_index::InvertedIndexAnalyzer::get_analyse_result(reader,
                                                                                 analyzer.get());
            } else {
                *out = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                        value, binding->index_properties);
            }
        } catch (const CLuceneError& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "search: analyzing '{}' failed: {}", value, e.what());
        } catch (const Exception& e) {
            return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                    "search: analyzing '{}' failed: {}", value, e.what());
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

using Clauses = std::vector<std::pair<logical::Occur, logical::NodePtr>>;

// How a clause joins term sets of its field: into an all-of set (true), an any-of set (false),
// or not at all. Optional clauses join only when they decide the match: a threshold of one,
// or no required clause beside them. Otherwise they only add to the score, and a threshold of
// two or more counts them one by one.
std::optional<bool> joined_kind(const logical::Bool& boolean, logical::Occur occur,
                                bool has_required) {
    switch (boolean.op) {
    case logical::BoolOp::kAnd:
        return true;
    case logical::BoolOp::kOr:
        return false;
    case logical::BoolOp::kOccur:
        if (occur == logical::Occur::kMust) {
            return true;
        }
        if (occur == logical::Occur::kShould &&
            (boolean.min_should_match == 1 || (boolean.min_should_match == 0 && !has_required))) {
            return false;
        }
        return std::nullopt;
    case logical::BoolOp::kNot:
        return std::nullopt;
    }
    return std::nullopt;
}

// A term set that can join others of its kind: no threshold, one term or a set of that kind,
// and a field whose compiler takes joined sets.
const logical::TermSet* joinable_set(const logical::Node& node, bool require_all,
                                     const FieldReaderResolver& resolver) {
    const auto* set = node.as<logical::TermSet>();
    if (set == nullptr || set->min_should_match > 0 ||
        (set->terms.size() > 1 && set->require_all != require_all)) {
        return nullptr;
    }
    const FieldReaderBinding* binding = resolver.find_binding(set->field.binding);
    if (binding == nullptr || binding->leaf_compiler == nullptr ||
        !binding->leaf_compiler->joins_term_sets()) {
        return nullptr;
    }
    return set;
}

bool has_repeated_term(std::vector<std::string> terms) {
    std::ranges::sort(terms);
    return std::ranges::adjacent_find(terms) != terms.end();
}

// Term sets of one field under an AND, an OR, or the required or optional clauses of an occur
// query join into one set at the place of the first. A nested-document mapper maps leaves one
// by one, so nothing joins under it; nor does a repeated term, which scores once per clause.
Clauses join_term_sets(const logical::Bool& boolean, const FieldReaderResolver& resolver) {
    if (resolver.maps_leaf_queries()) {
        return boolean.clauses;
    }
    struct Group {
        logical::Occur occur;
        bool require_all;
        const logical::TermSet* first;
        std::vector<std::string> terms;
        size_t members = 0;
        bool joins = false;
    };
    const bool has_required = std::ranges::any_of(boolean.clauses, [](const auto& clause) {
        return clause.first == logical::Occur::kMust;
    });
    std::vector<Group> groups;
    std::vector<std::optional<size_t>> group_of(boolean.clauses.size());
    for (size_t i = 0; i < boolean.clauses.size(); ++i) {
        const auto& [occur, child] = boolean.clauses[i];
        const std::optional<bool> kind = joined_kind(boolean, occur, has_required);
        const logical::TermSet* set =
                kind.has_value() ? joinable_set(*child, *kind, resolver) : nullptr;
        if (set == nullptr) {
            continue;
        }
        auto group = std::ranges::find_if(groups, [&](const Group& candidate) {
            return candidate.occur == occur && candidate.require_all == *kind &&
                   candidate.first->field.binding == set->field.binding;
        });
        if (group == groups.end()) {
            groups.push_back({.occur = occur,
                              .require_all = *kind,
                              .first = set,
                              .terms = {},
                              .members = 0,
                              .joins = false});
            group = std::prev(groups.end());
        }
        group->terms.insert(group->terms.end(), set->terms.begin(), set->terms.end());
        ++group->members;
        group_of[i] = static_cast<size_t>(group - groups.begin());
    }
    for (auto& group : groups) {
        group.joins = group.members > 1 && !has_repeated_term(group.terms);
    }
    Clauses planned;
    std::vector<bool> placed(groups.size(), false);
    for (size_t i = 0; i < boolean.clauses.size(); ++i) {
        if (!group_of[i].has_value() || !groups[*group_of[i]].joins) {
            planned.push_back(boolean.clauses[i]);
            continue;
        }
        if (placed[*group_of[i]]) {
            continue;
        }
        placed[*group_of[i]] = true;
        const Group& group = groups[*group_of[i]];
        planned.emplace_back(group.occur,
                             logical::make_node(logical::TermSet {.field = group.first->field,
                                                                  .terms = group.terms,
                                                                  .require_all = group.require_all,
                                                                  .min_should_match = 0}));
    }
    return planned;
}

// AND, OR and NOT ignore the per-clause occur; OCCUR keeps it and the threshold. A Boolean
// whose clauses all joined into one term set is that set.
Status compile_bool(const logical::Bool& boolean, const SearchLeafContext& ctx,
                    FieldReaderResolver& resolver, query_v2::QueryPtr* out) {
    const Clauses clauses = join_term_sets(boolean, resolver);
    if (clauses.size() == 1 && boolean.clauses.size() > 1) {
        return compile_node(*clauses.front().second, ctx, resolver, out, nullptr);
    }
    if (boolean.op == logical::BoolOp::kOccur) {
        auto builder = query_v2::create_occur_boolean_query_builder();
        builder->set_minimum_number_should_match(boolean.min_should_match);
        for (const auto& [occur, child] : clauses) {
            query_v2::QueryPtr child_query;
            std::string child_binding_key;
            RETURN_IF_ERROR(compile_node(*child, ctx, resolver, &child_query, &child_binding_key));
            builder->add(child_query, to_query_occur(occur), std::move(child_binding_key));
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
    for (const auto& [occur, child] : clauses) {
        query_v2::QueryPtr child_query;
        std::string child_binding_key;
        RETURN_IF_ERROR(compile_node(*child, ctx, resolver, &child_query, &child_binding_key));
        builder->add(child_query, std::move(child_binding_key));
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
        return compile_bool(*boolean, ctx, resolver, out);
    }
    const logical::FieldRef* field = node.field();
    DCHECK(field != nullptr);
    if (binding_key != nullptr) {
        *binding_key = field->binding;
    }
    if (node.as<logical::Unknown>() != nullptr) {
        *out = make_unknown_leaf_query(ctx.num_rows);
        return resolver.map_leaf_query(field->name, out);
    }
    const FieldReaderBinding* binding = resolver.find_binding(field->binding);
    if (binding == nullptr || binding->leaf_compiler == nullptr) {
        return Status::InternalError("search: field '{}' has no compiler for binding '{}'",
                                     field->name, field->binding);
    }
    const auto* set = node.as<logical::TermSet>();
    if (set != nullptr && set->min_should_match > 0) {
        RETURN_IF_ERROR(compile_term_threshold(*set, ctx, *binding->leaf_compiler, out));
    } else {
        RETURN_IF_ERROR(binding->leaf_compiler->compile(node, ctx, out));
    }
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
    static const std::unordered_map<std::string, int> empty_field_to_column_id;
    return evaluate_inverted_index_with_search_param(
            search_param, data_type_with_names, std::move(iterators), num_rows, bitmap_result,
            enable_cache, nullptr, empty_field_to_column_id);
}

Status FunctionSearch::evaluate_inverted_index_with_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
        std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
        InvertedIndexResultBitmap& bitmap_result, bool enable_cache,
        const IndexExecContext* index_exec_ctx,
        const std::unordered_map<std::string, int>& field_name_to_column_id,
        const std::shared_ptr<IndexQueryContext>& index_query_context) const {
    const bool is_nested_query = search_param.root.clause_type == "NESTED";
    if (is_nested_query && !is_nested_group_search_supported()) {
        return Status::NotSupported(
                "NESTED query requires NestedGroup support, which is unavailable in this build");
    }

    if (!is_nested_query && (iterators.empty() || data_type_with_names.empty())) {
        LOG(INFO) << "No indexed columns or iterators available, returning empty result, dsl:"
                  << search_param.original_dsl;
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    // Track overall query time (equivalent to inverted_index_query_timer in MATCH path).
    // Must be declared before the DSL cache lookup so that cache-hit fast paths are
    // also covered by the timer.
    int64_t query_timer_dummy = 0;
    OlapReaderStatistics* outer_stats = index_query_context ? index_query_context->stats : nullptr;
    SCOPED_RAW_TIMER(outer_stats ? &outer_stats->inverted_index_query_timer : &query_timer_dummy);

    // DSL result cache only stores bitmap/null bitmap. It does not store BM25 scores,
    // so score() queries must execute scorers to populate CollectionSimilarity.
    const bool enable_scoring =
            index_query_context != nullptr && index_query_context->collection_similarity != nullptr;
    // Also bypass the DSL cache when any referenced field is served by an SNII reader.
    auto* dsl_cache =
            enable_cache && !enable_scoring &&
                            !referenced_fields_contain_snii_reader(search_param.root, iterators)
                    ? InvertedIndexQueryCache::instance()
                    : nullptr;
    std::string seg_prefix;
    std::string dsl_sig;
    InvertedIndexQueryCache::CacheKey dsl_cache_key;
    bool cache_usable = false;
    if (dsl_cache) {
        seg_prefix = extract_segment_prefix(iterators);
        dsl_sig = build_dsl_signature(search_param);
        if (!seg_prefix.empty() && !dsl_sig.empty()) {
            dsl_cache_key = InvertedIndexQueryCache::CacheKey {
                    seg_prefix, "__search_dsl__", InvertedIndexQueryType::SEARCH_DSL_QUERY,
                    dsl_sig};
            cache_usable = true;
            InvertedIndexQueryCacheHandle dsl_cache_handle;
            bool dsl_hit = false;
            {
                int64_t lookup_dummy = 0;
                SCOPED_RAW_TIMER(outer_stats ? &outer_stats->inverted_index_lookup_timer
                                             : &lookup_dummy);
                dsl_hit = dsl_cache->lookup(dsl_cache_key, &dsl_cache_handle);
            }
            if (dsl_hit) {
                auto cached_bitmap = dsl_cache_handle.get_bitmap();
                if (cached_bitmap) {
                    if (outer_stats) {
                        outer_stats->inverted_index_query_cache_hit++;
                    }
                    // Also retrieve cached null bitmap for three-valued SQL logic
                    // (needed by compound operators NOT, OR, AND in VCompoundPred)
                    auto null_cache_key = InvertedIndexQueryCache::CacheKey {
                            seg_prefix, "__search_dsl__", InvertedIndexQueryType::SEARCH_DSL_QUERY,
                            dsl_sig + "__null"};
                    InvertedIndexQueryCacheHandle null_cache_handle;
                    std::shared_ptr<roaring::Roaring> null_bitmap;
                    if (dsl_cache->lookup(null_cache_key, &null_cache_handle)) {
                        null_bitmap = null_cache_handle.get_bitmap();
                    }
                    if (!null_bitmap) {
                        null_bitmap = std::make_shared<roaring::Roaring>();
                    }
                    bitmap_result =
                            InvertedIndexResultBitmap(cached_bitmap, std::move(null_bitmap));
                    return Status::OK();
                }
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
                                                  resolver, num_rows, index_exec_ctx,
                                                  field_name_to_column_id, row_bitmap));
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
                                              minimum_should_match, num_rows));
    }
    if (root_query == nullptr) {
        LOG(INFO) << "search: Query tree resolved to empty query, dsl:"
                  << search_param.original_dsl;
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    VariantSearchNullBitmapAdapter null_resolver(resolver);
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
    {
        int64_t exec_dummy = 0;
        SCOPED_RAW_TIMER(stats ? &stats->inverted_index_searcher_search_exec_timer : &exec_dummy);
        if (enable_scoring && !is_asc && top_k > 0) {
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

    // Extract NULL bitmap from three-valued logic scorer
    // The scorer correctly computes which documents evaluate to NULL based on query logic
    // For example: TRUE OR NULL = TRUE (not NULL), FALSE OR NULL = NULL
    std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
    if (exec_ctx.null_resolver) {
        auto scorer = weight->scorer(exec_ctx, root_binding_key);
        if (scorer && scorer->has_null_bitmap(exec_ctx.null_resolver)) {
            const auto* bitmap = scorer->get_null_bitmap(exec_ctx.null_resolver);
            if (bitmap != nullptr) {
                *null_bitmap = *bitmap;
                VLOG_TRACE << "search: Extracted NULL bitmap with " << null_bitmap->cardinality()
                           << " NULL documents";
            }
        }
    }

    VLOG_TRACE << "search: Before mask - true_bitmap=" << roaring->cardinality()
               << ", null_bitmap=" << null_bitmap->cardinality();

    // Create result and mask out NULLs (SQL WHERE clause semantics: only TRUE rows)
    bitmap_result = InvertedIndexResultBitmap(std::move(roaring), std::move(null_bitmap));
    bitmap_result.mask_out_null();

    VLOG_TRACE << "search: After mask - result_bitmap="
               << bitmap_result.get_data_bitmap()->cardinality();

    // Insert post-mask_out_null result into DSL cache for future reuse
    // Cache both data bitmap and null bitmap so compound operators (NOT, OR, AND)
    // can apply correct three-valued SQL logic on cache hit
    if (dsl_cache && cache_usable) {
        InvertedIndexQueryCacheHandle insert_handle;
        dsl_cache->insert(dsl_cache_key, bitmap_result.get_data_bitmap(), &insert_handle);
        if (bitmap_result.get_null_bitmap()) {
            auto null_cache_key = InvertedIndexQueryCache::CacheKey {
                    seg_prefix, "__search_dsl__", InvertedIndexQueryType::SEARCH_DSL_QUERY,
                    dsl_sig + "__null"};
            InvertedIndexQueryCacheHandle null_insert_handle;
            dsl_cache->insert(null_cache_key, bitmap_result.get_null_bitmap(), &null_insert_handle);
        }
    }

    return Status::OK();
}

// Aligned with FE QsClauseType enum - uses enum.name() as clause_type
Status FunctionSearch::build_query_recursive(
        const TSearchClause& clause, const std::shared_ptr<IndexQueryContext>& context,
        FieldReaderResolver& resolver, inverted_index::query_v2::QueryPtr* out,
        std::string* binding_key, const std::string& default_operator, int32_t minimum_should_match,
        uint32_t num_rows) const {
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
    return compile_node(*root, SearchLeafContext {.context = context, .num_rows = num_rows},
                        resolver, out, binding_key);
}

void register_function_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSearch>();
}

} // namespace doris
