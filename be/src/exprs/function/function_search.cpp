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
#include <CLucene/search/Scorer.h>
#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function/variant_inverted_index_search.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_profile.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "storage/index/inverted/query/query_helper.h"
#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
#include "storage/index/inverted/query_v2/regexp_query/regexp_query.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/query_v2/wildcard_query/wildcard_query.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/olap_common.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/types.h"
#include "util/debug_points.h"
#include "util/string_parser.hpp"
#include "util/string_util.h"
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

namespace {

bool is_nested_group_search_supported() {
    auto provider = segment_v2::create_nested_group_read_provider();
    return provider != nullptr && provider->should_enable_nested_group_read_path();
}

query_v2::QueryPtr make_unknown_query(uint32_t num_rows) {
    auto null_bitmap = std::make_shared<roaring::Roaring>();
    if (num_rows > 0) {
        null_bitmap->addRange(0, num_rows);
    }
    return std::make_shared<query_v2::BitSetQuery>(std::make_shared<roaring::Roaring>(),
                                                   std::move(null_bitmap));
}

DataTypePtr unwrap_direct_index_value_type(DataTypePtr column_type) {
    DataTypePtr value_type = remove_nullable(std::move(column_type));
    while (value_type != nullptr &&
           value_type->get_storage_field_type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        const auto* array_type = dynamic_cast<const DataTypeArray*>(value_type.get());
        if (array_type == nullptr) {
            return value_type;
        }
        value_type = remove_nullable(array_type->get_nested_type());
    }
    return value_type;
}

template <PrimitiveType primitive_type, typename CppType>
Status parse_integral_search_value(const std::string& value, Field* field) {
    StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
    CppType parsed =
            StringParser::string_to_int<CppType>(value.data(), value.size(), &parse_result);
    if (parse_result != StringParser::PARSE_SUCCESS) {
        return Status::InvalidArgument("failed to parse '{}' as {}", value,
                                       type_to_string(primitive_type));
    }
    *field = Field::create_field<primitive_type>(parsed);
    return Status::OK();
}

Status parse_scalar_search_value(const DataTypePtr& column_type, const std::string& value,
                                 Field* field) {
    if (column_type == nullptr || field == nullptr) {
        return Status::InvalidArgument("missing column type for scalar search value");
    }

    switch (column_type->get_storage_field_type()) {
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
        bool parsed = StringParser::string_to_bool(value.data(), value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("failed to parse '{}' as bool", value);
        }
        *field = Field::create_field<TYPE_BOOLEAN>(parsed);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return parse_integral_search_value<TYPE_TINYINT, Int8>(value, field);
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return parse_integral_search_value<TYPE_SMALLINT, Int16>(value, field);
    case FieldType::OLAP_FIELD_TYPE_INT:
        return parse_integral_search_value<TYPE_INT, Int32>(value, field);
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return parse_integral_search_value<TYPE_BIGINT, Int64>(value, field);
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return parse_integral_search_value<TYPE_LARGEINT, Int128>(value, field);
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
        Float32 parsed =
                StringParser::string_to_float<Float32>(value.data(), value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("failed to parse '{}' as float", value);
        }
        *field = Field::create_field<TYPE_FLOAT>(parsed);
        return Status::OK();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        StringParser::ParseResult parse_result = StringParser::PARSE_FAILURE;
        Float64 parsed =
                StringParser::string_to_float<Float64>(value.data(), value.size(), &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("failed to parse '{}' as double", value);
        }
        *field = Field::create_field<TYPE_DOUBLE>(parsed);
        return Status::OK();
    }
    default:
        return Status::NotSupported("scalar search does not support storage field type {}",
                                    static_cast<int>(column_type->get_storage_field_type()));
    }
}

InvertedIndexQueryType direct_index_query_type_for_clause(const std::string& clause_type) {
    if (clause_type == "TERM" || clause_type == "EXACT") {
        return InvertedIndexQueryType::EQUAL_QUERY;
    }
    return InvertedIndexQueryType::UNKNOWN_QUERY;
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

    // DSL result cache: reuse InvertedIndexQueryCache with SEARCH_DSL_QUERY type
    auto* dsl_cache = enable_cache ? InvertedIndexQueryCache::instance() : nullptr;
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

    bool enable_scoring = false;
    bool is_asc = false;
    size_t top_k = 0;
    if (index_query_context) {
        enable_scoring = index_query_context->collection_similarity != nullptr;
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
            query_v2::collect_multi_segment_top_k(
                    weight, exec_ctx, root_binding_key, top_k, roaring,
                    index_query_context->collection_similarity, use_wand);
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
FunctionSearch::ClauseTypeCategory FunctionSearch::get_clause_type_category(
        const std::string& clause_type) const {
    if (clause_type == "AND" || clause_type == "OR" || clause_type == "NOT" ||
        clause_type == "OCCUR_BOOLEAN" || clause_type == "NESTED") {
        return ClauseTypeCategory::COMPOUND;
    } else if (clause_type == "TERM" || clause_type == "PREFIX" || clause_type == "WILDCARD" ||
               clause_type == "REGEXP" || clause_type == "RANGE" || clause_type == "LIST" ||
               clause_type == "EXACT") {
        // Non-tokenized queries: exact matching, pattern matching, range, list operations
        return ClauseTypeCategory::NON_TOKENIZED;
    } else if (clause_type == "PHRASE" || clause_type == "MATCH" || clause_type == "ANY" ||
               clause_type == "ALL") {
        // Tokenized queries: phrase search, full-text search, multi-value matching
        // Note: ANY and ALL require tokenization of their input values
        return ClauseTypeCategory::TOKENIZED;
    } else {
        // Default to NON_TOKENIZED for unknown types
        LOG(WARNING) << "Unknown clause type '" << clause_type
                     << "', defaulting to NON_TOKENIZED category";
        return ClauseTypeCategory::NON_TOKENIZED;
    }
}

// Analyze query type for a specific field in the search clause
InvertedIndexQueryType FunctionSearch::analyze_field_query_type(const std::string& field_name,
                                                                const TSearchClause& clause) const {
    const std::string& clause_type = clause.clause_type;
    ClauseTypeCategory category = get_clause_type_category(clause_type);

    // Handle leaf queries - use direct mapping
    if (category != ClauseTypeCategory::COMPOUND) {
        // Check if this clause targets the specific field
        if (clause.field_name == field_name) {
            // Use direct mapping from clause_type to InvertedIndexQueryType
            return clause_type_to_query_type(clause_type);
        }
    }

    // Handle boolean queries - recursively analyze children
    if (!clause.children.empty()) {
        for (const auto& child_clause : clause.children) {
            // Recursively analyze each child
            InvertedIndexQueryType child_type = analyze_field_query_type(field_name, child_clause);
            // If this child targets the field (not default EQUAL_QUERY), return its query type
            if (child_type != InvertedIndexQueryType::UNKNOWN_QUERY) {
                return child_type;
            }
        }
    }

    // If no children target this field, return UNKNOWN_QUERY as default
    return InvertedIndexQueryType::UNKNOWN_QUERY;
}

// Map clause_type string to InvertedIndexQueryType
InvertedIndexQueryType FunctionSearch::clause_type_to_query_type(
        const std::string& clause_type) const {
    // Use static map for better performance and maintainability
    static const std::unordered_map<std::string, InvertedIndexQueryType> clause_type_map = {
            // Boolean operations
            {"AND", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"OR", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"NOT", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"OCCUR_BOOLEAN", InvertedIndexQueryType::BOOLEAN_QUERY},
            {"NESTED", InvertedIndexQueryType::BOOLEAN_QUERY},

            // Non-tokenized queries (exact matching, pattern matching)
            {"TERM", InvertedIndexQueryType::EQUAL_QUERY},
            {"PREFIX", InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY},
            {"WILDCARD", InvertedIndexQueryType::WILDCARD_QUERY},
            {"REGEXP", InvertedIndexQueryType::MATCH_REGEXP_QUERY},
            {"RANGE", InvertedIndexQueryType::RANGE_QUERY},
            {"LIST", InvertedIndexQueryType::LIST_QUERY},

            // Tokenized queries (full-text search, phrase search)
            {"PHRASE", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
            {"MATCH", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"ANY", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"ALL", InvertedIndexQueryType::MATCH_ALL_QUERY},

            // Exact match without tokenization
            {"EXACT", InvertedIndexQueryType::EQUAL_QUERY},
    };

    auto it = clause_type_map.find(clause_type);
    if (it != clause_type_map.end()) {
        return it->second;
    }

    // Unknown clause type
    LOG(WARNING) << "Unknown clause type '" << clause_type << "', defaulting to EQUAL_QUERY";
    return InvertedIndexQueryType::EQUAL_QUERY;
}

// Map Thrift TSearchOccur to query_v2::Occur
static query_v2::Occur map_thrift_occur(TSearchOccur::type thrift_occur) {
    switch (thrift_occur) {
    case TSearchOccur::MUST:
        return query_v2::Occur::MUST;
    case TSearchOccur::SHOULD:
        return query_v2::Occur::SHOULD;
    case TSearchOccur::MUST_NOT:
        return query_v2::Occur::MUST_NOT;
    default:
        return query_v2::Occur::MUST;
    }
}

Status FunctionSearch::build_query_recursive(
        const TSearchClause& clause, const std::shared_ptr<IndexQueryContext>& context,
        FieldReaderResolver& resolver, inverted_index::query_v2::QueryPtr* out,
        std::string* binding_key, const std::string& default_operator, int32_t minimum_should_match,
        uint32_t num_rows) const {
    DCHECK(out != nullptr);
    *out = nullptr;
    if (binding_key) {
        binding_key->clear();
    }

    const std::string& clause_type = clause.clause_type;

    // Handle MATCH_ALL_DOCS - matches all documents in the segment
    if (clause_type == "MATCH_ALL_DOCS") {
        *out = std::make_shared<query_v2::AllQuery>();
        return Status::OK();
    }

    // Handle OCCUR_BOOLEAN - Lucene-style boolean query with MUST/SHOULD/MUST_NOT
    if (clause_type == "OCCUR_BOOLEAN") {
        auto builder = segment_v2::inverted_index::query_v2::create_occur_boolean_query_builder();

        // Set minimum_should_match if specified
        if (clause.__isset.minimum_should_match) {
            builder->set_minimum_number_should_match(clause.minimum_should_match);
        }

        if (clause.__isset.children) {
            for (const auto& child_clause : clause.children) {
                query_v2::QueryPtr child_query;
                std::string child_binding_key;
                RETURN_IF_ERROR(build_query_recursive(child_clause, context, resolver, &child_query,
                                                      &child_binding_key, default_operator,
                                                      minimum_should_match, num_rows));

                // Determine occur type from child clause
                query_v2::Occur occur = query_v2::Occur::MUST; // default
                if (child_clause.__isset.occur) {
                    occur = map_thrift_occur(child_clause.occur);
                }

                builder->add(child_query, occur, std::move(child_binding_key));
            }
        }

        *out = builder->build();
        return Status::OK();
    }

    if (clause_type == "NESTED") {
        return Status::InvalidArgument("NESTED clause must be evaluated at top level");
    }

    // Handle standard boolean operators (AND/OR/NOT)
    if (clause_type == "AND" || clause_type == "OR" || clause_type == "NOT") {
        query_v2::OperatorType op = query_v2::OperatorType::OP_AND;
        if (clause_type == "OR") {
            op = query_v2::OperatorType::OP_OR;
        } else if (clause_type == "NOT") {
            op = query_v2::OperatorType::OP_NOT;
        }

        auto builder = create_operator_boolean_query_builder(op);
        if (clause.__isset.children) {
            for (const auto& child_clause : clause.children) {
                query_v2::QueryPtr child_query;
                std::string child_binding_key;
                RETURN_IF_ERROR(build_query_recursive(child_clause, context, resolver, &child_query,
                                                      &child_binding_key, default_operator,
                                                      minimum_should_match, num_rows));
                // Add all children including empty BitSetQuery
                // BooleanQuery will handle the logic:
                // - AND with empty bitmap → result is empty
                // - OR with empty bitmap → empty bitmap is ignored by OR logic
                // - NOT with empty bitmap → NOT(empty) = all rows (handled by BooleanQuery)
                builder->add(child_query, std::move(child_binding_key));
            }
        }

        *out = builder->build();
        return Status::OK();
    }

    return build_leaf_query(clause, context, resolver, out, binding_key, default_operator,
                            minimum_should_match, num_rows);
}

Status FunctionSearch::build_leaf_query(const TSearchClause& clause,
                                        const std::shared_ptr<IndexQueryContext>& context,
                                        FieldReaderResolver& resolver,
                                        inverted_index::query_v2::QueryPtr* out,
                                        std::string* binding_key,
                                        const std::string& default_operator,
                                        int32_t minimum_should_match, uint32_t num_rows) const {
    DCHECK(out != nullptr);
    *out = nullptr;
    if (binding_key) {
        binding_key->clear();
    }

    if (!clause.__isset.field_name || !clause.__isset.value) {
        return Status::InvalidArgument("search clause missing field_name or value");
    }

    const std::string& field_name = clause.field_name;
    const std::string& value = clause.value;
    const std::string& clause_type = clause.clause_type;

    auto query_type = clause_type_to_query_type(clause_type);
    // TERM, WILDCARD, PREFIX, and REGEXP in search DSL operate on individual index terms
    // (like Lucene TermQuery, WildcardQuery, PrefixQuery, RegexpQuery).
    // Override to MATCH_ANY_QUERY so select_best_reader() prefers the FULLTEXT reader
    // when multiple indexes exist on the same column (one tokenized, one untokenized).
    // Without this, these queries would select the untokenized index and try to match
    // patterns like "h*llo" against full strings ("hello world") instead of individual
    // tokens ("hello"), returning empty results.
    // EXACT must remain EQUAL_QUERY to prefer the untokenized STRING_TYPE reader.
    //
    // Safe for single-index columns: select_best_reader() has a single-reader fast path
    // that returns the only reader directly, bypassing the query_type preference logic.
    if (clause_type == "TERM" || clause_type == "WILDCARD" || clause_type == "PREFIX" ||
        clause_type == "REGEXP") {
        query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    }

    auto finish_leaf_query = [&](query_v2::QueryPtr query) -> Status {
        *out = std::move(query);
        return resolver.map_leaf_query(field_name, out);
    };

    FieldReaderBinding binding;
    RETURN_IF_ERROR(resolver.resolve(field_name, query_type, &binding));

    if (!binding.is_bound()) {
        LOG(INFO) << "search: No inverted index for field '" << field_name
                  << "' in this segment, clause_type='" << clause_type
                  << "', query_type=" << static_cast<int>(query_type)
                  << ", returning UNKNOWN bitmap";
        if (binding_key) {
            binding_key->clear();
        }
        return finish_leaf_query(make_unknown_query(num_rows));
    }

    if (binding_key) {
        *binding_key = binding.binding_key;
    }

    if (binding.use_direct_index_reader()) {
        auto direct_query_type = direct_index_query_type_for_clause(clause_type);
        if (direct_query_type == InvertedIndexQueryType::UNKNOWN_QUERY) {
            return finish_leaf_query(make_unknown_query(num_rows));
        }

        auto value_type = unwrap_direct_index_value_type(binding.column_type);
        Field param_value;
        auto parse_status = parse_scalar_search_value(value_type, value, &param_value);
        if (!parse_status.ok()) {
            LOG(INFO) << "search: scalar leaf value is unsupported, field=" << field_name
                      << ", value='" << value << "', reason=" << parse_status.to_string();
            return finish_leaf_query(make_unknown_query(num_rows));
        }

        auto* iterator = resolver.get_iterator(field_name);
        if (iterator == nullptr) {
            return finish_leaf_query(make_unknown_query(num_rows));
        }

        segment_v2::InvertedIndexParam param;
        param.column_name = binding.stored_field_name;
        param.column_type = value_type;
        param.query_value = param_value;
        param.query_type = direct_query_type;
        param.num_rows = num_rows;
        param.roaring = std::make_shared<roaring::Roaring>();
        RETURN_IF_ERROR(iterator->read_from_index(segment_v2::IndexParam {&param}));

        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        auto has_null = iterator->has_null();
        if (has_null.has_value() && has_null.value()) {
            segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iterator->read_null_bitmap(&null_bitmap_cache_handle));
            if (auto bitmap = null_bitmap_cache_handle.get_bitmap(); bitmap != nullptr) {
                null_bitmap = bitmap;
            }
        }
        return finish_leaf_query(std::make_shared<query_v2::BitSetQuery>(std::move(param.roaring),
                                                                         std::move(null_bitmap)));
    }

    if (binding.lucene_reader == nullptr) {
        return finish_leaf_query(make_unknown_query(num_rows));
    }

    FunctionSearch::ClauseTypeCategory category = get_clause_type_category(clause_type);
    std::wstring field_wstr = binding.stored_field_wstr;
    std::wstring value_wstr = StringHelper::to_wstring(value);

    auto make_term_query = [&](const std::wstring& term) -> query_v2::QueryPtr {
        return std::make_shared<query_v2::TermQuery>(context, field_wstr, term);
    };

    if (clause_type == "TERM") {
        bool should_analyze =
                inverted_index::InvertedIndexAnalyzer::should_analyzer(binding.index_properties);
        if (should_analyze) {
            if (binding.index_properties.empty()) {
                LOG(WARNING) << "search: analyzer required but index properties empty for field '"
                             << field_name << "'";
                return finish_leaf_query(make_term_query(value_wstr));
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: No terms found after tokenization for TERM query, field="
                             << field_name << ", value='" << value
                             << "', returning empty BitSetQuery";
                return finish_leaf_query(
                        std::make_shared<query_v2::BitSetQuery>(roaring::Roaring()));
            }

            if (term_infos.size() == 1) {
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                return finish_leaf_query(make_term_query(term_wstr));
            }

            // When minimum_should_match is specified, use OccurBooleanQuery
            // ES behavior: msm only applies to SHOULD clauses
            if (minimum_should_match > 0) {
                auto builder =
                        segment_v2::inverted_index::query_v2::create_occur_boolean_query_builder();
                builder->set_minimum_number_should_match(minimum_should_match);
                query_v2::Occur occur = (default_operator == "and") ? query_v2::Occur::MUST
                                                                    : query_v2::Occur::SHOULD;
                for (const auto& term_info : term_infos) {
                    std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                    builder->add(make_term_query(term_wstr), occur);
                }
                return finish_leaf_query(builder->build());
            }

            // Use default_operator to determine how to combine tokenized terms
            query_v2::OperatorType op_type = (default_operator == "and")
                                                     ? query_v2::OperatorType::OP_AND
                                                     : query_v2::OperatorType::OP_OR;
            auto builder = create_operator_boolean_query_builder(op_type);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                builder->add(make_term_query(term_wstr), binding.binding_key);
            }

            return finish_leaf_query(builder->build());
        }

        return finish_leaf_query(make_term_query(value_wstr));
    }

    if (category == FunctionSearch::ClauseTypeCategory::TOKENIZED) {
        if (clause_type == "PHRASE") {
            bool should_analyze = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            if (!should_analyze) {
                VLOG_DEBUG << "search: PHRASE on non-tokenized field '" << field_name
                           << "', falling back to TERM";
                return finish_leaf_query(make_term_query(value_wstr));
            }

            if (binding.index_properties.empty()) {
                LOG(WARNING) << "search: analyzer required but index properties empty for PHRASE "
                                "query on field '"
                             << field_name << "'";
                return finish_leaf_query(make_term_query(value_wstr));
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: No terms found after tokenization for PHRASE query, field="
                             << field_name << ", value='" << value
                             << "', returning empty BitSetQuery";
                return finish_leaf_query(
                        std::make_shared<query_v2::BitSetQuery>(roaring::Roaring()));
            }

            std::vector<TermInfo> phrase_term_infos =
                    QueryHelper::build_phrase_term_infos(term_infos);
            if (phrase_term_infos.size() == 1) {
                const auto& term_info = phrase_term_infos[0];
                if (term_info.is_single_term()) {
                    std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                    return finish_leaf_query(
                            std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr));
                } else {
                    auto builder =
                            create_operator_boolean_query_builder(query_v2::OperatorType::OP_OR);
                    for (const auto& term : term_info.get_multi_terms()) {
                        std::wstring term_wstr = StringHelper::to_wstring(term);
                        builder->add(make_term_query(term_wstr), binding.binding_key);
                    }
                    return finish_leaf_query(builder->build());
                }
            } else {
                if (QueryHelper::is_simple_phrase(phrase_term_infos)) {
                    return finish_leaf_query(std::make_shared<query_v2::PhraseQuery>(
                            context, field_wstr, phrase_term_infos));
                } else {
                    return finish_leaf_query(std::make_shared<query_v2::MultiPhraseQuery>(
                            context, field_wstr, phrase_term_infos));
                }
            }

            return Status::OK();
        }
        if (clause_type == "MATCH") {
            VLOG_DEBUG << "search: MATCH clause not implemented, fallback to TERM";
            return finish_leaf_query(make_term_query(value_wstr));
        }

        if (clause_type == "ANY" || clause_type == "ALL") {
            bool should_analyze = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            if (!should_analyze) {
                return finish_leaf_query(make_term_query(value_wstr));
            }

            if (binding.index_properties.empty()) {
                LOG(WARNING) << "search: index properties empty for tokenized clause '"
                             << clause_type << "' field=" << field_name;
                return finish_leaf_query(make_term_query(value_wstr));
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: tokenization yielded no terms for clause '" << clause_type
                             << "', field=" << field_name << ", returning empty BitSetQuery";
                return finish_leaf_query(
                        std::make_shared<query_v2::BitSetQuery>(roaring::Roaring()));
            }

            query_v2::OperatorType bool_type = query_v2::OperatorType::OP_OR;
            if (clause_type == "ALL") {
                bool_type = query_v2::OperatorType::OP_AND;
            }

            if (term_infos.size() == 1) {
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                return finish_leaf_query(make_term_query(term_wstr));
            }

            auto builder = create_operator_boolean_query_builder(bool_type);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                builder->add(make_term_query(term_wstr), binding.binding_key);
            }
            return finish_leaf_query(builder->build());
        }

        // Default tokenized clause fallback
        return finish_leaf_query(make_term_query(value_wstr));
    }

    if (category == FunctionSearch::ClauseTypeCategory::NON_TOKENIZED) {
        if (clause_type == "EXACT") {
            // EXACT match: exact string matching without tokenization
            // Note: EXACT prefers untokenized index (STRING_TYPE) which doesn't support lowercase
            // If only tokenized index exists, EXACT may return empty results because
            // tokenized indexes store individual tokens, not complete strings
            VLOG_DEBUG << "search: EXACT clause processed, field=" << field_name << ", value='"
                       << value << "'";
            return finish_leaf_query(make_term_query(value_wstr));
        }
        if (clause_type == "PREFIX") {
            // Apply lowercase only if:
            // 1. There's a parser/analyzer (otherwise lower_case has no effect on indexing)
            // 2. lower_case is explicitly set to "true"
            bool has_parser = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            std::string lowercase_setting =
                    get_parser_lowercase_from_properties(binding.index_properties);
            bool should_lowercase = has_parser && (lowercase_setting == INVERTED_INDEX_PARSER_TRUE);
            std::string pattern = should_lowercase ? to_lower(value) : value;
            VLOG_DEBUG << "search: PREFIX clause processed, field=" << field_name << ", pattern='"
                       << pattern << "' (original='" << value << "', has_parser=" << has_parser
                       << ", lower_case=" << lowercase_setting << ")";
            return finish_leaf_query(
                    std::make_shared<query_v2::WildcardQuery>(context, field_wstr, pattern));
        }

        if (clause_type == "WILDCARD") {
            // Standalone wildcard "*" matches all non-null values for this field
            // Consistent with ES query_string behavior where field:* becomes FieldExistsQuery
            if (value == "*") {
                VLOG_DEBUG << "search: WILDCARD '*' converted to AllQuery(nullable=true), field="
                           << field_name;
                return finish_leaf_query(std::make_shared<query_v2::AllQuery>(field_wstr, true));
            }
            // Apply lowercase only if:
            // 1. There's a parser/analyzer (otherwise lower_case has no effect on indexing)
            // 2. lower_case is explicitly set to "true"
            bool has_parser = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            std::string lowercase_setting =
                    get_parser_lowercase_from_properties(binding.index_properties);
            bool should_lowercase = has_parser && (lowercase_setting == INVERTED_INDEX_PARSER_TRUE);
            std::string pattern = should_lowercase ? to_lower(value) : value;
            VLOG_DEBUG << "search: WILDCARD clause processed, field=" << field_name << ", pattern='"
                       << pattern << "' (original='" << value << "', has_parser=" << has_parser
                       << ", lower_case=" << lowercase_setting << ")";
            return finish_leaf_query(
                    std::make_shared<query_v2::WildcardQuery>(context, field_wstr, pattern));
        }

        if (clause_type == "REGEXP") {
            // ES-compatible: regex patterns are NOT lowercased (case-sensitive matching)
            // This matches ES query_string behavior where regex patterns bypass analysis
            VLOG_DEBUG << "search: REGEXP clause processed, field=" << field_name << ", pattern='"
                       << value << "'";
            return finish_leaf_query(
                    std::make_shared<query_v2::RegexpQuery>(context, field_wstr, value));
        }

        if (clause_type == "RANGE" || clause_type == "LIST") {
            VLOG_DEBUG << "search: clause type '" << clause_type
                       << "' not implemented, fallback to TERM";
        }
        return finish_leaf_query(make_term_query(value_wstr));
    }

    LOG(WARNING) << "search: Unexpected clause type '" << clause_type << "', using TERM fallback";
    return finish_leaf_query(make_term_query(value_wstr));
}

void register_function_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSearch>();
}

} // namespace doris
