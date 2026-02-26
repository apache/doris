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

#include "vec/functions/function_search.h"

#include <CLucene/config/repl_wchar.h>
#include <CLucene/search/Scorer.h>
#include <glog/logging.h>

#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/all_query/all_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/bit_set_query/bit_set_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query_builder.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/operator.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/multi_phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/phrase_query/phrase_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/regexp_query/regexp_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/term_query/term_query.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/wildcard_query/wildcard_query.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"
#include "util/string_util.h"
#include "util/thrift_util.h"
#include "vec/columns/column_const.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

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

Status FieldReaderResolver::resolve(const std::string& field_name,
                                    InvertedIndexQueryType query_type,
                                    FieldReaderBinding* binding) {
    DCHECK(binding != nullptr);

    // Check if this is a variant subcolumn
    bool is_variant_sub = is_variant_subcolumn(field_name);

    auto data_it = _data_type_with_names.find(field_name);
    if (data_it == _data_type_with_names.end()) {
        // For variant subcolumns, not finding the index is normal (the subcolumn may not exist in this segment)
        // Return OK but with null binding to signal "no match"
        if (is_variant_sub) {
            VLOG_DEBUG << "Variant subcolumn '" << field_name
                       << "' not found in this segment, treating as no match";
            *binding = FieldReaderBinding();
            return Status::OK();
        }
        // For normal fields, this is an error
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "field '{}' not found in inverted index metadata", field_name);
    }

    const auto& stored_field_name = data_it->second.first;
    const auto binding_key = binding_key_for(stored_field_name, query_type);

    auto cache_it = _cache.find(binding_key);
    if (cache_it != _cache.end()) {
        *binding = cache_it->second;
        return Status::OK();
    }

    auto iterator_it = _iterators.find(field_name);
    if (iterator_it == _iterators.end() || iterator_it->second == nullptr) {
        // For variant subcolumns, not finding the iterator is normal
        if (is_variant_sub) {
            VLOG_DEBUG << "Variant subcolumn '" << field_name
                       << "' iterator not found in this segment, treating as no match";
            *binding = FieldReaderBinding();
            return Status::OK();
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "iterator not found for field '{}'", field_name);
    }

    auto* inverted_iterator = dynamic_cast<InvertedIndexIterator*>(iterator_it->second);
    if (inverted_iterator == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "iterator for field '{}' is not InvertedIndexIterator", field_name);
    }

    // For variant subcolumns, FE resolves the field pattern to a specific index and sends
    // its index_properties via TSearchFieldBinding. When FE picks an analyzer-based index,
    // upgrade EQUAL_QUERY/WILDCARD_QUERY to MATCH_ANY_QUERY so select_best_reader picks the
    // FULLTEXT reader instead of STRING_TYPE. Without this upgrade:
    // - TERM (EQUAL_QUERY) clauses would open the wrong (untokenized) index directory
    // - WILDCARD clauses would enumerate terms from the wrong index, returning empty results
    //
    // For regular (non-variant) columns with multiple indexes, the caller (build_leaf_query)
    // is responsible for passing the appropriate query_type: MATCH_ANY_QUERY for tokenized
    // queries (TERM) and EQUAL_QUERY for exact-match queries (EXACT). This ensures
    // select_best_reader picks FULLTEXT vs STRING_TYPE correctly without needing an explicit
    // analyzer key, since the query_type alone drives the reader type preference.
    InvertedIndexQueryType effective_query_type = query_type;
    auto fb_it = _field_binding_map.find(field_name);
    std::string analyzer_key;
    if (is_variant_sub && fb_it != _field_binding_map.end() &&
        fb_it->second->__isset.index_properties && !fb_it->second->index_properties.empty()) {
        analyzer_key = normalize_analyzer_key(
                build_analyzer_key_from_properties(fb_it->second->index_properties));
        if (inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    fb_it->second->index_properties) &&
            (effective_query_type == InvertedIndexQueryType::EQUAL_QUERY ||
             effective_query_type == InvertedIndexQueryType::WILDCARD_QUERY)) {
            effective_query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
        }
    }

    Result<InvertedIndexReaderPtr> reader_result;
    const auto& column_type = data_it->second.second;
    if (column_type) {
        reader_result = inverted_iterator->select_best_reader(column_type, effective_query_type,
                                                              analyzer_key);
    } else {
        reader_result = inverted_iterator->select_best_reader(analyzer_key);
    }

    if (!reader_result.has_value()) {
        return reader_result.error();
    }

    auto inverted_reader = reader_result.value();
    if (inverted_reader == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "selected reader is null for field '{}'", field_name);
    }

    auto index_file_reader = inverted_reader->get_index_file_reader();
    if (index_file_reader == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "index file reader is null for field '{}'", field_name);
    }

    // Use InvertedIndexSearcherCache to avoid re-opening index files repeatedly
    auto index_file_key =
            index_file_reader->get_index_file_cache_key(&inverted_reader->get_index_meta());
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    InvertedIndexCacheHandle searcher_cache_handle;
    bool cache_hit = InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                                    &searcher_cache_handle);

    std::shared_ptr<lucene::index::IndexReader> reader_holder;
    if (cache_hit) {
        auto searcher_variant = searcher_cache_handle.get_index_searcher();
        auto* searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr && *searcher_ptr != nullptr) {
            reader_holder = std::shared_ptr<lucene::index::IndexReader>(
                    (*searcher_ptr)->getReader(),
                    [](lucene::index::IndexReader*) { /* lifetime managed by searcher cache */ });
        }
    }

    if (!reader_holder) {
        // Cache miss: open directory, build IndexSearcher, insert into cache
        RETURN_IF_ERROR(
                index_file_reader->init(config::inverted_index_read_buffer_size, _context->io_ctx));
        auto directory = DORIS_TRY(
                index_file_reader->open(&inverted_reader->get_index_meta(), _context->io_ctx));

        auto index_searcher_builder = DORIS_TRY(
                IndexSearcherBuilder::create_index_searcher_builder(inverted_reader->type()));
        auto searcher_result =
                DORIS_TRY(index_searcher_builder->get_index_searcher(directory.get()));
        auto reader_size = index_searcher_builder->get_reader_size();

        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(std::move(searcher_result),
                                                                       reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       &searcher_cache_handle);

        auto new_variant = searcher_cache_handle.get_index_searcher();
        auto* new_ptr = std::get_if<FulltextIndexSearcherPtr>(&new_variant);
        if (new_ptr != nullptr && *new_ptr != nullptr) {
            reader_holder = std::shared_ptr<lucene::index::IndexReader>(
                    (*new_ptr)->getReader(),
                    [](lucene::index::IndexReader*) { /* lifetime managed by searcher cache */ });
        }

        if (!reader_holder) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "failed to build IndexSearcher for field '{}'", field_name);
        }
    }

    _searcher_cache_handles.push_back(std::move(searcher_cache_handle));

    FieldReaderBinding resolved;
    resolved.logical_field_name = field_name;
    resolved.stored_field_name = stored_field_name;
    resolved.stored_field_wstr = StringHelper::to_wstring(resolved.stored_field_name);
    resolved.column_type = column_type;
    resolved.query_type = effective_query_type;
    resolved.inverted_reader = inverted_reader;
    resolved.lucene_reader = reader_holder;
    // Prefer FE-provided index_properties (needed for variant subcolumn field_pattern matching)
    // Reuse fb_it from earlier lookup above.
    if (fb_it != _field_binding_map.end() && fb_it->second->__isset.index_properties &&
        !fb_it->second->index_properties.empty()) {
        resolved.index_properties = fb_it->second->index_properties;
    } else {
        resolved.index_properties = inverted_reader->get_index_properties();
    }
    resolved.binding_key = binding_key;
    resolved.analyzer_key =
            normalize_analyzer_key(build_analyzer_key_from_properties(resolved.index_properties));

    _binding_readers[binding_key] = reader_holder;
    _field_readers[resolved.stored_field_wstr] = reader_holder;
    _readers.emplace_back(reader_holder);
    _cache.emplace(binding_key, resolved);
    *binding = resolved;
    return Status::OK();
}

Status FunctionSearch::execute_impl(FunctionContext* /*context*/, Block& /*block*/,
                                    const ColumnNumbers& /*arguments*/, uint32_t /*result*/,
                                    size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

// Enhanced implementation: Handle new parameter structure (DSL + SlotReferences)
Status FunctionSearch::evaluate_inverted_index(
        const ColumnsWithTypeAndName& arguments,
        const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<IndexIterator*> iterators, uint32_t num_rows,
        const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/,
        InvertedIndexResultBitmap& bitmap_result) const {
    return Status::OK();
}

Status FunctionSearch::evaluate_inverted_index_with_search_param(
        const TSearchParam& search_param,
        const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                data_type_with_names,
        std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
        InvertedIndexResultBitmap& bitmap_result, bool enable_cache) const {
    if (iterators.empty() || data_type_with_names.empty()) {
        LOG(INFO) << "No indexed columns or iterators available, returning empty result, dsl:"
                  << search_param.original_dsl;
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

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
            if (dsl_cache->lookup(dsl_cache_key, &dsl_cache_handle)) {
                auto cached_bitmap = dsl_cache_handle.get_bitmap();
                if (cached_bitmap) {
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
        }
    }

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    // Pass field_bindings to resolver for variant subcolumn detection
    FieldReaderResolver resolver(data_type_with_names, iterators, context,
                                 search_param.field_bindings);

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

    query_v2::QueryPtr root_query;
    std::string root_binding_key;
    RETURN_IF_ERROR(build_query_recursive(search_param.root, context, resolver, &root_query,
                                          &root_binding_key, default_operator,
                                          minimum_should_match));
    if (root_query == nullptr) {
        LOG(INFO) << "search: Query tree resolved to empty query, dsl:"
                  << search_param.original_dsl;
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = num_rows;
    exec_ctx.readers = resolver.readers();
    exec_ctx.reader_bindings = resolver.reader_bindings();
    exec_ctx.field_reader_bindings = resolver.field_readers();
    for (const auto& [binding_key, binding] : resolver.binding_cache()) {
        if (binding_key.empty()) {
            continue;
        }
        query_v2::FieldBindingContext binding_ctx;
        binding_ctx.logical_field_name = binding.logical_field_name;
        binding_ctx.stored_field_name = binding.stored_field_name;
        binding_ctx.stored_field_wstr = binding.stored_field_wstr;
        exec_ctx.binding_fields.emplace(binding_key, std::move(binding_ctx));
    }

    class ResolverAdapter final : public query_v2::NullBitmapResolver {
    public:
        explicit ResolverAdapter(const FieldReaderResolver& resolver) : _resolver(resolver) {}

        segment_v2::IndexIterator* iterator_for(const query_v2::Scorer& /*scorer*/,
                                                const std::string& logical_field) const override {
            if (logical_field.empty()) {
                return nullptr;
            }
            return _resolver.get_iterator(logical_field);
        }

    private:
        const FieldReaderResolver& _resolver;
    };

    ResolverAdapter null_resolver(resolver);
    exec_ctx.null_resolver = &null_resolver;

    auto weight = root_query->weight(false);
    if (!weight) {
        LOG(WARNING) << "search: Failed to build query weight";
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    auto scorer = weight->scorer(exec_ctx, root_binding_key);
    if (!scorer) {
        LOG(WARNING) << "search: Failed to build scorer";
        bitmap_result = InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(),
                                                  std::make_shared<roaring::Roaring>());
        return Status::OK();
    }

    std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
    uint32_t doc = scorer->doc();
    uint32_t matched_docs = 0;
    while (doc != query_v2::TERMINATED) {
        roaring->add(doc);
        ++matched_docs;
        doc = scorer->advance();
    }

    VLOG_DEBUG << "search: Query completed, matched " << matched_docs << " documents";

    // Extract NULL bitmap from three-valued logic scorer
    // The scorer correctly computes which documents evaluate to NULL based on query logic
    // For example: TRUE OR NULL = TRUE (not NULL), FALSE OR NULL = NULL
    std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
    if (scorer->has_null_bitmap(exec_ctx.null_resolver)) {
        const auto* bitmap = scorer->get_null_bitmap(exec_ctx.null_resolver);
        if (bitmap != nullptr) {
            *null_bitmap = *bitmap;
            VLOG_TRACE << "search: Extracted NULL bitmap with " << null_bitmap->cardinality()
                       << " NULL documents";
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
        clause_type == "OCCUR_BOOLEAN") {
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

Status FunctionSearch::build_query_recursive(const TSearchClause& clause,
                                             const std::shared_ptr<IndexQueryContext>& context,
                                             FieldReaderResolver& resolver,
                                             inverted_index::query_v2::QueryPtr* out,
                                             std::string* binding_key,
                                             const std::string& default_operator,
                                             int32_t minimum_should_match) const {
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
                                                      minimum_should_match));

                // Determine occur type from child clause
                query_v2::Occur occur = query_v2::Occur::MUST; // default
                if (child_clause.__isset.occur) {
                    occur = map_thrift_occur(child_clause.occur);
                }

                builder->add(child_query, occur);
            }
        }

        *out = builder->build();
        return Status::OK();
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
                                                      minimum_should_match));
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
                            minimum_should_match);
}

Status FunctionSearch::build_leaf_query(const TSearchClause& clause,
                                        const std::shared_ptr<IndexQueryContext>& context,
                                        FieldReaderResolver& resolver,
                                        inverted_index::query_v2::QueryPtr* out,
                                        std::string* binding_key,
                                        const std::string& default_operator,
                                        int32_t minimum_should_match) const {
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

    FieldReaderBinding binding;
    RETURN_IF_ERROR(resolver.resolve(field_name, query_type, &binding));

    // Check if binding is empty (variant subcolumn not found in this segment)
    if (binding.lucene_reader == nullptr) {
        VLOG_DEBUG << "build_leaf_query: Variant subcolumn '" << field_name
                   << "' has no index in this segment, creating empty BitSetQuery (no matches)";
        // Variant subcolumn doesn't exist - create empty BitSetQuery (no matches)
        *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
        if (binding_key) {
            binding_key->clear();
        }
        return Status::OK();
    }

    if (binding_key) {
        *binding_key = binding.binding_key;
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
                *out = make_term_query(value_wstr);
                return Status::OK();
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: No terms found after tokenization for TERM query, field="
                             << field_name << ", value='" << value
                             << "', returning empty BitSetQuery";
                *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
                return Status::OK();
            }

            if (term_infos.size() == 1) {
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                *out = make_term_query(term_wstr);
                return Status::OK();
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
                *out = builder->build();
                return Status::OK();
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

            *out = builder->build();
            return Status::OK();
        }

        *out = make_term_query(value_wstr);
        return Status::OK();
    }

    if (category == FunctionSearch::ClauseTypeCategory::TOKENIZED) {
        if (clause_type == "PHRASE") {
            bool should_analyze = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            if (!should_analyze) {
                VLOG_DEBUG << "search: PHRASE on non-tokenized field '" << field_name
                           << "', falling back to TERM";
                *out = make_term_query(value_wstr);
                return Status::OK();
            }

            if (binding.index_properties.empty()) {
                LOG(WARNING) << "search: analyzer required but index properties empty for PHRASE "
                                "query on field '"
                             << field_name << "'";
                *out = make_term_query(value_wstr);
                return Status::OK();
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: No terms found after tokenization for PHRASE query, field="
                             << field_name << ", value='" << value
                             << "', returning empty BitSetQuery";
                *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
                return Status::OK();
            }

            std::vector<TermInfo> phrase_term_infos =
                    QueryHelper::build_phrase_term_infos(term_infos);
            if (phrase_term_infos.size() == 1) {
                const auto& term_info = phrase_term_infos[0];
                if (term_info.is_single_term()) {
                    std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                    *out = std::make_shared<query_v2::TermQuery>(context, field_wstr, term_wstr);
                } else {
                    auto builder =
                            create_operator_boolean_query_builder(query_v2::OperatorType::OP_OR);
                    for (const auto& term : term_info.get_multi_terms()) {
                        std::wstring term_wstr = StringHelper::to_wstring(term);
                        builder->add(make_term_query(term_wstr), binding.binding_key);
                    }
                    *out = builder->build();
                }
            } else {
                if (QueryHelper::is_simple_phrase(phrase_term_infos)) {
                    *out = std::make_shared<query_v2::PhraseQuery>(context, field_wstr,
                                                                   phrase_term_infos);
                } else {
                    *out = std::make_shared<query_v2::MultiPhraseQuery>(context, field_wstr,
                                                                        phrase_term_infos);
                }
            }

            return Status::OK();
        }
        if (clause_type == "MATCH") {
            VLOG_DEBUG << "search: MATCH clause not implemented, fallback to TERM";
            *out = make_term_query(value_wstr);
            return Status::OK();
        }

        if (clause_type == "ANY" || clause_type == "ALL") {
            bool should_analyze = inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    binding.index_properties);
            if (!should_analyze) {
                *out = make_term_query(value_wstr);
                return Status::OK();
            }

            if (binding.index_properties.empty()) {
                LOG(WARNING) << "search: index properties empty for tokenized clause '"
                             << clause_type << "' field=" << field_name;
                *out = make_term_query(value_wstr);
                return Status::OK();
            }

            std::vector<TermInfo> term_infos =
                    inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                            value, binding.index_properties);
            if (term_infos.empty()) {
                LOG(WARNING) << "search: tokenization yielded no terms for clause '" << clause_type
                             << "', field=" << field_name << ", returning empty BitSetQuery";
                *out = std::make_shared<query_v2::BitSetQuery>(roaring::Roaring());
                return Status::OK();
            }

            query_v2::OperatorType bool_type = query_v2::OperatorType::OP_OR;
            if (clause_type == "ALL") {
                bool_type = query_v2::OperatorType::OP_AND;
            }

            if (term_infos.size() == 1) {
                std::wstring term_wstr = StringHelper::to_wstring(term_infos[0].get_single_term());
                *out = make_term_query(term_wstr);
                return Status::OK();
            }

            auto builder = create_operator_boolean_query_builder(bool_type);
            for (const auto& term_info : term_infos) {
                std::wstring term_wstr = StringHelper::to_wstring(term_info.get_single_term());
                builder->add(make_term_query(term_wstr), binding.binding_key);
            }
            *out = builder->build();
            return Status::OK();
        }

        // Default tokenized clause fallback
        *out = make_term_query(value_wstr);
        return Status::OK();
    }

    if (category == FunctionSearch::ClauseTypeCategory::NON_TOKENIZED) {
        if (clause_type == "EXACT") {
            // EXACT match: exact string matching without tokenization
            // Note: EXACT prefers untokenized index (STRING_TYPE) which doesn't support lowercase
            // If only tokenized index exists, EXACT may return empty results because
            // tokenized indexes store individual tokens, not complete strings
            *out = make_term_query(value_wstr);
            VLOG_DEBUG << "search: EXACT clause processed, field=" << field_name << ", value='"
                       << value << "'";
            return Status::OK();
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
            *out = std::make_shared<query_v2::WildcardQuery>(context, field_wstr, pattern);
            VLOG_DEBUG << "search: PREFIX clause processed, field=" << field_name << ", pattern='"
                       << pattern << "' (original='" << value << "', has_parser=" << has_parser
                       << ", lower_case=" << lowercase_setting << ")";
            return Status::OK();
        }

        if (clause_type == "WILDCARD") {
            // Standalone wildcard "*" matches all non-null values for this field
            // Consistent with ES query_string behavior where field:* becomes FieldExistsQuery
            if (value == "*") {
                *out = std::make_shared<query_v2::AllQuery>(field_wstr, true);
                VLOG_DEBUG << "search: WILDCARD '*' converted to AllQuery(nullable=true), field="
                           << field_name;
                return Status::OK();
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
            *out = std::make_shared<query_v2::WildcardQuery>(context, field_wstr, pattern);
            VLOG_DEBUG << "search: WILDCARD clause processed, field=" << field_name << ", pattern='"
                       << pattern << "' (original='" << value << "', has_parser=" << has_parser
                       << ", lower_case=" << lowercase_setting << ")";
            return Status::OK();
        }

        if (clause_type == "REGEXP") {
            // ES-compatible: regex patterns are NOT lowercased (case-sensitive matching)
            // This matches ES query_string behavior where regex patterns bypass analysis
            *out = std::make_shared<query_v2::RegexpQuery>(context, field_wstr, value);
            VLOG_DEBUG << "search: REGEXP clause processed, field=" << field_name << ", pattern='"
                       << value << "'";
            return Status::OK();
        }

        if (clause_type == "RANGE" || clause_type == "LIST") {
            VLOG_DEBUG << "search: clause type '" << clause_type
                       << "' not implemented, fallback to TERM";
        }
        *out = make_term_query(value_wstr);
        return Status::OK();
    }

    LOG(WARNING) << "search: Unexpected clause type '" << clause_type << "', using TERM fallback";
    *out = make_term_query(value_wstr);
    return Status::OK();
}

void register_function_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSearch>();
}

} // namespace doris::vectorized
