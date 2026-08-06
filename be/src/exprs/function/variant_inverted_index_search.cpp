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

#include "exprs/function/variant_inverted_index_search.h"

#include <CLucene/config/repl_wchar.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "exprs/function/function_search.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_searcher.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/doc_set.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/segment/segment.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/utils.h"
#include "util/debug_points.h"
#include "util/time.h"

namespace doris {

namespace query_v2 = segment_v2::inverted_index::query_v2;

namespace {

void add_search_binding_diagnostic(const std::shared_ptr<IndexQueryContext>& context,
                                   const std::string& diagnostic) {
    VLOG_DEBUG << diagnostic;
    if (context != nullptr && context->stats != nullptr) {
        context->stats->inverted_index_stats.add_binding_diagnostic(diagnostic);
    }
}

} // namespace

FieldReaderResolver::FieldReaderResolver(
        const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
        const std::unordered_map<std::string, IndexIterator*>& iterators,
        std::shared_ptr<IndexQueryContext> context,
        const std::vector<TSearchFieldBinding>& field_bindings)
        : _data_type_with_names(data_type_with_names),
          _iterators(iterators),
          _context(std::move(context)),
          _field_bindings(field_bindings) {
    for (const auto& binding : _field_bindings) {
        if (binding.__isset.is_variant_subcolumn && binding.is_variant_subcolumn) {
            _variant_subcolumn_fields.insert(binding.field_name);
        }
        _field_binding_map[binding.field_name] = &binding;
    }
}

Status FieldReaderResolver::resolve(const std::string& field_name,
                                    InvertedIndexQueryType query_type,
                                    FieldReaderBinding* binding) {
    DCHECK(binding != nullptr);

    const bool is_variant_sub = is_variant_subcolumn(field_name);

    auto data_it = _data_type_with_names.find(field_name);
    if (data_it == _data_type_with_names.end()) {
        if (is_variant_sub) {
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=no_metadata "
                                "logical_field={} query_type={} reason=field_not_found",
                                field_name, query_type_to_string(query_type)));
            *binding = FieldReaderBinding();
            return Status::OK();
        }
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
        if (is_variant_sub) {
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=no_iterator "
                                "logical_field={} stored_field={} query_type={} "
                                "reason=iterator_not_found",
                                field_name, stored_field_name, query_type_to_string(query_type)));
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

    InvertedIndexQueryType effective_query_type = query_type;
    const auto& column_type = data_it->second.second;
    const bool is_text_field =
            column_type != nullptr && is_string_type(column_type->get_storage_field_type());
    auto fb_it = _field_binding_map.find(field_name);
    std::string analyzer_key;
    if (is_text_field && is_variant_sub && fb_it != _field_binding_map.end() &&
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
    if (column_type) {
        reader_result = inverted_iterator->select_best_reader(column_type, effective_query_type,
                                                              is_text_field ? analyzer_key : "");
    } else {
        reader_result = inverted_iterator->select_best_reader(is_text_field ? analyzer_key : "");
    }

    if (!reader_result.has_value()) {
        if (is_variant_sub) {
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=reject "
                                "logical_field={} stored_field={} query_type={} "
                                "effective_query_type={} analyzer_key={} reason={}",
                                field_name, stored_field_name, query_type_to_string(query_type),
                                query_type_to_string(effective_query_type), analyzer_key,
                                reader_result.error().to_string()));
        }
        return reader_result.error();
    }

    auto inverted_reader = reader_result.value();
    if (inverted_reader == nullptr) {
        if (is_variant_sub) {
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=reject "
                                "logical_field={} stored_field={} query_type={} "
                                "effective_query_type={} reason=selected_reader_null",
                                field_name, stored_field_name, query_type_to_string(query_type),
                                query_type_to_string(effective_query_type)));
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "selected reader is null for field '{}'", field_name);
    }

    FieldReaderBinding resolved;
    resolved.logical_field_name = field_name;
    resolved.stored_field_name = stored_field_name;
    resolved.stored_field_wstr = StringHelper::to_wstring(resolved.stored_field_name);
    resolved.column_type = column_type;
    resolved.query_type = effective_query_type;
    resolved.inverted_reader = inverted_reader;
    resolved.binding_key = binding_key;
    resolved.state = SearchFieldBindingState::BOUND;
    if (fb_it != _field_binding_map.end() && fb_it->second->__isset.index_properties &&
        !fb_it->second->index_properties.empty()) {
        resolved.index_properties = fb_it->second->index_properties;
    } else {
        resolved.index_properties = inverted_reader->get_index_properties();
    }
    resolved.analyzer_key =
            normalize_analyzer_key(build_analyzer_key_from_properties(resolved.index_properties));

    auto index_file_reader = inverted_reader->get_index_file_reader();
    if (index_file_reader == nullptr) {
        if (is_variant_sub) {
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=reject "
                                "logical_field={} stored_field={} index_id={} suffix={} "
                                "reason=index_file_reader_null",
                                field_name, stored_field_name, inverted_reader->get_index_id(),
                                inverted_reader->get_index_meta().get_index_suffix()));
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "index file reader is null for field '{}'", field_name);
    }

    if (inverted_reader->type() == InvertedIndexReaderType::BKD) {
        _cache.emplace(binding_key, resolved);
        if (is_variant_sub) {
            bool index_file_exists = false;
            auto probe_status = index_file_reader->index_file_exist(
                    &inverted_reader->get_index_meta(), &index_file_exists);
            add_search_binding_diagnostic(
                    _context,
                    fmt::format("[VariantSearchBinding] phase=field_resolve result=selected_direct "
                                "logical_field={} stored_field={} query_type={} "
                                "effective_query_type={} index_id={} suffix={} reader_type={} "
                                "index_file_exists={} probe_status={} index_file={}",
                                field_name, stored_field_name, query_type_to_string(query_type),
                                query_type_to_string(effective_query_type),
                                inverted_reader->get_index_id(),
                                inverted_reader->get_index_meta().get_index_suffix(),
                                reader_type_to_string(inverted_reader->type()), index_file_exists,
                                probe_status.ok() ? "OK" : probe_status.to_string(),
                                index_file_reader->get_index_file_path(
                                        &inverted_reader->get_index_meta())));
        }
        *binding = resolved;
        return Status::OK();
    }

    auto index_file_key =
            index_file_reader->get_index_file_cache_key(&inverted_reader->get_index_meta());
    InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
    InvertedIndexCacheHandle searcher_cache_handle;

    bool searcher_cache_enabled =
            _context->runtime_state != nullptr &&
            _context->runtime_state->query_options().enable_inverted_index_searcher_cache;

    bool cache_hit = false;
    if (searcher_cache_enabled) {
        int64_t lookup_dummy = 0;
        SCOPED_RAW_TIMER(_context->stats ? &_context->stats->inverted_index_lookup_timer
                                         : &lookup_dummy);
        cache_hit = InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                                   &searcher_cache_handle);
    }

    std::shared_ptr<lucene::index::IndexReader> reader_holder;
    if (cache_hit) {
        if (_context->stats) {
            _context->stats->inverted_index_searcher_cache_hit++;
        }
        auto searcher_variant = searcher_cache_handle.get_index_searcher();
        auto* searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr && *searcher_ptr != nullptr) {
            reader_holder = std::shared_ptr<lucene::index::IndexReader>(
                    (*searcher_ptr)->getReader(), [](lucene::index::IndexReader*) {});
        }
    }

    if (!reader_holder) {
        if (_context->stats) {
            _context->stats->inverted_index_searcher_cache_miss++;
        }
        int64_t dummy_timer = 0;
        SCOPED_RAW_TIMER(_context->stats ? &_context->stats->inverted_index_searcher_open_timer
                                         : &dummy_timer);
        RETURN_IF_ERROR(
                index_file_reader->init(config::inverted_index_read_buffer_size, _context->io_ctx));
        auto directory = DORIS_TRY(
                index_file_reader->open(&inverted_reader->get_index_meta(), _context->io_ctx));

        auto index_searcher_builder = DORIS_TRY(
                IndexSearcherBuilder::create_index_searcher_builder(inverted_reader->type()));
        auto searcher_result =
                DORIS_TRY(index_searcher_builder->get_index_searcher(directory.get()));
        auto reader_size = index_searcher_builder->get_reader_size();

        auto* stream = static_cast<DorisCompoundReader*>(directory.get())->getDorisIndexInput();
        DBUG_EXECUTE_IF(
                "FieldReaderResolver.resolve.io_ctx", ({
                    const auto* cur_io_ctx = (const io::IOContext*)stream->getIoContext();
                    if (cur_io_ctx->file_cache_stats) {
                        if (cur_io_ctx->file_cache_stats != &_context->stats->file_cache_stats) {
                            LOG(FATAL) << "search: io_ctx file_cache_stats mismatch: "
                                       << cur_io_ctx->file_cache_stats << " vs "
                                       << &_context->stats->file_cache_stats;
                        }
                    }
                }));
        stream->setIoContext(nullptr);
        stream->setIndexFile(false);

        auto* cache_value = new InvertedIndexSearcherCache::CacheValue(std::move(searcher_result),
                                                                       reader_size, UnixMillis());
        InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                       &searcher_cache_handle);

        auto new_variant = searcher_cache_handle.get_index_searcher();
        auto* new_ptr = std::get_if<FulltextIndexSearcherPtr>(&new_variant);
        if (new_ptr != nullptr && *new_ptr != nullptr) {
            reader_holder = std::shared_ptr<lucene::index::IndexReader>(
                    (*new_ptr)->getReader(), [](lucene::index::IndexReader*) {});
        }

        if (!reader_holder) {
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "failed to build IndexSearcher for field '{}'", field_name);
        }
    }

    _searcher_cache_handles.push_back(std::move(searcher_cache_handle));

    resolved.lucene_reader = reader_holder;
    _binding_readers[binding_key] = reader_holder;
    _field_readers[resolved.stored_field_wstr] = reader_holder;
    _readers.emplace_back(reader_holder);
    _cache.emplace(binding_key, resolved);
    if (is_variant_sub) {
        bool index_file_exists = false;
        auto probe_status = index_file_reader->index_file_exist(&inverted_reader->get_index_meta(),
                                                                &index_file_exists);
        add_search_binding_diagnostic(
                _context,
                fmt::format(
                        "[VariantSearchBinding] phase=field_resolve result=selected "
                        "logical_field={} stored_field={} query_type={} effective_query_type={} "
                        "index_id={} suffix={} reader_type={} analyzer_key={} "
                        "field_pattern={} index_file_exists={} probe_status={} "
                        "searcher_cache={} index_file={}",
                        field_name, stored_field_name, query_type_to_string(query_type),
                        query_type_to_string(effective_query_type), inverted_reader->get_index_id(),
                        inverted_reader->get_index_meta().get_index_suffix(),
                        reader_type_to_string(inverted_reader->type()), resolved.analyzer_key,
                        inverted_reader->get_index_meta().field_pattern(), index_file_exists,
                        probe_status.ok() ? "OK" : probe_status.to_string(),
                        cache_hit ? "hit" : "miss",
                        index_file_reader->get_index_file_path(
                                &inverted_reader->get_index_meta())));
    }
    *binding = resolved;
    return Status::OK();
}

segment_v2::IndexIterator* VariantSearchNullBitmapAdapter::iterator_for(
        const query_v2::Scorer& /*scorer*/, const std::string& logical_field) const {
    if (logical_field.empty()) {
        return nullptr;
    }
    return _resolver.get_iterator(logical_field);
}

void populate_variant_search_binding_context(const FieldReaderResolver& resolver,
                                             query_v2::QueryExecutionContext* exec_ctx) {
    DCHECK(exec_ctx != nullptr);
    exec_ctx->readers = resolver.readers();
    exec_ctx->reader_bindings = resolver.reader_bindings();
    exec_ctx->field_reader_bindings = resolver.field_readers();
    for (const auto& [binding_key, binding] : resolver.binding_cache()) {
        if (binding_key.empty()) {
            continue;
        }
        query_v2::FieldBindingContext binding_ctx;
        binding_ctx.logical_field_name = binding.logical_field_name;
        binding_ctx.stored_field_name = binding.stored_field_name;
        binding_ctx.stored_field_wstr = binding.stored_field_wstr;
        exec_ctx->binding_fields.emplace(binding_key, std::move(binding_ctx));
    }
}

query_v2::QueryExecutionContext build_variant_search_query_execution_context(
        uint32_t segment_num_rows, const FieldReaderResolver& resolver,
        query_v2::NullBitmapResolver* null_resolver) {
    query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = segment_num_rows;
    populate_variant_search_binding_context(resolver, &exec_ctx);
    exec_ctx.null_resolver = null_resolver;
    return exec_ctx;
}

namespace {

class VariantNestedDocMappingWeight final : public query_v2::Weight {
public:
    VariantNestedDocMappingWeight(
            query_v2::WeightPtr child_weight,
            std::vector<const segment_v2::NestedGroupReader*> child_to_parent_chain,
            const segment_v2::NestedGroupReadProvider* read_provider,
            segment_v2::ColumnIteratorOptions column_iter_opts)
            : _child_weight(std::move(child_weight)),
              _child_to_parent_chain(std::move(child_to_parent_chain)),
              _read_provider(read_provider),
              _column_iter_opts(std::move(column_iter_opts)) {}

    query_v2::ScorerPtr scorer(const query_v2::QueryExecutionContext& context,
                               const std::string& binding_key) override {
        if (_child_weight == nullptr || _read_provider == nullptr ||
            _child_to_parent_chain.empty()) {
            return std::make_shared<query_v2::EmptyScorer>();
        }

        auto child_scorer = _child_weight->scorer(context, binding_key);
        if (child_scorer == nullptr) {
            return std::make_shared<query_v2::EmptyScorer>();
        }

        roaring::Roaring child_true;
        uint32_t doc = child_scorer->doc();
        while (doc != query_v2::TERMINATED) {
            child_true.add(doc);
            doc = child_scorer->advance();
        }

        auto mapped_true = std::make_shared<roaring::Roaring>();
        if (!child_true.isEmpty()) {
            auto status = _read_provider->map_elements_to_parent_ords(
                    _child_to_parent_chain, _column_iter_opts, child_true, mapped_true.get());
            if (!status.ok()) {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "failed to map nested search true bitmap: {}", status.to_string());
            }
        }

        std::shared_ptr<roaring::Roaring> mapped_null;
        if (child_scorer->has_null_bitmap(context.null_resolver)) {
            const auto* child_null = child_scorer->get_null_bitmap(context.null_resolver);
            if (child_null != nullptr && !child_null->isEmpty()) {
                mapped_null = std::make_shared<roaring::Roaring>();
                auto status = _read_provider->map_elements_to_parent_ords(
                        _child_to_parent_chain, _column_iter_opts, *child_null, mapped_null.get());
                if (!status.ok()) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "failed to map nested search null bitmap: {}",
                                    status.to_string());
                }
                *mapped_null -= *mapped_true;
                if (mapped_null->isEmpty()) {
                    mapped_null.reset();
                }
            }
        }

        if (mapped_true->isEmpty() && (mapped_null == nullptr || mapped_null->isEmpty())) {
            return std::make_shared<query_v2::EmptyScorer>();
        }
        return std::make_shared<query_v2::BitSetScorer>(std::move(mapped_true),
                                                        std::move(mapped_null));
    }

private:
    query_v2::WeightPtr _child_weight;
    std::vector<const segment_v2::NestedGroupReader*> _child_to_parent_chain;
    const segment_v2::NestedGroupReadProvider* _read_provider;
    segment_v2::ColumnIteratorOptions _column_iter_opts;
};

class VariantNestedDocMappingQuery final : public query_v2::Query {
public:
    VariantNestedDocMappingQuery(
            query_v2::QueryPtr child_query,
            std::vector<const segment_v2::NestedGroupReader*> child_to_parent_chain,
            const segment_v2::NestedGroupReadProvider* read_provider,
            segment_v2::ColumnIteratorOptions column_iter_opts)
            : _child_query(std::move(child_query)),
              _child_to_parent_chain(std::move(child_to_parent_chain)),
              _read_provider(read_provider),
              _column_iter_opts(std::move(column_iter_opts)) {}

    query_v2::WeightPtr weight(bool enable_scoring) override {
        if (_child_query == nullptr) {
            return nullptr;
        }
        return std::make_shared<VariantNestedDocMappingWeight>(_child_query->weight(enable_scoring),
                                                               _child_to_parent_chain,
                                                               _read_provider, _column_iter_opts);
    }

private:
    query_v2::QueryPtr _child_query;
    std::vector<const segment_v2::NestedGroupReader*> _child_to_parent_chain;
    const segment_v2::NestedGroupReadProvider* _read_provider;
    segment_v2::ColumnIteratorOptions _column_iter_opts;
};

bool starts_with_root_field(const std::string& logical_field_name, const std::string& root_field) {
    if (logical_field_name == root_field) {
        return true;
    }
    return logical_field_name.size() > root_field.size() &&
           logical_field_name.compare(0, root_field.size(), root_field) == 0 &&
           logical_field_name[root_field.size()] == '.';
}

} // namespace

query_v2::QueryPtr make_variant_nested_doc_mapping_query(
        query_v2::QueryPtr child_query,
        std::vector<const segment_v2::NestedGroupReader*> child_to_parent_chain,
        const segment_v2::NestedGroupReadProvider* read_provider,
        segment_v2::ColumnIteratorOptions column_iter_opts) {
    if (child_to_parent_chain.empty()) {
        return child_query;
    }
    return std::make_shared<VariantNestedDocMappingQuery>(
            std::move(child_query), std::move(child_to_parent_chain), read_provider,
            std::move(column_iter_opts));
}

Status map_variant_nested_leaf_query_to_active_group(const VariantNestedDocMapperContext& context,
                                                     const std::string& logical_field_name,
                                                     query_v2::QueryPtr* query) {
    if (query == nullptr || *query == nullptr || context.variant_reader == nullptr ||
        context.read_provider == nullptr || context.active_group_chain.empty() ||
        context.root_field.empty()) {
        return Status::OK();
    }
    if (!starts_with_root_field(logical_field_name, context.root_field)) {
        return Status::OK();
    }

    std::string relative_path;
    if (logical_field_name.size() > context.root_field.size()) {
        relative_path = logical_field_name.substr(context.root_field.size() + 1);
    }
    if (relative_path.empty()) {
        return Status::OK();
    }

    auto [found, leaf_group_chain, _] =
            context.variant_reader->collect_nested_group_chain(relative_path);
    if (!found) {
        return Status::OK();
    }
    if (leaf_group_chain.size() < context.active_group_chain.size()) {
        return Status::InvalidArgument(
                "nested search leaf field '{}' is outside active nested path", logical_field_name);
    }
    for (size_t i = 0; i < context.active_group_chain.size(); ++i) {
        if (leaf_group_chain[i] != context.active_group_chain[i]) {
            return Status::InvalidArgument(
                    "nested search leaf field '{}' is outside active nested path",
                    logical_field_name);
        }
    }
    if (leaf_group_chain.size() == context.active_group_chain.size()) {
        return Status::OK();
    }

    std::vector<const segment_v2::NestedGroupReader*> child_to_parent_chain(
            leaf_group_chain.begin() + context.active_group_chain.size(), leaf_group_chain.end());
    *query = make_variant_nested_doc_mapping_query(std::move(*query),
                                                   std::move(child_to_parent_chain),
                                                   context.read_provider, context.column_iter_opts);
    return Status::OK();
}

Status VariantNestedSearchEvaluator::evaluate(
        const TSearchParam& search_param, const TSearchClause& nested_clause,
        const std::shared_ptr<segment_v2::IndexQueryContext>& context,
        FieldReaderResolver& resolver, uint32_t num_rows, const IndexExecContext* index_exec_ctx,
        const std::unordered_map<std::string, int>& field_name_to_column_id,
        std::shared_ptr<roaring::Roaring>& result_bitmap) const {
    (void)num_rows;
    (void)field_name_to_column_id;
    if (!(nested_clause.__isset.nested_path)) {
        return Status::InvalidArgument("NESTED clause missing nested_path");
    }
    if (!(nested_clause.__isset.children) || nested_clause.children.empty()) {
        return Status::InvalidArgument("NESTED clause missing inner query");
    }
    if (result_bitmap == nullptr) {
        result_bitmap = std::make_shared<roaring::Roaring>();
    } else {
        *result_bitmap = roaring::Roaring();
    }

    std::string root_field = nested_clause.nested_path;
    auto dot_pos = nested_clause.nested_path.find('.');
    if (dot_pos != std::string::npos) {
        root_field = nested_clause.nested_path.substr(0, dot_pos);
    }
    if (index_exec_ctx == nullptr || index_exec_ctx->segment() == nullptr) {
        return Status::InvalidArgument("NESTED query requires IndexExecContext with valid segment");
    }
    auto* segment = index_exec_ctx->segment();
    const int32_t ordinal = segment->tablet_schema()->field_index(root_field);
    if (ordinal < 0) {
        return Status::InvalidArgument("Column '{}' not found in tablet schema for nested query",
                                       root_field);
    }
    const ColumnId column_id = static_cast<ColumnId>(ordinal);

    std::shared_ptr<segment_v2::ColumnReader> column_reader;
    RETURN_IF_ERROR(segment->get_column_reader(segment->tablet_schema()->column(column_id),
                                               &column_reader,
                                               index_exec_ctx->column_iter_opts().stats));
    auto* variant_reader = dynamic_cast<segment_v2::VariantColumnReader*>(column_reader.get());
    if (variant_reader == nullptr) {
        return Status::InvalidArgument("Column '{}' is not VARIANT for nested query", root_field);
    }

    std::string array_path;
    if (dot_pos == std::string::npos) {
        array_path = std::string(segment_v2::kRootNestedGroupPath);
    } else {
        array_path = nested_clause.nested_path.substr(dot_pos + 1);
    }

    auto [found, group_chain, _] = variant_reader->collect_nested_group_chain(array_path);
    if (!found || group_chain.empty()) {
        return Status::OK();
    }

    auto read_provider = segment_v2::create_nested_group_read_provider();
    if (!read_provider || !read_provider->should_enable_nested_group_read_path()) {
        return Status::NotSupported(
                "NestedGroup search is an enterprise capability, not available in this build");
    }

    auto& leaf_group = group_chain.back();
    uint64_t total_elements = 0;
    RETURN_IF_ERROR(read_provider->get_total_elements(index_exec_ctx->column_iter_opts(),
                                                      leaf_group, &total_elements));
    if (total_elements == 0) {
        return Status::OK();
    }
    if (total_elements > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("nested element_count exceeds uint32_t max");
    }

    std::string default_operator = "or";
    if (search_param.__isset.default_operator && !search_param.default_operator.empty()) {
        default_operator = search_param.default_operator;
    }
    int32_t minimum_should_match = -1;
    if (search_param.__isset.minimum_should_match) {
        minimum_should_match = search_param.minimum_should_match;
    }

    query_v2::QueryPtr inner_query;
    std::string inner_binding_key;
    VariantNestedDocMapperContext mapper_context;
    mapper_context.root_field = root_field;
    mapper_context.active_group_chain = group_chain;
    mapper_context.variant_reader = variant_reader;
    mapper_context.read_provider = read_provider.get();
    mapper_context.column_iter_opts = index_exec_ctx->column_iter_opts();
    resolver.set_leaf_query_mapper(
            [mapper_context](const std::string& logical_field_name, query_v2::QueryPtr* query) {
                return map_variant_nested_leaf_query_to_active_group(mapper_context,
                                                                     logical_field_name, query);
            });
    struct ScopedLeafMapperReset {
        FieldReaderResolver& resolver;
        ~ScopedLeafMapperReset() { resolver.set_leaf_query_mapper(nullptr); }
    } mapper_reset {resolver};
    RETURN_IF_ERROR(_function_search.build_query_recursive(
            nested_clause.children[0], context, resolver, &inner_query, &inner_binding_key,
            default_operator, minimum_should_match, static_cast<uint32_t>(total_elements)));
    if (inner_query == nullptr) {
        return Status::OK();
    }

    VariantSearchNullBitmapAdapter null_resolver(resolver);
    query_v2::QueryExecutionContext exec_ctx = build_variant_search_query_execution_context(
            static_cast<uint32_t>(total_elements), resolver, &null_resolver);

    auto weight = inner_query->weight(false);
    if (!weight) {
        return Status::OK();
    }
    auto scorer = weight->scorer(exec_ctx, inner_binding_key);
    if (!scorer) {
        return Status::OK();
    }

    roaring::Roaring element_bitmap;
    uint32_t doc = scorer->doc();
    while (doc != query_v2::TERMINATED) {
        element_bitmap.add(doc);
        doc = scorer->advance();
    }

    if (scorer->has_null_bitmap(exec_ctx.null_resolver)) {
        const auto* bitmap = scorer->get_null_bitmap(exec_ctx.null_resolver);
        if (bitmap != nullptr && !bitmap->isEmpty()) {
            element_bitmap -= *bitmap;
        }
    }

    roaring::Roaring parent_bitmap;
    RETURN_IF_ERROR(read_provider->map_elements_to_parent_ords(
            group_chain, index_exec_ctx->column_iter_opts(), element_bitmap, &parent_bitmap));
    *result_bitmap = std::move(parent_bitmap);
    return Status::OK();
}

} // namespace doris
