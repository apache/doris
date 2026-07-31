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

#include <CLucene.h>
#include <gen_cpp/Exprs_types.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/columns_with_type_and_name.h"
#include "core/data_type/data_type.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query_v2/query.h"
#include "storage/index/inverted/query_v2/weight.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"

namespace doris::segment_v2::inverted_index::query_v2 {
class Query;
}

namespace doris::segment_v2 {
class NestedGroupReadProvider;
struct NestedGroupReader;
class VariantColumnReader;
} // namespace doris::segment_v2

namespace doris {

using namespace doris::segment_v2;

class FunctionSearch;
class IndexExecContext;

using SearchLeafQueryMapper = std::function<Status(
        const std::string&, std::shared_ptr<segment_v2::inverted_index::query_v2::Query>*)>;

enum class SearchFieldBindingState {
    BOUND,
    MISSING_IN_SEGMENT,
};

struct FieldReaderBinding {
    std::string logical_field_name;
    std::string stored_field_name;
    std::wstring stored_field_wstr;
    DataTypePtr column_type;
    InvertedIndexQueryType query_type;
    InvertedIndexReaderPtr inverted_reader;
    std::shared_ptr<lucene::index::IndexReader> lucene_reader;
    std::map<std::string, std::string> index_properties;
    std::string binding_key;
    std::string analyzer_key;
    SearchFieldBindingState state = SearchFieldBindingState::MISSING_IN_SEGMENT;

    bool is_bound() const {
        return state == SearchFieldBindingState::BOUND || inverted_reader != nullptr ||
               lucene_reader != nullptr;
    }
    bool use_direct_index_reader() const {
        return is_bound() && inverted_reader != nullptr && lucene_reader == nullptr;
    }
};

class FieldReaderResolver {
public:
    FieldReaderResolver(
            const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
            const std::unordered_map<std::string, IndexIterator*>& iterators,
            std::shared_ptr<IndexQueryContext> context,
            const std::vector<TSearchFieldBinding>& field_bindings = {});

    Status resolve(const std::string& field_name, InvertedIndexQueryType query_type,
                   FieldReaderBinding* binding);

    bool is_variant_subcolumn(const std::string& field_name) const {
        return _variant_subcolumn_fields.count(field_name) > 0;
    }

    const std::vector<std::shared_ptr<lucene::index::IndexReader>>& readers() const {
        return _readers;
    }

    const std::unordered_map<std::string, std::shared_ptr<lucene::index::IndexReader>>&
    reader_bindings() const {
        return _binding_readers;
    }

    const std::unordered_map<std::wstring, std::shared_ptr<lucene::index::IndexReader>>&
    field_readers() const {
        return _field_readers;
    }

    const std::unordered_map<std::string, FieldReaderBinding>& binding_cache() const {
        return _cache;
    }

    IndexIterator* get_iterator(const std::string& field_name) const {
        auto it = _iterators.find(field_name);
        return (it != _iterators.end()) ? it->second : nullptr;
    }

    void set_leaf_query_mapper(SearchLeafQueryMapper mapper) {
        _leaf_query_mapper = std::move(mapper);
    }

    Status map_leaf_query(
            const std::string& field_name,
            std::shared_ptr<segment_v2::inverted_index::query_v2::Query>* query) const {
        if (!_leaf_query_mapper || query == nullptr || *query == nullptr) {
            return Status::OK();
        }
        return _leaf_query_mapper(field_name, query);
    }

private:
    std::string binding_key_for(const std::string& stored_field_name,
                                InvertedIndexQueryType query_type) const {
        return stored_field_name + "#" + std::to_string(static_cast<int>(query_type));
    }

    const std::unordered_map<std::string, IndexFieldNameAndTypePair>& _data_type_with_names;
    const std::unordered_map<std::string, IndexIterator*>& _iterators;
    std::shared_ptr<IndexQueryContext> _context;
    std::vector<TSearchFieldBinding> _field_bindings;
    std::unordered_map<std::string, const TSearchFieldBinding*> _field_binding_map;
    std::unordered_set<std::string> _variant_subcolumn_fields;
    std::unordered_map<std::string, FieldReaderBinding> _cache;
    std::vector<std::shared_ptr<lucene::index::IndexReader>> _readers;
    std::unordered_map<std::string, std::shared_ptr<lucene::index::IndexReader>> _binding_readers;
    std::unordered_map<std::wstring, std::shared_ptr<lucene::index::IndexReader>> _field_readers;
    std::vector<segment_v2::InvertedIndexCacheHandle> _searcher_cache_handles;
    SearchLeafQueryMapper _leaf_query_mapper;
};

class VariantSearchNullBitmapAdapter final : public inverted_index::query_v2::NullBitmapResolver {
public:
    explicit VariantSearchNullBitmapAdapter(const FieldReaderResolver& resolver)
            : _resolver(resolver) {}

    segment_v2::IndexIterator* iterator_for(const inverted_index::query_v2::Scorer& scorer,
                                            const std::string& logical_field) const override;

private:
    const FieldReaderResolver& _resolver;
};

void populate_variant_search_binding_context(
        const FieldReaderResolver& resolver,
        inverted_index::query_v2::QueryExecutionContext* exec_ctx);

inverted_index::query_v2::QueryExecutionContext build_variant_search_query_execution_context(
        uint32_t segment_num_rows, const FieldReaderResolver& resolver,
        inverted_index::query_v2::NullBitmapResolver* null_resolver);

struct VariantNestedDocMapperContext {
    std::string root_field;
    std::vector<const segment_v2::NestedGroupReader*> active_group_chain;
    const segment_v2::VariantColumnReader* variant_reader = nullptr;
    const segment_v2::NestedGroupReadProvider* read_provider = nullptr;
    segment_v2::ColumnIteratorOptions column_iter_opts;
};

Status map_variant_nested_leaf_query_to_active_group(const VariantNestedDocMapperContext& context,
                                                     const std::string& logical_field_name,
                                                     inverted_index::query_v2::QueryPtr* query);

inverted_index::query_v2::QueryPtr make_variant_nested_doc_mapping_query(
        inverted_index::query_v2::QueryPtr child_query,
        std::vector<const segment_v2::NestedGroupReader*> child_to_parent_chain,
        const segment_v2::NestedGroupReadProvider* read_provider,
        segment_v2::ColumnIteratorOptions column_iter_opts);

class VariantNestedSearchEvaluator {
public:
    explicit VariantNestedSearchEvaluator(const FunctionSearch& function_search)
            : _function_search(function_search) {}

    Status evaluate(const TSearchParam& search_param, const TSearchClause& nested_clause,
                    const std::shared_ptr<segment_v2::IndexQueryContext>& context,
                    FieldReaderResolver& resolver, uint32_t num_rows,
                    const IndexExecContext* index_exec_ctx,
                    const std::unordered_map<std::string, int>& field_name_to_column_id,
                    std::shared_ptr<roaring::Roaring>& result_bitmap) const;

private:
    const FunctionSearch& _function_search;
};

} // namespace doris
