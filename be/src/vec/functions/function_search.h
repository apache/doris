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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

CL_NS_USE(index)
namespace doris::vectorized {

using namespace doris::segment_v2;

struct FieldReaderBinding {
    std::string logical_field_name;
    std::string stored_field_name;
    std::wstring stored_field_wstr;
    vectorized::DataTypePtr column_type;
    InvertedIndexQueryType query_type;
    InvertedIndexReaderPtr inverted_reader;
    std::shared_ptr<lucene::index::IndexReader> lucene_reader;
    std::map<std::string, std::string> index_properties;
    std::string binding_key;
};

class FieldReaderResolver {
public:
    FieldReaderResolver(
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    data_type_with_names,
            const std::unordered_map<std::string, IndexIterator*>& iterators,
            std::shared_ptr<IndexQueryContext> context)
            : _data_type_with_names(data_type_with_names),
              _iterators(iterators),
              _context(std::move(context)) {}

    Status resolve(const std::string& field_name, InvertedIndexQueryType query_type,
                   FieldReaderBinding* binding);

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

private:
    std::string binding_key_for(const std::string& stored_field_name,
                                InvertedIndexQueryType query_type) const {
        return stored_field_name + "#" + std::to_string(static_cast<int>(query_type));
    }

    const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
            _data_type_with_names;
    const std::unordered_map<std::string, IndexIterator*>& _iterators;
    std::shared_ptr<IndexQueryContext> _context;
    std::unordered_map<std::string, FieldReaderBinding> _cache;
    std::vector<std::shared_ptr<lucene::index::IndexReader>> _readers;
    std::unordered_map<std::string, std::shared_ptr<lucene::index::IndexReader>> _binding_readers;
    std::unordered_map<std::wstring, std::shared_ptr<lucene::index::IndexReader>> _field_readers;
};

class FunctionSearch : public IFunction {
public:
    static constexpr auto name = "search";

    static FunctionPtr create() { return std::make_shared<FunctionSearch>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    // We manage nulls explicitly for index pushdown only.
    bool use_default_implementation_for_nulls() const override { return false; }
    bool is_use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_constants() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& /*block*/,
                        const ColumnNumbers& /*arguments*/, uint32_t /*result*/,
                        size_t /*input_rows_count*/) const override;

    bool can_push_down_to_index() const override { return true; }

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<IndexIterator*> iterators, uint32_t num_rows,
            InvertedIndexResultBitmap& bitmap_result) const override;

    Status evaluate_inverted_index_with_search_param(
            const TSearchParam& search_param,
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    data_type_with_names,
            std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
            InvertedIndexResultBitmap& bitmap_result) const;

    // Public methods for testing
    enum class ClauseTypeCategory {
        NON_TOKENIZED, // TERM, PREFIX, WILDCARD, REGEXP, RANGE, LIST - no tokenization, use EQUAL_QUERY
        TOKENIZED,     // PHRASE, MATCH, ANY, ALL - need tokenization, use MATCH_ANY_QUERY
        COMPOUND       // AND, OR, NOT - boolean operations
    };

    ClauseTypeCategory get_clause_type_category(const std::string& clause_type) const;

    // Analyze query type for a specific field in the search clause
    InvertedIndexQueryType analyze_field_query_type(const std::string& field_name,
                                                    const TSearchClause& clause) const;

    // Map clause_type string to InvertedIndexQueryType
    InvertedIndexQueryType clause_type_to_query_type(const std::string& clause_type) const;

    Status build_query_recursive(const FunctionSearch& function, const TSearchClause& clause,
                                 const std::shared_ptr<IndexQueryContext>& context,
                                 FieldReaderResolver& resolver,
                                 inverted_index::query_v2::QueryPtr* out,
                                 std::string* binding_key) const;

    Status build_leaf_query(const FunctionSearch& function, const TSearchClause& clause,
                            const std::shared_ptr<IndexQueryContext>& context,
                            FieldReaderResolver& resolver, inverted_index::query_v2::QueryPtr* out,
                            std::string* binding_key) const;

    Status collect_query_null_bitmap(
            const TSearchClause& clause,
            const std::unordered_map<std::string, IndexIterator*>& iterators,
            std::shared_ptr<roaring::Roaring>& null_bitmap) const;

    bool should_mask_null_values(const TSearchClause& clause) const;

private:
    Status collect_query_null_bitmap_internal(
            const TSearchClause& clause,
            const std::unordered_map<std::string, IndexIterator*>& iterators,
            std::shared_ptr<roaring::Roaring>& null_bitmap, bool collect_nulls) const;

    bool is_or_clause_null_safe(const TSearchClause& clause) const;
};

} // namespace doris::vectorized
