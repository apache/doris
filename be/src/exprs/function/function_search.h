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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/variant_inverted_index_search.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/query_v2/boolean_query/operator_boolean_query.h"

CL_NS_USE(index)
namespace doris {

using namespace doris::segment_v2;

class IndexExecContext;

class FunctionSearch : public IFunction {
public:
    static constexpr auto name = "search";

    static FunctionPtr create() { return std::make_shared<FunctionSearch>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    // We manage nulls explicitly for index pushdown only.
    bool use_default_implementation_for_nulls() const override { return false; }
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
            const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<IndexIterator*> iterators, uint32_t num_rows,
            const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/,
            InvertedIndexResultBitmap& bitmap_result) const override;

    Status evaluate_inverted_index_with_search_param(
            const TSearchParam& search_param,
            const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
            std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
            InvertedIndexResultBitmap& bitmap_result, bool enable_cache = true) const;

    Status evaluate_inverted_index_with_search_param(
            const TSearchParam& search_param,
            const std::unordered_map<std::string, IndexFieldNameAndTypePair>& data_type_with_names,
            std::unordered_map<std::string, IndexIterator*> iterators, uint32_t num_rows,
            InvertedIndexResultBitmap& bitmap_result, bool enable_cache,
            const IndexExecContext* index_exec_ctx,
            const std::unordered_map<std::string, int>& field_name_to_column_id,
            const std::shared_ptr<IndexQueryContext>& index_query_context = nullptr) const;

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

    Status build_query_recursive(const TSearchClause& clause,
                                 const std::shared_ptr<IndexQueryContext>& context,
                                 FieldReaderResolver& resolver,
                                 inverted_index::query_v2::QueryPtr* out, std::string* binding_key,
                                 const std::string& default_operator, int32_t minimum_should_match,
                                 uint32_t num_rows = 0) const;

    Status build_leaf_query(const TSearchClause& clause,
                            const std::shared_ptr<IndexQueryContext>& context,
                            FieldReaderResolver& resolver, inverted_index::query_v2::QueryPtr* out,
                            std::string* binding_key, const std::string& default_operator,
                            int32_t minimum_should_match, uint32_t num_rows = 0) const;
};

} // namespace doris
