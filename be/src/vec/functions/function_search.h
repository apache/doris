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

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/boolean_query/boolean_query.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

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
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override;

    Status evaluate_inverted_index_with_search_param(
            const TSearchParam& search_param,
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    data_type_with_names,
            std::unordered_map<std::string, segment_v2::IndexIterator*> iterators,
            uint32_t num_rows, segment_v2::InvertedIndexResultBitmap& bitmap_result) const;

private:
    std::shared_ptr<segment_v2::inverted_index::query_v2::BooleanQuery> build_query_from_clause(
            const TSearchClause& clause,
            const std::shared_ptr<segment_v2::IndexQueryContext>& context,
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    data_type_with_names,
            const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const;

    std::shared_ptr<segment_v2::inverted_index::query_v2::Query> build_leaf_query(
            const TSearchClause& clause,
            const std::shared_ptr<segment_v2::IndexQueryContext>& context,
            const std::unordered_map<std::string, vectorized::IndexFieldNameAndTypePair>&
                    data_type_with_names,
            const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const;

    enum class ClauseTypeCategory {
        NON_TOKENIZED, // TERM, PREFIX, WILDCARD, REGEXP, RANGE, LIST - no tokenization, use EQUAL_QUERY
        TOKENIZED,     // PHRASE, MATCH, ANY, ALL - need tokenization, use MATCH_ANY_QUERY
        COMPOUND       // AND, OR, NOT - boolean operations
    };

    ClauseTypeCategory get_clause_type_category(const std::string& clause_type) const;

    segment_v2::InvertedIndexQueryType analyze_query_type(const TSearchClause& clause) const;

    segment_v2::InvertedIndexReaderPtr get_field_inverted_reader(
            const std::string& field_name,
            const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators,
            const vectorized::DataTypePtr& column_type = nullptr,
            segment_v2::InvertedIndexQueryType query_type =
                    segment_v2::InvertedIndexQueryType::EQUAL_QUERY) const;

    std::map<std::string, std::string> get_field_index_properties(
            const std::string& field_name,
            const std::unordered_map<std::string, segment_v2::IndexIterator*>& iterators) const;
};

} // namespace doris::vectorized
