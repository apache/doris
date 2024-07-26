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

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vmatch_predicate.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

const std::string MATCH_ANY_FUNCTION = "match_any";
const std::string MATCH_ALL_FUNCTION = "match_all";
const std::string MATCH_PHRASE_FUNCTION = "match_phrase";
const std::string MATCH_PHRASE_PREFIX_FUNCTION = "match_phrase_prefix";
const std::string MATCH_PHRASE_REGEXP_FUNCTION = "match_regexp";

class FunctionMatchBase : public IFunction {
public:
    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    String get_name() const override { return "match"; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override;

    virtual Status execute_match(FunctionContext* context, const std::string& column_name,
                                 const std::string& match_query_str, size_t input_rows_count,
                                 const ColumnString* string_col,
                                 InvertedIndexCtx* inverted_index_ctx,
                                 const ColumnArray::Offsets64* array_offsets,
                                 ColumnUInt8::Container& result) const = 0;

    doris::segment_v2::InvertedIndexQueryType get_query_type_from_fn_name() const;

    std::vector<std::string> analyse_data_token(const std::string& column_name,
                                                InvertedIndexCtx* inverted_index_ctx,
                                                const ColumnString* string_col,
                                                int32_t current_block_row_idx,
                                                const ColumnArray::Offsets64* array_offsets,
                                                int32_t& current_src_array_offset) const;

    Status check(FunctionContext* context, const std::string& function_name) const;
};

class FunctionMatchAny : public FunctionMatchBase {
public:
    static constexpr auto name = "match_any";
    static FunctionPtr create() { return std::make_shared<FunctionMatchAny>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override;
};

class FunctionMatchAll : public FunctionMatchBase {
public:
    static constexpr auto name = "match_all";
    static FunctionPtr create() { return std::make_shared<FunctionMatchAll>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override;
};

class FunctionMatchPhrase : public FunctionMatchBase {
public:
    static constexpr auto name = "match_phrase";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override;
};

class FunctionMatchPhrasePrefix : public FunctionMatchBase {
public:
    static constexpr auto name = "match_phrase_prefix";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrasePrefix>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override;
};

class FunctionMatchRegexp : public FunctionMatchBase {
public:
    static constexpr auto name = "match_regexp";
    static FunctionPtr create() { return std::make_shared<FunctionMatchRegexp>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override;
};

class FunctionMatchPhraseEdge : public FunctionMatchBase {
public:
    static constexpr auto name = "match_phrase_edge";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhraseEdge>(); }

    String get_name() const override { return name; }

    Status execute_match(FunctionContext* context, const std::string& column_name,
                         const std::string& match_query_str, size_t input_rows_count,
                         const ColumnString* string_col, InvertedIndexCtx* inverted_index_ctx,
                         const ColumnArray::Offsets64* array_offsets,
                         ColumnUInt8::Container& result) const override {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "FunctionMatchPhraseEdge not support execute_match");
    }
};

} // namespace doris::vectorized
