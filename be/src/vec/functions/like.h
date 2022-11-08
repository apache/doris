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

#include <hs/hs.h>

#include <functional>
#include <memory>

#include "runtime/string_search.hpp"
#include "runtime/string_value.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_set.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// TODO: replace with std::string_view when `LikeSearchState.substring_pattern` can
// construct from std::string_view.
struct LikeSearchState {
    char escape_char;

    /// Holds the string the StringValue points to and is set any time StringValue is
    /// used.
    std::string search_string;

    std::string pattern_str;

    /// Used for LIKE predicates if the pattern is a constant argument, and is either a
    /// constant string or has a constant string at the beginning or end of the pattern.
    /// This will be set in order to check for that pattern in the corresponding part of
    /// the string.
    StringRef search_string_sv;

    /// Used for LIKE predicates if the pattern is a constant argument and has a constant
    /// string in the middle of it. This will be use in order to check for the substring
    /// in the value.
    doris::StringSearch substring_pattern;

    /// Used for RLIKE and REGEXP predicates if the pattern is a constant argument.
    std::unique_ptr<re2::RE2> regex;

    template <typename Deleter, Deleter deleter>
    struct HyperscanDeleter {
        template <typename T>
        void operator()(T* ptr) const {
            deleter(ptr);
        }
    };

    // hyperscan compiled pattern database and scratch space, reused for performance
    std::unique_ptr<hs_database_t, HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>
            hs_database;
    std::unique_ptr<hs_scratch_t, HyperscanDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>
            hs_scratch;

    // hyperscan match callback
    static int hs_match_handler(unsigned int /* from */,       // NOLINT
                                unsigned long long /* from */, // NOLINT
                                unsigned long long /* to */,   // NOLINT
                                unsigned int /* flags */, void* ctx) {
        // set result to 1 for matched row
        *((unsigned char*)ctx) = 1;
        /// return non-zero to indicate hyperscan stop after first matched
        return 1;
    }

    LikeSearchState() : escape_char('\\') {}

    Status clone(LikeSearchState& cloned);

    void set_search_string(const std::string& search_string_arg) {
        search_string = search_string_arg;
        search_string_sv = StringRef(search_string);
        substring_pattern.set_pattern(&search_string_sv);
    }
};

using LikeFn = std::function<doris::Status(LikeSearchState*, const ColumnString&,
                                           const StringValue&, ColumnUInt8::Container&)>;

using LikePredicateFn = std::function<doris::Status(
        LikeSearchState*, const PredicateColumnType<TYPE_STRING>&, const StringValue&,
        ColumnUInt8::Container&, uint16_t* sel, size_t sz)>;

using ScalarLikeFn = std::function<doris::Status(LikeSearchState*, const StringRef&,
                                                 const StringValue&, unsigned char*)>;

struct LikeState {
    LikeSearchState search_state;
    LikeFn function;
    // Two functions below are used only for predicate.
    LikePredicateFn predicate_like_function;
    ScalarLikeFn scalar_function;
};

class FunctionLikeBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) override;

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

protected:
    Status vector_const(const ColumnString& values, const StringRef* pattern_val,
                        ColumnUInt8::Container& result, const LikeFn& function,
                        LikeSearchState* search_state);

    Status execute_substring(const ColumnString::Chars& values,
                             const ColumnString::Offsets& value_offsets,
                             ColumnUInt8::Container& result, LikeSearchState* search_state);

    static Status constant_starts_with_fn(LikeSearchState* state, const ColumnString& val,
                                          const StringValue& pattern,
                                          ColumnUInt8::Container& result);

    static Status constant_ends_with_fn(LikeSearchState* state, const ColumnString& val,
                                        const StringValue& pattern, ColumnUInt8::Container& result);

    static Status constant_equals_fn(LikeSearchState* state, const ColumnString& val,
                                     const StringValue& pattern, ColumnUInt8::Container& result);

    static Status constant_substring_fn(LikeSearchState* state, const ColumnString& val,
                                        const StringValue& pattern, ColumnUInt8::Container& result);

    static Status constant_regex_fn(LikeSearchState* state, const ColumnString& val,
                                    const StringValue& pattern, ColumnUInt8::Container& result);

    static Status regexp_fn(LikeSearchState* state, const ColumnString& val,
                            const StringValue& pattern, ColumnUInt8::Container& result);

    // These functions below are used only for predicate.
    static Status constant_regex_fn_predicate(LikeSearchState* state,
                                              const PredicateColumnType<TYPE_STRING>& val,
                                              const StringValue& pattern,
                                              ColumnUInt8::Container& result, uint16_t* sel,
                                              size_t sz);

    static Status regexp_fn_predicate(LikeSearchState* state,
                                      const PredicateColumnType<TYPE_STRING>& val,
                                      const StringValue& pattern, ColumnUInt8::Container& result,
                                      uint16_t* sel, size_t sz);

    static Status constant_starts_with_fn_predicate(LikeSearchState* state,
                                                    const PredicateColumnType<TYPE_STRING>& val,
                                                    const StringValue& pattern,
                                                    ColumnUInt8::Container& result, uint16_t* sel,
                                                    size_t sz);

    static Status constant_ends_with_fn_predicate(LikeSearchState* state,
                                                  const PredicateColumnType<TYPE_STRING>& val,
                                                  const StringValue& pattern,
                                                  ColumnUInt8::Container& result, uint16_t* sel,
                                                  size_t sz);

    static Status constant_equals_fn_predicate(LikeSearchState* state,
                                               const PredicateColumnType<TYPE_STRING>& val,
                                               const StringValue& pattern,
                                               ColumnUInt8::Container& result, uint16_t* sel,
                                               size_t sz);

    static Status constant_substring_fn_predicate(LikeSearchState* state,
                                                  const PredicateColumnType<TYPE_STRING>& val,
                                                  const StringValue& pattern,
                                                  ColumnUInt8::Container& result, uint16_t* sel,
                                                  size_t sz);

    static Status constant_starts_with_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                 const StringValue& pattern, unsigned char* result);

    static Status constant_ends_with_fn_scalar(LikeSearchState* state, const StringRef& val,
                                               const StringValue& pattern, unsigned char* result);

    static Status constant_equals_fn_scalar(LikeSearchState* state, const StringRef& val,
                                            const StringValue& pattern, unsigned char* result);

    static Status constant_substring_fn_scalar(LikeSearchState* state, const StringRef& val,
                                               const StringValue& pattern, unsigned char* result);

    static Status constant_regex_fn_scalar(LikeSearchState* state, const StringRef& val,
                                           const StringValue& pattern, unsigned char* result);

    static Status regexp_fn_scalar(LikeSearchState* state, const StringRef& val,
                                   const StringValue& pattern, unsigned char* result);

    // hyperscan compile expression to database and allocate scratch space
    static Status hs_prepare(FunctionContext* context, const char* expression,
                             hs_database_t** database, hs_scratch_t** scratch);
};

class FunctionLike : public FunctionLikeBase {
public:
    static constexpr auto name = "like";

    static FunctionPtr create() { return std::make_shared<FunctionLike>(); }

    String get_name() const override { return name; }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    friend struct LikeSearchState;

private:
    static Status like_fn(LikeSearchState* state, const ColumnString& val,
                          const StringValue& pattern, ColumnUInt8::Container& result);

    static Status like_fn_predicate(LikeSearchState* state,
                                    const PredicateColumnType<TYPE_STRING>& val,
                                    const StringValue& pattern, ColumnUInt8::Container& result,
                                    uint16_t* sel, size_t sz);

    static Status like_fn_scalar(LikeSearchState* state, const StringValue& val,
                                 const StringValue& pattern, unsigned char* result);

    static void convert_like_pattern(LikeSearchState* state, const std::string& pattern,
                                     std::string* re_pattern);

    static void remove_escape_character(std::string* search_string);
};

class FunctionRegexp : public FunctionLikeBase {
public:
    static constexpr auto name = "regexp";

    static FunctionPtr create() { return std::make_shared<FunctionRegexp>(); }

    String get_name() const override { return name; }

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;
};

} // namespace doris::vectorized
