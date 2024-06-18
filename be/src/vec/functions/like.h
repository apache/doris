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

#include <hs/hs_common.h>
#include <hs/hs_runtime.h>
#include <re2/re2.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <functional>
#include <memory>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/string_search.hpp"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

// TODO: replace with std::string_view when `LikeSearchState.substring_pattern` can
// construct from std::string_view.
struct LikeSearchState {
    char escape_char;

    /// Holds the string the StringRef points to and is set any time StringRef is
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

using LikeFn = std::function<doris::Status(LikeSearchState*, const ColumnString&, const StringRef&,
                                           ColumnUInt8::Container&)>;

using ScalarLikeFn = std::function<doris::Status(LikeSearchState*, const StringRef&,
                                                 const StringRef&, unsigned char*)>;

using VectorLikeFn = std::function<doris::Status(const ColumnString&, const ColumnString&,
                                                 ColumnUInt8::Container&)>;

struct LikeState {
    bool is_like_pattern;
    LikeSearchState search_state;
    LikeFn function;
    ScalarLikeFn scalar_function;
};

struct VectorPatternSearchState {
    MutableColumnPtr _search_strings;
    std::string _search_string;
    VectorLikeFn _vector_function;
    bool _pattern_matched;

    VectorPatternSearchState(VectorLikeFn vector_function)
            : _search_strings(ColumnString::create()),
              _vector_function(vector_function),
              _pattern_matched(true) {}

    virtual ~VectorPatternSearchState() = default;

    virtual void like_pattern_match(const std::string& pattern_str) = 0;

    virtual void regexp_pattern_match(const std::string& pattern_str) = 0;
};

using VPatternSearchStateSPtr = std::shared_ptr<VectorPatternSearchState>;

class FunctionLikeBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override;

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    friend struct VectorAllpassSearchState;
    friend struct VectorEqualSearchState;
    friend struct VectorSubStringSearchState;
    friend struct VectorStartsWithSearchState;
    friend struct VectorEndsWithSearchState;

protected:
    Status vector_const(const ColumnString& values, const StringRef* pattern_val,
                        ColumnUInt8::Container& result, const LikeFn& function,
                        LikeSearchState* search_state) const;

    Status vector_non_const(const ColumnString& values, const ColumnString& patterns,
                            ColumnUInt8::Container& result, LikeState* state,
                            size_t input_rows_count) const;

    Status execute_substring(const ColumnString::Chars& values,
                             const ColumnString::Offsets& value_offsets,
                             ColumnUInt8::Container& result, LikeSearchState* search_state) const;

    template <bool LIKE_PATTERN>
    static VPatternSearchStateSPtr pattern_type_recognition(const ColumnString& patterns);

    static Status constant_allpass_fn(LikeSearchState* state, const ColumnString& val,
                                      const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_allpass_fn_scalar(LikeSearchState* state, const StringRef& val,
                                             const StringRef& pattern, unsigned char* result);

    static Status vector_allpass_fn(const ColumnString& vals, const ColumnString& search_strings,
                                    ColumnUInt8::Container& result);

    static Status constant_starts_with_fn(LikeSearchState* state, const ColumnString& val,
                                          const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_starts_with_fn_scalar(LikeSearchState* state, const StringRef& val,
                                                 const StringRef& pattern, unsigned char* result);

    static Status vector_starts_with_fn(const ColumnString& vals,
                                        const ColumnString& search_strings,
                                        ColumnUInt8::Container& result);

    static Status constant_ends_with_fn(LikeSearchState* state, const ColumnString& val,
                                        const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_ends_with_fn_scalar(LikeSearchState* state, const StringRef& val,
                                               const StringRef& pattern, unsigned char* result);

    static Status vector_ends_with_fn(const ColumnString& vals, const ColumnString& search_strings,
                                      ColumnUInt8::Container& result);

    static Status constant_equals_fn(LikeSearchState* state, const ColumnString& val,
                                     const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_equals_fn_scalar(LikeSearchState* state, const StringRef& val,
                                            const StringRef& pattern, unsigned char* result);

    static Status vector_equals_fn(const ColumnString& vals, const ColumnString& search_strings,
                                   ColumnUInt8::Container& result);

    static Status constant_substring_fn(LikeSearchState* state, const ColumnString& val,
                                        const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_substring_fn_scalar(LikeSearchState* state, const StringRef& val,
                                               const StringRef& pattern, unsigned char* result);

    static Status vector_substring_fn(const ColumnString& vals, const ColumnString& search_strings,
                                      ColumnUInt8::Container& result);

    static Status constant_regex_fn(LikeSearchState* state, const ColumnString& val,
                                    const StringRef& pattern, ColumnUInt8::Container& result);

    static Status constant_regex_fn_scalar(LikeSearchState* state, const StringRef& val,
                                           const StringRef& pattern, unsigned char* result);

    static Status regexp_fn(LikeSearchState* state, const ColumnString& val,
                            const StringRef& pattern, ColumnUInt8::Container& result);

    static Status regexp_fn_scalar(LikeSearchState* state, const StringRef& val,
                                   const StringRef& pattern, unsigned char* result);

    // hyperscan compile expression to database and allocate scratch space
    static Status hs_prepare(FunctionContext* context, const char* expression,
                             hs_database_t** database, hs_scratch_t** scratch);
};

class FunctionLike : public FunctionLikeBase {
public:
    static constexpr auto name = "like";

    static FunctionPtr create() { return std::make_shared<FunctionLike>(); }

    String get_name() const override { return name; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;

    friend struct LikeSearchState;
    friend struct VectorAllpassSearchState;
    friend struct VectorEqualSearchState;
    friend struct VectorSubStringSearchState;
    friend struct VectorStartsWithSearchState;
    friend struct VectorEndsWithSearchState;

private:
    static Status like_fn(LikeSearchState* state, const ColumnString& val, const StringRef& pattern,
                          ColumnUInt8::Container& result);

    static Status like_fn_scalar(LikeSearchState* state, const StringRef& val,
                                 const StringRef& pattern, unsigned char* result);

    static void convert_like_pattern(LikeSearchState* state, const std::string& pattern,
                                     std::string* re_pattern);

    static void remove_escape_character(std::string* search_string);
};

class FunctionRegexp : public FunctionLikeBase {
public:
    static constexpr auto name = "regexp";
    static constexpr auto alias = "rlike";

    static FunctionPtr create() { return std::make_shared<FunctionRegexp>(); }

    String get_name() const override { return name; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override;
};

} // namespace doris::vectorized
