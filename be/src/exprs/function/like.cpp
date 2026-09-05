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

#include "exprs/function/like.h"

#include <fmt/format.h>
#include <hs/hs_compile.h>

#include <cstddef>
#include <ostream>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/exec_env.h"
#include "storage/index/inverted/gram/gram_family.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/snii_index_reader.h"
#include "util/hyperscan_util.h"

namespace doris {

// A regex to match any regex pattern is equivalent to a substring search.
static const RE2 SUBSTRING_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 ENDS_WITH_RE(R"((?:\.\*)*([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)");

// A regex to match any regex pattern which is equivalent to matching a constant string
// at the end of the string values.
static const RE2 STARTS_WITH_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)(?:\.\*)*)");

// A regex to match any regex pattern which is equivalent to a constant string match.
static const RE2 EQUALS_RE(R"(\^([^\.\^\{\[\(\|\)\]\}\+\*\?\$\\]*)\$)");
// A regex to match .*
static const RE2 ALLPASS_RE(R"((\.\*)+)");

// Like patterns
static const re2::RE2 LIKE_SUBSTRING_RE(R"((?:%+)(((\\_)|([^%_\\]))+)(?:%+))");
static const re2::RE2 LIKE_ENDS_WITH_RE("(?:%+)(((\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_STARTS_WITH_RE(R"((((\\%)|(\\_)|([^%_\\]))+)(?:%+))");
static const re2::RE2 LIKE_EQUALS_RE("(((\\\\_)|([^%_]))+)");
static const re2::RE2 LIKE_ALLPASS_RE("%+");

struct VectorAllpassSearchState : public VectorPatternSearchState {
    VectorAllpassSearchState() : VectorPatternSearchState(FunctionLikeBase::vector_allpass_fn) {}

    ~VectorAllpassSearchState() override = default;

    void like_pattern_match(const std::string& pattern_str) override {
        if (!pattern_str.empty() && RE2::FullMatch(pattern_str, LIKE_ALLPASS_RE)) {
            _search_strings->insert_default();
        } else {
            _pattern_matched = false;
        }
    }

    void regexp_pattern_match(const std::string& pattern_str) override {
        if (RE2::FullMatch(pattern_str, ALLPASS_RE)) {
            _search_strings->insert_default();
        } else {
            _pattern_matched = false;
        }
    }
};

struct VectorEqualSearchState : public VectorPatternSearchState {
    VectorEqualSearchState() : VectorPatternSearchState(FunctionLikeBase::vector_equals_fn) {}

    ~VectorEqualSearchState() override = default;

    void like_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (pattern_str.empty() || RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &_search_string)) {
            FunctionLike::remove_escape_character(&_search_string);
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }

    void regexp_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, EQUALS_RE, &_search_string)) {
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }
};

struct VectorSubStringSearchState : public VectorPatternSearchState {
    VectorSubStringSearchState()
            : VectorPatternSearchState(FunctionLikeBase::vector_substring_fn) {}

    ~VectorSubStringSearchState() override = default;

    void like_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &_search_string)) {
            FunctionLike::remove_escape_character(&_search_string);
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }

    void regexp_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &_search_string)) {
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }
};

struct VectorStartsWithSearchState : public VectorPatternSearchState {
    VectorStartsWithSearchState()
            : VectorPatternSearchState(FunctionLikeBase::vector_starts_with_fn) {}

    ~VectorStartsWithSearchState() override = default;

    void like_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &_search_string)) {
            FunctionLike::remove_escape_character(&_search_string);
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }

    void regexp_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &_search_string)) {
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }
};

struct VectorEndsWithSearchState : public VectorPatternSearchState {
    VectorEndsWithSearchState() : VectorPatternSearchState(FunctionLikeBase::vector_ends_with_fn) {}

    ~VectorEndsWithSearchState() override = default;

    void like_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &_search_string)) {
            FunctionLike::remove_escape_character(&_search_string);
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }

    void regexp_pattern_match(const std::string& pattern_str) override {
        _search_string.clear();
        if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &_search_string)) {
            _search_strings->insert_data(_search_string.c_str(), _search_string.size());
        } else {
            _pattern_matched = false;
        }
    }
};

Status LikeSearchState::clone(LikeSearchState& cloned) const {
    cloned.set_search_string(search_string);
    cloned.enable_hyperscan_fallback = enable_hyperscan_fallback;

    std::string re_pattern;
    FunctionLike::convert_like_pattern(this, pattern_str, &re_pattern);
    if (hs_database) { // use hyperscan
        hs_database_t* database = nullptr;
        hs_scratch_t* scratch = nullptr;
        RETURN_IF_ERROR(FunctionLike::hs_prepare(nullptr, re_pattern.c_str(), &database, &scratch));

        cloned.hs_database.reset(database);
        cloned.hs_scratch.reset(scratch);
    } else { // fallback to re2
        cloned.hs_database.reset();
        cloned.hs_scratch.reset();

        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        cloned.regex = std::make_unique<RE2>(re_pattern, opts);
        if (!cloned.regex->ok()) {
            return Status::InternalError("Invalid regex expression: {}", re_pattern);
        }
    }

    return Status::OK();
}

Status FunctionLikeBase::constant_allpass_fn(const LikeSearchState* state, const ColumnString& vals,
                                             const StringRef& pattern,
                                             ColumnUInt8::Container& result) {
    memset(result.data(), 1, vals.size());
    return Status::OK();
}

Status FunctionLikeBase::constant_allpass_fn_scalar(const LikeSearchState* state,
                                                    const StringRef& val, const StringRef& pattern,
                                                    unsigned char* result) {
    *result = 1;
    return Status::OK();
}

Status FunctionLikeBase::vector_allpass_fn(const ColumnString& vals,
                                           const ColumnString& search_strings,
                                           ColumnUInt8::Container& result) {
    DCHECK(vals.size() == search_strings.size());
    DCHECK(vals.size() == result.size());
    memset(result.data(), 1, vals.size());
    return Status::OK();
}

Status FunctionLikeBase::constant_starts_with_fn(const LikeSearchState* state,
                                                 const ColumnString& val, const StringRef& pattern,
                                                 ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        const auto& str_ref = val.get_data_at(i);
        result[i] = (str_ref.size >= state->search_string_sv.size) &&
                    str_ref.start_with(state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_starts_with_fn_scalar(const LikeSearchState* state,
                                                        const StringRef& val,
                                                        const StringRef& pattern,
                                                        unsigned char* result) {
    *result = (val.size >= state->search_string_sv.size) &&
              (state->search_string_sv == val.substring(0, state->search_string_sv.size));
    return Status::OK();
}

Status FunctionLikeBase::vector_starts_with_fn(const ColumnString& vals,
                                               const ColumnString& search_strings,
                                               ColumnUInt8::Container& result) {
    DCHECK(vals.size() == search_strings.size());
    DCHECK(vals.size() == result.size());
    auto sz = vals.size();
    for (size_t i = 0; i < sz; ++i) {
        const auto& str_sv = vals.get_data_at(i);
        const auto& search_string_sv = search_strings.get_data_at(i);
        result[i] = (str_sv.size >= search_string_sv.size) && str_sv.start_with(search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_ends_with_fn(const LikeSearchState* state,
                                               const ColumnString& val, const StringRef& pattern,
                                               ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        const auto& str_ref = val.get_data_at(i);
        result[i] = (str_ref.size >= state->search_string_sv.size) &&
                    str_ref.end_with(state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_ends_with_fn_scalar(const LikeSearchState* state,
                                                      const StringRef& val,
                                                      const StringRef& pattern,
                                                      unsigned char* result) {
    *result = (val.size >= state->search_string_sv.size) &&
              (state->search_string_sv == val.substring(val.size - state->search_string_sv.size,
                                                        state->search_string_sv.size));
    return Status::OK();
}

Status FunctionLikeBase::vector_ends_with_fn(const ColumnString& vals,
                                             const ColumnString& search_strings,
                                             ColumnUInt8::Container& result) {
    DCHECK(vals.size() == search_strings.size());
    DCHECK(vals.size() == result.size());
    auto sz = vals.size();
    for (size_t i = 0; i < sz; ++i) {
        const auto& str_sv = vals.get_data_at(i);
        const auto& search_string_sv = search_strings.get_data_at(i);
        result[i] = (str_sv.size >= search_string_sv.size) && str_sv.end_with(search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_equals_fn(const LikeSearchState* state, const ColumnString& val,
                                            const StringRef& pattern,
                                            ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        result[i] = (val.get_data_at(i) == state->search_string_sv);
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_equals_fn_scalar(const LikeSearchState* state,
                                                   const StringRef& val, const StringRef& pattern,
                                                   unsigned char* result) {
    *result = (val == state->search_string_sv);
    return Status::OK();
}

Status FunctionLikeBase::vector_equals_fn(const ColumnString& vals,
                                          const ColumnString& search_strings,
                                          ColumnUInt8::Container& result) {
    DCHECK(vals.size() == search_strings.size());
    DCHECK(vals.size() == result.size());
    auto sz = vals.size();
    for (size_t i = 0; i < sz; ++i) {
        const auto& str_sv = vals.get_data_at(i);
        const auto& search_string_sv = search_strings.get_data_at(i);
        result[i] = str_sv == search_string_sv;
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_substring_fn(const LikeSearchState* state,
                                               const ColumnString& val, const StringRef& pattern,
                                               ColumnUInt8::Container& result) {
    auto sz = val.size();
    for (size_t i = 0; i < sz; i++) {
        if (state->search_string_sv.size == 0) {
            result[i] = true;
            continue;
        }
        result[i] = state->substring_pattern.search(val.get_data_at(i)) != -1;
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_substring_fn_scalar(const LikeSearchState* state,
                                                      const StringRef& val,
                                                      const StringRef& pattern,
                                                      unsigned char* result) {
    if (state->search_string_sv.size == 0) {
        *result = true;
        return Status::OK();
    }
    *result = state->substring_pattern.search(val) != -1;
    return Status::OK();
}

Status FunctionLikeBase::vector_substring_fn(const ColumnString& vals,
                                             const ColumnString& search_strings,
                                             ColumnUInt8::Container& result) {
    DCHECK(vals.size() == search_strings.size());
    DCHECK(vals.size() == result.size());
    auto sz = vals.size();
    for (size_t i = 0; i < sz; ++i) {
        const auto& str_sv = vals.get_data_at(i);
        const auto& search_string_sv = search_strings.get_data_at(i);
        if (search_string_sv.size == 0) {
            result[i] = true;
            continue;
        }
        doris::StringSearch substring_search(&search_string_sv);
        result[i] = substring_search.search(str_sv) != -1;
    }
    return Status::OK();
}

Status FunctionLikeBase::constant_regex_fn_scalar(const LikeSearchState* state,
                                                  const StringRef& val, const StringRef& pattern,
                                                  unsigned char* result) {
    if (state->hs_database) { // use hyperscan
        auto ret = hs_scan(state->hs_database.get(), val.data, (int)val.size, 0,
                           state->hs_scratch.get(), doris::LikeSearchState::hs_match_handler,
                           (void*)result);
        if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
            return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
        }
    } else if (state->boost_regex) { // use boost::regex for advanced features
        *result = boost::regex_search(val.data, val.data + val.size, *state->boost_regex);
    } else { // fallback to re2
        *result = RE2::PartialMatch(re2::StringPiece(val.data, val.size), *state->regex);
    }

    return Status::OK();
}

Status FunctionLikeBase::regexp_fn_scalar(const LikeSearchState* state, const StringRef& val,
                                          const StringRef& pattern, unsigned char* result) {
    RE2::Options opts;
    opts.set_never_nl(false);
    opts.set_dot_nl(true);
    re2::RE2 re(re2::StringPiece(pattern.data, pattern.size), opts);
    if (re.ok()) {
        *result = RE2::PartialMatch(re2::StringPiece(val.data, val.size), re);
    } else {
        return Status::RuntimeError("Invalid pattern: {}", pattern.debug_string());
    }

    return Status::OK();
}

Status FunctionLikeBase::constant_regex_fn(const LikeSearchState* state, const ColumnString& val,
                                           const StringRef& pattern,
                                           ColumnUInt8::Container& result) {
    auto sz = val.size();
    if (state->hs_database) { // use hyperscan
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            auto ret = hs_scan(state->hs_database.get(), str_ref.data, (int)str_ref.size, 0,
                               state->hs_scratch.get(), doris::LikeSearchState::hs_match_handler,
                               (void*)(result.data() + i));
            if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
                return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
            }
        }
    } else if (state->boost_regex) { // use boost::regex for advanced features
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            *(result.data() + i) = boost::regex_search(str_ref.data, str_ref.data + str_ref.size,
                                                       *state->boost_regex);
        }
    } else { // fallback to re2
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            *(result.data() + i) =
                    RE2::PartialMatch(re2::StringPiece(str_ref.data, str_ref.size), *state->regex);
        }
    }

    return Status::OK();
}

Status FunctionLikeBase::regexp_fn(const LikeSearchState* state, const ColumnString& val,
                                   const StringRef& pattern, ColumnUInt8::Container& result) {
    std::string re_pattern(pattern.data, pattern.size);

    hs_database_t* database = nullptr;
    hs_scratch_t* scratch = nullptr;
    auto hs_status = hs_prepare(nullptr, re_pattern.c_str(), &database, &scratch);
    if (hs_status.ok()) { // use hyperscan
        auto sz = val.size();
        for (size_t i = 0; i < sz; i++) {
            const auto& str_ref = val.get_data_at(i);
            auto ret =
                    hs_scan(database, str_ref.data, (int)str_ref.size, 0, scratch,
                            doris::LikeSearchState::hs_match_handler, (void*)(result.data() + i));
            if (ret != HS_SUCCESS && ret != HS_SCAN_TERMINATED) {
                return Status::RuntimeError(fmt::format("hyperscan error: {}", ret));
            }
        }

        hs_free_scratch(scratch);
        hs_free_database(database);
    } else { // fallback to re2
        if (!state->enable_hyperscan_fallback) {
            return hs_status;
        }
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        re2::RE2 re(re_pattern, opts);
        if (re.ok()) {
            auto sz = val.size();
            for (size_t i = 0; i < sz; i++) {
                const auto& str_ref = val.get_data_at(i);
                *(result.data() + i) =
                        RE2::PartialMatch(re2::StringPiece(str_ref.data, str_ref.size), re);
            }
        } else {
            return Status::RuntimeError("Invalid pattern: {}", pattern.debug_string());
        }
    }

    return Status::OK();
}

// hyperscan compile expression to database and allocate scratch space
bool FunctionLikeBase::should_fallback_to_re2(std::string_view regexp) {
    return is_hyperscan_regexp_expensive(regexp);
}

Status FunctionLikeBase::hs_prepare(FunctionContext* context, const char* expression,
                                    hs_database_t** database, hs_scratch_t** scratch) {
    if (should_fallback_to_re2(expression)) {
        *database = nullptr;
        *scratch = nullptr;
        // Callers either fall back to RE2 or return this status based on the session variable.
        return Status::RuntimeError<false>(HYPERSCAN_BOUNDED_REPEAT_ERROR);
    }

    hs_compile_error_t* compile_err;
    auto res = hs_compile(expression, HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                          HS_MODE_BLOCK, nullptr, database, &compile_err);

    if (res != HS_SUCCESS) {
        *database = nullptr;
        std::string error_message = compile_err->message;
        hs_free_compile_error(compile_err);
        // Callers either fall back to RE2 or return this status based on the session variable.
        return Status::RuntimeError<false>("hs_compile regex pattern error:" + error_message);
    }
    hs_free_compile_error(compile_err);

    if (hs_alloc_scratch(*database, scratch) != HS_SUCCESS) {
        hs_free_database(*database);
        *database = nullptr;
        *scratch = nullptr;
        // Callers either fall back to RE2 or return this status based on the session variable.
        return Status::RuntimeError<false>("hs_alloc_scratch allocate scratch space error");
    }

    return Status::OK();
}

Status FunctionLikeBase::execute_impl(FunctionContext* context, Block& block,
                                      const ColumnNumbers& arguments, uint32_t result,
                                      size_t input_rows_count) const {
    const auto values_col =
            block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    const auto* values = check_and_get_column<ColumnString>(values_col.get());

    if (!values) {
        return Status::InternalError("Not supported input arguments types");
    }
    // result column
    auto res = ColumnUInt8::create();
    ColumnUInt8::Container& vec_res = res->get_data();
    // set default value to 0, and match functions only need to set 1/true
    vec_res.resize_fill(input_rows_count);
    auto* state = reinterpret_cast<LikeState*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    // for constant_substring_fn, use long run length search for performance
    if (constant_substring_fn ==
        *(state->function
                  .target<doris::Status (*)(const LikeSearchState* state, const ColumnString&,
                                            const StringRef&, ColumnUInt8::Container&)>())) {
        RETURN_IF_ERROR(execute_substring(values->get_chars(), values->get_offsets(), vec_res,
                                          &state->search_state));
    } else {
        const auto pattern_col = block.get_by_position(arguments[1]).column;
        if (const auto* str_patterns = check_and_get_column<ColumnString>(pattern_col.get())) {
            RETURN_IF_ERROR(
                    vector_non_const(*values, *str_patterns, vec_res, state, input_rows_count));
        } else if (const auto* const_patterns =
                           check_and_get_column<ColumnConst>(pattern_col.get())) {
            const auto& pattern_val = const_patterns->get_data_at(0);
            RETURN_IF_ERROR(vector_const(*values, &pattern_val, vec_res, state->function,
                                         &state->search_state));
        } else {
            return Status::InternalError("Not supported input arguments types");
        }
    }
    block.replace_by_position(result, std::move(res));
    return Status::OK();
}

Status FunctionLikeBase::execute_substring(const ColumnString::Chars& values,
                                           const ColumnString::Offsets& value_offsets,
                                           ColumnUInt8::Container& result,
                                           LikeSearchState* search_state) const {
    // treat continuous multi string data as a long string data
    const UInt8* begin = values.data();
    const UInt8* end = begin + values.size();
    const UInt8* pos = begin;

    /// Current index in the array of strings.
    size_t i = 0;
    size_t needle_size = search_state->substring_pattern.get_pattern_length();

    /// We will search for the next occurrence in all strings at once.
    while (pos < end) {
        // search return matched substring start offset
        pos = (UInt8*)search_state->substring_pattern.search((char*)pos, end - pos);
        if (pos >= end) {
            break;
        }

        /// Determine which index it refers to.
        /// begin + value_offsets[i] is the start offset of string at i+1
        while (i < value_offsets.size() && begin + value_offsets[i] < pos) {
            ++i;
        }

        /// We check that the entry does not pass through the boundaries of strings.
        if (pos + needle_size <= begin + value_offsets[i]) {
            result[i] = 1;
        }

        // move to next string offset
        pos = begin + value_offsets[i];
        ++i;
    }

    return Status::OK();
}

Status FunctionLikeBase::vector_const(const ColumnString& values, const StringRef* pattern_val,
                                      ColumnUInt8::Container& result, const LikeFn& function,
                                      LikeSearchState* search_state) const {
    RETURN_IF_ERROR((function)(search_state, values,
                               *reinterpret_cast<const StringRef*>(pattern_val), result));
    return Status::OK();
}

template <bool LIKE_PATTERN>
VPatternSearchStateSPtr FunctionLikeBase::pattern_type_recognition(const ColumnString& patterns) {
    VPatternSearchStateSPtr allpass_state = std::make_shared<VectorAllpassSearchState>();
    VPatternSearchStateSPtr equal_state = std::make_shared<VectorEqualSearchState>();
    VPatternSearchStateSPtr substring_state = std::make_shared<VectorSubStringSearchState>();
    VPatternSearchStateSPtr starts_with_state = std::make_shared<VectorStartsWithSearchState>();
    VPatternSearchStateSPtr ends_with_state = std::make_shared<VectorEndsWithSearchState>();
    size_t size = patterns.size();

    for (size_t i = 0; i < size; ++i) {
        if (!allpass_state->_pattern_matched && !equal_state->_pattern_matched &&
            !substring_state->_pattern_matched && !starts_with_state->_pattern_matched &&
            !ends_with_state->_pattern_matched) {
            return nullptr;
        }
        std::string pattern_str = patterns.get_data_at(i).to_string();
        if (allpass_state->_pattern_matched) {
            if constexpr (LIKE_PATTERN) {
                allpass_state->like_pattern_match(pattern_str);
            } else {
                allpass_state->regexp_pattern_match(pattern_str);
            }
        }
        if (equal_state->_pattern_matched) {
            if constexpr (LIKE_PATTERN) {
                equal_state->like_pattern_match(pattern_str);
            } else {
                equal_state->regexp_pattern_match(pattern_str);
            }
        }
        if (substring_state->_pattern_matched) {
            if constexpr (LIKE_PATTERN) {
                substring_state->like_pattern_match(pattern_str);
            } else {
                substring_state->regexp_pattern_match(pattern_str);
            }
        }
        if (starts_with_state->_pattern_matched) {
            if constexpr (LIKE_PATTERN) {
                starts_with_state->like_pattern_match(pattern_str);
            } else {
                starts_with_state->regexp_pattern_match(pattern_str);
            }
        }
        if (ends_with_state->_pattern_matched) {
            if constexpr (LIKE_PATTERN) {
                ends_with_state->like_pattern_match(pattern_str);
            } else {
                ends_with_state->regexp_pattern_match(pattern_str);
            }
        }
    }

    if (allpass_state->_pattern_matched) {
        return allpass_state;
    } else if (equal_state->_pattern_matched) {
        return equal_state;
    } else if (substring_state->_pattern_matched) {
        return substring_state;
    } else if (starts_with_state->_pattern_matched) {
        return starts_with_state;
    } else if (ends_with_state->_pattern_matched) {
        return ends_with_state;
    } else {
        return nullptr;
    }
}

Status FunctionLikeBase::vector_non_const(const ColumnString& values, const ColumnString& patterns,
                                          ColumnUInt8::Container& result, LikeState* state,
                                          size_t input_rows_count) const {
    ColumnString::MutablePtr replaced_patterns;
    VPatternSearchStateSPtr vector_search_state;
    if (state->is_like_pattern) {
        if (state->has_custom_escape) {
            replaced_patterns = ColumnString::create();
            for (int i = 0; i < input_rows_count; ++i) {
                std::string val =
                        replace_pattern_by_escape(patterns.get_data_at(i), state->escape_char);
                replaced_patterns->insert_data(val.c_str(), val.size());
            }
            vector_search_state = pattern_type_recognition<true>(*replaced_patterns);
        } else {
            vector_search_state = pattern_type_recognition<true>(patterns);
        }
    } else {
        vector_search_state = pattern_type_recognition<false>(patterns);
    }

    const ColumnString& real_pattern = state->has_custom_escape ? *replaced_patterns : patterns;

    if (vector_search_state == nullptr) {
        // pattern type recognition failed, use default case
        for (int i = 0; i < input_rows_count; ++i) {
            const auto pattern_val = real_pattern.get_data_at(i);
            const auto value_val = values.get_data_at(i);
            RETURN_IF_ERROR((state->scalar_function)(&state->search_state, value_val, pattern_val,
                                                     &result[i]));
        }
        return Status::OK();
    }
    const auto* search_strings =
            static_cast<const ColumnString*>(vector_search_state->_search_strings.get());
    return (vector_search_state->_vector_function)(values, *search_strings, result);
}

Status FunctionLike::like_fn(const LikeSearchState* state, const ColumnString& val,
                             const StringRef& pattern, ColumnUInt8::Container& result) {
    std::string re_pattern;
    convert_like_pattern(state, std::string(pattern.data, pattern.size), &re_pattern);
    return regexp_fn(state, val, {re_pattern.c_str(), re_pattern.size()}, result);
}

Status FunctionLike::like_fn_scalar(const LikeSearchState* state, const StringRef& val,
                                    const StringRef& pattern, unsigned char* result) {
    // Try to use fast path to avoid regex compilation
    std::string search_string;
    LikeFastPath fast_path = extract_like_fast_path(pattern.data, pattern.size, search_string);

    switch (fast_path) {
    case LikeFastPath::ALLPASS:
        *result = 1;
        return Status::OK();
    case LikeFastPath::EQUALS:
        *result = (val.size == search_string.size() &&
                   (search_string.empty() ||
                    memcmp(val.data, search_string.data(), search_string.size()) == 0));
        return Status::OK();
    case LikeFastPath::STARTS_WITH:
        *result = (val.size >= search_string.size() &&
                   memcmp(val.data, search_string.data(), search_string.size()) == 0);
        return Status::OK();
    case LikeFastPath::ENDS_WITH:
        *result = (val.size >= search_string.size() &&
                   memcmp(val.data + val.size - search_string.size(), search_string.data(),
                          search_string.size()) == 0);
        return Status::OK();
    case LikeFastPath::SUBSTRING:
        if (search_string.empty()) {
            *result = 1;
        } else {
            // Use memmem for substring search
            *result = (memmem(val.data, val.size, search_string.data(), search_string.size()) !=
                       nullptr);
        }
        return Status::OK();
    case LikeFastPath::REGEX:
    default:
        // Fall back to regex matching
        std::string re_pattern;
        convert_like_pattern(state, std::string(pattern.data, pattern.size), &re_pattern);
        return regexp_fn_scalar(state, StringRef(val.data, val.size),
                                {re_pattern.c_str(), re_pattern.size()}, result);
    }
}

void FunctionLike::convert_like_pattern(const LikeSearchState* state, const std::string& pattern,
                                        std::string* re_pattern) {
    re_pattern->clear();

    if (pattern.empty()) {
        re_pattern->append("^$");
        return;
    }

    // add ^ to pattern head to match line head
    if (!pattern.empty() && pattern[0] != '%') {
        re_pattern->append("^");
    }

    // expect % and _, all chars should keep it literal mean.
    for (size_t i = 0; i < pattern.size(); i++) {
        char c = pattern[i];
        if (c == '\\' && i + 1 < pattern.size()) {
            char next_c = pattern[i + 1];
            if (next_c == '%' || next_c == '_') {
                // convert "\%" and "\_" to literal "%" and "_"
                re_pattern->append(1, next_c);
                i++;
                continue;
            } else if (next_c == '\\') {
                // keep valid escape "\\"
                re_pattern->append("\\\\");
                i++;
                continue;
            }
        }

        if (c == '%') {
            re_pattern->append(".*");
        } else if (c == '_') {
            re_pattern->append(".");
        } else {
            // special for hyperscan: [, ], (, ), {, }, -, *, +, \, |, /, :, ^, ., $, ?
            if (c == '[' || c == ']' || c == '(' || c == ')' || c == '{' || c == '}' || c == '-' ||
                c == '*' || c == '+' || c == '\\' || c == '|' || c == '/' || c == ':' || c == '^' ||
                c == '.' || c == '$' || c == '?') {
                re_pattern->append(1, '\\');
            }
            re_pattern->append(1, c);
        }
    }

    // add $ to pattern tail to match line tail
    if (!pattern.empty() && re_pattern->back() != '*') {
        re_pattern->append("$");
    }
}

void FunctionLike::remove_escape_character(std::string* search_string) {
    std::string tmp_search_string;
    tmp_search_string.swap(*search_string);
    int64_t len = tmp_search_string.length();
    // sometime 'like' may allowed converted to 'equals/start_with/end_with/sub_with'
    // so we need to remove escape from pattern to construct search string and use to do 'equals/start_with/end_with/sub_with'
    for (int i = 0; i < len;) {
        if (tmp_search_string[i] == '\\' && i + 1 < len &&
            (tmp_search_string[i + 1] == '%' || tmp_search_string[i + 1] == '_' ||
             tmp_search_string[i + 1] == '\\')) {
            search_string->append(1, tmp_search_string[i + 1]);
            i += 2;
        } else {
            search_string->append(1, tmp_search_string[i]);
            i++;
        }
    }
}

bool re2_full_match(const std::string& str, const RE2& re, std::vector<std::string>& results) {
    if (!re.ok()) {
        return false;
    }

    std::vector<RE2::Arg> arguments;
    std::vector<RE2::Arg*> arguments_ptrs;
    std::size_t args_count = re.NumberOfCapturingGroups();
    arguments.resize(args_count);
    arguments_ptrs.resize(args_count);
    results.resize(args_count);
    for (std::size_t i = 0; i < args_count; ++i) {
        arguments[i] = &results[i];
        arguments_ptrs[i] = &arguments[i];
    }

    return RE2::FullMatchN(str, re, arguments_ptrs.data(), (int)args_count);
}

void verbose_log_match(const std::string& str, const std::string& pattern_name, const RE2& re) {
    std::vector<std::string> results;
    VLOG_DEBUG << "arg str: " << str << ", size: " << str.size() << ", pattern " << pattern_name
               << ": " << re.pattern() << ", size: " << re.pattern().size();
    if (re2_full_match(str, re, results)) {
        for (int i = 0; i < results.size(); ++i) {
            VLOG_DEBUG << "match " << i << ": " << results[i] << ", size: " << results[i].size();
        }
    } else {
        VLOG_DEBUG << "no match";
    }
}

Status FunctionLike::construct_like_const_state(FunctionContext* context, const StringRef& pattern,
                                                std::shared_ptr<LikeState>& state,
                                                bool try_hyperscan) {
    std::string pattern_str;
    if (state->has_custom_escape) {
        pattern_str = replace_pattern_by_escape(pattern, state->escape_char);
    } else {
        pattern_str = pattern.to_string();
    }
    state->search_state.pattern_str = pattern_str;
    std::string search_string;

    if (!pattern_str.empty() && RE2::FullMatch(pattern_str, LIKE_ALLPASS_RE)) {
        state->search_state.set_search_string("");
        state->function = constant_allpass_fn;
        state->scalar_function = constant_allpass_fn_scalar;
    } else if (pattern_str.empty() || RE2::FullMatch(pattern_str, LIKE_EQUALS_RE, &search_string)) {
        if (VLOG_DEBUG_IS_ON) {
            verbose_log_match(pattern_str, "LIKE_EQUALS_RE", LIKE_EQUALS_RE);
            VLOG_DEBUG << "search_string : " << search_string << ", size: " << search_string.size();
        }
        remove_escape_character(&search_string);
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "search_string escape removed: " << search_string
                       << ", size: " << search_string.size();
        }
        state->search_state.set_search_string(search_string);
        state->function = constant_equals_fn;
        state->scalar_function = constant_equals_fn_scalar;
    } else if (RE2::FullMatch(pattern_str, LIKE_STARTS_WITH_RE, &search_string)) {
        if (VLOG_DEBUG_IS_ON) {
            verbose_log_match(pattern_str, "LIKE_STARTS_WITH_RE", LIKE_STARTS_WITH_RE);
            VLOG_DEBUG << "search_string : " << search_string << ", size: " << search_string.size();
        }
        remove_escape_character(&search_string);
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "search_string escape removed: " << search_string
                       << ", size: " << search_string.size();
        }
        state->search_state.set_search_string(search_string);
        state->function = constant_starts_with_fn;
        state->scalar_function = constant_starts_with_fn_scalar;
    } else if (RE2::FullMatch(pattern_str, LIKE_ENDS_WITH_RE, &search_string)) {
        if (VLOG_DEBUG_IS_ON) {
            verbose_log_match(pattern_str, "LIKE_ENDS_WITH_RE", LIKE_ENDS_WITH_RE);
            VLOG_DEBUG << "search_string : " << search_string << ", size: " << search_string.size();
        }
        remove_escape_character(&search_string);
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "search_string escape removed: " << search_string
                       << ", size: " << search_string.size();
        }
        state->search_state.set_search_string(search_string);
        state->function = constant_ends_with_fn;
        state->scalar_function = constant_ends_with_fn_scalar;
    } else if (RE2::FullMatch(pattern_str, LIKE_SUBSTRING_RE, &search_string)) {
        if (VLOG_DEBUG_IS_ON) {
            verbose_log_match(pattern_str, "LIKE_SUBSTRING_RE", LIKE_SUBSTRING_RE);
            VLOG_DEBUG << "search_string : " << search_string << ", size: " << search_string.size();
        }
        remove_escape_character(&search_string);
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "search_string escape removed: " << search_string
                       << ", size: " << search_string.size();
        }
        state->search_state.set_search_string(search_string);
        state->function = constant_substring_fn;
        state->scalar_function = constant_substring_fn_scalar;
    } else {
        std::string re_pattern;
        convert_like_pattern(&state->search_state, pattern_str, &re_pattern);
        if (VLOG_DEBUG_IS_ON) {
            VLOG_DEBUG << "hyperscan, pattern str: " << pattern_str
                       << ", size: " << pattern_str.size() << ", re pattern: " << re_pattern
                       << ", size: " << re_pattern.size();
        }

        hs_database_t* database = nullptr;
        hs_scratch_t* scratch = nullptr;
        Status hs_status;
        if (try_hyperscan) {
            hs_status = hs_prepare(context, re_pattern.c_str(), &database, &scratch);
        }
        if (try_hyperscan && hs_status.ok()) {
            // use hyperscan
            state->search_state.hs_database.reset(database);
            state->search_state.hs_scratch.reset(scratch);
        } else {
            // fallback to re2
            if (try_hyperscan && !state->search_state.enable_hyperscan_fallback) {
                return hs_status;
            }
            // reset hs_database to nullptr to indicate not use hyperscan
            state->search_state.hs_database.reset();
            state->search_state.hs_scratch.reset();

            RE2::Options opts;
            opts.set_never_nl(false);
            opts.set_dot_nl(true);
            state->search_state.regex = std::make_unique<RE2>(re_pattern, opts);
            if (!state->search_state.regex->ok()) {
                return Status::InternalError("Invalid regex expression: {}(origin: {})", re_pattern,
                                             pattern_str);
            }
        }

        state->function = constant_regex_fn;
        state->scalar_function = constant_regex_fn_scalar;
    }
    return Status::OK();
}

Status FunctionLike::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    std::shared_ptr<LikeState> state = std::make_shared<LikeState>();
    state->is_like_pattern = true;
    state->search_state.enable_hyperscan_fallback =
            context->state()->query_options().enable_hyperscan_fallback;
    state->function = like_fn;
    state->scalar_function = like_fn_scalar;
    if (context->is_col_constant(2)) {
        state->has_custom_escape = true;
        const auto escape_col = context->get_constant_col(2)->column_ptr;
        const auto& escape = escape_col->get_data_at(0);
        if (escape.size != 1) {
            return Status::InternalError("Escape character must be a single character, got: {}",
                                         escape.to_string());
        }
        state->escape_char = escape.data[0];
    }
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);
        RETURN_IF_ERROR(construct_like_const_state(context, pattern, state));
    }
    context->set_function_state(scope, state);

    return Status::OK();
}

Status FunctionRegexpLike::open(FunctionContext* context,
                                FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::THREAD_LOCAL) {
        return Status::OK();
    }
    std::shared_ptr<LikeState> state = std::make_shared<LikeState>();
    context->set_function_state(scope, state);
    state->is_like_pattern = false;
    state->search_state.enable_hyperscan_fallback =
            context->state()->query_options().enable_hyperscan_fallback;
    state->function = regexp_fn;
    state->scalar_function = regexp_fn_scalar;
    if (context->is_col_constant(1)) {
        const auto pattern_col = context->get_constant_col(1)->column_ptr;
        const auto& pattern = pattern_col->get_data_at(0);

        std::string pattern_str = pattern.to_string();
        std::string search_string;
        if (RE2::FullMatch(pattern_str, ALLPASS_RE)) {
            state->search_state.set_search_string("");
            state->function = constant_allpass_fn;
            state->scalar_function = constant_allpass_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, EQUALS_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_equals_fn;
            state->scalar_function = constant_equals_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, STARTS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_starts_with_fn;
            state->scalar_function = constant_starts_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, ENDS_WITH_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_ends_with_fn;
            state->scalar_function = constant_ends_with_fn_scalar;
        } else if (RE2::FullMatch(pattern_str, SUBSTRING_RE, &search_string)) {
            state->search_state.set_search_string(search_string);
            state->function = constant_substring_fn;
            state->scalar_function = constant_substring_fn_scalar;
        } else {
            hs_database_t* database = nullptr;
            hs_scratch_t* scratch = nullptr;
            auto hs_status = hs_prepare(context, pattern_str.c_str(), &database, &scratch);
            if (hs_status.ok()) {
                // use hyperscan
                state->search_state.hs_database.reset(database);
                state->search_state.hs_scratch.reset(scratch);
            } else {
                // fallback to re2
                if (!state->search_state.enable_hyperscan_fallback) {
                    return hs_status;
                }
                // reset hs_database to nullptr to indicate not use hyperscan
                state->search_state.hs_database.reset();
                state->search_state.hs_scratch.reset();
                RE2::Options opts;
                opts.set_never_nl(false);
                opts.set_dot_nl(true);
                state->search_state.regex = std::make_unique<RE2>(pattern_str, opts);
                if (!state->search_state.regex->ok()) {
                    if (!context->state()->enable_extended_regex()) {
                        return Status::InternalError(
                                "Invalid regex expression: {}. Error: {}. If you need advanced "
                                "regex features, try setting enable_extended_regex=true",
                                pattern_str, state->search_state.regex->error());
                    }

                    // RE2 failed, fallback to Boost.Regex
                    // This handles advanced regex features like zero-width assertions
                    state->search_state.regex.reset();
                    try {
                        state->search_state.boost_regex =
                                std::make_unique<boost::regex>(pattern_str);
                    } catch (const boost::regex_error& e) {
                        return Status::InternalError("Invalid regex expression: {}. Error: {}",
                                                     pattern_str, e.what());
                    }
                }
            }
            state->function = constant_regex_fn;
            state->scalar_function = constant_regex_fn_scalar;
        }
    }
    return Status::OK();
}

// R8 (unity build): the storage target merges several .cpp files into one translation unit, so
// identically named symbols in anonymous namespaces collide across files; file-scope helpers
// always go into a namespace private to this file.
namespace like_gram_index_detail {

// Conservative shape check for the LIKE ESCAPE clause. Only two shapes may be pushed down:
//   * arguments.size() == 1: no ESCAPE clause. Nereids' like function has only the 2-ary
//     (col, pattern) and the 3-ary (col, pattern, escape) signature
//     (fe/.../nereids/trees/expressions/Like.java:37-48), and the column reaches iterators as a
//     slot_ref, so "arguments holds only the pattern" is equivalent to "there is no third
//     argument" -- provided ESCAPE is always a literal.
//   * arguments.size() == 2, the second argument is a constant column, and its value is exactly
//     the default backslash.
// Any other shape (more than 2 arguments, a non-constant ESCAPE column, a NULL ESCAPE, an
// ESCAPE that is not a backslash) is refused -- the P0 compiler only implements the default
// backslash escaping semantics.
//
// Known limitation (accepted in P0, tightened in P1): VExpr::_evaluate_inverted_index only
// collects **literal** sub-expressions into arguments (be/src/exprs/vexpr.cpp:998-1002). On the
// FE side ESCAPE is an arbitrary expression (getExpression(ctx.escape) in
// LogicalPlanBuilder.java:5116-5126), so a non-literal ESCAPE sub-expression never appears in
// arguments:
//   - if it is a slot_ref carrying an index, iterators gains an extra entry and is rejected by
//     the iterators.size() != 1 check above;
//   - if it is a slot_ref without an index, iterators still holds one entry and arguments still
//     holds only the pattern, which is indistinguishable here from "no ESCAPE clause". The
//     function-side signature has no VExpr, so get_num_children() is out of reach and P0 cannot
//     decide at this layer; P1 needs to pass the original child count (or the ESCAPE
//     sub-expression itself) into evaluate_inverted_index and tighten this.
bool like_escape_is_default_backslash(const ColumnsWithTypeAndName& arguments) {
    if (arguments.size() == 1) {
        return true;
    }
    if (arguments.size() != 2) {
        VLOG_DEBUG << "gram index push-down skipped: unexpected LIKE argument count "
                   << arguments.size();
        return false;
    }
    if (!is_column_const(*arguments[1].column)) {
        VLOG_DEBUG << "gram index push-down skipped: LIKE ESCAPE is not a constant column";
        return false;
    }
    Field escape_field;
    arguments[1].column->get(0, escape_field);
    if (escape_field.is_null() || escape_field.get<TYPE_STRING>() != "\\") {
        VLOG_DEBUG << "gram index push-down skipped: custom LIKE ESCAPE is not supported";
        return false;
    }
    return true;
}

// Preconditions: master switch / iterator shape / arguments and data_type_with_names shape /
// LIKE ESCAPE shape (see like_escape_is_default_backslash) / pattern being a non-NULL constant.
// Writes the pattern into *pattern and returns true when all of them hold; returns false as
// soon as one does not -- the caller then simply returns OK() without writing bitmap_result
// (Ruling R26: an inapplicable index may only cost the speedup, it must never raise an error).
bool check_preconditions(bool is_like, const ColumnsWithTypeAndName& arguments,
                         const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
                         const std::vector<segment_v2::IndexIterator*>& iterators,
                         std::string* pattern) {
    if (!config::enable_gram_index_regexp) {
        VLOG_DEBUG << "gram index push-down skipped: enable_gram_index_regexp is false";
        return false;
    }
    if (iterators.size() != 1 || iterators[0] == nullptr) {
        VLOG_DEBUG << "gram index push-down skipped: unsupported iterators shape (size="
                   << iterators.size() << ")";
        return false;
    }
    if (arguments.empty() || data_type_with_names.size() != 1) {
        VLOG_DEBUG << "gram index push-down skipped: unsupported arguments/column shape";
        return false;
    }
    if (is_like && !like_escape_is_default_backslash(arguments)) {
        return false;
    }
    if (!is_column_const(*arguments[0].column)) {
        VLOG_DEBUG << "gram index push-down skipped: pattern column is not constant";
        return false;
    }
    Field pattern_field;
    arguments[0].column->get(0, pattern_field);
    if (pattern_field.is_null()) {
        VLOG_DEBUG << "gram index push-down skipped: pattern is NULL";
        return false;
    }
    *pattern = pattern_field.get<TYPE_STRING>();
    return true;
}

// Resolve the gram scheme from the iterator's FULLTEXT reader properties. nullopt is returned
// when there is no FULLTEXT reader, the reader is not a SNII reader (storage-format fence, see
// below), resolve_gram_scheme decides this is not a gram-family analyzer, or the policy manager
// throws because a policy is missing -- all of these mean "this index cannot use gram
// acceleration", not a query error.
//
// Storage-format fence (Ruling R30, layer 1): only SniiIndexReader understands
// GRAM_BOOLEAN_QUERY. Segments in CLucene format (V1/V2/V3, whose reader is
// FullTextIndexReader / StringTypeInvertedIndexReader) must never receive a GRAM_BOOLEAN_QUERY
// even when their property table names a gram-family analyzer (old segments of the very same
// table look exactly like that: identical index definition, different segment format) -- their
// query() would hand the serialized boolean query to QueryFactory as ordinary text to be
// tokenized, with unpredictable results. So right after obtaining the reader we determine its
// concrete type with dynamic_cast: anything that is not a SNII reader counts as "no usable
// scheme" and silently skips the acceleration.
// Layer 2 of the fence sits at the query() entry of the CLucene readers
// (inverted_index_reader.cpp), and layer 3 is the catch-all degradation in dispatch_query --
// the three hold independently, so bypassing any one of them still cannot produce a wrong
// result.
//
// Scheme invariance (Ruling R28): P0 simply takes "the scheme resolved at query time" to be
// "the scheme used at write time", because in P0 the two are necessarily identical:
//   * FE has no ALTER INDEX POLICY statement at all (indexpolicy/ only has CREATE / DROP /
//     SHOW), so a policy's properties are immutable once created;
//   * dropping a referenced policy is refused: when an analyzer is referenced by an index,
//     IndexPolicyMgr.checkAnalyzerNotUsedByIndex (IndexPolicyMgr.java:612-634) throws
//     DdlException; when a tokenizer / token_filter / char_filter is referenced by an analyzer
//     or a normalizer, IndexPolicyMgr.checkPolicyNotReferenced (same file, 670-699, together
//     with checkFilterReference 701-713) throws DdlException; dropIndexPolicy (566-600) chains
//     both checks.
// That is, no link of the "index -> analyzer -> tokenizer" chain can be modified or dropped
// while the index still exists. On top of that, P1 will also compare the gram_scheme recorded
// in the segment (the core metadata written by Task 12), covering the scheme drift that future
// ALTER or cross-version rebuilds could introduce.
std::optional<segment_v2::gram::GramScheme> resolve_scheme(segment_v2::IndexIterator* iter) {
    auto reader = iter->get_reader(segment_v2::InvertedIndexReaderType::FULLTEXT);
    if (reader == nullptr) {
        VLOG_DEBUG << "gram index push-down skipped: no FULLTEXT reader";
        return std::nullopt;
    }
    auto* snii_reader = dynamic_cast<segment_v2::SniiIndexReader*>(reader.get());
    if (snii_reader == nullptr) {
        VLOG_DEBUG << "gram index push-down skipped: reader is not a SNII reader "
                      "(GRAM_BOOLEAN_QUERY is not supported by CLucene-format indexes)";
        return std::nullopt;
    }
    try {
        auto scheme = segment_v2::gram::resolve_gram_scheme(
                snii_reader->get_index_properties(), ExecEnv::GetInstance()->index_policy_mgr());
        if (!scheme.has_value()) {
            VLOG_DEBUG << "gram index push-down skipped: index is not a gram-family analyzer";
        }
        return scheme;
    } catch (const Exception& e) {
        VLOG_DEBUG << "gram index push-down skipped: gram scheme resolution threw: " << e.what();
        return std::nullopt;
    } catch (const std::exception& e) {
        VLOG_DEBUG << "gram index push-down skipped: gram scheme resolution threw: " << e.what();
        return std::nullopt;
    }
}

// Compile pattern into a GramQuery and issue a GRAM_BOOLEAN_QUERY to the iterator. An internal
// compiler failure, a compilation result of ALL (gram_index_uncompilable, counted in Task 11),
// and **any** non-OK status returned by read_from_index all collapse into OK without writing
// *bitmap_result.
//
// Catch-all degradation (Ruling R29): deliberately no allow-list of "error codes known to be
// degradable" is kept here. The gram index is purely an accelerator -- rows outside the bitmap
// certainly do not match, and rows inside it are re-verified by the row-level path -- so
// whatever goes wrong on the index side (a corrupted segment, an S3 read timeout, an error code
// added in the future, ...), the only correct behaviour is always the same: skip the
// acceleration and fall back to a full scan. An allow-list that misses one error code turns a
// LIKE/REGEXP query that could have returned results into a failure, which is a worse
// regression than being slow.
//
// The only exceptions are the two kinds of status that mean "the query as a whole should
// already be stopping"; swallowing them would only let the query keep doing useless work (and
// hide the real cause), so they are rethrown as-is:
//   * ErrorCode::CANCELLED -- the query has been cancelled;
//   * ErrorCode::MEM_LIMIT_EXCEEDED / ErrorCode::MEM_ALLOC_FAILED -- memory limit exceeded /
//     allocation failed; falling back to a full scan would only use more memory.
Status dispatch_query(bool is_like, const segment_v2::gram::GramScheme& scheme,
                      const std::string& pattern, segment_v2::IndexIterator* iter,
                      const IndexFieldNameAndTypePair& data_type_with_name, uint32_t num_rows,
                      segment_v2::InvertedIndexResultBitmap* bitmap_result) {
    segment_v2::gram::RegexGramCompiler compiler(scheme);
    segment_v2::gram::GramQuery query;
    Status compile_status = is_like ? compiler.compile_like(pattern, &query)
                                    : compiler.compile_regexp(pattern, &query);
    if (!compile_status.ok()) {
        // compile_* only returns non-OK when an internal assertion fails; even then, a problem
        // on the index side may only degrade to "no acceleration", it must never make the
        // LIKE/REGEXP query fail.
        VLOG_DEBUG << "gram index push-down skipped: compiler returned " << compile_status;
        return Status::OK();
    }
    if (query.is_all()) {
        VLOG_DEBUG << "gram index push-down skipped: compiled gram query is ALL";
        return Status::OK();
    }

    segment_v2::InvertedIndexParam param;
    param.column_name = data_type_with_name.first;
    param.column_type = data_type_with_name.second;
    param.query_value = Field::create_field<TYPE_STRING>(query.serialize());
    param.query_type = segment_v2::InvertedIndexQueryType::GRAM_BOOLEAN_QUERY;
    param.num_rows = num_rows;
    param.roaring = std::make_shared<roaring::Roaring>();

    Status query_status = iter->read_from_index(&param);
    if (!query_status.ok()) {
        if (query_status.is<ErrorCode::CANCELLED>() ||
            query_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() ||
            query_status.is<ErrorCode::MEM_ALLOC_FAILED>()) {
            // The query as a whole should already be stopping: rethrow as-is (see above).
            return query_status;
        }
        // Every other error only degrades to "no acceleration". LOG_EVERY_N rather than VLOG,
        // because this path already means "the index could not be used" and deserves a trace at
        // the default log level, while still not flooding the log once per segment.
        LOG_EVERY_N(WARNING, 100) << "gram index push-down skipped, read_from_index returned "
                                  << query_status;
        return Status::OK();
    }

    segment_v2::InvertedIndexResultBitmap result(param.roaring, nullptr);
    result.set_approximate(true);
    *bitmap_result = result;
    return Status::OK();
}

} // namespace like_gram_index_detail

Status FunctionLikeBase::evaluate_gram_index(
        GramCompileKind kind, const ColumnsWithTypeAndName& arguments,
        const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
        std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
        segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
    const bool is_like = (kind == GramCompileKind::LIKE);

    std::string pattern;
    if (!like_gram_index_detail::check_preconditions(is_like, arguments, data_type_with_names,
                                                     iterators, &pattern)) {
        return Status::OK();
    }

    auto* iter = iterators[0];
    auto scheme = like_gram_index_detail::resolve_scheme(iter);
    if (!scheme.has_value()) {
        return Status::OK();
    }

    return like_gram_index_detail::dispatch_query(
            is_like, *scheme, pattern, iter, data_type_with_names[0], num_rows, &bitmap_result);
}

void register_function_like(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLike>();
}

void register_function_regexp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRegexpLike>();
    factory.register_alias(FunctionRegexpLike::name, FunctionRegexpLike::alias);
}
} // namespace doris
