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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_LIKE_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_LIKE_PREDICATE_H

#include <re2/re2.h>

#include <memory>
#include <string>

#include "exprs/predicate.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/string_search.hpp"

namespace doris {

class LikePredicate {
public:
    static void init();

private:
    typedef doris_udf::BooleanVal (*LikePredicateFunction)(doris_udf::FunctionContext*,
                                                           const doris_udf::StringVal&,
                                                           const doris_udf::StringVal&);

    struct LikePredicateState {
        char escape_char;

        /// This is the function, set in the prepare function, that will be used to determine
        /// the value of the predicate. It will be set depending on whether the expression is
        /// a LIKE, RLIKE or REGEXP predicate, whether the pattern is a constant argument
        /// and whether the pattern has any constant substrings. If the pattern is not a
        /// constant argument, none of the following fields can be set because we cannot know
        /// the format of the pattern in the prepare function and must deal with each pattern
        /// separately.
        LikePredicateFunction function;

        /// Holds the string the StringValue points to and is set any time StringValue is
        /// used.
        std::string search_string;

        /// Used for LIKE predicates if the pattern is a constant argument, and is either a
        /// constant string or has a constant string at the beginning or end of the pattern.
        /// This will be set in order to check for that pattern in the corresponding part of
        /// the string.
        StringValue search_string_sv;

        /// Used for LIKE predicates if the pattern is a constant argument and has a constant
        /// string in the middle of it. This will be use in order to check for the substring
        /// in the value.
        StringSearch substring_pattern;

        /// Used for RLIKE and REGEXP predicates if the pattern is a constant argument.
        std::unique_ptr<re2::RE2> regex;

        LikePredicateState() : escape_char('\\') {}

        void set_search_string(const std::string& search_string_arg) {
            search_string = search_string_arg;
            search_string_sv = StringValue(search_string);
            substring_pattern = StringSearch(&search_string_sv);
        }
    };

    friend class OpcodeRegistry;

    static void like_prepare(doris_udf::FunctionContext* context,
                             doris_udf::FunctionContext::FunctionStateScope scope);

    static doris_udf::BooleanVal like(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& val,
                                      const doris_udf::StringVal& pattern);

    static void like_close(doris_udf::FunctionContext* context,
                           doris_udf::FunctionContext::FunctionStateScope scope);

    static void regex_prepare(doris_udf::FunctionContext* context,
                              doris_udf::FunctionContext::FunctionStateScope scope);

    static doris_udf::BooleanVal regex(doris_udf::FunctionContext* context,
                                       const doris_udf::StringVal& val,
                                       const doris_udf::StringVal& pattern);

    /// Prepare function for regexp_like() when a third optional parameter is used
    static void regexp_like_prepare(doris_udf::FunctionContext* context,
                                    doris_udf::FunctionContext::FunctionStateScope scope);

    /// Handles regexp_like() when 3 parameters are passed to it
    static doris_udf::BooleanVal regexp_like(doris_udf::FunctionContext* context,
                                             const doris_udf::StringVal& val,
                                             const doris_udf::StringVal& pattern,
                                             const doris_udf::StringVal& match_parameter);

    static void regex_close(doris_udf::FunctionContext*,
                            doris_udf::FunctionContext::FunctionStateScope scope);

    static doris_udf::BooleanVal regex_fn(doris_udf::FunctionContext* context,
                                          const doris_udf::StringVal& val,
                                          const doris_udf::StringVal& pattern);

    static doris_udf::BooleanVal like_fn(doris_udf::FunctionContext* context,
                                         const doris_udf::StringVal& val,
                                         const doris_udf::StringVal& pattern);

    /// Handling of like predicates that map to strstr
    static doris_udf::BooleanVal constant_substring_fn(doris_udf::FunctionContext* context,
                                                       const doris_udf::StringVal& val,
                                                       const doris_udf::StringVal& pattern);

    /// Handling of like predicates that can be implemented using strncmp
    static doris_udf::BooleanVal constant_starts_with_fn(doris_udf::FunctionContext* context,
                                                         const doris_udf::StringVal& val,
                                                         const doris_udf::StringVal& pattern);

    /// Handling of like predicates that can be implemented using strncmp
    static doris_udf::BooleanVal constant_ends_with_fn(doris_udf::FunctionContext* context,
                                                       const doris_udf::StringVal& val,
                                                       const doris_udf::StringVal& pattern);

    /// Handling of like predicates that can be implemented using strcmp
    static doris_udf::BooleanVal constant_equals_fn(doris_udf::FunctionContext* context,
                                                    const doris_udf::StringVal& val,
                                                    const doris_udf::StringVal& pattern);

    static doris_udf::BooleanVal constant_regex_fn_partial(doris_udf::FunctionContext* context,
                                                           const doris_udf::StringVal& val,
                                                           const doris_udf::StringVal& pattern);

    static doris_udf::BooleanVal constant_regex_fn(doris_udf::FunctionContext* context,
                                                   const doris_udf::StringVal& val,
                                                   const doris_udf::StringVal& pattern);

    static doris_udf::BooleanVal regex_match(doris_udf::FunctionContext* context,
                                             const doris_udf::StringVal& val,
                                             const doris_udf::StringVal& pattern,
                                             bool is_like_pattern);

    /// Convert a LIKE pattern (with embedded % and _) into the corresponding
    /// regular expression pattern. Escaped chars are copied verbatim.
    static void convert_like_pattern(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& pattern, std::string* re_pattern);

    static void remove_escape_character(std::string* search_string);
};

} // namespace doris

#endif
