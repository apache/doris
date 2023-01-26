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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/string-functions.h
// and modified by Doris

#pragma once

#include <re2/re2.h>

#include <iomanip>
#include <locale>
#include <sstream>
#include <string_view>

#include "gutil/strings/numbers.h"
#include "vec/common/string_ref.h"

namespace doris {

class OpcodeRegistry;

class StringFunctions {
public:
    static void init();

    static doris_udf::StringVal substring(doris_udf::FunctionContext* context,
                                          const doris_udf::StringVal& str,
                                          const doris_udf::IntVal& pos,
                                          const doris_udf::IntVal& len);
    static doris_udf::StringVal substring(doris_udf::FunctionContext* context,
                                          const doris_udf::StringVal& str,
                                          const doris_udf::IntVal& pos);
    static doris_udf::StringVal left(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& str, const doris_udf::IntVal& len);
    static doris_udf::StringVal right(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& str,
                                      const doris_udf::IntVal& len);
    static doris_udf::BooleanVal starts_with(doris_udf::FunctionContext* context,
                                             const doris_udf::StringVal& str,
                                             const doris_udf::StringVal& prefix);
    static doris_udf::BooleanVal ends_with(doris_udf::FunctionContext* context,
                                           const doris_udf::StringVal& str,
                                           const doris_udf::StringVal& suffix);
    static doris_udf::BooleanVal null_or_empty(doris_udf::FunctionContext* context,
                                               const doris_udf::StringVal& str);
    static doris_udf::BooleanVal not_null_or_empty(doris_udf::FunctionContext* context,
                                                   const doris_udf::StringVal& str);
    static doris_udf::StringVal space(doris_udf::FunctionContext* context,
                                      const doris_udf::IntVal& len);
    static doris_udf::StringVal repeat(doris_udf::FunctionContext* context,
                                       const doris_udf::StringVal& str, const doris_udf::IntVal& n);
    static doris_udf::StringVal lpad(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& str, const doris_udf::IntVal& len,
                                     const doris_udf::StringVal& pad);
    static doris_udf::StringVal rpad(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& str, const doris_udf::IntVal& len,
                                     const doris_udf::StringVal& pad);
    static doris_udf::StringVal append_trailing_char_if_absent(
            doris_udf::FunctionContext* context, const doris_udf::StringVal& str,
            const doris_udf::StringVal& trailing_char);
    static doris_udf::IntVal length(doris_udf::FunctionContext* context,
                                    const doris_udf::StringVal& str);
    static doris_udf::IntVal char_utf8_length(doris_udf::FunctionContext* context,
                                              const doris_udf::StringVal& str);
    static doris_udf::StringVal lower(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& str);
    static doris_udf::StringVal upper(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& str);
    static doris_udf::StringVal initcap(doris_udf::FunctionContext* context,
                                        const doris_udf::StringVal& str);
    static doris_udf::StringVal reverse(doris_udf::FunctionContext* context,
                                        const doris_udf::StringVal& str);
    static doris_udf::StringVal trim(doris_udf::FunctionContext* context,
                                     const doris_udf::StringVal& str);
    static doris_udf::StringVal ltrim(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& str);
    static doris_udf::StringVal rtrim(doris_udf::FunctionContext* context,
                                      const doris_udf::StringVal& str);
    static doris_udf::IntVal ascii(doris_udf::FunctionContext* context,
                                   const doris_udf::StringVal& str);
    static doris_udf::IntVal instr(doris_udf::FunctionContext* context,
                                   const doris_udf::StringVal& str, const doris_udf::StringVal&);
    static doris_udf::IntVal locate(doris_udf::FunctionContext* context,
                                    const doris_udf::StringVal& str, const doris_udf::StringVal&);
    static doris_udf::IntVal locate_pos(doris_udf::FunctionContext* context,
                                        const doris_udf::StringVal& str,
                                        const doris_udf::StringVal&, const doris_udf::IntVal&);

    static bool set_re2_options(const doris_udf::StringVal& match_parameter, std::string* error_str,
                                re2::RE2::Options* opts);

    static void regexp_prepare(doris_udf::FunctionContext*,
                               doris_udf::FunctionContext::FunctionStateScope);
    static StringVal regexp_extract(doris_udf::FunctionContext*, const doris_udf::StringVal& str,
                                    const doris_udf::StringVal& pattern,
                                    const doris_udf::BigIntVal& index);
    static StringVal regexp_replace(doris_udf::FunctionContext*, const doris_udf::StringVal& str,
                                    const doris_udf::StringVal& pattern,
                                    const doris_udf::StringVal& replace);
    static void regexp_close(doris_udf::FunctionContext*,
                             doris_udf::FunctionContext::FunctionStateScope);
    static StringVal concat(doris_udf::FunctionContext*, int num_children, const StringVal* strs);
    static StringVal concat_ws(doris_udf::FunctionContext*, const doris_udf::StringVal& sep,
                               int num_children, const doris_udf::StringVal* strs);
    static StringVal elt(doris_udf::FunctionContext*, const doris_udf::IntVal& pos,
                         int num_children, const StringVal* strs);
    static IntVal find_in_set(doris_udf::FunctionContext*, const doris_udf::StringVal& str,
                              const doris_udf::StringVal& str_set);

    static void parse_url_prepare(doris_udf::FunctionContext*,
                                  doris_udf::FunctionContext::FunctionStateScope);
    static StringVal parse_url(doris_udf::FunctionContext*, const doris_udf::StringVal& url,
                               const doris_udf::StringVal& part);
    static StringVal parse_url_key(doris_udf::FunctionContext*, const doris_udf::StringVal& url,
                                   const doris_udf::StringVal& key,
                                   const doris_udf::StringVal& part);
    static void parse_url_close(doris_udf::FunctionContext*,
                                doris_udf::FunctionContext::FunctionStateScope);

    static StringVal split_part(FunctionContext* context, const StringVal& content,
                                const StringVal& delimiter, const IntVal& field);

    static StringVal replace(FunctionContext* context, const StringVal& origStr,
                             const StringVal& oldStr, const StringVal& newStr);

    static doris_udf::IntVal bit_length(doris_udf::FunctionContext* context,
                                        const doris_udf::StringVal& str);

    static doris_udf::StringVal uuid(doris_udf::FunctionContext*);

    // The caller owns the returned regex. Returns nullptr if the pattern could not be compiled.
    static re2::RE2* compile_regex(const StringVal& pattern, std::string* error_str,
                                   const StringVal& match_parameter);
};
} // namespace doris
