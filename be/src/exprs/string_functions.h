// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_STRING_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_STRING_FUNCTIONS_H

#include <re2/re2.h>

#include "runtime/string_value.h"
#include "runtime/string_search.hpp"

namespace palo {

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
public:
    static void init();

    static palo_udf::StringVal substring(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str, 
        const palo_udf::IntVal& pos, const palo_udf::IntVal& len);
    static palo_udf::StringVal substring(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str, 
        const palo_udf::IntVal& pos);
    static palo_udf::StringVal left(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str, 
        const palo_udf::IntVal& len);
    static palo_udf::StringVal right(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str, 
        const palo_udf::IntVal& len);
    static palo_udf::StringVal space(
        palo_udf::FunctionContext* context, const palo_udf::IntVal& len); 
    static palo_udf::StringVal repeat(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::IntVal& n); 
    static palo_udf::StringVal lpad(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::IntVal& len, const palo_udf::StringVal& pad); 
    static palo_udf::StringVal rpad(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::IntVal& len, const palo_udf::StringVal& pad); 
    static palo_udf::IntVal length(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal lower(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal upper(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal reverse(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal trim(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal ltrim(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::StringVal rtrim(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::IntVal ascii(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str);
    static palo_udf::IntVal instr(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::StringVal&);
    static palo_udf::IntVal locate(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::StringVal&);
    static palo_udf::IntVal locate_pos(
        palo_udf::FunctionContext* context, const palo_udf::StringVal& str,
        const palo_udf::StringVal&, const palo_udf::IntVal&);

    static bool set_re2_options(
        const palo_udf::StringVal& match_parameter, 
        std::string* error_str,
        re2::RE2::Options* opts);

    static void regexp_prepare(
        palo_udf::FunctionContext*, 
        palo_udf::FunctionContext::FunctionStateScope);
    static StringVal regexp_extract(
        palo_udf::FunctionContext*, const palo_udf::StringVal& str,
        const palo_udf::StringVal& pattern, const palo_udf::BigIntVal& index);
    static StringVal regexp_replace(
        palo_udf::FunctionContext*, const palo_udf::StringVal& str,
        const palo_udf::StringVal& pattern, const palo_udf::StringVal& replace);
    static void regexp_close(
        palo_udf::FunctionContext*, 
        palo_udf::FunctionContext::FunctionStateScope);
#if 0
    static void RegexpMatchCountPrepare(FunctionContext* context,
                                        FunctionContext::FunctionStateScope scope);
    static IntVal RegexpMatchCount2Args(FunctionContext* context, const StringVal& str,
                                        const StringVal& pattern);
    static IntVal RegexpMatchCount4Args(FunctionContext* context, const StringVal& str,
                                        const StringVal& pattern, const IntVal& start_pos,
                                        const StringVal& match_parameter);
#endif
    static StringVal concat(
        palo_udf::FunctionContext*, 
        int num_children, 
        const StringVal* strs);
    static StringVal concat_ws(
        palo_udf::FunctionContext*, 
        const palo_udf::StringVal& sep, 
        int num_children,
        const palo_udf::StringVal* strs);
    static IntVal find_in_set(
        palo_udf::FunctionContext*, 
        const palo_udf::StringVal& str,
        const palo_udf::StringVal& str_set);

    static void parse_url_prepare(
        palo_udf::FunctionContext*,
        palo_udf::FunctionContext::FunctionStateScope);
    static StringVal parse_url(
        palo_udf::FunctionContext*, 
        const palo_udf::StringVal& url,
        const palo_udf::StringVal& part);
    static StringVal parse_url_key(
        palo_udf::FunctionContext*, 
        const palo_udf::StringVal& url,
        const palo_udf::StringVal& key, 
        const palo_udf::StringVal& part);
    static void parse_url_close(
        palo_udf::FunctionContext*,
        palo_udf::FunctionContext::FunctionStateScope);
};

}

#endif
