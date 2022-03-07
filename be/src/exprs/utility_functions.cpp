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

#include "exprs/utility_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/monotime.h"

namespace doris {

void UtilityFunctions::init() {}

StringVal UtilityFunctions::version(FunctionContext* ctx) {
    return AnyValUtil::from_string_temp(ctx, "5.1.0");
}

BooleanVal UtilityFunctions::sleep(FunctionContext* ctx, const IntVal& seconds) {
    if (seconds.is_null) {
        return BooleanVal::null();
    }
    SleepFor(MonoDelta::FromSeconds(seconds.val));
    return BooleanVal(true);
}

static void convert_to_string(doris_udf::StringVal& src, std::string& str_dst) {
    for (int i = 0; i < src.len; i++) {
        str_dst += src.ptr[i];
    }
    return;
}

static BooleanVal split_string(std::string& str_src, std::vector<std::string>& str_vec_dst) {
    std::string s;
    std::stringstream ss;
    ss << str_src;
    while (std::getline(ss, s, '.')) {
        // return when find a non-digital separated by "." , such as all.8.9 or Chinese, etc.
        if (s.find_first_not_of("0123456789") != std::string::npos) {
            return BooleanVal(false);
        }
        str_vec_dst.push_back(s);
    }
    return BooleanVal(true);
}

static BooleanVal compare_splitted_string(std::vector<std::string>& v1, 
                                          std::vector<std::string>& v2,
                                          std::string& str_op) {
    if (v1.size() == 0 || v2.size() == 0) {
        return BooleanVal(false);
    }

    int min_size = std::min(v1.size(), v2.size());
    for (int i = 0; i < min_size; ++i) {
        // TODO: only implement the comparison logic of ">=" and "<=" now
        if (atoi(v1[i].c_str()) > atoi(v2[i].c_str())) {
            return (str_op == ">=") ? BooleanVal(true) : BooleanVal(false);
        } else if (atoi(v1[i].c_str()) < atoi(v2[i].c_str())) {
            return (str_op == "<=") ? BooleanVal(true) : BooleanVal(false);
        } else {
            continue;
        }
    }

    if (v1.size() < v2.size()) {
        return (str_op == ">=") ? BooleanVal(true) : BooleanVal(false);
    } else if (v1.size() > v2.size()) {
        return (str_op == "<=") ? BooleanVal(true) : BooleanVal(false);
    }

    return BooleanVal(true);
}

// This function will do compare version according to the following rules:
// 1.Only compare the digital version, and only implement the comparison logic of ">=" and "<=", 
// Chinese and English, -, "", etc. are all false compared with any other version.
// 2.Compare the shortest vector that can be covered by truncating according to .,and each segment
// compares with the normal version. 
// 3.In the digital version, the main version is the larger, such as 11.1.1 > 11.1.1.0
BooleanVal UtilityFunctions::compare_version(FunctionContext* context,
                                    doris_udf::StringVal& version1,
                                    doris_udf::StringVal& op,
                                    doris_udf::StringVal& version2) {
    std::vector<std::string> v1, v2;
    std::string str_version1, str_version2, str_op;

    if (version1.is_null || version2.is_null || op.is_null) {
        return BooleanVal(false);
    }

    convert_to_string(version1, str_version1);
    convert_to_string(version2, str_version2);
    convert_to_string(op, str_op);

    if (split_string(str_version1, v1) != BooleanVal(true)
        || split_string(str_version2, v2) != BooleanVal(true)) {
        return BooleanVal(false);
    }

    return compare_splitted_string(v1, v2, str_op);
}

} // namespace doris
