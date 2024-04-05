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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/parse-util.cc
// and modified by Doris

#include "util/parse_util.h"

#include "util/string_parser.hpp"

namespace doris {

int64_t ParseUtil::parse_mem_spec(const std::string& mem_spec_str, int64_t parent_limit,
                                  int64_t physical_mem, bool* is_percent) {
    if (mem_spec_str.empty()) {
        return 0;
    }

    // Assume last character indicates unit or percent.
    int32_t number_str_len = mem_spec_str.size() - 1;
    *is_percent = false;
    int64_t multiplier = -1;

    // Look for accepted suffix character.
    switch (*mem_spec_str.rbegin()) {
    case 't':
    case 'T':
        // Terabytes.
        multiplier = 1024L * 1024L * 1024L * 1024L;
        break;
    case 'g':
    case 'G':
        // Gigabytes.
        multiplier = 1024L * 1024L * 1024L;
        break;
    case 'm':
    case 'M':
        // Megabytes.
        multiplier = 1024L * 1024L;
        break;
    case 'k':
    case 'K':
        // Kilobytes
        multiplier = 1024L;
        break;
    case 'b':
    case 'B':
        break;
    case '%':
        *is_percent = true;
        break;
    default:
        // No unit was given. Default to bytes.
        number_str_len = mem_spec_str.size();
        break;
    }

    StringParser::ParseResult result;
    int64_t bytes = -1;

    if (multiplier != -1 || *is_percent) {
        // Parse float - MB or GB or percent
        double limit_val =
                StringParser::string_to_float<double>(mem_spec_str.data(), number_str_len, &result);

        if (result != StringParser::PARSE_SUCCESS) {
            return -1;
        }

        if (multiplier != -1) {
            bytes = int64_t(multiplier * limit_val);
        } else if (*is_percent) {
            if (parent_limit == -1) {
                bytes = int64_t(static_cast<double>(limit_val) / 100.0 * physical_mem);
            } else {
                bytes = int64_t(static_cast<double>(limit_val) / 100.0 * parent_limit);
            }
        }
    } else {
        // Parse int - bytes
        int64_t limit_val =
                StringParser::string_to_int<int64_t>(mem_spec_str.data(), number_str_len, &result);

        if (result != StringParser::PARSE_SUCCESS) {
            return -1;
        }
        bytes = limit_val;
    }

    // Accept -1 as indicator for infinite memory that we report by a 0 return value.
    if (bytes == -1) {
        return 0;
    }
    return bytes;
}

} // namespace doris
