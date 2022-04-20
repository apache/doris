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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/parse-util.h
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON_UTIL_PARSE_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_PARSE_UTIL_H

#include <string>

namespace doris {

// Utility class for parsing information from strings.
class ParseUtil {
public:
    // Parses mem_spec_str and returns the memory size in bytes.
    // Sets *is_percent to indicate whether the given spec is in percent.
    // Accepted formats:
    // '<int>[bB]?'  -> bytes (default if no unit given)
    // '<float>[mM]' -> megabytes
    // '<float>[gG]' -> in gigabytes
    // '<int>%'      -> in percent of MemInfo::physical_mem()
    // Requires MemInfo to be initialized for the '%' spec to work.
    // Returns 0 if mem_spec_str is empty or '-1'.
    // Returns -1 if parsing failed.
    // if is_percent, return the percent of parent_limit.
    // if parent_limit is -1, use physical_mem as parent limit.
    static int64_t parse_mem_spec(const std::string& mem_spec_str, int64_t parent_limit,
                                  int64_t physical_mem, bool* is_percent);
};

} // namespace doris

#endif
