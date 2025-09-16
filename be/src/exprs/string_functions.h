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

#include <memory>
#include <string>

#include "vec/common/string_ref.h"

namespace doris {

class StringFunctions {
public:
    static bool set_re2_options(const doris::StringRef& match_parameter, std::string* error_str,
                                re2::RE2::Options* opts);

    // The caller owns the returned regex. Returns nullptr if the pattern could not be compiled.
    static bool compile_regex(const StringRef& pattern, std::string* error_str,
                              const StringRef& match_parameter, const StringRef& options_value,
                              std::unique_ptr<re2::RE2>& re);
};
} // namespace doris
