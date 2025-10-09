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

#include <cstddef>
#include <cstdint>

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct LocaleLib {
    size_t count;
    const char* name;
    const char** type_name;
};

struct LocaleValue {
    uint32_t number;
    const char* name;
    const char* description;
    bool is_ascii;

    LocaleLib* month_names;
    LocaleLib* ab_month_names;
    LocaleLib* day_names;
    LocaleLib* ab_day_names;
};

#include "common/compile_check_end.h"

extern LocaleValue locale_en_US;
extern LocaleValue* locale_values[];
extern LocaleValue* default_locale_value;

LocaleValue* locale_value_by_name(const char* name, std::size_t length);
LocaleValue* locale_value_by_number(std::uint32_t number);

} // namespace doris::vectorized
