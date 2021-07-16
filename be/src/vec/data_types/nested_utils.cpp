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

#include "vec/data_types/nested_utils.h"

#include "vec/columns/column_const.h"
#include "vec/common/string_utils/string_utils.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

namespace Nested {

std::string concatenate_name(const std::string& nested_table_name,
                             const std::string& nested_field_name) {
    return nested_table_name + "." + nested_field_name;
}

/** Name can be treated as compound if and only if both parts are simple identifiers.
  */
std::pair<std::string, std::string> splitName(const std::string& name) {
    const char* begin = name.data();
    const char* pos = begin;
    const char* end = begin + name.size();

    if (pos >= end || !is_valid_identifier_begin(*pos)) return {name, {}};

    ++pos;

    while (pos < end && is_word_char_ascii(*pos)) ++pos;

    if (pos >= end || *pos != '.') return {name, {}};

    const char* first_end = pos;
    ++pos;
    const char* second_begin = pos;

    if (pos >= end || !is_valid_identifier_begin(*pos)) return {name, {}};

    ++pos;

    while (pos < end && is_word_char_ascii(*pos)) ++pos;

    if (pos != end) return {name, {}};

    return {{begin, first_end}, {second_begin, end}};
}

std::string extract_table_name(const std::string& nested_name) {
    auto splitted = splitName(nested_name);
    return splitted.first;
}

} // namespace Nested

} // namespace doris::vectorized
