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

#include "vhive_utils.h"

#include <algorithm>
#include <regex>
#include <sstream>

namespace doris {
namespace vectorized {

const std::regex VHiveUtils::PATH_CHAR_TO_ESCAPE("[\\x00-\\x1F\"#%'*/:=?\\\\\\x7F\\{\\[\\]\\^]");

std::string VHiveUtils::make_partition_name(const std::vector<THiveColumn>& columns,
                                            const std::vector<int>& partition_columns_input_index,
                                            const std::vector<std::string>& values) {
    std::stringstream partition_name_stream;

    for (size_t i = 0; i < partition_columns_input_index.size(); i++) {
        if (i > 0) {
            partition_name_stream << '/';
        }
        std::string column = columns[partition_columns_input_index[i]].name;
        std::string value = values[i];
        std::transform(column.begin(), column.end(), column.begin(),
                       [&](char c) { return std::tolower(c); });
        partition_name_stream << escape_path_name(column) << '=' << escape_path_name(value);
    }

    return partition_name_stream.str();
}

std::string VHiveUtils::escape_path_name(const std::string& path) {
    if (path.empty()) {
        return "__HIVE_DEFAULT_PARTITION__";
    }

    std::smatch match;
    if (!std::regex_search(path, match, PATH_CHAR_TO_ESCAPE)) {
        return path;
    }

    std::stringstream ss;
    size_t from_index = 0;
    auto begin = path.begin();
    auto end = path.end();
    while (std::regex_search(begin + from_index, end, match, PATH_CHAR_TO_ESCAPE)) {
        size_t escape_at_index = match.position() + from_index;
        if (escape_at_index > from_index) {
            ss << path.substr(from_index, escape_at_index - from_index);
        }
        char c = path[escape_at_index];
        ss << '%' << std::hex << std::uppercase << static_cast<int>(c >> 4)
           << static_cast<int>(c & 0xF);
        from_index = escape_at_index + 1;
    }
    if (from_index < path.length()) {
        ss << path.substr(from_index);
    }
    return ss.str();
}
} // namespace vectorized
} // namespace doris