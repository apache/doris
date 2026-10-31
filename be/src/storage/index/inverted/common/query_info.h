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

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace doris::segment_v2 {

class TermInfo {
public:
    using Term = std::variant<std::string, std::vector<std::string>>;

    Term term;
    int32_t position = 0;

    bool is_single_term() const { return std::holds_alternative<std::string>(term); }
    bool is_multi_terms() const { return std::holds_alternative<std::vector<std::string>>(term); }

    const std::string& get_single_term() const { return std::get<std::string>(term); }
    const std::vector<std::string>& get_multi_terms() const {
        return std::get<std::vector<std::string>>(term);
    }
};

// The name BM25 statistics of a MATCH are kept under. Two indexes of one column hold different
// terms and lengths under the same field, so the field alone cannot key them: over Apple and
// apple, an index that folds case has df(apple) = 2 and one that does not has df(apple) = 1.
inline std::wstring match_statistics_field(const std::wstring& field, int64_t index_id) {
    return field + L"@" + std::to_wstring(index_id);
}

} // namespace doris::segment_v2
