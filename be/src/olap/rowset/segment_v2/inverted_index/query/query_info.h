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

#include <variant>
#include <vector>

namespace doris::segment_v2 {

struct TermInfo {
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

struct InvertedIndexQueryInfo {
    std::wstring field_name;
    std::vector<TermInfo> term_infos;
    int32_t slop = 0;
    bool ordered = false;

    std::string generate_tokens_key() const {
        std::string key;
        for (const auto& token : term_infos) {
            key += token.get_single_term() + std::to_string(token.position) + " ";
        }
        if (!key.empty()) {
            key.pop_back();
        }
        return key;
    }
};

} // namespace doris::segment_v2