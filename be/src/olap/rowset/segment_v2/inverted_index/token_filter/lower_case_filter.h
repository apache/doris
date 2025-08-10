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

#include <unicode/ucasemap.h>

#include "token_filter.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

/**
 * @brief A token filter that converts Unicode text to lowercase using ICU library.
 * 
 * This filter handles full Unicode case conversion, not just ASCII characters.
 * It uses ICU's ucasemap functionality to properly handle case folding for all Unicode characters.
 */
class LowerCaseFilter : public DorisTokenFilter {
public:
    LowerCaseFilter(const TokenStreamPtr& in)
            : DorisTokenFilter(in), _ucsm(nullptr, &ucasemap_close) {}

    ~LowerCaseFilter() override = default;

    void initialize() {
        UErrorCode status = U_ZERO_ERROR;
        auto* ucsm = ucasemap_open("", 0, &status);
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::RUNTIME_ERROR,
                            "Failed to open UCaseMap. ICU Error: " + std::to_string(status) +
                                    " - " + u_errorName(status));
        }
        _ucsm.reset(ucsm);
    }

    Token* next(Token* t) override {
        if (_in->next(t) == nullptr) {
            return nullptr;
        }
        std::string_view term(t->termBuffer<char>(), t->termLength<char>());

        size_t max_len = term.size() * 2;
        if (_lower_term.size() < max_len) {
            _lower_term.resize(max_len);
        }

        UErrorCode status = U_ZERO_ERROR;
        int32_t result_len = ucasemap_utf8ToLower(_ucsm.get(), _lower_term.data(), max_len,
                                                  term.data(), term.size(), &status);
        if (U_FAILURE(status)) {
            LOG(WARNING) << "Failed to convert to lowercase. "
                         << "Term: '" << term << "', "
                         << "ICU Error: " << status << " - " << u_errorName(status)
                         << ", Buffer size: " << max_len << std::endl;
            return nullptr;
        }

        set_text(t, std::string_view(_lower_term.data(), result_len));
        return t;
    }

    void reset() override { DorisTokenFilter::reset(); }

private:
    std::unique_ptr<UCaseMap, decltype(&ucasemap_close)> _ucsm;
    std::string _lower_term;
};
using LowerCaseFilterPtr = std::shared_ptr<LowerCaseFilter>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index