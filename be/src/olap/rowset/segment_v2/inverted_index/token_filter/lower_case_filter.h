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

#include <iostream>

#include "token_filter.h"

namespace doris::segment_v2::inverted_index {

class LowerCaseFilter : public DorisTokenFilter {
public:
    LowerCaseFilter(const TokenStreamPtr& in) : DorisTokenFilter(in) {}
    ~LowerCaseFilter() override = default;

    Token* next(Token* t) override {
        if (_in->next(t) == nullptr) {
            return nullptr;
        }

        std::string_view term(t->termBuffer<char>(), t->termLength<char>());
        _uni_str = icu::UnicodeString::fromUTF8(icu::StringPiece(term.data(), term.size()));
        _lower_term.clear();
        _uni_str.toLower().toUTF8String(_lower_term);

        t->set(_lower_term.data(), 0, _lower_term.size());
        return t;
    }

    void reset() override { DorisTokenFilter::reset(); }

private:
    icu::UnicodeString _uni_str;
    std::string _lower_term;
};
using LowerCaseFilterPtr = std::shared_ptr<LowerCaseFilter>;

} // namespace doris::segment_v2::inverted_index