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

#include "storage/index/inverted/analysis/char_filter/char_filter_factory.h"
#include "storage/index/inverted/analysis/char_filter/char_replace_char_filter.h"

namespace doris::segment_v2::inverted_index {

static const std::string CHAR_REPLACE_PATTERN = "pattern";
static const std::string CHAR_REPLACE_REPLACEMENT = "replacement";

static const std::string CHAR_REPLACE_DEFAULT_PATTERN = ",._";

class CharReplaceCharFilterFactory : public CharFilterFactory {
public:
    CharReplaceCharFilterFactory() = default;
    // Ready to use without initialize(), which checks the settings of a custom char filter: the
    // index char filter (char_filter_pattern and char_filter_replacement) comes this way.
    CharReplaceCharFilterFactory(std::string pattern, std::string replacement)
            : _pattern(std::move(pattern)), _replacement(std::move(replacement)) {}
    ~CharReplaceCharFilterFactory() override = default;

    void initialize(const Settings& settings) override {
        _pattern = settings.get_string(CHAR_REPLACE_PATTERN, CHAR_REPLACE_DEFAULT_PATTERN);
        if (_pattern.empty()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Missing '${CHAR_REPLACE_PATTERN}' for char_replace filter type");
        }
        for (char ch : _pattern) {
            unsigned int uc = static_cast<unsigned char>(ch);
            if (uc > 255) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Invalid '${CHAR_REPLACE_PATTERN}' for char_replace "
                                "filter type: each char must "
                                "be in [0,255]");
            }
        }
        _replacement = settings.get_string(CHAR_REPLACE_REPLACEMENT, " ");
        if (_replacement.size() != 1) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Invalid '${CHAR_REPLACE_REPLACEMENT}' for char_replace "
                            "filter type: must be exactly 1 byte");
        }
        unsigned int rep = static_cast<unsigned char>(_replacement[0]);
        if (rep > 255) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Invalid '${CHAR_REPLACE_REPLACEMENT}' for char_replace "
                            "filter type: must be in [0,255]");
        }
    }

    std::unique_ptr<TextCharFilter> create() const override {
        return std::make_unique<CharReplaceCharFilter>(_pattern, _replacement);
    }

private:
    std::string _pattern;
    std::string _replacement;
};

} // namespace doris::segment_v2::inverted_index