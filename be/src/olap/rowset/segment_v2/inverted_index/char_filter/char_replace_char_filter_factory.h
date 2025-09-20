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

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_replace_char_filter.h"

namespace doris::segment_v2::inverted_index {

static const std::string CHAR_REPLACE_CHAR_FILTER_TYPE = "char_filter_type";
static const std::string CHAR_REPLACE_CHAR_FILTER_PATTERN = "char_filter_pattern";
static const std::string CHAR_REPLACE_CHAR_FILTER_REPLACEMENT = "char_filter_replacement";

static const std::string CHAR_REPLACE_TYPE = "char_replace";

class CharReplaceCharFilterFactory : public CharFilterFactory {
public:
    CharReplaceCharFilterFactory() = default;
    ~CharReplaceCharFilterFactory() override = default;

    void initialize(const Settings& settings) override {
        _pattern = settings.get_string(CHAR_REPLACE_CHAR_FILTER_PATTERN);
        if (_pattern.empty()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Missing 'char_filter_pattern' for 'char_replace' filter type");
        }
        _replacement = settings.get_string(CHAR_REPLACE_CHAR_FILTER_REPLACEMENT, " ");
    }

    ReaderPtr create(const ReaderPtr& reader) override {
        auto r = std::make_shared<CharReplaceCharFilter>(reader, _pattern, _replacement);
        r->initialize();
        return r;
    }

private:
    std::string _pattern;
    std::string _replacement;
};

} // namespace doris::segment_v2::inverted_index