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

#include <memory>

#include "common/exception.h"
#include "storage/index/inverted/token_filter/common_grams_filter.h"
#include "storage/index/inverted/token_filter/token_filter_factory.h"

namespace doris::segment_v2::inverted_index {

class CommonGramsFilterFactory final : public TokenFilterFactory {
public:
    explicit CommonGramsFilterFactory(std::shared_ptr<const CommonWordSet> common_words = nullptr)
            : _common_words(std::move(common_words)) {}

    void initialize(const Settings& settings) override {
        if (!settings.empty()) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "common_grams does not accept settings");
        }
        if (_common_words == nullptr) {
            _common_words = CommonWordSet::default_word_set();
        }
    }

    TokenFilterPtr create(const TokenStreamPtr& in) override {
        return std::make_shared<CommonGramsFilter>(in, _common_words, _output_mode);
    }

    void set_common_words(std::shared_ptr<const CommonWordSet> common_words) {
        _common_words = std::move(common_words);
    }

    void set_output_mode(CommonGramsOutputMode output_mode) { _output_mode = output_mode; }

    const std::shared_ptr<const CommonWordSet>& common_words() const { return _common_words; }

private:
    std::shared_ptr<const CommonWordSet> _common_words;
    CommonGramsOutputMode _output_mode = CommonGramsOutputMode::kLogical;
};

class CommonGramsPositionFilterFactory final : public TokenFilterFactory {
public:
    void initialize(const Settings&) override {}

    TokenFilterPtr create(const TokenStreamPtr& in) override {
        return std::make_shared<CommonGramsPositionFilter>(in);
    }

    PositionCapability position_capability() const override {
        return PositionCapability::kAlwaysUnitIncrement;
    }
};

class CommonGramsQueryFilterFactory final : public TokenFilterFactory {
public:
    explicit CommonGramsQueryFilterFactory(std::shared_ptr<const CommonWordSet> common_words)
            : _common_words(std::move(common_words)) {}

    void initialize(const Settings&) override {}

    TokenFilterPtr create(const TokenStreamPtr& in) override {
        return std::make_shared<CommonGramsQueryFilter>(in, _common_words);
    }

    const std::shared_ptr<const CommonWordSet>& common_words() const { return _common_words; }

private:
    std::shared_ptr<const CommonWordSet> _common_words;
};

class CommonGramsPhrasePrefixFilterFactory final : public TokenFilterFactory {
public:
    explicit CommonGramsPhrasePrefixFilterFactory(std::shared_ptr<const CommonWordSet> common_words)
            : _common_words(std::move(common_words)) {}

    void initialize(const Settings&) override {}

    TokenFilterPtr create(const TokenStreamPtr& in) override {
        return std::make_shared<CommonGramsPhrasePrefixFilter>(in, _common_words);
    }

    const std::shared_ptr<const CommonWordSet>& common_words() const { return _common_words; }

private:
    std::shared_ptr<const CommonWordSet> _common_words;
};

} // namespace doris::segment_v2::inverted_index
