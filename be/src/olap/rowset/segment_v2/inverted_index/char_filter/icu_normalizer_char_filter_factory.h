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

#include <unicode/filteredbrk.h>
#include <unicode/normalizer2.h>
#include <unicode/uniset.h>
#include <unicode/unistr.h>

#include <boost/algorithm/string.hpp>
#include <memory>
#include <string>

#include "char_filter_factory.h"
#include "common/exception.h"
#include "icu_normalizer_char_filter.h"

using boost::algorithm::to_lower_copy;
using boost::algorithm::trim_copy;

namespace doris::segment_v2::inverted_index {

class ICUNormalizerCharFilterFactory : public CharFilterFactory {
public:
    ICUNormalizerCharFilterFactory() = default;
    ~ICUNormalizerCharFilterFactory() override = default;

    void initialize(const Settings& settings) override {
        std::string name = settings.get_string("name", "nfkc_cf");
        std::string mode = settings.get_string("mode", "compose");
        std::string unicode_set_filter = settings.get_string("unicode_set_filter", "");
        if (mode != "compose" && mode != "decompose") {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "ICUNormalizerCharFilterFactory: mode must be 'compose' or "
                            "'decompose', got: " +
                                    mode);
        }

        UErrorCode status = U_ZERO_ERROR;
        const icu::Normalizer2* base = get_normalizer(name, mode, status);
        if (U_FAILURE(status) || base == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Failed to get normalizer instance for '" + name + "' with mode '" +
                                    mode + "': " + std::string(u_errorName(status)));
        }

        if (unicode_set_filter.empty()) {
            _normalizer =
                    std::shared_ptr<const icu::Normalizer2>(base, [](const icu::Normalizer2*) {});
            return;
        }

        _unicode_set = std::make_unique<icu::UnicodeSet>(
                icu::UnicodeString::fromUTF8(unicode_set_filter), status);
        if (U_FAILURE(status)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Failed to parse unicode_set_filter: " +
                                                                 std::string(u_errorName(status)));
        }
        if (_unicode_set->isEmpty()) {
            _normalizer =
                    std::shared_ptr<const icu::Normalizer2>(base, [](const icu::Normalizer2*) {});
            return;
        }
        _unicode_set->freeze();

        _normalizer = std::make_shared<icu::FilteredNormalizer2>(*base, *_unicode_set);
    }

    ReaderPtr create(const ReaderPtr& in) override {
        if (!_normalizer) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "ICUNormalizerCharFilterFactory not initialized. Call initialize() "
                            "first.");
        }
        auto filter = std::make_shared<ICUNormalizerCharFilter>(in, _normalizer);
        filter->initialize();
        return filter;
    }

private:
    static const icu::Normalizer2* get_normalizer(const std::string& name, const std::string& mode,
                                                  UErrorCode& status) {
        std::string lower_name = to_lower_copy(trim_copy(name));
        UNormalization2Mode icu_mode = (mode == "compose" ? UNORM2_COMPOSE : UNORM2_DECOMPOSE);
        if (lower_name == "nfc" || lower_name == "nfkc" || lower_name == "nfkc_cf") {
            return icu::Normalizer2::getInstance(nullptr, lower_name.c_str(), icu_mode, status);
        }

        if (lower_name == "nfd") {
            return icu::Normalizer2::getNFDInstance(status);
        } else if (lower_name == "nfkd") {
            return icu::Normalizer2::getNFKDInstance(status);
        }

        status = U_ILLEGAL_ARGUMENT_ERROR;
        return nullptr;
    }

    std::unique_ptr<icu::UnicodeSet> _unicode_set;
    std::shared_ptr<const icu::Normalizer2> _normalizer;
};
using ICUNormalizerCharFilterFactoryPtr = std::shared_ptr<ICUNormalizerCharFilterFactory>;

} // namespace doris::segment_v2::inverted_index
