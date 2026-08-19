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

#include "exprs/function/functions_multi_string_search.cpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace doris {

TEST(FunctionsMultiStringSearchTest, KeepRegexpsAliveAfterCacheEviction) {
    using Impl = FunctionMultiMatchAnyImpl<TYPE_TINYINT, MultiMatchTraits::Find::Any, false>;

    std::vector<String> target_patterns = {"needle"};
    std::vector<StringRef> target_refs = {{target_patterns[0].data(), target_patterns[0].size()}};
    const size_t target_bucket =
            multiregexps::GlobalCacheTable::getBucketIndexFor(target_patterns, std::nullopt);

    std::vector<String> collision_patterns;
    for (size_t i = 0;; ++i) {
        collision_patterns = {"collision-" + std::to_string(i)};
        if (multiregexps::GlobalCacheTable::getBucketIndexFor(collision_patterns, std::nullopt) ==
            target_bucket) {
            break;
        }
    }
    std::vector<StringRef> collision_refs = {
            {collision_patterns[0].data(), collision_patterns[0].size()}};

    multiregexps::RegexpsPtr target_regexps;
    multiregexps::ScratchPtr target_scratch;
    ASSERT_TRUE(
            Impl::prepare_regexps_and_scratch(target_refs, target_regexps, target_scratch).ok());

    auto collision_owner = multiregexps::getOrSet<false, false>(collision_refs, std::nullopt);
    ASSERT_NE(nullptr, collision_owner->get());

    Impl::ResultType result = 0;
    const std::string haystack = "find the needle";
    const hs_error_t err = hs_scan(target_regexps->getDB(), haystack.data(),
                                   static_cast<unsigned>(haystack.size()), 0, target_scratch.get(),
                                   Impl::on_match, &result);

    EXPECT_EQ(HS_SCAN_TERMINATED, err);
    EXPECT_EQ(1, result);
}

} // namespace doris
