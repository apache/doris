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

#include "exprs/string_functions.h"

#include <gtest/gtest.h>
#include <re2/re2.h>

#include <memory>

namespace doris {

class StringFunctionsTest : public ::testing::Test {
protected:
    StringRef create_string_ref(const std::string& str) {
        return StringRef(str.c_str(), str.size());
    }
};

TEST_F(StringFunctionsTest, TestSetRe2Options) {
    re2::RE2::Options opts;
    std::string error_str;

    // Test case sensitivity options
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref("i"), &error_str, &opts));
    EXPECT_FALSE(opts.case_sensitive());

    opts = re2::RE2::Options(); // Reset options
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref("c"), &error_str, &opts));
    EXPECT_TRUE(opts.case_sensitive());

    // Test multiline options
    opts = re2::RE2::Options();
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref("m"), &error_str, &opts));
    EXPECT_TRUE(opts.posix_syntax());
    EXPECT_FALSE(opts.one_line());

    // Test newline options
    opts = re2::RE2::Options();
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref("n"), &error_str, &opts));
    EXPECT_FALSE(opts.never_nl());
    EXPECT_TRUE(opts.dot_nl());

    // Test combined options
    opts = re2::RE2::Options();
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref("icmn"), &error_str, &opts));
    EXPECT_TRUE(opts.case_sensitive());
    EXPECT_TRUE(opts.posix_syntax());
    EXPECT_FALSE(opts.one_line());
    EXPECT_FALSE(opts.never_nl());
    EXPECT_TRUE(opts.dot_nl());

    // Test empty options
    opts = re2::RE2::Options();
    EXPECT_TRUE(StringFunctions::set_re2_options(create_string_ref(""), &error_str, &opts));

    // Test invalid options
    opts = re2::RE2::Options();
    EXPECT_FALSE(StringFunctions::set_re2_options(create_string_ref("x"), &error_str, &opts));
    EXPECT_EQ(error_str, "Illegal match parameter x");
}

TEST_F(StringFunctionsTest, TestCompileRegex) {
    std::string error_str;
    std::unique_ptr<re2::RE2> re;

    // Test valid pattern
    EXPECT_TRUE(StringFunctions::compile_regex(create_string_ref("abc"), &error_str,
                                               create_string_ref(""), create_string_ref(""), re));
    EXPECT_TRUE(re != nullptr);
    EXPECT_TRUE(re->ok());

    // Test case insensitive
    EXPECT_TRUE(StringFunctions::compile_regex(create_string_ref("abc"), &error_str,
                                               create_string_ref("i"), create_string_ref(""), re));
    EXPECT_TRUE(re != nullptr);
    EXPECT_TRUE(re->ok());
    EXPECT_FALSE(re->options().case_sensitive());

    // Test invalid pattern
    EXPECT_FALSE(StringFunctions::compile_regex(create_string_ref("a(bc"), &error_str,
                                                create_string_ref(""), create_string_ref(""), re));
    EXPECT_TRUE(re == nullptr);
    EXPECT_FALSE(error_str.empty());

    // Test empty pattern
    EXPECT_TRUE(StringFunctions::compile_regex(create_string_ref(""), &error_str,
                                               create_string_ref(""), create_string_ref(""), re));
    EXPECT_TRUE(re != nullptr);
    EXPECT_TRUE(re->ok());

    re.reset();
    // Test invalid match parameter
    EXPECT_FALSE(StringFunctions::compile_regex(create_string_ref("abc"), &error_str,
                                                create_string_ref("x"), create_string_ref(""), re));
    EXPECT_TRUE(re == nullptr);
    EXPECT_EQ(error_str, "Illegal match parameter x");

    // Test combined options
    EXPECT_TRUE(StringFunctions::compile_regex(create_string_ref("abc"), &error_str,
                                               create_string_ref("imn"), create_string_ref(""),
                                               re));
    EXPECT_TRUE(re != nullptr);
    EXPECT_TRUE(re->ok());
    EXPECT_FALSE(re->options().case_sensitive());
    EXPECT_FALSE(re->options().one_line());
    EXPECT_TRUE(re->options().dot_nl());
}

} // namespace doris