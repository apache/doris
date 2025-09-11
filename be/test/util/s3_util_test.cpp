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

#include "util/s3_util.h"

#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "util/s3_uri.h"

namespace doris {

class S3UTILTest : public testing::Test {
public:
    S3UTILTest() = default;
    ~S3UTILTest() = default;
}; // end class S3UTILTest

TEST_F(S3UTILTest, hide_access_key_empty) {
    EXPECT_EQ("", hide_access_key(""));
}

TEST_F(S3UTILTest, hide_access_key_single_char) {
    EXPECT_EQ("x", hide_access_key("A"));
}

TEST_F(S3UTILTest, hide_access_key_two_chars) {
    EXPECT_EQ("xx", hide_access_key("AB"));
}

TEST_F(S3UTILTest, hide_access_key_three_chars) {
    EXPECT_EQ("xBx", hide_access_key("ABC"));
}

TEST_F(S3UTILTest, hide_access_key_four_chars) {
    EXPECT_EQ("xBCx", hide_access_key("ABCD"));
}

TEST_F(S3UTILTest, hide_access_key_six_chars) {
    EXPECT_EQ("xBCDEx", hide_access_key("ABCDEF"));
}

TEST_F(S3UTILTest, hide_access_key_seven_chars) {
    EXPECT_EQ("xBCDEFx", hide_access_key("ABCDEFG"));
}

TEST_F(S3UTILTest, hide_access_key_normal_length) {
    EXPECT_EQ("xxxDEFGHIxxx", hide_access_key("ABCDEFGHIJKL"));
}

TEST_F(S3UTILTest, hide_access_key_long_key) {
    std::string long_key = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    std::string result = hide_access_key(long_key);
    EXPECT_EQ("xxxxxxxxxxxxxxxPQRSTUxxxxxxxxxxxxxxx", result);
}

TEST_F(S3UTILTest, hide_access_key_typical_aws_key) {
    std::string aws_key = "AKIAIOSFODNN7EXAMPLE";
    std::string result = hide_access_key(aws_key);
    EXPECT_EQ("xxxxxxxFODNN7xxxxxxx", result);
}

} // end namespace doris
