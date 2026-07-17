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

#include "common/config.h"
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

TEST_F(S3UTILTest, is_s3_express_context) {
    EXPECT_TRUE(is_s3_express("bucket--use1-az4--x-s3",
                              "https://s3.us-east-1.amazonaws.com"));
    EXPECT_TRUE(is_s3_express("bucket--use1-az4--x-s3",
                              "bucket--use1-az4--x-s3.s3express-use1-az4.us-east-1."
                              "amazonaws.com"));
    EXPECT_TRUE(is_s3_express("bucket--cnn1-az1--x-s3",
                              "https://s3.cn-north-1.amazonaws.com.cn"));

    // S3-compatible services must retain their existing endpoint and checksum behavior,
    // even if a bucket happens to use the Directory Bucket suffix.
    EXPECT_FALSE(is_s3_express("bucket--use1-az4--x-s3", "https://minio.example.com"));
    EXPECT_FALSE(is_s3_express("bucket--use1-az4--x-s3",
                               "https://s3.us-east-1.amazonaws.com.example.com"));
    EXPECT_FALSE(is_s3_express("bucket--x-s3", "https://s3.us-east-1.amazonaws.com"));
    EXPECT_FALSE(is_s3_express("bucket--zone--x-s3", "https://s3.us-east-1.amazonaws.com"));
    EXPECT_FALSE(is_s3_express("bucket", "https://example.com/s3express/path"));
}

// Verifies that check_s3_rate_limiter_config_changed() rebuilds the global GET rate
// limiter when the related configs change. This is the behavior the cloud vault refresh
// thread relies on to apply dynamically modified s3_get_* rate limiter configs without
// having to (re)create an S3 client.
TEST_F(S3UTILTest, check_s3_rate_limiter_config_changed_rebuilds_limiter) {
    auto* get_limiter = S3ClientFactory::instance().rate_limiter(S3RateLimitType::GET);
    ASSERT_NE(get_limiter, nullptr);

    // Save originals so other tests are not affected.
    const int64_t orig_tps = config::s3_get_token_per_second;
    const int64_t orig_bucket = config::s3_get_bucket_tokens;
    const int64_t orig_limit = config::s3_get_token_limit;

    // Establish a known baseline (no count limit, no throttling).
    config::s3_get_token_per_second = 1000000000;
    config::s3_get_bucket_tokens = 1000000000;
    config::s3_get_token_limit = 0;
    check_s3_rate_limiter_config_changed();

    // Impose a hard request-count limit of 3. Since the limit value changes (0 -> 3),
    // the limiter is rebuilt with a fresh counter.
    config::s3_get_token_limit = 3;
    check_s3_rate_limiter_config_changed();

    // The bucket/speed are huge so add() never throttles (returns 0); only the count
    // limit takes effect: the first 3 requests pass, the 4th is rejected (-1).
    EXPECT_GE(get_limiter->add(1), 0);
    EXPECT_GE(get_limiter->add(1), 0);
    EXPECT_GE(get_limiter->add(1), 0);
    EXPECT_LT(get_limiter->add(1), 0);

    // Raise the limit. The checker must rebuild the limiter so the exhausted counter is
    // reset; otherwise the next request would still be rejected.
    config::s3_get_token_limit = 100;
    check_s3_rate_limiter_config_changed();
    EXPECT_GE(get_limiter->add(1), 0);

    // Restore original configs and apply them back to the limiter.
    config::s3_get_token_per_second = orig_tps;
    config::s3_get_bucket_tokens = orig_bucket;
    config::s3_get_token_limit = orig_limit;
    check_s3_rate_limiter_config_changed();
}

} // end namespace doris
