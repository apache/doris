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

#include "cpp/obj-client/auth/gcp/gcs_signed_url.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>

namespace doris {
namespace {

TEST(GcsSignedUrlTest, BuildsV4UrlAndPassesCanonicalStringToSigner) {
    GcsV4SignedUrlOptions options {
            .endpoint = "storage.googleapis.com/",
            .bucket = "test-bucket",
            .key = "error_log/a b+%.csv",
            .signer_email = "signer@my-project.iam.gserviceaccount.com",
            .expiration_secs = 60,
    };
    std::string captured_string_to_sign;
    auto result = build_gcs_v4_signed_url(
            options, std::chrono::system_clock::time_point {},
            [&](std::string_view string_to_sign) {
                captured_string_to_sign = string_to_sign;
                return GcsSignBlobResult {.signature = std::string("\x00\xab\xff", 3)};
            });

    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signed_url,
              "https://storage.googleapis.com/test-bucket/error_log/a%20b%2B%25.csv?"
              "X-Goog-Algorithm=GOOG4-RSA-SHA256&"
              "X-Goog-Credential=signer%40my-project.iam.gserviceaccount.com%2F19700101%2Fauto%"
              "2Fstorage%2Fgoog4_request&X-Goog-Date=19700101T000000Z&X-Goog-Expires=60&"
              "X-Goog-SignedHeaders=host&X-Goog-Signature=00abff");

    EXPECT_EQ(captured_string_to_sign,
              "GOOG4-RSA-SHA256\n19700101T000000Z\n"
              "19700101/auto/storage/goog4_request\n"
              "f741db2e699530357ae34622ae689c7ec43e10e418af0ff7f1487beb1f144ce5");
}

TEST(GcsSignedUrlTest, DefaultHttpsPortProducesTheSameSignatureAndUrl) {
    GcsV4SignedUrlOptions options {
            .endpoint = "https://storage.googleapis.com",
            .bucket = "test-bucket",
            .key = "error_log/id",
            .signer_email = "signer@my-project.iam.gserviceaccount.com",
            .expiration_secs = 60,
    };
    std::string without_port;
    auto expected = build_gcs_v4_signed_url(options, std::chrono::system_clock::time_point {},
                                            [&](std::string_view value) {
                                                without_port = value;
                                                return GcsSignBlobResult {.signature = "test"};
                                            });
    ASSERT_TRUE(expected.ok()) << expected.error;
    for (const auto* endpoint :
         {"https://storage.googleapis.com:443", "storage.googleapis.com:443/"}) {
        options.endpoint = endpoint;
        std::string with_port;
        auto result = build_gcs_v4_signed_url(options, std::chrono::system_clock::time_point {},
                                              [&](std::string_view value) {
                                                  with_port = value;
                                                  return GcsSignBlobResult {.signature = "test"};
                                              });
        ASSERT_TRUE(result.ok()) << result.error;
        EXPECT_EQ(result.signed_url, expected.signed_url);
        EXPECT_EQ(with_port, without_port);
    }
}

TEST(GcsSignedUrlTest, RejectsBucketEndpointsBeforeSigning) {
    GcsV4SignedUrlOptions options {
            .endpoint = "",
            .bucket = "test-bucket",
            .key = "error_log/id",
            .signer_email = "signer@my-project.iam.gserviceaccount.com",
            .expiration_secs = 60,
    };
    for (const auto* endpoint : {"https://test-bucket.storage.googleapis.com",
                                 "https://test-bucket.us-central1-storage.googleapis.com",
                                 "https://test-bucket.storage.us-central1.rep.googleapis.com"}) {
        options.endpoint = endpoint;
        bool signer_called = false;
        auto result = build_gcs_v4_signed_url(options, std::chrono::system_clock::time_point {},
                                              [&](std::string_view) {
                                                  signer_called = true;
                                                  return GcsSignBlobResult {.signature = "test"};
                                              });
        EXPECT_FALSE(result.ok()) << endpoint;
        EXPECT_FALSE(signer_called);
    }
}

TEST(GcsSignedUrlTest, PreservesLiteralLeadingSlashInObjectKey) {
    GcsV4SignedUrlOptions options {
            .endpoint = "https://storage.googleapis.com",
            .bucket = "test-bucket",
            .key = "/error_log/id",
            .signer_email = "signer@my-project.iam.gserviceaccount.com",
            .expiration_secs = 60,
    };
    auto result = build_gcs_v4_signed_url(
            options, std::chrono::system_clock::time_point {},
            [](std::string_view) { return GcsSignBlobResult {.signature = "test"}; });
    ASSERT_TRUE(result.ok()) << result.error;
    EXPECT_TRUE(result.signed_url.starts_with(
            "https://storage.googleapis.com/test-bucket//error_log/id?"));
}

TEST(GcsSignedUrlTest, RejectsInvalidExpirationBeforeSigning) {
    GcsV4SignedUrlOptions options {
            .endpoint = "storage.googleapis.com",
            .bucket = "test-bucket",
            .key = "error_log/file",
            .signer_email = "signer@my-project.iam.gserviceaccount.com",
            .expiration_secs = 7 * 24 * 60 * 60 + 1,
    };
    bool signer_called = false;
    auto result = build_gcs_v4_signed_url(options, std::chrono::system_clock::time_point {},
                                          [&](std::string_view) {
                                              signer_called = true;
                                              return GcsSignBlobResult {};
                                          });

    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(signer_called);
    EXPECT_TRUE(result.signed_url.empty());
}

} // namespace
} // namespace doris
