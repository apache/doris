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

#include "io/obj_storage/obj_storage_uri.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris {

// ObjStorageUri::parse_s3 keeps the rules of the former S3URI; these are its original cases.
class ObjStorageUriS3RulesTest : public testing::Test {};

TEST_F(ObjStorageUriS3RulesTest, LocationParsing) {
    std::string p1 = "s3://bucket/path/to/file";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("bucket", uri1->bucket);
    EXPECT_EQ("path/to/file", uri1->key);
}

TEST_F(ObjStorageUriS3RulesTest, PathLocationParsing) {
    std::string p1 = "s3://bucket/path/";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("bucket", uri1->bucket);
    EXPECT_EQ("path/", uri1->key);
}

TEST_F(ObjStorageUriS3RulesTest, EncodedString) {
    std::string p1 = "s3://bucket/path%20to%20file";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("bucket", uri1->bucket);
    EXPECT_EQ("path%20to%20file", uri1->key);
}

TEST_F(ObjStorageUriS3RulesTest, HttpURI) {
    std::string p1 = "http://a.b.com/bucket/path/to/file";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("bucket", uri1->bucket);
    EXPECT_EQ("path/to/file", uri1->key);

    std::string p2 = "https://a.b.com/bucket/path/to/file";
    auto uri2 = io::ObjStorageUri::parse_s3(p2);
    ASSERT_TRUE(uri2.has_value()) << uri2.error();
    EXPECT_EQ("bucket", uri2->bucket);
    EXPECT_EQ("path/to/file", uri2->key);
}

TEST_F(ObjStorageUriS3RulesTest, InvalidSchema) {
    std::string p1 = "xxx://a.b.com/bucket/path/to/file";
    EXPECT_FALSE(io::ObjStorageUri::parse_s3(p1).has_value());
}

TEST_F(ObjStorageUriS3RulesTest, MissingKey) {
    std::string p1 = "https://bucket/";
    EXPECT_FALSE(io::ObjStorageUri::parse_s3(p1).has_value());

    std::string p2 = "s3://bucket/";
    EXPECT_FALSE(io::ObjStorageUri::parse_s3(p2).has_value());

    std::string p3 = "http://a.b.com/bucket/";
    EXPECT_FALSE(io::ObjStorageUri::parse_s3(p3).has_value());

    std::string p4 = "http://a.b.com/";
    EXPECT_FALSE(io::ObjStorageUri::parse_s3(p4).has_value());
}

TEST_F(ObjStorageUriS3RulesTest, RelativePathing) {
    std::string p1 = "/path/to/file";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("/path/to/file", uri1->key);
}

TEST_F(ObjStorageUriS3RulesTest, QueryAndFragment) {
    std::string p1 = "s3://bucket/path/to/file?query=foo#bar";
    auto uri1 = io::ObjStorageUri::parse_s3(p1);
    ASSERT_TRUE(uri1.has_value()) << uri1.error();
    EXPECT_EQ("bucket", uri1->bucket);
    EXPECT_EQ("path/to/file", uri1->key);
}

TEST_F(ObjStorageUriS3RulesTest, BucketOnlyKeyOnlyAndOriginal) {
    // s3://bucket keeps the historical key "/".
    auto bucket_only = io::ObjStorageUri::parse_s3("s3://bucket");
    ASSERT_TRUE(bucket_only.has_value()) << bucket_only.error();
    EXPECT_EQ("bucket", bucket_only->bucket);
    EXPECT_EQ("/", bucket_only->key);

    // No scheme: a key relative to the binding's bucket.
    auto key_only = io::ObjStorageUri::parse_s3("dir/file");
    ASSERT_TRUE(key_only.has_value()) << key_only.error();
    EXPECT_EQ("", key_only->bucket);
    EXPECT_EQ("dir/file", key_only->key);

    // The location is kept as given.
    auto http = io::ObjStorageUri::parse_s3("https://a.b.com/bucket/k?x=1");
    ASSERT_TRUE(http.has_value()) << http.error();
    EXPECT_EQ("https://a.b.com/bucket/k?x=1", http->original);
    EXPECT_EQ("k", http->key);
}

} // end namespace doris
