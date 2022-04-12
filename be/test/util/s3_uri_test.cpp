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

#include "util/s3_uri.h"

#include <gtest/gtest.h>

#include <string>

#include "util/logging.h"

namespace doris {

class S3URITest : public testing::Test {
public:
    S3URITest() {}
    ~S3URITest() {}
}; // end class StringParserTest

TEST_F(S3URITest, LocationParsing) {
    std::string p1 = "s3://bucket/path/to/file";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path/to/file", uri1.get_key());
}

TEST_F(S3URITest, PathLocationParsing) {
    std::string p1 = "s3://bucket/path/";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path/", uri1.get_key());
}

TEST_F(S3URITest, EncodedString) {
    std::string p1 = "s3://bucket/path%20to%20file";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path%20to%20file", uri1.get_key());
}

TEST_F(S3URITest, MissingKey) {
    std::string p1 = "https://bucket/";
    S3URI uri1(p1);
    EXPECT_FALSE(uri1.parse());
    std::string p2 = "s3://bucket/";
    S3URI uri2(p2);
    EXPECT_FALSE(uri2.parse());
}

TEST_F(S3URITest, RelativePathing) {
    std::string p1 = "/path/to/file";
    S3URI uri1(p1);
    EXPECT_FALSE(uri1.parse());
}

TEST_F(S3URITest, InvalidScheme) {
    std::string p1 = "ftp://bucket/";
    S3URI uri1(p1);
    EXPECT_FALSE(uri1.parse());
}

TEST_F(S3URITest, QueryAndFragment) {
    std::string p1 = "s3://bucket/path/to/file?query=foo#bar";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path/to/file", uri1.get_key());
}

} // end namespace doris
