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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

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

TEST_F(S3URITest, HttpURI) {
    std::string p1 = "http://a.b.com/bucket/path/to/file";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path/to/file", uri1.get_key());

    std::string p2 = "https://a.b.com/bucket/path/to/file";
    S3URI uri2(p2);
    EXPECT_TRUE(uri2.parse());
    EXPECT_EQ("bucket", uri2.get_bucket());
    EXPECT_EQ("path/to/file", uri2.get_key());
}

TEST_F(S3URITest, AzureDataLakeURI) {
    S3URI uri("abfss://container@account.dfs.core.windows.net/path/to/file.parquet");
    ASSERT_TRUE(uri.parse().ok());
    EXPECT_EQ("container", uri.get_bucket());
    EXPECT_EQ("path/to/file.parquet", uri.get_key());
    EXPECT_EQ("account.dfs.core.windows.net", uri.get_endpoint());
    EXPECT_EQ("account", uri.get_account());
    EXPECT_TRUE(uri.is_azure());

    S3URI uppercase("ABFSS://container@account.dfs.core.windows.net/path/file.parquet");
    ASSERT_TRUE(uppercase.parse().ok());
    EXPECT_EQ("container", uppercase.get_bucket());
    EXPECT_EQ("path/file.parquet", uppercase.get_key());
    EXPECT_TRUE(uppercase.is_azure());

    S3URI wasbs("wasbs://container@account.blob.core.windows.net/path/to/file.parquet");
    ASSERT_TRUE(wasbs.parse().ok());
    EXPECT_EQ("container", wasbs.get_bucket());
    EXPECT_EQ("path/to/file.parquet", wasbs.get_key());
    EXPECT_EQ("account.blob.core.windows.net", wasbs.get_endpoint());
    EXPECT_EQ("account", wasbs.get_account());
    EXPECT_TRUE(wasbs.is_azure());

    S3URI https("https://account.blob.core.windows.net/container/path/to/file.parquet");
    ASSERT_TRUE(https.parse().ok());
    EXPECT_EQ("container", https.get_bucket());
    EXPECT_EQ("path/to/file.parquet", https.get_key());
    EXPECT_EQ("account.blob.core.windows.net", https.get_endpoint());
    EXPECT_EQ("account", https.get_account());
    EXPECT_TRUE(https.is_azure());
}

TEST_F(S3URITest, InvalidAzureDataLakeAuthority) {
    S3URI missing_container("abfss://@account.dfs.core.windows.net/path/file.parquet");
    EXPECT_FALSE(missing_container.parse().ok());

    S3URI missing_account("abfss://container@/path/file.parquet");
    EXPECT_FALSE(missing_account.parse().ok());

    S3URI duplicate_separator(
            "abfss://container@account@other.dfs.core.windows.net/path/file.parquet");
    EXPECT_FALSE(duplicate_separator.parse().ok());
}

TEST_F(S3URITest, AzureParsingErrorsDoNotEchoLocations) {
    for (const auto* location :
         {"abfss:///hidden-object?sig=sentinel",
          "abfss://@account.dfs.core.windows.net/hidden-object?sig=sentinel",
          "abfss://container@/hidden-object?sig=sentinel",
          "WASBS://container@account@other.blob.core.windows.net/hidden-object?sig=sentinel",
          "https://account.blob.core.windows.net//hidden-object?sig=sentinel"}) {
        S3URI uri(location);
        auto status = uri.parse();
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.to_string().find("sentinel"), std::string::npos);
        EXPECT_EQ(status.to_string().find("hidden-object"), std::string::npos);
        EXPECT_EQ(status.to_string().find("://"), std::string::npos);
    }
}

TEST_F(S3URITest, ParsingErrorsRedactQueriesBeforeProviderSelection) {
    for (const auto* location : {"https://custom.example.com//hidden-object?sig=sentinel",
                                 "s3:///hidden-object?sig=sentinel#fragment-sentinel",
                                 "unknown://host/hidden-object#fragment-sentinel"}) {
        S3URI uri(location);
        auto status = uri.parse();
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.to_string().find("sentinel"), std::string::npos);
        EXPECT_NE(status.to_string().find("hidden-object"), std::string::npos);
    }

    S3URI custom("https://custom.example.com//hidden-object?sig=sentinel");
    auto status = custom.parse(true);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.to_string().find("sentinel"), std::string::npos);
    EXPECT_EQ(status.to_string().find("hidden-object"), std::string::npos);
}

TEST_F(S3URITest, SchemeSplitPreservesSeparatorsInObjectNames) {
    for (const auto* location :
         {"s3://container/path/http://example//file",
          "abfss://container@account.dfs.core.windows.net/path/http://example//file",
          "https://account.blob.core.windows.net/container/path/http://example//file"}) {
        S3URI uri(location);
        ASSERT_TRUE(uri.parse().ok());
        EXPECT_EQ(uri.get_key(), "path/http://example//file");
        EXPECT_EQ(uri.get_bucket(), "container");
    }
}

TEST_F(S3URITest, IcebergAdlsPathsPreserveLiteralPercentSequences) {
    for (const auto* scheme : {"abfs", "abfss", "wasb", "wasbs"}) {
        for (const auto* key :
             {"data/p=a%2Fb/file.parquet", "path/a%20b+%2520/http://example//file",
              "path/100%/%2/%GG", "path/a b "}) {
            const std::string service =
                    std::string_view(scheme).starts_with("wasb") ? "blob" : "dfs";
            S3URI uri(std::string(scheme) + "://container@account." + service +
                      ".core.windows.net/" + key);
            ASSERT_TRUE(uri.parse().ok());
            EXPECT_EQ(uri.get_key(), key);
            ASSERT_TRUE(uri.parse(true).ok());
            EXPECT_EQ(uri.get_key(), key);
        }
    }
}

TEST_F(S3URITest, AzureHttpPercentDecodingOccursExactlyOnceAndPreservesPlus) {
    S3URI uri("https://account.blob.core.windows.net/container/a%20b+%2520//c%3Ad%2Fe%3Ff%23g");
    ASSERT_TRUE(uri.parse().ok());
    EXPECT_EQ(uri.get_key(), "a b+%20//c:d/e?f#g");
    ASSERT_TRUE(uri.parse(true).ok());
    EXPECT_EQ(uri.get_key(), "a b+%20//c:d/e?f#g");

    S3URI custom("https://custom.example.com/container/a%20b+%2520");
    ASSERT_TRUE(custom.parse().ok());
    EXPECT_FALSE(custom.is_azure());
    EXPECT_EQ(custom.get_key(), "a%20b+%2520");
    ASSERT_TRUE(custom.parse(true).ok());
    EXPECT_EQ(custom.get_key(), "a b+%20");

    S3URI raw("a%20b+%2520");
    ASSERT_TRUE(raw.parse(true).ok());
    EXPECT_EQ(raw.get_key(), "a%20b+%2520");
    S3URI legacy("s3://container/a%20b+%2520");
    ASSERT_TRUE(legacy.parse(true).ok());
    EXPECT_EQ(legacy.get_key(), "a%20b+%2520");
}

TEST_F(S3URITest, RejectsInvalidAzureHttpPercentEncodingWithoutEchoingInput) {
    for (const auto* key : {"path%", "path%1", "path%1g", "path%gg"}) {
        S3URI uri(std::string("https://account.blob.core.windows.net/container/") + key +
                  "?sig=secret");
        auto status = uri.parse();
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.to_string().find("sig=secret"), std::string::npos);
    }
}

TEST_F(S3URITest, AzureHostRecognitionDoesNotClaimUnrelatedHttpDomains) {
    EXPECT_TRUE(S3URI::is_azure_endpoint("ACCOUNT.blob.core.windows.net:443"));
    EXPECT_TRUE(S3URI::is_azure_endpoint("account.dfs.core.chinacloudapi.cn"));
    EXPECT_TRUE(S3URI::is_azure_endpoint("account.blob.core.usgovcloudapi.net"));
    EXPECT_TRUE(S3URI::is_azure_endpoint("account.blob.core.cloudapi.de"));
    EXPECT_FALSE(S3URI::is_azure_endpoint("service.blob.example.com"));
    EXPECT_FALSE(S3URI::is_azure_endpoint("account.blob.core.windows.net.example.com"));
    EXPECT_FALSE(S3URI::is_azure_endpoint("onelake.dfs.fabric.microsoft.com"));
}

TEST_F(S3URITest, InvalidSchema) {
    std::string p1 = "xxx://a.b.com/bucket/path/to/file";
    S3URI uri1(p1);
    EXPECT_FALSE(uri1.parse());
}

TEST_F(S3URITest, MissingKey) {
    std::string p1 = "https://bucket/";
    S3URI uri1(p1);
    EXPECT_FALSE(uri1.parse());

    std::string p2 = "s3://bucket/";
    S3URI uri2(p2);
    EXPECT_FALSE(uri2.parse());

    std::string p3 = "http://a.b.com/bucket/";
    S3URI uri3(p3);
    EXPECT_FALSE(uri3.parse());

    std::string p4 = "http://a.b.com/";
    S3URI uri4(p4);
    EXPECT_FALSE(uri4.parse());
}

TEST_F(S3URITest, RelativePathing) {
    std::string p1 = "/path/to/file";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("/path/to/file", uri1.get_key());
}

TEST_F(S3URITest, QueryAndFragment) {
    std::string p1 = "s3://bucket/path/to/file?query=foo#bar";
    S3URI uri1(p1);
    EXPECT_TRUE(uri1.parse());
    EXPECT_EQ("bucket", uri1.get_bucket());
    EXPECT_EQ("path/to/file", uri1.get_key());
}

} // end namespace doris
