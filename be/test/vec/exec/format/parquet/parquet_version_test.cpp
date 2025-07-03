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

#include <gtest/gtest.h>

#include <regex>

#include "vec/exec/format/parquet/parquet_common.h"

namespace doris {
namespace vectorized {
class ParquetVersionTest : public testing::Test {
public:
    ParquetVersionTest() = default;
};

TEST_F(ParquetVersionTest, test_version_parser) {
    std::unique_ptr<ParsedVersion> parsed_version;

    Status status = VersionParser::parse("parquet-mr version 1.6.0 (build abcd)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", "abcd"), *parsed_version);

    status = VersionParser::parse("parquet-mr version 1.6.22rc99-SNAPSHOT (build abcd)",
                                  &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.22rc99-SNAPSHOT", "abcd"), *parsed_version);

    status = VersionParser::parse("unparseable string", &parsed_version);
    EXPECT_FALSE(status.ok());

    // missing semver
    status = VersionParser::parse("parquet-mr version (build abcd)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, "abcd"), *parsed_version);

    status = VersionParser::parse("parquet-mr version  (build abcd)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, "abcd"), *parsed_version);

    // missing build hash
    status = VersionParser::parse("parquet-mr version 1.6.0 (build )", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version 1.6.0 (build)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version (build)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version (build )", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);

    // Missing entire build section
    status = VersionParser::parse("parquet-mr version 1.6.0", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version 1.8.0rc4", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.8.0rc4", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version 1.8.0rc4-SNAPSHOT", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);

    // Various spaces
    status = VersionParser::parse("parquet-mr     version    1.6.0", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr     version    1.8.0rc4", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.8.0rc4", std::nullopt), *parsed_version);

    status =
            VersionParser::parse("parquet-mr      version    1.8.0rc4-SNAPSHOT  ", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.8.0rc4-SNAPSHOT", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr      version", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr version 1.6.0 (  build )", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr     version 1.6.0 (    build)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", "1.6.0", std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr     version (    build)", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);

    status = VersionParser::parse("parquet-mr    version    (build    )", &parsed_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(ParsedVersion("parquet-mr", std::nullopt, std::nullopt), *parsed_version);
}

void assertLessThan(const std::string& a, const std::string& b) {
    std::unique_ptr<SemanticVersion> version_a;
    Status status = SemanticVersion::parse(a, &version_a);
    EXPECT_TRUE(status.ok());
    std::unique_ptr<SemanticVersion> version_b;
    status = SemanticVersion::parse(b, &version_b);
    EXPECT_TRUE(status.ok());
    EXPECT_LT(version_a->compare_to(*version_b), 0) << a << " should be < " << b;
    EXPECT_GT(version_b->compare_to(*version_a), 0) << b << " should be > " << a;
}

void assertEqualTo(const std::string& a, const std::string& b) {
    std::unique_ptr<SemanticVersion> version_a;
    Status status = SemanticVersion::parse(a, &version_a);
    EXPECT_TRUE(status.ok());
    std::unique_ptr<SemanticVersion> version_b;
    status = SemanticVersion::parse(b, &version_b);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(version_a->compare_to(*version_b), 0) << a << " should equal " << b;
}

TEST_F(ParquetVersionTest, test_compare) {
    EXPECT_EQ(SemanticVersion(1, 8, 1).compare_to(SemanticVersion(1, 8, 1)), 0);
    EXPECT_LT(SemanticVersion(1, 8, 0).compare_to(SemanticVersion(1, 8, 1)), 0);
    EXPECT_GT(SemanticVersion(1, 8, 2).compare_to(SemanticVersion(1, 8, 1)), 0);

    EXPECT_EQ(SemanticVersion(1, 8, 1).compare_to(SemanticVersion(1, 8, 1)), 0);
    EXPECT_LT(SemanticVersion(1, 8, 0).compare_to(SemanticVersion(1, 8, 1)), 0);
    EXPECT_GT(SemanticVersion(1, 8, 2).compare_to(SemanticVersion(1, 8, 1)), 0);

    EXPECT_LT(SemanticVersion(1, 7, 0).compare_to(SemanticVersion(1, 8, 0)), 0);
    EXPECT_GT(SemanticVersion(1, 9, 0).compare_to(SemanticVersion(1, 8, 0)), 0);

    EXPECT_LT(SemanticVersion(0, 0, 0).compare_to(SemanticVersion(1, 0, 0)), 0);
    EXPECT_GT(SemanticVersion(2, 0, 0).compare_to(SemanticVersion(1, 0, 0)), 0);

    EXPECT_LT(SemanticVersion(1, 8, 100).compare_to(SemanticVersion(1, 9, 0)), 0);

    EXPECT_GT(SemanticVersion(1, 8, 0).compare_to(SemanticVersion(1, 8, 0, true)), 0);
    EXPECT_EQ(SemanticVersion(1, 8, 0, true).compare_to(SemanticVersion(1, 8, 0, true)), 0);
    EXPECT_LT(SemanticVersion(1, 8, 0, true).compare_to(SemanticVersion(1, 8, 0)), 0);
}

TEST_F(ParquetVersionTest, test_semver_prerelease_examples) {
    std::vector<std::string> examples = {"1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta",
                                         "1.0.0-beta",  "1.0.0-beta.2",  "1.0.0-beta.11",
                                         "1.0.0-rc.1",  "1.0.0"};
    for (size_t i = 0; i < examples.size() - 1; ++i) {
        assertLessThan(examples[i], examples[i + 1]);
        assertEqualTo(examples[i], examples[i]);
    }
    assertEqualTo(examples.back(), examples.back());
}

TEST_F(ParquetVersionTest, test_semver_build_info_examples) {
    assertEqualTo("1.0.0-alpha+001", "1.0.0-alpha+001");
    assertEqualTo("1.0.0-alpha", "1.0.0-alpha+001");
    assertEqualTo("1.0.0+20130313144700", "1.0.0+20130313144700");
    assertEqualTo("1.0.0", "1.0.0+20130313144700");
    assertEqualTo("1.0.0-beta+exp.sha.5114f85", "1.0.0-beta+exp.sha.5114f85");
    assertEqualTo("1.0.0-beta", "1.0.0-beta+exp.sha.5114f85");
}

TEST_F(ParquetVersionTest, test_unknown_comparisons) {
    assertLessThan("1.0.0rc0-alpha+001", "1.0.0-alpha");
}

TEST_F(ParquetVersionTest, test_distribution_versions) {
    assertEqualTo("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.1");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.1-SNAPSHOT");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.6.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh6.0.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0");
    assertLessThan("1.5.0-cdh5.5.0", "1.5.0-cdh5.5.0-SNAPSHOT");
}

TEST_F(ParquetVersionTest, test_parse) {
    std::unique_ptr<SemanticVersion> semantic_version;
    Status status = SemanticVersion::parse("1.8.0", &semantic_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(*semantic_version, SemanticVersion(1, 8, 0));
    status = SemanticVersion::parse("1.8.0rc3", &semantic_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(*semantic_version, SemanticVersion(1, 8, 0, true));
    status = SemanticVersion::parse("1.8.0rc3-SNAPSHOT", &semantic_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(*semantic_version, SemanticVersion(1, 8, 0, "rc3", "SNAPSHOT", std::nullopt));
    status = SemanticVersion::parse("1.8.0-SNAPSHOT", &semantic_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(*semantic_version, SemanticVersion(1, 8, 0, std::nullopt, "SNAPSHOT", std::nullopt));
    status = SemanticVersion::parse("1.5.0-cdh5.5.0", &semantic_version);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(*semantic_version, SemanticVersion(1, 5, 0, std::nullopt, "cdh5.5.0", std::nullopt));
}

} // namespace vectorized
} // namespace doris
