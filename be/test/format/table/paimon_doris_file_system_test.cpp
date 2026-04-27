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

#include "format/table/paimon_doris_file_system.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/config.h"

namespace doris {

class PaimonDorisFileSystemTest : public testing::Test {
protected:
    void SetUp() override { saved_mappings_ = config::paimon_file_system_scheme_mappings; }

    void TearDown() override { config::paimon_file_system_scheme_mappings = saved_mappings_; }

    std::vector<std::string> saved_mappings_;
};

TEST_F(PaimonDorisFileSystemTest, UsesDefaultSchemeMappings) {
    EXPECT_EQ(TFileType::FILE_LOCAL, paimon::map_scheme_to_file_type("file"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("jfs"));
    EXPECT_EQ(TFileType::FILE_S3, paimon::map_scheme_to_file_type("s3a"));
    EXPECT_EQ(TFileType::FILE_S3, paimon::map_scheme_to_file_type("gs"));
    EXPECT_EQ(TFileType::FILE_HTTP, paimon::map_scheme_to_file_type("https"));
    EXPECT_EQ(TFileType::FILE_BROKER, paimon::map_scheme_to_file_type("ofs"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("unknown"));
}

TEST_F(PaimonDorisFileSystemTest, ParsesUrisWithAndWithoutAuthority) {
    auto s3_uri = paimon::parse_uri("s3://bucket/path/to/file");
    EXPECT_EQ("s3", s3_uri.scheme);
    EXPECT_EQ("bucket", s3_uri.authority);

    auto hdfs_uri = paimon::parse_uri("hdfs:///warehouse/table");
    EXPECT_EQ("hdfs", hdfs_uri.scheme);
    EXPECT_TRUE(hdfs_uri.authority.empty());

    auto invalid_uri = paimon::parse_uri("/no/scheme/path");
    EXPECT_TRUE(invalid_uri.scheme.empty());
    EXPECT_TRUE(invalid_uri.authority.empty());
}

TEST_F(PaimonDorisFileSystemTest, AllowsOverridingSchemeMappingsFromConfig) {
    config::paimon_file_system_scheme_mappings = {"file=local", "jfs = s3", "gs = hdfs",
                                                  "custom-http = http", "custom-broker = broker"};

    EXPECT_EQ(TFileType::FILE_LOCAL, paimon::map_scheme_to_file_type("file"));
    EXPECT_EQ(TFileType::FILE_S3, paimon::map_scheme_to_file_type("JFS"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("gs"));
    EXPECT_EQ(TFileType::FILE_HTTP, paimon::map_scheme_to_file_type("custom-http"));
    EXPECT_EQ(TFileType::FILE_BROKER, paimon::map_scheme_to_file_type("custom-broker"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("still-unknown"));
}

TEST_F(PaimonDorisFileSystemTest, IgnoresMalformedSchemeMappings) {
    config::paimon_file_system_scheme_mappings = {
            "missing-separator", "=s3", "unknown-target = invalid", " custom-scheme = s3 "};

    EXPECT_EQ(TFileType::FILE_S3, paimon::map_scheme_to_file_type("custom-scheme"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("missing-separator"));
    EXPECT_EQ(TFileType::FILE_HDFS, paimon::map_scheme_to_file_type("unknown-target"));
}

TEST_F(PaimonDorisFileSystemTest, ReplacesSchemesAndNormalizesLocalPaths) {
    EXPECT_EQ("s3://bucket/path", paimon::replace_scheme("gs://bucket/path", "s3"));
    EXPECT_EQ("/tmp/data", paimon::normalize_local_path("file:///tmp/data"));
    EXPECT_EQ("/tmp/data", paimon::normalize_local_path("file://localhost/tmp/data"));
    EXPECT_EQ("/plain/path", paimon::normalize_local_path("/plain/path"));
}

TEST_F(PaimonDorisFileSystemTest, NormalizesPathsForDifferentFileTypes) {
    paimon::ParsedUri local_uri {"file", ""};
    EXPECT_EQ("/tmp/data", paimon::normalize_path_for_type("file:///tmp/data", local_uri,
                                                           TFileType::FILE_LOCAL));

    paimon::ParsedUri gs_uri {"gs", "bucket"};
    EXPECT_EQ("s3://bucket/path",
              paimon::normalize_path_for_type("gs://bucket/path", gs_uri, TFileType::FILE_S3));

    paimon::ParsedUri hdfs_uri {"hdfs", "namenode:8020"};
    EXPECT_EQ("/warehouse/table",
              paimon::normalize_path_for_type("hdfs://namenode:8020/warehouse/table", hdfs_uri,
                                              TFileType::FILE_HDFS));
}

TEST_F(PaimonDorisFileSystemTest, BuildsCacheKeysByFileType) {
    paimon::ParsedUri local_uri {"file", ""};
    EXPECT_EQ("local", paimon::build_fs_cache_key(TFileType::FILE_LOCAL, local_uri, "default"));

    paimon::ParsedUri s3_uri {"s3", "bucket"};
    EXPECT_EQ("s3://bucket", paimon::build_fs_cache_key(TFileType::FILE_S3, s3_uri, "default"));

    paimon::ParsedUri http_uri {"http", "host:80"};
    EXPECT_EQ("http://host:80",
              paimon::build_fs_cache_key(TFileType::FILE_HTTP, http_uri, "default"));

    paimon::ParsedUri empty_hdfs_uri;
    EXPECT_EQ("hdfs://fallback",
              paimon::build_fs_cache_key(TFileType::FILE_HDFS, empty_hdfs_uri, "hdfs://fallback"));
}

TEST_F(PaimonDorisFileSystemTest, ConvertsDorisStatusToPaimonStatus) {
    auto ok_status = paimon::to_paimon_status(Status::OK());
    EXPECT_TRUE(ok_status.ok());

    auto not_exist_status = paimon::to_paimon_status(Status::NotFound("missing object"));
    EXPECT_TRUE(not_exist_status.IsNotExist());

    auto exist_status = paimon::to_paimon_status(Status::AlreadyExist("already exists"));
    EXPECT_TRUE(exist_status.IsExist());

    auto invalid_status = paimon::to_paimon_status(Status::InvalidArgument("bad input"));
    EXPECT_FALSE(invalid_status.ok());
    EXPECT_NE(std::string::npos, invalid_status.ToString().find("bad input"));
}

TEST_F(PaimonDorisFileSystemTest, RegisterFunctionIsSafeToCall) {
    EXPECT_NO_THROW(register_paimon_doris_file_system());
}

} // namespace doris
