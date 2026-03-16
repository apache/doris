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

} // namespace doris
