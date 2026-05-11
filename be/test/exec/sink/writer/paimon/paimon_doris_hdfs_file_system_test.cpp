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

#include "exec/sink/writer/paimon/paimon_doris_hdfs_file_system.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

TEST(PaimonDorisHdfsFileSystemTest, ExtractsFsNameFromSupportedSchemes) {
    EXPECT_EQ("hdfs://namenode:8020",
              extract_hdfs_fs_name_for_test("hdfs://namenode:8020/warehouse/table"));
    EXPECT_EQ("dfs://cluster", extract_hdfs_fs_name_for_test("dfs://cluster/path/to/file"));
    EXPECT_EQ("", extract_hdfs_fs_name_for_test("s3://bucket/path"));
    EXPECT_EQ("", extract_hdfs_fs_name_for_test("hdfs:///missing-authority"));
}

TEST(PaimonDorisHdfsFileSystemTest, ConvertsDorisStatusToPaimonStatus) {
    auto ok_status = to_paimon_status_for_test(Status::OK());
    EXPECT_TRUE(ok_status.ok());

    auto io_status = to_paimon_status_for_test(Status::IOError("io failure"));
    EXPECT_FALSE(io_status.ok());
    EXPECT_NE(std::string::npos, io_status.ToString().find("io failure"));
}

TEST(PaimonDorisHdfsFileSystemTest, RegisterFunctionIsSafeToCall) {
    EXPECT_NO_THROW(ensure_paimon_doris_hdfs_file_system_registered());
    EXPECT_NO_THROW(ensure_paimon_doris_hdfs_file_system_registered());
}

} // namespace doris::vectorized
