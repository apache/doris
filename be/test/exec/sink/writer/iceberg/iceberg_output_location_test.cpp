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

#include "exec/sink/writer/iceberg/iceberg_output_location.h"

#include <gtest/gtest.h>

namespace doris {

TEST(IcebergOutputLocationTest, WritesUnderTheNormalizedFormAndCommitsUnderTheTableForm) {
    IcebergOutputLocation location("s3://b/db/t/data", "oss://b/db/t/data");

    EXPECT_EQ("s3://b/db/t/data/delete_pos_1.parquet", location.write_path("delete_pos_1.parquet"));
    EXPECT_EQ("oss://b/db/t/data/delete_pos_1.parquet",
              location.commit_path("delete_pos_1.parquet"));
}

TEST(IcebergOutputLocationTest, JoinsDirectoriesWithAndWithoutTrailingSlashOnce) {
    // write.data.path is taken verbatim, so either form may end with '/'.
    IcebergOutputLocation location("s3://b/db/t/data/", "oss://b/db/t/data");

    EXPECT_EQ("s3://b/db/t/data/f.parquet", location.write_path("f.parquet"));
    EXPECT_EQ("oss://b/db/t/data/f.parquet", location.commit_path("f.parquet"));

    IcebergOutputLocation partition = location.subdirectory("p=1/q=2");
    EXPECT_EQ("s3://b/db/t/data/p=1/q=2/f.parquet", partition.write_path("f.parquet"));
    EXPECT_EQ("oss://b/db/t/data/p=1/q=2/f.parquet", partition.commit_path("f.parquet"));
}

TEST(IcebergOutputLocationTest, KeepsTheTableFormWhenNormalizationRewritesTheAuthority) {
    // FE rewrites more than the scheme for Azure: the account moves out of the authority.
    IcebergOutputLocation location(
            "s3://container/warehouse/t/data",
            "abfss://container@account.dfs.core.windows.net/warehouse/t/data");

    IcebergOutputLocation partition = location.subdirectory("dt=2024-01-01");
    EXPECT_EQ("s3://container/warehouse/t/data/dt=2024-01-01/f.orc", partition.write_path("f.orc"));
    EXPECT_EQ("abfss://container@account.dfs.core.windows.net/warehouse/t/data/dt=2024-01-01/f.orc",
              partition.commit_path("f.orc"));
}

} // namespace doris
