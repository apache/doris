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

#include "runtime/snapshot_loader.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>

#include "gtest/gtest_pred_impl.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

TEST(SnapshotLoaderTest, NormalCase) {
    StorageEngine engine({});
    SnapshotLoader loader(engine, ExecEnv::GetInstance(), 1L, 2L);

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678",
                                                                     &tablet_id, &schema_hash);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(1234, tablet_id);
    EXPECT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id,
                                                              &schema_hash);
    EXPECT_FALSE(st.ok());

    std::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_FALSE(st.ok());

    std::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    EXPECT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    EXPECT_TRUE(st.ok());
    std::filesystem::remove_all("./ss_test/");

    std::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    EXPECT_EQ(0, files.size());
    std::filesystem::remove_all("./ss_test/");

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    EXPECT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005",
                                                &tablet_id);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(10005, tablet_id);
}

} // namespace doris
