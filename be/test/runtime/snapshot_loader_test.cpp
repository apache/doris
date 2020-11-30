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

#include <boost/filesystem.hpp>

#include "runtime/exec_env.h"
#include "util/cpu_info.h"

#define private public // hack compiler
#define protected public

#include "runtime/snapshot_loader.h"

namespace doris {

class SnapshotLoaderTest : public testing::Test {
public:
    SnapshotLoaderTest() {}

private:
    ExecEnv* _exec_env;
};

TEST_F(SnapshotLoaderTest, NormalCase) {
    SnapshotLoader loader(_exec_env, 1L, 2L);

    ASSERT_TRUE(loader._end_with("abt.dat", ".dat"));
    ASSERT_FALSE(loader._end_with("abt.dat", ".da"));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678",
                                                                     &tablet_id, &schema_hash);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1234, tablet_id);
    ASSERT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id,
                                                              &schema_hash);
    ASSERT_FALSE(st.ok());

    boost::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_FALSE(st.ok());

    boost::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_TRUE(st.ok());
    boost::filesystem::remove_all("./ss_test/");

    boost::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    ASSERT_EQ(0, files.size());
    boost::filesystem::remove_all("./ss_test/");

    std::string snapshot_file;
    std::string tablet_file;
    loader._assemble_file_name("/snapshot/path", "/tablet/path", 1234, 2, 5, 12345, 1, ".dat",
                               &snapshot_file, &tablet_file);
    ASSERT_EQ("/snapshot/path/1234_2_5_12345_1.dat", snapshot_file);
    ASSERT_EQ("/tablet/path/1234_2_5_12345_1.dat", tablet_file);

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    ASSERT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005",
                                                &tablet_id);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(10005, tablet_id);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
