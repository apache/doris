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
#include "load/group_commit/wal/wal_manager.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {

class WalManagerTest : public testing::Test {
public:
    WalManagerTest() { init(); }
    void init();

    void TearDown() override {
        Status st = io::global_local_filesystem()->delete_directory(_wal_dir);
        if (!st.ok()) {
            LOG(WARNING) << "fail to delete " << _wal_dir;
        } else {
            LOG(INFO) << "delete " << _wal_dir;
        }
        WARN_IF_ERROR(io::global_local_filesystem()->delete_directory(_wal_dir),
                      fmt::format("fail to delete dir={}", _wal_dir));
        SAFE_STOP(_env->_wal_manager);
        delete _env->cluster_info();
        _env->clear_wal_mgr();
    }

private:
    void _prepare();

    ExecEnv* _env = nullptr;
    std::string _wal_dir = std::string(getenv("DORIS_HOME")) + "/wal_test";
    int64_t _db_id = 1;
    int64_t _tb_id = 2;
    int64_t _txn_id_1 = 123;
    int64_t _txn_id_2 = 456;
    uint32_t _version_0 = 0;
    uint32_t _version_1 = 1;
    int64_t _backend_id = 1001;
    std::string _label_1 = "test1";
    std::string _label_2 = "test2";
};

void WalManagerTest::_prepare() {
    Status st = io::global_local_filesystem()->create_directory(_wal_dir);
    if (!st.ok()) {
        LOG(WARNING) << "fail to create dir  " << _wal_dir;
    }
}
void WalManagerTest::init() {
    config::group_commit_wal_max_disk_limit = "100M";
    _prepare();
    WARN_IF_ERROR(io::global_local_filesystem()->create_directory(
                          _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id)),
                  "fail to creat directory");

    _env = ExecEnv::GetInstance();
    _env->set_cluster_info(new ClusterInfo());
    _env->_cluster_info->master_fe_addr.hostname = "host name";
    _env->_cluster_info->master_fe_addr.port = _backend_id;
    _env->_cluster_info->backend_id = 1001;
    _env->set_wal_mgr(WalManager::create_unique(_env, _wal_dir));
    std::string base_path;
    auto st = _env->_wal_manager->_init_wal_dirs_info();
    st = _env->_wal_manager->create_wal_path(_db_id, _tb_id, _txn_id_1, _label_1, base_path,
                                             _version_0);
    std::string src = "./be/test/exec/test_data/wal_scanner/wal_version0";
    std::string dst = _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id) + "/" +
                      std::to_string(_version_0) + "_" + std::to_string(_backend_id) + "_" +
                      std::to_string(_txn_id_1) + "_" + _label_1;
    std::filesystem::copy(src, dst);
    st = _env->_wal_manager->create_wal_path(_db_id, _tb_id, _txn_id_2, _label_2, base_path,
                                             _version_1);
    src = "./be/test/exec/test_data/wal_scanner/wal_version1";
    dst = _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id) + "/" +
          std::to_string(_version_1) + "_" + std::to_string(_backend_id) + "_" +
          std::to_string(_txn_id_2) + "_" + _label_2;
    std::filesystem::copy(src, dst);
}

TEST_F(WalManagerTest, TestLastReplayWalFailedReason) {
    const auto origin_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("WalTable.replay_wals.stop");
    Defer defer([origin_enable_debug_points]() {
        DebugPoints::instance()->remove("WalTable.replay_wals.stop");
        config::enable_debug_points = origin_enable_debug_points;
    });

    const int64_t wal_id = 789;
    const std::string label = "test_last_replay_failed_reason";
    const std::string wal_path = _wal_dir + "/" + std::to_string(_db_id) + "/" +
                                 std::to_string(_tb_id) + "/" + std::to_string(_version_1) + "_" +
                                 std::to_string(_backend_id) + "_" + std::to_string(wal_id) + "_" +
                                 label;
    std::filesystem::copy_file("./be/test/exec/test_data/wal_scanner/wal_version1", wal_path,
                               std::filesystem::copy_options::overwrite_existing);

    WalTable wal_table(_env, _db_id, _tb_id);
    wal_table.add_wal(wal_id, wal_path);
    EXPECT_EQ(wal_table.replay_wals(), Status::OK());
    auto failed_reason = wal_table.get_last_replay_wal_failed_reason();
    EXPECT_NE(failed_reason.find("WalTable.replay_wals.stop"), failed_reason.npos);
    EXPECT_NE(failed_reason.find(wal_path), failed_reason.npos);

    DebugPoints::instance()->remove("WalTable.replay_wals.stop");
    EXPECT_EQ(wal_table.replay_wals(), Status::OK());
    EXPECT_TRUE(wal_table.get_last_replay_wal_failed_reason().empty());
}

TEST_F(WalManagerTest, TestDynamicWalSpaceLimt) {
    // 1T
    size_t available_bytes = 1099511627776;
    size_t wal_limit_bytes;

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "0%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 0);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "5%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    wal_limit_bytes = size_t(available_bytes * 0.05);
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, wal_limit_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "50%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    wal_limit_bytes = size_t(available_bytes * 0.5);
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, wal_limit_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "200%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    wal_limit_bytes = available_bytes * 2;
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, wal_limit_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "-10%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::InternalError(""));
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, available_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "0";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 0);

    // 1M
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "1048576";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 1048576);

    // 1G
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "1073741824";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 1073741824);

    // 100G
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "107374182400";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 107374182400);

    // 1M
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "1M";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 1048576);

    // 1G
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "1G";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 1073741824);

    // 100G
    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "100G";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, 107374182400);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "-1024";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::InternalError(""));
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, available_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "-1M";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::InternalError(""));
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, available_bytes);
}

} // namespace doris
