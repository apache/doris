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
#include "olap/wal_manager.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "common/config.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "io/fs/local_file_system.h"
#include "olap/options.h"
#include "runtime/decimalv2_value.h"
#include "runtime/exec_env.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/types.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/proto_util.h"

namespace doris {

extern TLoadTxnBeginResult k_stream_load_begin_result;
extern Status k_stream_load_exec_status;

ExecEnv* _env = nullptr;
std::string wal_dir = std::string(getenv("DORIS_HOME")) + "/wal_test";

class WalManagerTest : public testing::Test {
public:
    WalManagerTest() {}
    virtual ~WalManagerTest() {}
    void SetUp() override {
        prepare();
        _env = ExecEnv::GetInstance();
        _env->_master_info = new TMasterInfo();
        _env->_master_info->network_address.hostname = "host name";
        _env->_master_info->network_address.port = 1234;
        _env->new_load_stream_mgr() = NewLoadStreamMgr::create_shared();
        _env->_internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
        _env->_function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
        _env->_stream_load_executor = StreamLoadExecutor::create_shared(_env);
        _env->_store_paths = {StorePath(std::filesystem::current_path(), 0)};
        _env->_wal_manager = WalManager::create_shared(_env, wal_dir);
        k_stream_load_begin_result = TLoadTxnBeginResult();
    }
    void TearDown() override {
        static_cast<void>(io::global_local_filesystem()->delete_directory(wal_dir));
        SAFE_STOP(_env->_wal_manager);
        SAFE_DELETE(_env->_function_client_cache);
        SAFE_DELETE(_env->_internal_client_cache);
        SAFE_DELETE(_env->_master_info);
    }

    void prepare() { static_cast<void>(io::global_local_filesystem()->create_directory(wal_dir)); }

    void createWal(const std::string& wal_path) {
        auto wal_writer = WalWriter(wal_path);
        static_cast<void>(wal_writer.init());
        static_cast<void>(wal_writer.finalize());
    }
};

TEST_F(WalManagerTest, recovery_normal) {
    _env->wal_mgr()->wal_limit_test_bytes = 1099511627776;
    k_stream_load_exec_status = Status::OK();

    std::string db_id = "1";
    int64_t tb_1_id = 1;
    std::string wal_100_id = "100";
    std::string wal_101_id = "101";
    int64_t tb_2_id = 2;
    std::string wal_200_id = "200";
    std::string wal_201_id = "201";

    std::filesystem::create_directory(wal_dir + "/" + db_id);
    std::filesystem::create_directory(wal_dir + "/" + db_id + "/" + std::to_string(tb_1_id));
    std::string wal_100 = wal_dir + "/" + db_id + "/" + std::to_string(tb_1_id) + "/" + wal_100_id;
    std::string wal_101 = wal_dir + "/" + db_id + "/" + std::to_string(tb_1_id) + "/" + wal_101_id;
    createWal(wal_100);
    createWal(wal_101);

    std::filesystem::create_directory(wal_dir + "/" + db_id);
    std::filesystem::create_directory(wal_dir + "/" + db_id + "/" + std::to_string(tb_2_id));
    std::string wal_200 = wal_dir + "/" + db_id + "/" + std::to_string(tb_2_id) + "/" + wal_200_id;
    std::string wal_201 = wal_dir + "/" + db_id + "/" + std::to_string(tb_2_id) + "/" + wal_201_id;
    createWal(wal_200);
    createWal(wal_201);
    static_cast<void>(_env->wal_mgr()->init());

    while (_env->wal_mgr()->get_wal_table_size(tb_1_id) > 0 ||
           _env->wal_mgr()->get_wal_table_size(tb_2_id) > 0) {
        sleep(1);
        continue;
    }
    ASSERT_TRUE(!std::filesystem::exists(wal_100));
    ASSERT_TRUE(!std::filesystem::exists(wal_101));
    ASSERT_TRUE(!std::filesystem::exists(wal_200));
    ASSERT_TRUE(!std::filesystem::exists(wal_201));
}

TEST_F(WalManagerTest, TestDynamicWalSpaceLimt) {
    auto wal_mgr = WalManager::create_shared(_env, config::group_commit_wal_path);
    static_cast<void>(wal_mgr->init());
    _env->set_wal_mgr(wal_mgr);

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
    wal_limit_bytes = available_bytes * 0.05;
    EXPECT_EQ(_env->wal_mgr()->wal_limit_test_bytes, wal_limit_bytes);

    _env->wal_mgr()->wal_limit_test_bytes = available_bytes;
    config::group_commit_wal_max_disk_limit = "50%";
    EXPECT_EQ(_env->wal_mgr()->_init_wal_dirs_info(), Status::OK());
    wal_limit_bytes = available_bytes * 0.5;
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