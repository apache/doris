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

#include "udf/python/python_server.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"

namespace doris {

namespace fs = std::filesystem;

class PythonServerTest : public ::testing::Test {
protected:
    std::string test_dir_;
    const char* original_doris_home_ = nullptr;
    int original_max_python_process_num_ = 0;

    void SetUp() override {
        test_dir_ = fs::temp_directory_path().string() + "/python_server_test_" +
                    std::to_string(getpid()) + "_" + std::to_string(rand());
        fs::create_directories(test_dir_);

        original_doris_home_ = std::getenv("DORIS_HOME");
        original_max_python_process_num_ = config::max_python_process_num;
    }

    void TearDown() override {
        // Restore configuration
        config::max_python_process_num = original_max_python_process_num_;

        if (!test_dir_.empty() && fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }

        if (original_doris_home_) {
            setenv("DORIS_HOME", original_doris_home_, 1);
        } else {
            unsetenv("DORIS_HOME");
        }
    }

    // Create a fake Python script that creates a socket file and keeps running
    // Arg: script prints "Python X.Y.Z" for version detection
    std::string create_fake_python_with_socket_creation(const std::string& version = "3.9.16") {
        std::string bin_dir = test_dir_ + "/bin";
        std::string python_path = bin_dir + "/python3";
        fs::create_directories(bin_dir);

        // Create fake Python script
        // Behavior: 1. If the arg is --version, print the version
        //            2. Otherwise, create the socket file and wait
        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then\n";
        ofs << "    echo 'Python " << version << "'\n";
        ofs << "    exit 0\n";
        ofs << "fi\n";
        // Extract socket path prefix from args and create the socket file
        // Arg format: -u script.py grpc+unix:///tmp/doris_python_udf
        ofs << "SOCKET_PREFIX=\"$3\"\n";
        ofs << "# Extract path part (strip grpc+unix://)\n";
        ofs << "SOCKET_BASE=\"${SOCKET_PREFIX#grpc+unix://}\"\n";
        ofs << "SOCKET_FILE=\"${SOCKET_BASE}_$$.sock\"\n";
        ofs << "# Create socket file\n";
        ofs << "touch \"$SOCKET_FILE\"\n";
        ofs << "# Wait to be terminated\n";
        ofs << "trap 'rm -f \"$SOCKET_FILE\"; exit 0' TERM INT\n";
        ofs << "while true; do sleep 1; done\n";
        ofs.close();
        fs::permissions(python_path, fs::perms::owner_all);

        return python_path;
    }

    // Set DORIS_HOME and create flight server script directory
    void setup_doris_home() {
        setenv("DORIS_HOME", test_dir_.c_str(), 1);
        std::string plugin_dir = test_dir_ + "/plugins/python_udf";
        fs::create_directories(plugin_dir);
        // Create an empty python_server.py (won't be executed because we use fake Python)
        std::ofstream ofs(plugin_dir + "/python_server.py");
        ofs << "# fake server\n";
        ofs.close();
    }
};

// ============================================================================
// PythonServerManager::instance() - 单例测试
// ============================================================================

TEST_F(PythonServerTest, SingletonReturnsSameInstance) {
    PythonServerManager& mgr1 = PythonServerManager::instance();
    PythonServerManager& mgr2 = PythonServerManager::instance();

    // Verify both calls return the same instance
    EXPECT_EQ(&mgr1, &mgr2);
}

// ============================================================================
// PythonServerManager::get_process() - 获取进程测试
// ============================================================================

TEST_F(PythonServerTest, GetProcessFromEmptyPoolReturnsError) {
    PythonServerManager mgr;

    PythonVersion version("3.9.16", "/fake/path", "/fake/python");
    ProcessPtr process;

    Status status = mgr.get_process(version, &process);

    // Verify: empty pool should return an error with message containing "pool is empty"
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("pool is empty") != std::string::npos);
    EXPECT_EQ(process, nullptr);
}

// ============================================================================
// PythonServerManager::fork() - 进程创建测试
// ============================================================================

TEST_F(PythonServerTest, ForkWithNonExistentPythonReturnsError) {
    PythonServerManager mgr;

    PythonVersion invalid_version("3.9.16", test_dir_, test_dir_ + "/nonexistent_python");

    ProcessPtr process;
    Status status = mgr.fork(invalid_version, &process);

    // Verify: non-existent Python should return an error
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
}

TEST_F(PythonServerTest, ForkWithMissingFlightServerReturnsError) {
    PythonServerManager mgr;

    // Set DORIS_HOME to test directory (no flight server script)
    setenv("DORIS_HOME", test_dir_.c_str(), 1);

    // Create a fake python executable
    std::string python_path = test_dir_ + "/bin/python3";
    fs::create_directories(test_dir_ + "/bin");
    {
        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\nexit 1"; // exits immediately
    }
    fs::permissions(python_path, fs::perms::owner_all);

    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr process;
    Status status = mgr.fork(version, &process);

    // 验证：flight server 脚本不存在，fork 应失败
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
}

TEST_F(PythonServerTest, ForkWithProcessThatExitsImmediatelyReturnsError) {
    PythonServerManager mgr;

    // 设置 DORIS_HOME
    setenv("DORIS_HOME", test_dir_.c_str(), 1);

    // Create flight server directory structure
    std::string plugin_dir = test_dir_ + "/plugins/python_udf";
    fs::create_directories(plugin_dir);

    // Create a fake python_server.py (will be executed by Python but exits immediately)
    std::string server_path = plugin_dir + "/python_server.py";
    {
        std::ofstream ofs(server_path);
        ofs << "import sys; sys.exit(1)";
    }

    // Create a fake python executable
    std::string python_path = test_dir_ + "/bin/python3";
    fs::create_directories(test_dir_ + "/bin");
    {
        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\nexit 1"; // exits immediately, does not create socket file
    }
    fs::permissions(python_path, fs::perms::owner_all);

    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr process;
    Status status = mgr.fork(version, &process);

    // Verify: process exits immediately (socket file not created), should return an error
    EXPECT_FALSE(status.ok());
    // Error message should contain socket-related content
    std::string err_msg = status.to_string();
    EXPECT_TRUE(err_msg.find("socket") != std::string::npos ||
                err_msg.find("start") != std::string::npos);
}

// ============================================================================
// PythonServerManager::ensure_pool_initialized() - 池初始化测试
// ============================================================================

TEST_F(PythonServerTest, EnsurePoolInitializedWithInvalidVersionFails) {
    PythonServerManager mgr;

    PythonVersion invalid_version("3.99.99", "/non/existent/path", "/non/existent/python");

    Status status = mgr.ensure_pool_initialized(invalid_version);

    // Verify: invalid version should cause initialization to fail
    EXPECT_FALSE(status.ok());
    // Error message should indicate all process creations failed
    EXPECT_TRUE(status.to_string().find("Failed") != std::string::npos ||
                status.to_string().find("failed") != std::string::npos);
}

// ============================================================================
// PythonServerManager::shutdown() - 关闭测试
// ============================================================================

TEST_F(PythonServerTest, ShutdownEmptyManagerDoesNotCrash) {
    PythonServerManager mgr;

    // Verify: calling shutdown on empty manager does not crash
    EXPECT_NO_THROW(mgr.shutdown());
}

TEST_F(PythonServerTest, ShutdownCalledMultipleTimesDoesNotCrash) {
    PythonServerManager mgr;

    // Verify: calling shutdown multiple times does not crash
    EXPECT_NO_THROW({
        mgr.shutdown();
        mgr.shutdown();
        mgr.shutdown();
    });
}

TEST_F(PythonServerTest, ShutdownAfterFailedInitializationDoesNotCrash) {
    PythonServerManager mgr;

    // Try initialization first (expected to fail)
    PythonVersion invalid_version("3.99.99", "/bad/path", "/bad/python");
    Status status = mgr.ensure_pool_initialized(invalid_version);
    EXPECT_FALSE(status.ok());

    // Verify: calling shutdown after failed initialization does not crash
    EXPECT_NO_THROW(mgr.shutdown());
}

// ============================================================================
// PythonServerManager::get_client() - 获取客户端测试
// ============================================================================

TEST_F(PythonServerTest, GetClientWithInvalidVersionFails) {
    PythonServerManager mgr;

    PythonVersion invalid_version("3.9.16", "/invalid/path", "/invalid/python");
    PythonUDFMeta meta;
    meta.name = "test_udf";
    meta.symbol = "test_func";
    meta.runtime_version = "3.9.16";
    meta.type = PythonUDFLoadType::INLINE;
    meta.client_type = PythonClientType::UDF;

    std::shared_ptr<PythonUDFClient> client;
    Status status = mgr.get_client(meta, invalid_version, &client);

    // Verify: getting client with invalid version should fail
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(client, nullptr);
}

// ============================================================================
// 配置测试
// ============================================================================

TEST_F(PythonServerTest, MaxPythonProcessNumConfigIsAccessible) {
    // Verify configuration value is accessible and within a valid range
    int max_num = config::max_python_process_num;
    EXPECT_GE(max_num, 0); // 0 means use number of CPU cores
}

// ============================================================================
// 析构函数测试
// ============================================================================

TEST_F(PythonServerTest, DestructorCleansUpResources) {
    // Create and destroy manager to ensure no memory leaks or crashes
    {
        PythonServerManager mgr;
        // Try some operations (they fail but should not affect destructor)
        PythonVersion invalid_version("3.9.16", "/bad", "/bad");
        ProcessPtr process;
        Status status = mgr.fork(invalid_version, &process);
        EXPECT_FALSE(status.ok());
    }
    // If we reach here without crashing, destructor works properly
    SUCCEED();
}

// ============================================================================
// 使用假 Python 脚本测试成功路径
// ============================================================================

TEST_F(PythonServerTest, ForkSuccessWithFakePython) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr process;
    Status status = mgr.fork(version, &process);

    // Verify fork succeeded
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());
    EXPECT_GT(process->get_child_pid(), 0);

    // Verify socket path is correct
    std::string uri = process->get_uri();
    EXPECT_TRUE(uri.find("grpc+unix://") != std::string::npos);

    // Cleanup
    process->shutdown();
    EXPECT_TRUE(process->is_shutdown());
}

TEST_F(PythonServerTest, EnsurePoolInitializedSuccess) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    // Limit process pool to 1 to speed up the test
    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    Status status = mgr.ensure_pool_initialized(version);

    // Verify pool initialization succeeded
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Cleanup
    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedIdempotent) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // First initialization
    Status status1 = mgr.ensure_pool_initialized(version);
    EXPECT_TRUE(status1.ok()) << status1.to_string();

    // Second initialization should return immediately (version already initialized)
    Status status2 = mgr.ensure_pool_initialized(version);
    EXPECT_TRUE(status2.ok()) << status2.to_string();

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessFromInitializedPool) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Initialize the pool first
    Status init_status = mgr.ensure_pool_initialized(version);
    EXPECT_TRUE(init_status.ok()) << init_status.to_string();

    // Get a process
    ProcessPtr process;
    Status status = mgr.get_process(version, &process);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessLoadBalancing) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    // Create a pool with 2 processes
    config::max_python_process_num = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    Status init_status = mgr.ensure_pool_initialized(version);
    EXPECT_TRUE(init_status.ok()) << init_status.to_string();

    // Get multiple processes to verify load balancing
    ProcessPtr p1, p2, p3, p4;
    EXPECT_TRUE(mgr.get_process(version, &p1).ok());
    EXPECT_TRUE(mgr.get_process(version, &p2).ok());
    EXPECT_TRUE(mgr.get_process(version, &p3).ok());
    EXPECT_TRUE(mgr.get_process(version, &p4).ok());

    // With 2 processes, load balancing distributes requests across different processes
    // p1 and p2 may be same or different processes
    EXPECT_NE(p1, nullptr);
    EXPECT_NE(p2, nullptr);

    mgr.shutdown();
}

TEST_F(PythonServerTest, ShutdownWithRunningProcesses) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Initialize the pool
    Status init_status = mgr.ensure_pool_initialized(version);
    EXPECT_TRUE(init_status.ok()) << init_status.to_string();

    // Get a process reference
    ProcessPtr process;
    EXPECT_TRUE(mgr.get_process(version, &process).ok());
    EXPECT_TRUE(process->is_alive());

    // Shutdown should terminate all processes
    mgr.shutdown();

    // Process should be shut down
    EXPECT_TRUE(process->is_shutdown());
}

TEST_F(PythonServerTest, MultipleVersionPools) {
    setup_doris_home();

    // Create two fake Pythons with different versions
    std::string python39_path = test_dir_ + "/bin/python3.9";
    std::string python310_path = test_dir_ + "/bin/python3.10";
    fs::create_directories(test_dir_ + "/bin");

    // Python 3.9
    {
        std::ofstream ofs(python39_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then echo 'Python 3.9.16'; exit 0; fi\n";
        ofs << "SOCKET_BASE=\"${3#grpc+unix://}\"\n";
        ofs << "touch \"${SOCKET_BASE}_$$.sock\"\n";
        ofs << "trap 'rm -f \"${SOCKET_BASE}_$$.sock\"; exit 0' TERM INT\n";
        ofs << "while true; do sleep 1; done\n";
    }
    fs::permissions(python39_path, fs::perms::owner_all);

    // Python 3.10
    {
        std::ofstream ofs(python310_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then echo 'Python 3.10.0'; exit 0; fi\n";
        ofs << "SOCKET_BASE=\"${3#grpc+unix://}\"\n";
        ofs << "touch \"${SOCKET_BASE}_$$.sock\"\n";
        ofs << "trap 'rm -f \"${SOCKET_BASE}_$$.sock\"; exit 0' TERM INT\n";
        ofs << "while true; do sleep 1; done\n";
    }
    fs::permissions(python310_path, fs::perms::owner_all);

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version39("3.9.16", test_dir_, python39_path);
    PythonVersion version310("3.10.0", test_dir_, python310_path);

    // 初始化两个版本的池
    EXPECT_TRUE(mgr.ensure_pool_initialized(version39).ok());
    EXPECT_TRUE(mgr.ensure_pool_initialized(version310).ok());

    // 从两个池获取进程
    ProcessPtr p39, p310;
    EXPECT_TRUE(mgr.get_process(version39, &p39).ok());
    EXPECT_TRUE(mgr.get_process(version310, &p310).ok());

    // 验证是不同的进程
    EXPECT_NE(p39->get_child_pid(), p310->get_child_pid());

    mgr.shutdown();
}

} // namespace doris
