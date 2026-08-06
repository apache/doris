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

#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"

namespace doris {

namespace fs = std::filesystem;
namespace bp = boost::process;

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

    std::string create_fake_python_with_delay_and_socket_creation(const std::string& binary_name,
                                                                  const std::string& version,
                                                                  int delay_ms) {
        std::string bin_dir = test_dir_ + "/bin";
        std::string python_path = bin_dir + "/" + binary_name;
        fs::create_directories(bin_dir);

        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then\n";
        ofs << "    echo 'Python " << version << "'\n";
        ofs << "    exit 0\n";
        ofs << "fi\n";
        ofs << "sleep " << (delay_ms / 1000.0) << "\n";
        ofs << "SOCKET_PREFIX=\"$3\"\n";
        ofs << "SOCKET_BASE=\"${SOCKET_PREFIX#grpc+unix://}\"\n";
        ofs << "SOCKET_FILE=\"${SOCKET_BASE}_$$.sock\"\n";
        ofs << "touch \"$SOCKET_FILE\"\n";
        ofs << "trap 'rm -f \"$SOCKET_FILE\"; exit 0' TERM INT\n";
        ofs << "while true; do sleep 1; done\n";
        ofs.close();
        fs::permissions(python_path, fs::perms::owner_all);

        return python_path;
    }

    std::string create_fake_python_without_socket_creation(const std::string& binary_name,
                                                           const std::string& version) {
        std::string bin_dir = test_dir_ + "/bin";
        std::string python_path = bin_dir + "/" + binary_name;
        fs::create_directories(bin_dir);

        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then\n";
        ofs << "    echo 'Python " << version << "'\n";
        ofs << "    exit 0\n";
        ofs << "fi\n";
        ofs << "trap '' TERM\n";
        ofs << "while true; do sleep 1; done\n";
        ofs.close();
        fs::permissions(python_path, fs::perms::owner_all);

        return python_path;
    }

    std::string create_fake_python_with_one_stuck_and_others_socket(const std::string& binary_name,
                                                                    const std::string& version) {
        std::string bin_dir = test_dir_ + "/bin";
        std::string python_path = bin_dir + "/" + binary_name;
        std::string first_start_dir = test_dir_ + "/first_python_start";
        fs::create_directories(bin_dir);

        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\n";
        ofs << "if [ \"$1\" = \"--version\" ]; then\n";
        ofs << "    echo 'Python " << version << "'\n";
        ofs << "    exit 0\n";
        ofs << "fi\n";
        ofs << "if mkdir \"" << first_start_dir << "\" 2>/dev/null; then\n";
        ofs << "    trap '' TERM\n";
        ofs << "    while true; do sleep 1; done\n";
        ofs << "fi\n";
        ofs << "SOCKET_PREFIX=\"$3\"\n";
        ofs << "SOCKET_BASE=\"${SOCKET_PREFIX#grpc+unix://}\"\n";
        ofs << "SOCKET_FILE=\"${SOCKET_BASE}_$$.sock\"\n";
        ofs << "touch \"$SOCKET_FILE\"\n";
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

    ProcessPtr create_sleep_process() {
        bp::ipstream output_stream;
        std::string sleep_path = fs::exists("/bin/sleep") ? "/bin/sleep" : "/usr/bin/sleep";
        bp::child child(sleep_path, "60", bp::std_out > output_stream, bp::std_err > bp::null);
        return std::make_shared<PythonUDFProcess>(std::move(child), std::move(output_stream));
    }

    template <typename VersionedPoolPtr>
    Status get_process_with_retry(
            PythonServerManager& mgr, const PythonVersion& version,
            const VersionedPoolPtr& versioned_pool, ProcessPtr* process,
            std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
        Status last_status;
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            last_status = mgr._get_process(version, versioned_pool, process);
            if (last_status.ok()) {
                return last_status;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return last_status;
    }
};

// ============================================================================
// PythonServerManager::instance() - singleton test
// ============================================================================

TEST_F(PythonServerTest, SingletonReturnsSameInstance) {
    PythonServerManager& mgr1 = PythonServerManager::instance();
    PythonServerManager& mgr2 = PythonServerManager::instance();

    // Verify both calls return the same instance
    EXPECT_EQ(&mgr1, &mgr2);
}

// ============================================================================
// PythonServerManager::_get_process() - process retrieval test
// ============================================================================

TEST_F(PythonServerTest, EnsurePoolInitializedCanInitializeEmptyPoolForTest) {
    PythonServerManager mgr;

    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");
    PythonVersion version("3.9.16", test_dir_, python_path);
    config::max_python_process_num = 1;

    mgr.set_process_pool_for_test(version, {}, false);
    auto pool_result = mgr._ensure_pool_initialized(version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    ProcessPtr process;
    Status status = mgr._get_process(version, pool_result.value(), &process);

    EXPECT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    mgr.shutdown();
}

// ============================================================================
// PythonServerManager::fork() - process creation test
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

    // Verify: when the flight server script does not exist, fork should fail
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
}

TEST_F(PythonServerTest, ForkWithProcessThatExitsImmediatelyReturnsError) {
    PythonServerManager mgr;

    // Set DORIS_HOME
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

TEST_F(PythonServerTest, ForkWithoutSocketCreationReturnsAfterBoundedTerminate) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_without_socket_creation("python3.no_socket_direct", "3.9.16");

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto start = std::chrono::steady_clock::now();
    ProcessPtr process;
    Status status = mgr.fork(version, &process);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
    EXPECT_NE(status.to_string().find("socket file not found"), std::string::npos);
    EXPECT_LT(elapsed.count(), 2000);
}

TEST_F(PythonServerTest, ForkEnqueuesBackgroundReapWhenKilledStartFailureIsNotReaped) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_without_socket_creation("python3.no_socket_reap", "3.9.16");

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // SIGKILL not becoming reapable inside the bounded wait depends on kernel state. Force only the
    // wait results so this test covers PythonServerManager::fork() handing waitpid ownership to the
    // shared background reaper instead of detaching and losing the pid.
    PythonUDFProcess::force_child_exit_timeouts_for_test(2);
    ProcessPtr process;
    Status status = mgr.fork(version, &process);
    PythonUDFProcess::force_child_exit_timeouts_for_test(0);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
    EXPECT_NE(status.to_string().find("process did not exit after SIGKILL"), std::string::npos);

    std::string status_text = status.to_string();
    size_t pid_pos = status_text.find("pid=");
    ASSERT_NE(pid_pos, std::string::npos) << status_text;
    pid_t child_pid = static_cast<pid_t>(std::stol(status_text.substr(pid_pos + 4)));
    EXPECT_TRUE(PythonUDFProcess::wait_background_reaped_for_test(child_pid,
                                                                  std::chrono::milliseconds(5000)));
}

// ============================================================================
// PythonServerManager::_ensure_pool_initialized() - pool initialization test
// ============================================================================

TEST_F(PythonServerTest, EnsurePoolInitializedWithInvalidVersionFails) {
    PythonServerManager mgr;
    config::max_python_process_num = 1;

    PythonVersion invalid_version("3.99.99", "/non/existent/path", "/non/existent/python");

    auto result = mgr._ensure_pool_initialized(invalid_version);

    // Verify: invalid version should cause initialization to fail
    EXPECT_FALSE(result.has_value());
    // Error message should indicate process creation failure or bounded initialization timeout.
    EXPECT_TRUE(result.error().to_string().find("Failed") != std::string::npos ||
                result.error().to_string().find("failed") != std::string::npos ||
                result.error().to_string().find("Timed out") != std::string::npos);
}

TEST_F(PythonServerTest, EnsurePoolInitializedReturnsImmediatelyWhenAllWorkersFail) {
    PythonServerManager mgr;
    config::max_python_process_num = 2;

    PythonVersion invalid_version("3.9.16", test_dir_, test_dir_ + "/missing_python");

    auto start = std::chrono::steady_clock::now();
    auto result = mgr._ensure_pool_initialized(invalid_version);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    EXPECT_FALSE(result.has_value());
    EXPECT_LT(elapsed.count(), 500);
}

TEST_F(PythonServerTest, EnsurePoolInitializedAfterShutdownReturnsServiceUnavailable) {
    PythonServerManager mgr;
    mgr.shutdown();

    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto result = mgr._ensure_pool_initialized(version);

    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().to_string().find("shutting down"), std::string::npos);
}

// ============================================================================
// PythonServerManager::shutdown() - shutdown test
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
    config::max_python_process_num = 1;

    // Try initialization first (expected to fail)
    PythonVersion invalid_version("3.99.99", "/bad/path", "/bad/python");
    auto result = mgr._ensure_pool_initialized(invalid_version);
    EXPECT_FALSE(result.has_value());

    // Verify: calling shutdown after failed initialization does not crash
    EXPECT_NO_THROW(mgr.shutdown());
}

TEST_F(PythonServerTest, GetProcessFromStoppedPoolReturnsUnavailable) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);
    auto pool_result = mgr._ensure_pool_initialized(version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    mgr.shutdown();

    ProcessPtr process;
    Status status = mgr._get_process(version, pool_result.value(), &process);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
    EXPECT_NE(status.to_string().find("stopped"), std::string::npos);
}

TEST_F(PythonServerTest, ClearUdafStateCacheWithoutProcessesIsNoOp) {
    PythonServerManager mgr;

    EXPECT_NO_THROW(mgr.clear_udaf_state_cache(12345));
}

TEST_F(PythonServerTest, ClearModuleCacheWithoutProcessesIsNoOp) {
    PythonServerManager mgr;

    auto status = mgr.clear_module_cache("/tmp/python_udf_cache");
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(PythonServerTest, BroadcastActionWithInvalidProcessUriReturnsError) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/bin/python3");
    ProcessPtr process = create_sleep_process();
    ASSERT_NE(process, nullptr);
    ASSERT_TRUE(process->is_alive());
    process->set_uri_for_test("invalid-python-flight-uri");

    mgr.set_process_pool_for_test(version, {process});
    auto status = mgr.broadcast_action_to_processes_for_test(
            "clear_udaf_state_cache", R"({"function_id": 12345})", "function_id=12345");

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("clear_udaf_state_cache failed for function_id=12345"),
              std::string::npos);
    EXPECT_NE(status.to_string().find("success=0, failed=1"), std::string::npos);

    mgr.shutdown();
}

// ============================================================================
// PythonServerManager::get_client() - client retrieval test
// ============================================================================

TEST_F(PythonServerTest, GetClientWithInvalidVersionFails) {
    PythonServerManager mgr;
    config::max_python_process_num = 1;

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
// configuration test
// ============================================================================

TEST_F(PythonServerTest, MaxPythonProcessNumConfigIsAccessible) {
    // Verify configuration value is accessible and within a valid range
    int max_num = config::max_python_process_num;
    EXPECT_GE(max_num, 0); // 0 means use number of CPU cores
}

// ============================================================================
// destructor test
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
// success-path test using a fake Python script
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

    auto result = mgr._ensure_pool_initialized(version);

    // Verify pool initialization succeeded
    EXPECT_TRUE(result.has_value()) << result.error().to_string();

    // Cleanup
    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedLogsProgressWhileWaitingForSlowProcess) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_with_delay_and_socket_creation("python3.delayed", "3.9.16", 50);

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto result = mgr._ensure_pool_initialized(version);

    EXPECT_TRUE(result.has_value()) << result.error().to_string();

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedRetriesAfterInitFailureWithBoundedWait) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_without_socket_creation("python3.no_socket", "3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto start = std::chrono::steady_clock::now();
    auto result = mgr._ensure_pool_initialized(version);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    EXPECT_FALSE(result.has_value());
    EXPECT_LT(elapsed.count(), 2000);

    start = std::chrono::steady_clock::now();
    auto retry_result = mgr._ensure_pool_initialized(version);
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    EXPECT_FALSE(retry_result.has_value());
    EXPECT_LT(elapsed.count(), 2000);

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedSucceedsWithOneStuckWorkerAndOneUsableWorker) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_with_one_stuck_and_others_socket("python3.mixed", "3.9.16");

    config::max_python_process_num = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto start = std::chrono::steady_clock::now();
    auto result = mgr._ensure_pool_initialized(version);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_LT(elapsed.count(), 2000);
    EXPECT_TRUE(mgr.process_pool_is_initializing_for_test(version));

    ProcessPtr process;
    EXPECT_TRUE(mgr._get_process(version, result.value(), &process).ok());
    ASSERT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    for (int i = 0; i < 20 && !mgr.process_pool_is_initialized_for_test(version); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(mgr.process_pool_is_initialized_for_test(version));

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedIdempotent) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // First initialization
    auto result1 = mgr._ensure_pool_initialized(version);
    EXPECT_TRUE(result1.has_value()) << result1.error().to_string();

    // Second initialization should return immediately (version already initialized)
    auto result2 = mgr._ensure_pool_initialized(version);
    EXPECT_TRUE(result2.has_value()) << result2.error().to_string();

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessFromInitializedPool) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Initialize the pool first
    auto init_result = mgr._ensure_pool_initialized(version);
    EXPECT_TRUE(init_result.has_value()) << init_result.error().to_string();

    // Get a process
    ProcessPtr process;
    Status status = mgr._get_process(version, init_result.value(), &process);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessRecreatesDeadProcessWhenNoAliveProcess) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto pool_result = mgr._ensure_pool_initialized(version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    ProcessPtr first_process;
    ASSERT_TRUE(mgr._get_process(version, pool_result.value(), &first_process).ok());
    ASSERT_NE(first_process, nullptr);
    ASSERT_TRUE(first_process->is_alive());
    pid_t first_pid = first_process->get_child_pid();

    first_process->shutdown();
    ASSERT_FALSE(first_process->is_alive());

    ProcessPtr replacement;
    Status status = get_process_with_retry(mgr, version, pool_result.value(), &replacement);

    EXPECT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(replacement, nullptr);
    EXPECT_TRUE(replacement->is_alive());
    EXPECT_NE(replacement->get_child_pid(), first_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessSkipsDeadProcessWhenAliveProcessExists) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 3;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr alive_process;
    ASSERT_TRUE(mgr.fork(version, &alive_process).ok());
    ASSERT_NE(alive_process, nullptr);
    ASSERT_TRUE(alive_process->is_alive());

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(version, {alive_process, dead_process});
    auto pool_result = mgr._ensure_pool_initialized(version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    ProcessPtr selected;
    Status status = mgr._get_process(version, pool_result.value(), &selected);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(selected, alive_process);
    auto pool_snapshot = mgr.process_pool_snapshot_for_test(version);
    ASSERT_EQ(pool_snapshot.size(), 2);
    EXPECT_FALSE(pool_snapshot[1]->is_alive());
    EXPECT_EQ(pool_snapshot[1]->get_child_pid(), dead_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessLoadBalancing) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    // Create a pool with 2 processes
    config::max_python_process_num = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto init_result = mgr._ensure_pool_initialized(version);
    EXPECT_TRUE(init_result.has_value()) << init_result.error().to_string();

    // Get multiple processes to verify load balancing
    ProcessPtr p1, p2, p3, p4;
    EXPECT_TRUE(mgr._get_process(version, init_result.value(), &p1).ok());
    EXPECT_TRUE(mgr._get_process(version, init_result.value(), &p2).ok());
    EXPECT_TRUE(mgr._get_process(version, init_result.value(), &p3).ok());
    EXPECT_TRUE(mgr._get_process(version, init_result.value(), &p4).ok());

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
    auto init_result = mgr._ensure_pool_initialized(version);
    EXPECT_TRUE(init_result.has_value()) << init_result.error().to_string();

    // Get a process reference
    ProcessPtr process;
    EXPECT_TRUE(mgr._get_process(version, init_result.value(), &process).ok());
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

    // Initialize pools for two versions
    auto pool39_result = mgr._ensure_pool_initialized(version39);
    auto pool310_result = mgr._ensure_pool_initialized(version310);
    EXPECT_TRUE(pool39_result.has_value()) << pool39_result.error().to_string();
    EXPECT_TRUE(pool310_result.has_value()) << pool310_result.error().to_string();

    // Retrieve processes from both pools
    ProcessPtr p39, p310;
    EXPECT_TRUE(mgr._get_process(version39, pool39_result.value(), &p39).ok());
    EXPECT_TRUE(mgr._get_process(version310, pool310_result.value(), &p310).ok());

    // Verify they are different processes
    EXPECT_NE(p39->get_child_pid(), p310->get_child_pid());

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedForDifferentVersionsDoesNotShareVersionLock) {
    setup_doris_home();

    config::max_python_process_num = 1;

    std::string python39_path =
            create_fake_python_with_delay_and_socket_creation("python3.9", "3.9.16", 50);
    std::string python310_path =
            create_fake_python_with_delay_and_socket_creation("python3.10", "3.10.0", 50);

    PythonServerManager mgr;
    PythonVersion version39("3.9.16", test_dir_, python39_path);
    PythonVersion version310("3.10.0", test_dir_, python310_path);

    auto start = std::chrono::steady_clock::now();
    auto future39 = std::async(std::launch::async,
                               [&]() { return mgr._ensure_pool_initialized(version39); });
    auto future310 = std::async(std::launch::async,
                                [&]() { return mgr._ensure_pool_initialized(version310); });

    auto result39 = future39.get();
    auto result310 = future310.get();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

    EXPECT_TRUE(result39.has_value()) << result39.error().to_string();
    EXPECT_TRUE(result310.has_value()) << result310.error().to_string();
    // Keep the assertion loose for ASAN/CI scheduling while still catching full init-timeout
    // serialization between versions.
    EXPECT_LT(elapsed.count(), 2000);

    mgr.shutdown();
}

// ============================================================================
// PythonServerManager::_check_and_recreate_processes() - health-check recreation test
// ============================================================================

TEST_F(PythonServerTest, CheckAndRecreateProcessesRecreatesDeadProcess) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 3;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr alive_process;
    ASSERT_TRUE(mgr.fork(version, &alive_process).ok());
    ASSERT_NE(alive_process, nullptr);
    ASSERT_TRUE(alive_process->is_alive());

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid_before = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(version, {alive_process, dead_process, nullptr});

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(version);
    ASSERT_EQ(pool_snapshot.size(), 3);
    EXPECT_EQ(pool_snapshot[0], alive_process);

    ProcessPtr recreated = pool_snapshot[1];
    ASSERT_NE(recreated, nullptr);
    EXPECT_TRUE(recreated->is_alive());
    EXPECT_NE(recreated->get_child_pid(), dead_pid_before);
    ASSERT_NE(pool_snapshot[2], nullptr);
    EXPECT_TRUE(pool_snapshot[2]->is_alive());

    mgr.shutdown();
}

TEST_F(PythonServerTest, CheckAndRecreateProcessesSkipsRepairingPool) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(version, {dead_process});
    auto pool_result = mgr._ensure_pool_initialized(version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();
    {
        std::lock_guard<std::mutex> lock(pool_result.value()->mutex);
        pool_result.value()->repairing = true;
    }

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(version);
    ASSERT_EQ(pool_snapshot.size(), 1);
    ASSERT_NE(pool_snapshot[0], nullptr);
    EXPECT_FALSE(pool_snapshot[0]->is_alive());
    EXPECT_EQ(pool_snapshot[0]->get_child_pid(), dead_pid);
    {
        std::lock_guard<std::mutex> lock(pool_result.value()->mutex);
        pool_result.value()->repairing = false;
    }

    mgr.shutdown();
}

TEST_F(PythonServerTest, CheckAndRecreateProcessesSkipsUninitializedPool) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(version, {dead_process}, false);

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(version);
    ASSERT_EQ(pool_snapshot.size(), 1);
    ASSERT_NE(pool_snapshot[0], nullptr);
    EXPECT_FALSE(pool_snapshot[0]->is_alive());
    EXPECT_EQ(pool_snapshot[0]->get_child_pid(), dead_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, CheckAndRecreateProcessesKeepsDeadSlotsWhenRecreateFails) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    config::max_python_process_num = 2;

    PythonServerManager mgr;
    PythonVersion live_version("3.9.16", test_dir_, python_path);

    ProcessPtr dead_process_1;
    ASSERT_TRUE(mgr.fork(live_version, &dead_process_1).ok());
    ASSERT_NE(dead_process_1, nullptr);
    dead_process_1->shutdown();
    ASSERT_FALSE(dead_process_1->is_alive());

    ProcessPtr dead_process_2;
    ASSERT_TRUE(mgr.fork(live_version, &dead_process_2).ok());
    ASSERT_NE(dead_process_2, nullptr);
    dead_process_2->shutdown();
    ASSERT_FALSE(dead_process_2->is_alive());

    PythonVersion invalid_version("3.9.16", test_dir_, test_dir_ + "/bin/nonexistent_python");
    mgr.set_process_pool_for_test(invalid_version, {dead_process_1, dead_process_2});

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(invalid_version);
    ASSERT_EQ(pool_snapshot.size(), 2);
    EXPECT_FALSE(pool_snapshot[0]->is_alive());
    EXPECT_FALSE(pool_snapshot[1]->is_alive());

    mgr.shutdown();
}

TEST_F(PythonServerTest, ReadProcessMemoryCurrentProcessSucceeds) {
    PythonServerManager mgr;
    size_t rss_bytes = 0;

    Status status = mgr._read_process_memory(getpid(), &rss_bytes);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_GT(rss_bytes, 0);
}

TEST_F(PythonServerTest, ReadProcessMemoryInvalidPidFails) {
    PythonServerManager mgr;
    size_t rss_bytes = 0;

    Status status = mgr._read_process_memory(-1, &rss_bytes);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("/proc/-1/statm"), std::string::npos);
}

} // namespace doris
