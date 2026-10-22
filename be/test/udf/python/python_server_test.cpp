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

#include <algorithm>
#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udtf_client.h"
#include "util/cpu_info.h"
#include "util/defer_op.h"

namespace doris {

namespace fs = std::filesystem;
namespace bp = boost::process;

class PythonServerTest : public ::testing::Test {
protected:
    std::string test_dir_;
    std::optional<std::string> original_doris_home_;
    int test_pool_size_ = 1;

    void SetUp() override {
        test_dir_ = fs::temp_directory_path().string() + "/python_server_test_" +
                    std::to_string(getpid()) + "_" + std::to_string(rand());
        fs::create_directories(test_dir_);

        if (const char* doris_home = std::getenv("DORIS_HOME")) {
            original_doris_home_ = doris_home;
        }
    }

    void TearDown() override {
        // Restore configuration

        if (!test_dir_.empty() && fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }

        if (original_doris_home_) {
            setenv("DORIS_HOME", original_doris_home_->c_str(), 1);
        } else {
            unsetenv("DORIS_HOME");
        }
    }

    void check_dropped_process_memory(bool repair) {
        setup_doris_home();
        auto python_path = create_fake_python_with_socket_creation();
        PythonVersion version("3.9.16", test_dir_, python_path);
        SCOPED_TRACE(repair ? "repaired process" : "initial process");
        PythonServerManager mgr;
        auto meta = test_meta(version);
        std::shared_ptr<PythonUDFPoolLease> lease;
        auto pool = mgr._ensure_pool_initialized(meta, version, nullptr, &lease);
        ASSERT_TRUE(pool.has_value()) << pool.error().to_string();
        ASSERT_TRUE(
                mgr.wait_for_process_pool_initialized_for_test(meta.id, std::chrono::seconds(5)));
        // Model the ProcessPtr retained by PythonClient during an active exchange.
        ProcessPtr client_process;
        ASSERT_TRUE(mgr._get_process(version, pool.value(), &client_process).ok());
        if (repair) {
            client_process->shutdown();
            client_process.reset();
            mgr._check_and_recreate_processes();
            ASSERT_TRUE(mgr._get_process(version, pool.value(), &client_process).ok());
        }
        mgr._refresh_memory_stats();
        ASSERT_GT(mgr._mem_tracker.consumption(), 0);

        std::weak_ptr<PythonUDFProcess> process_lifetime = client_process;
        mgr.drop_function(meta.id);
        EXPECT_EQ(mgr.process_pool_snapshot_for_test(meta.id).size(), 1);
        mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
        ASSERT_TRUE(client_process->is_alive());
        mgr._refresh_memory_stats();
        EXPECT_GT(mgr._mem_tracker.consumption(), 0);

        client_process.reset();
        lease.reset();
        mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
        EXPECT_TRUE(process_lifetime.expired());
        mgr._refresh_memory_stats();
        EXPECT_EQ(mgr._mem_tracker.consumption(), 0);
    }

    PythonUDFMeta test_meta(const PythonVersion& version) const {
        PythonUDFMeta meta;
        meta.id = static_cast<int64_t>(std::hash<std::string>()(version.to_string()) & INT64_MAX);
        meta.pool_size = test_pool_size_;
        return meta;
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
        ofs << "if [ ! -f \"$2\" ]; then exit 2; fi\n";
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
        ofs << "if [ ! -f \"$2\" ]; then exit 2; fi\n";
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
        ofs << "if [ ! -f \"$2\" ]; then exit 2; fi\n";
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
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Prove that the same executable and arguments start correctly while the production server
    // entry exists, then remove only that entry to isolate the failure cause.
    ProcessPtr healthy_process;
    ASSERT_TRUE(mgr.fork(version, &healthy_process).ok());
    ASSERT_NE(healthy_process, nullptr);
    healthy_process->shutdown();

    ASSERT_TRUE(fs::remove(test_dir_ + "/plugins/python_udf/python_server.py"));
    ProcessPtr process;
    Status status = mgr.fork(version, &process);

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
    test_pool_size_ = 1;

    PythonVersion invalid_version("3.99.99", "/non/existent/path", "/non/existent/python");

    auto result = mgr._ensure_pool_initialized(test_meta(invalid_version), invalid_version);

    // Verify: invalid version should cause initialization to fail
    EXPECT_FALSE(result.has_value());
    // Error message should indicate process creation failure or bounded initialization timeout.
    EXPECT_TRUE(result.error().to_string().find("Failed") != std::string::npos ||
                result.error().to_string().find("failed") != std::string::npos ||
                result.error().to_string().find("Timed out") != std::string::npos);
}

TEST_F(PythonServerTest, EnsurePoolInitializedRetriesAfterRuntimeIsRepaired) {
    setup_doris_home();
    test_pool_size_ = 1;

    // The first executable starts but never publishes its Flight socket. This reproduces a real
    // worker-start failure and verifies that the pool does not remain stuck in INITIALIZING.
    std::string python_path = create_fake_python_without_socket_creation("python3", "3.9.16");
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonServerManager mgr;

    struct GenerationCompletionLatch {
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<int>* generation = nullptr;
        bool worker_finished = false;
    };
    auto generation_completion = std::make_shared<GenerationCompletionLatch>();
    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_point {[sync_point]() {
        sync_point->disable_processing();
        sync_point->clear_call_back(
                "PythonServerManager::_ensure_pool_initialized:generation_started");
        sync_point->clear_call_back(
                "PythonServerManager::_ensure_pool_initialized:init_worker_finished");
    }};
    sync_point->set_call_back(
            "PythonServerManager::_ensure_pool_initialized:generation_started",
            [generation_completion, version](auto&& args) {
                const auto* started_version = try_any_cast<const PythonVersion*>(args.at(0));
                if (*started_version != version) {
                    return;
                }
                std::lock_guard lock(generation_completion->mutex);
                generation_completion->generation = try_any_cast<std::atomic<int>*>(args.at(1));
            });
    sync_point->set_call_back(
            "PythonServerManager::_ensure_pool_initialized:init_worker_finished",
            [generation_completion, version](auto&& args) {
                const auto* finished_version = try_any_cast<const PythonVersion*>(args.at(0));
                if (*finished_version != version) {
                    return;
                }
                auto* finished_generation = try_any_cast<std::atomic<int>*>(args.at(1));
                {
                    std::lock_guard lock(generation_completion->mutex);
                    if (finished_generation != generation_completion->generation) {
                        return;
                    }
                    generation_completion->worker_finished = true;
                }
                generation_completion->cv.notify_all();
            });
    sync_point->enable_processing();

    auto failed_result = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_FALSE(failed_result.has_value());

    // Wait for both parts of the failed generation to finish. Otherwise an old detached worker can
    // execute the repaired file and publish its process into the next generation's pool.
    {
        std::unique_lock lock(generation_completion->mutex);
        ASSERT_TRUE(generation_completion->cv.wait_for(lock, std::chrono::seconds(5), [&]() {
            return generation_completion->worker_finished;
        }));
    }
    ASSERT_TRUE(mgr.wait_for_process_pool_initialization_finished_for_test(
            test_meta(version).id, std::chrono::seconds(5)));

    // Repair the runtime in place and retry the same version key. Production can hit this when an
    // environment or server entry is fixed after a transient initialization failure.
    ASSERT_EQ(create_fake_python_with_socket_creation("3.9.16"), python_path);
    auto recovered_result = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(recovered_result.has_value()) << recovered_result.error().to_string();

    ProcessPtr process;
    ASSERT_TRUE(mgr._get_process(version, recovered_result.value(), &process).ok());
    ASSERT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedAfterShutdownReturnsServiceUnavailable) {
    PythonServerManager mgr;
    mgr.shutdown();

    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto result = mgr._ensure_pool_initialized(test_meta(version), version);

    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().to_string().find("shutting down"), std::string::npos);
}

// ============================================================================
// PythonServerManager::shutdown() - shutdown test
// ============================================================================

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
    test_pool_size_ = 1;

    // Try initialization first (expected to fail)
    PythonVersion invalid_version("3.99.99", "/bad/path", "/bad/python");
    auto result = mgr._ensure_pool_initialized(test_meta(invalid_version), invalid_version);
    EXPECT_FALSE(result.has_value());

    // Verify: calling shutdown after failed initialization does not crash
    EXPECT_NO_THROW(mgr.shutdown());
}

TEST_F(PythonServerTest, GetProcessFromStoppedPoolReturnsUnavailable) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);
    auto pool_result = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    mgr.shutdown();

    ProcessPtr process;
    Status status = mgr._get_process(version, pool_result.value(), &process);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(process, nullptr);
    EXPECT_NE(status.to_string().find("stopped"), std::string::npos);
}

TEST_F(PythonServerTest, DropUnknownFunctionIsNoOp) {
    PythonServerManager mgr;

    EXPECT_NO_THROW(mgr.drop_function(12345));
}

TEST_F(PythonServerTest, GetClientWithInvalidVersionFails) {
    PythonServerManager mgr;
    test_pool_size_ = 1;

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
// destructor test
// ============================================================================

TEST_F(PythonServerTest, DestructorCleansUpResources) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");
    test_pool_size_ = 1;

    ProcessPtr process;
    std::string socket_path;
    {
        PythonServerManager mgr;
        PythonVersion version("3.9.16", test_dir_, python_path);
        auto pool_result = mgr._ensure_pool_initialized(test_meta(version), version);
        ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();
        ASSERT_TRUE(mgr._get_process(version, pool_result.value(), &process).ok());
        ASSERT_NE(process, nullptr);
        ASSERT_TRUE(process->is_alive());
        socket_path = process->get_socket_file_path();
        ASSERT_TRUE(fs::exists(socket_path));
    }

    EXPECT_TRUE(process->is_shutdown());
    EXPECT_FALSE(process->is_alive());
    EXPECT_FALSE(fs::exists(socket_path));
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
    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto result = mgr._ensure_pool_initialized(test_meta(version), version);

    // Verify pool initialization succeeded
    EXPECT_TRUE(result.has_value()) << result.error().to_string();

    // Cleanup
    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedSucceedsWithOneStuckWorkerAndOneUsableWorker) {
    setup_doris_home();
    std::string python_path =
            create_fake_python_with_one_stuck_and_others_socket("python3.mixed", "3.9.16");

    test_pool_size_ = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto result = mgr._ensure_pool_initialized(test_meta(version), version);

    ASSERT_TRUE(result.has_value()) << result.error().to_string();

    ProcessPtr process;
    EXPECT_TRUE(mgr._get_process(version, result.value(), &process).ok());
    ASSERT_NE(process, nullptr);
    EXPECT_TRUE(process->is_alive());

    // Wait on the same condition variable used by the production coordinator instead of polling
    // or asserting on a racy intermediate state.
    EXPECT_TRUE(mgr.wait_for_process_pool_initialized_for_test(test_meta(version).id,
                                                               std::chrono::milliseconds(2000)));
    EXPECT_TRUE(mgr.process_pool_is_initialized_for_test(test_meta(version).id));

    mgr.shutdown();
}

TEST_F(PythonServerTest, EnsurePoolInitializedIdempotent) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // First initialization
    auto result1 = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(result1.has_value()) << result1.error().to_string();
    auto first_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
    ASSERT_EQ(first_snapshot.size(), 1);
    ASSERT_NE(first_snapshot[0], nullptr);
    pid_t first_pid = first_snapshot[0]->get_child_pid();

    // Re-initialization must reuse both the versioned pool and its live process.
    auto result2 = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(result2.has_value()) << result2.error().to_string();
    EXPECT_EQ(result2.value(), result1.value());
    auto second_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
    ASSERT_EQ(second_snapshot.size(), 1);
    EXPECT_EQ(second_snapshot[0], first_snapshot[0]);
    EXPECT_EQ(second_snapshot[0]->get_child_pid(), first_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, GetProcessFromInitializedPool) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Initialize the pool first
    auto init_result = mgr._ensure_pool_initialized(test_meta(version), version);
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

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    auto pool_result = mgr._ensure_pool_initialized(test_meta(version), version);
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

    test_pool_size_ = 3;

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

    mgr.set_process_pool_for_test(test_meta(version), version, {alive_process, dead_process});
    auto pool_result = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();

    ProcessPtr selected;
    Status status = mgr._get_process(version, pool_result.value(), &selected);

    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(selected, alive_process);
    auto pool_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
    ASSERT_EQ(pool_snapshot.size(), 2);
    EXPECT_FALSE(pool_snapshot[1]->is_alive());
    EXPECT_EQ(pool_snapshot[1]->get_child_pid(), dead_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, RejectsInvalidPoolSizeBeforeStartingProcesses) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    for (const int pool_size : {0, -1}) {
        meta.pool_size = pool_size;
        auto result = mgr._ensure_pool_initialized(meta, version);
        ASSERT_FALSE(result.has_value());
        EXPECT_NE(result.error().to_string().find("pool_size must be a positive integer"),
                  std::string::npos);
    }
    mgr.shutdown();
}

TEST_F(PythonServerTest, CapsPoolSizeBeforeAllocatingProcessSlots) {
    CpuInfo::init();
    const int cores = CpuInfo::num_cores();
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    for (const int requested : {1, 3, cores, cores + 1, 10000}) {
        PythonUDFMeta meta;
        meta.id = requested;
        meta.pool_size = requested;
        auto result = mgr._get_or_create_process_pool(meta, version, nullptr);
        ASSERT_TRUE(result.has_value()) << result.error().to_string();
        EXPECT_EQ(std::min(requested, cores), result.value()->pool_size);
        EXPECT_TRUE(result.value()->processes.empty());
    }
}

TEST_F(PythonServerTest, DefaultAutoSuspendReclaimsAfterFourHoursWithoutClients) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    PythonUDFMeta meta;
    ASSERT_EQ(14400, meta.auto_suspend);
    std::shared_ptr<PythonUDFPoolLease> lease;
    auto result = mgr._get_or_create_process_pool(meta, version, nullptr, &lease);
    ASSERT_TRUE(result.has_value());
    auto pool = result.value();
    ASSERT_EQ(14400, pool->auto_suspend);
    EXPECT_EQ(std::min(3, CpuInfo::num_cores()), pool->pool_size);
    mgr._reclaim_idle_pools(pool->idle_since + std::chrono::hours(5));
    EXPECT_EQ(pool, mgr._get_or_create_process_pool(meta, version, nullptr).value());
    lease.reset();
    mgr._reclaim_idle_pools(pool->idle_since + std::chrono::seconds(14399));
    EXPECT_EQ(pool, mgr._get_or_create_process_pool(meta, version, nullptr).value());
    mgr._reclaim_idle_pools(pool->idle_since + std::chrono::seconds(14400));
    EXPECT_NE(pool, mgr._get_or_create_process_pool(meta, version, nullptr).value());
}

TEST_F(PythonServerTest, RejectsAutoSuspendBelowMinusOneBeforeStartingProcesses) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    PythonUDFMeta meta;
    meta.auto_suspend = -2;
    auto result = mgr._get_or_create_process_pool(meta, version, nullptr);
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().to_string().find("auto_suspend must be -1 or a non-negative integer"),
              std::string::npos);
}

TEST_F(PythonServerTest, GetProcessSelectsLeastLoadedProcess) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    mgr.set_process_pool_for_test(test_meta(version), version,
                                  {create_sleep_process(), create_sleep_process()});

    auto init_result = mgr._ensure_pool_initialized(test_meta(version), version);
    EXPECT_TRUE(init_result.has_value()) << init_result.error().to_string();

    // Keeping the first process shared_ptr raises its use_count, so select the other
    // live process. An implementation that always returns pool[0] fails this assertion.
    ProcessPtr p1, p2;
    ASSERT_TRUE(mgr._get_process(version, init_result.value(), &p1).ok());
    ASSERT_NE(p1, nullptr);
    ASSERT_TRUE(mgr._get_process(version, init_result.value(), &p2).ok());
    ASSERT_NE(p2, nullptr);
    EXPECT_NE(p1->get_child_pid(), p2->get_child_pid());

    mgr.shutdown();
}

TEST_F(PythonServerTest, ShutdownWithRunningProcesses) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 2;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    // Initialize the pool
    auto init_result = mgr._ensure_pool_initialized(test_meta(version), version);
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

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version39("3.9.16", test_dir_, python39_path);
    PythonVersion version310("3.10.0", test_dir_, python310_path);

    // Initialize pools for two versions
    auto pool39_result = mgr._ensure_pool_initialized(test_meta(version39), version39);
    auto pool310_result = mgr._ensure_pool_initialized(test_meta(version310), version310);
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

    test_pool_size_ = 1;

    std::string python39_path = create_fake_python_with_socket_creation("3.9.16");
    std::string python310_path = test_dir_ + "/bin/python3.10";
    ASSERT_TRUE(fs::copy_file(python39_path, python310_path));
    fs::permissions(python310_path, fs::perms::owner_all);

    PythonServerManager mgr;
    PythonVersion version39("3.9.16", test_dir_, python39_path);
    PythonVersion version310("3.10.0", test_dir_, python310_path);

    struct ForkBarrier {
        std::mutex mutex;
        std::condition_variable cv;
        int entries = 0;
        bool released = false;
    };
    auto fork_barrier = std::make_shared<ForkBarrier>();
    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_point {[fork_barrier, sync_point]() {
        {
            std::lock_guard lock(fork_barrier->mutex);
            fork_barrier->released = true;
        }
        fork_barrier->cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_call_back("PythonServerManager::fork:before_process_start");
        sync_point->clear_call_back(
                "PythonServerManager::_ensure_pool_initialized:process_pool_init_timeout");
    }};
    // Keep callers inside initialization longer than the fork barrier. A manager-wide lock must
    // therefore fail the barrier instead of serializing through the short BE_TEST pool timeout.
    sync_point->set_call_back(
            "PythonServerManager::_ensure_pool_initialized:process_pool_init_timeout",
            [](auto&& args) {
                auto* timeout = try_any_cast<std::chrono::milliseconds*>(args.front());
                *timeout = std::chrono::seconds(10);
            });
    sync_point->set_call_back(
            "PythonServerManager::fork:before_process_start",
            [fork_barrier, version39, version310](auto&& args) {
                if (args.empty()) {
                    return;
                }
                const auto* entering_version = try_any_cast<const PythonVersion*>(args.front());
                if (*entering_version != version39 && *entering_version != version310) {
                    return;
                }
                std::unique_lock lock(fork_barrier->mutex);
                ++fork_barrier->entries;
                fork_barrier->cv.notify_all();
                fork_barrier->cv.wait(lock, [&]() { return fork_barrier->released; });
            });
    sync_point->enable_processing();

    auto future39 = std::async(std::launch::async, [&]() {
        return mgr._ensure_pool_initialized(test_meta(version39), version39);
    });
    auto future310 = std::async(std::launch::async, [&]() {
        return mgr._ensure_pool_initialized(test_meta(version310), version310);
    });

    bool both_forks_entered = false;
    {
        std::unique_lock lock(fork_barrier->mutex);
        both_forks_entered = fork_barrier->cv.wait_for(
                lock, std::chrono::seconds(5), [&]() { return fork_barrier->entries >= 2; });
        fork_barrier->released = true;
    }
    fork_barrier->cv.notify_all();

    // The assertion below is the concurrency evidence. Pool completion also has a separate short
    // BE_TEST timeout, so its status must not turn host scheduling after this barrier into noise.
    static_cast<void>(future39.get());
    static_cast<void>(future310.get());

    mgr.shutdown();

    EXPECT_TRUE(both_forks_entered)
            << "Both version pools must enter fork before either process starts";
}

TEST_F(PythonServerTest, FunctionsWithSameRuntimeHaveIndependentFixedPools) {
    PythonServerManager mgr;
    setup_doris_home();
    auto python_path = create_fake_python_with_socket_creation();
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonUDFMeta first;
    first.id = 101;
    first.pool_size = 1;
    PythonUDFMeta second;
    second.id = 102;
    second.pool_size = 2;
    auto first_result = mgr._ensure_pool_initialized(first, version);
    ASSERT_TRUE(first_result.has_value()) << first_result.error().to_string();
    auto second_result = mgr._ensure_pool_initialized(second, version);
    ASSERT_TRUE(second_result.has_value()) << second_result.error().to_string();
    ASSERT_TRUE(mgr.wait_for_process_pool_initialized_for_test(101, std::chrono::seconds(5)));
    ASSERT_TRUE(mgr.wait_for_process_pool_initialized_for_test(102, std::chrono::seconds(5)));
    auto first_pool = mgr.process_pool_snapshot_for_test(101);
    auto second_pool = mgr.process_pool_snapshot_for_test(102);
    ASSERT_EQ(first_pool.size(), 1);
    ASSERT_EQ(second_pool.size(), 2);
    EXPECT_NE(first_pool[0]->get_child_pid(), second_pool[0]->get_child_pid());
    EXPECT_NE(first_pool[0]->get_child_pid(), second_pool[1]->get_child_pid());
    ProcessPtr first_client, second_client;
    ASSERT_TRUE(mgr._get_process(version, first_result.value(), &first_client).ok());
    ASSERT_TRUE(mgr._get_process(version, second_result.value(), &second_client).ok());
    EXPECT_NE(first_client, second_client);
}

TEST_F(PythonServerTest, DropRejectsNewClientsUntilHealthCheckReclaimsThePool) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    std::shared_ptr<PythonUDFPoolLease> lease;
    auto pool = mgr._get_or_create_process_pool(meta, version, nullptr, &lease).value();

    mgr.drop_function(meta.id);
    ASSERT_TRUE(mgr._process_pools.contains(meta.id));
    EXPECT_EQ(PythonServerManager::PoolState::STOPPED, pool->state);
    EXPECT_TRUE(process->is_alive());

    // UDAF can encounter a new GROUP BY key after DROP while an earlier group's client is alive.
    // All three client factories must reject this pool instead of creating a replacement.
    std::shared_ptr<PythonUDFClient> udf;
    std::shared_ptr<PythonUDAFClient> udaf;
    std::shared_ptr<PythonUDTFClient> udtf;
    EXPECT_FALSE(mgr.get_client(meta, version, &udf).ok());
    EXPECT_FALSE(mgr.get_client(meta, version, &udaf).ok());
    EXPECT_FALSE(mgr.get_client(meta, version, &udtf).ok());
    EXPECT_EQ(pool, mgr._process_pools.at(meta.id));
    EXPECT_EQ(1, pool->active_clients);

    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_TRUE(mgr._process_pools.contains(meta.id));
    EXPECT_TRUE(process->is_alive());
    mgr.drop_function(meta.id); // Duplicate cleanup must keep the same STOPPED pool.
    EXPECT_EQ(pool, mgr._process_pools.at(meta.id));

    lease.reset();
    EXPECT_TRUE(mgr._process_pools.contains(meta.id));
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_FALSE(mgr._process_pools.contains(meta.id));
    EXPECT_FALSE(process->is_alive());
}

TEST_F(PythonServerTest, HealthCheckReclaimsStoppedPoolsRegardlessOfAutoSuspend) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    for (int64_t timeout : {-1, 0, 3600}) {
        auto meta = test_meta(version);
        meta.id = 3000 + timeout;
        meta.auto_suspend = timeout;
        auto process = create_sleep_process();
        mgr.set_process_pool_for_test(meta, version, {process});
        mgr.drop_function(meta.id);
        ASSERT_TRUE(mgr._process_pools.contains(meta.id));
        EXPECT_TRUE(process->is_alive());
        mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
        EXPECT_FALSE(mgr._process_pools.contains(meta.id));
        EXPECT_FALSE(process->is_alive());
    }
}

TEST_F(PythonServerTest, MonitorReclaimsDroppedPoolAfterLastClientCloses) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    PythonClient client;
    ASSERT_TRUE(mgr._get_or_create_process_pool(meta, version, nullptr, &client._pool_lease)
                        .has_value());
    client._process = process;
    mgr.drop_function(meta.id);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_TRUE(process->is_alive());
    ASSERT_TRUE(client.close().ok());

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(40);
    while (process->is_alive() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(process->is_alive());
    EXPECT_TRUE(mgr.process_pool_snapshot_for_test(meta.id).empty());
}

TEST_F(PythonServerTest, DropDefersProcessCleanupAndDoesNotAffectOtherFunction) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    PythonUDFMeta first;
    first.id = 101;
    PythonUDFMeta second;
    second.id = 102;
    auto active = create_sleep_process();
    mgr.set_process_pool_for_test(first, version, {active});
    mgr.set_process_pool_for_test(second, version, {create_sleep_process()});
    mgr.drop_function(101);
    EXPECT_TRUE(active->is_alive());
    EXPECT_EQ(mgr.process_pool_snapshot_for_test(101).size(), 1);
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_FALSE(active->is_alive());
    EXPECT_TRUE(mgr.process_pool_snapshot_for_test(101).empty());
    EXPECT_EQ(mgr.process_pool_snapshot_for_test(102).size(), 1);
    active.reset();
}

TEST_F(PythonServerTest, DroppedClientProcessesRemainCountedUntilReleased) {
    check_dropped_process_memory(false);
}

TEST_F(PythonServerTest, RepairedDroppedClientProcessesRemainCountedUntilReleased) {
    check_dropped_process_memory(true);
}

TEST_F(PythonServerTest, DropDuringInitializationKeepsPeriodicMemoryAccounting) {
    setup_doris_home();
    auto python_path = create_fake_python_with_socket_creation();
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonServerManager mgr;
    struct ForkBarrier {
        std::mutex mutex;
        std::condition_variable cv;
        int entries = 0;
        bool released = false;
    };
    auto barrier = std::make_shared<ForkBarrier>();
    auto* sync_point = SyncPoint::get_instance();
    Defer clear_sync_point {[barrier, sync_point]() {
        {
            std::scoped_lock lock(barrier->mutex);
            barrier->released = true;
        }
        barrier->cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_call_back("PythonServerManager::fork:before_process_start");
    }};
    sync_point->set_call_back("PythonServerManager::fork:before_process_start", [barrier](auto&&) {
        std::unique_lock lock(barrier->mutex);
        if (++barrier->entries == 2) {
            barrier->cv.wait(lock, [&]() { return barrier->released; });
        }
    });
    sync_point->enable_processing();
    auto meta = test_meta(version);
    meta.pool_size = 2;
    std::shared_ptr<PythonUDFPoolLease> lease;
    auto pool = mgr._ensure_pool_initialized(meta, version, nullptr, &lease);
    ASSERT_TRUE(pool.has_value()) << pool.error().to_string();
    ProcessPtr client_process;
    ASSERT_TRUE(mgr._get_process(version, pool.value(), &client_process).ok());
    mgr.drop_function(meta.id);
    ASSERT_TRUE(client_process->is_alive());

    // Exercise the real periodic sampler: no explicit _refresh_memory_stats() call.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(35);
    while (mgr._mem_tracker.consumption() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_GT(mgr._mem_tracker.consumption(), 0);
}

TEST_F(PythonServerTest, ShutdownStopsDroppedClientProcesses) {
    setup_doris_home();
    auto python_path = create_fake_python_with_socket_creation();
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonServerManager mgr;
    auto meta = test_meta(version);
    auto pool = mgr._ensure_pool_initialized(meta, version);
    ASSERT_TRUE(pool.has_value()) << pool.error().to_string();
    ASSERT_TRUE(mgr.wait_for_process_pool_initialized_for_test(meta.id, std::chrono::seconds(5)));
    ProcessPtr client_process;
    ASSERT_TRUE(mgr._get_process(version, pool.value(), &client_process).ok());
    mgr.drop_function(meta.id);
    ASSERT_TRUE(client_process->is_alive());

    mgr.shutdown();
    EXPECT_FALSE(client_process->is_alive());
}

TEST_F(PythonServerTest, AutoSuspendDestroysIdleProcessesAndAllowsRecreation) {
    setup_doris_home();
    auto python_path = create_fake_python_with_socket_creation();
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonServerManager mgr;
    auto meta = test_meta(version);
    meta.auto_suspend = 0;
    auto result = mgr._ensure_pool_initialized(meta, version);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    ASSERT_TRUE(mgr.wait_for_process_pool_initialized_for_test(meta.id, std::chrono::seconds(5)));
    auto old_pool = result.value();
    auto process = mgr.process_pool_snapshot_for_test(meta.id).front();
    ASSERT_TRUE(process->is_alive());

    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_FALSE(process->is_alive());
    EXPECT_TRUE(mgr.process_pool_snapshot_for_test(meta.id).empty());
    auto reopened = mgr._ensure_pool_initialized(meta, version);
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_NE(old_pool, reopened.value());
}

TEST_F(PythonServerTest, AutoSuspendWaitsForTheConfiguredIdleDuration) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    meta.auto_suspend = 3;
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    auto pool = mgr._get_or_create_process_pool(meta, version, nullptr).value();
    auto idle_since = pool->idle_since;
    mgr._reclaim_idle_pools(idle_since + std::chrono::seconds(2));
    EXPECT_TRUE(process->is_alive());
    mgr._reclaim_idle_pools(idle_since + std::chrono::seconds(3));
    EXPECT_FALSE(process->is_alive());
}

TEST_F(PythonServerTest, ExplicitMinusOneKeepsIdleProcessesAlive) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    meta.auto_suspend = -1;
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now() + std::chrono::hours(24));
    EXPECT_TRUE(process->is_alive());
}

TEST_F(PythonServerTest, AutoSuspendWaitsForLastClientAndStartsANewIdlePeriod) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    meta.auto_suspend = 3;
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    std::shared_ptr<PythonUDFPoolLease> first;
    std::shared_ptr<PythonUDFPoolLease> second;
    auto pool = mgr._get_or_create_process_pool(meta, version, nullptr, &first).value();
    ASSERT_TRUE(mgr._get_or_create_process_pool(meta, version, nullptr, &second).has_value());
    auto future = std::chrono::steady_clock::now() + std::chrono::hours(24);
    mgr._reclaim_idle_pools(future);
    EXPECT_TRUE(process->is_alive());
    first.reset();
    mgr._reclaim_idle_pools(future);
    EXPECT_TRUE(process->is_alive());
    second.reset();
    EXPECT_EQ(0, pool->active_clients);
    mgr._reclaim_idle_pools(pool->idle_since + std::chrono::seconds(2));
    EXPECT_TRUE(process->is_alive());
    mgr._reclaim_idle_pools(pool->idle_since + std::chrono::seconds(3));
    EXPECT_FALSE(process->is_alive());
}

TEST_F(PythonServerTest, AutoSuspendClientCloseReleasesLeaseWithoutAWriter) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    meta.auto_suspend = 0;
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    PythonClient client;
    auto pool =
            mgr._get_or_create_process_pool(meta, version, nullptr, &client._pool_lease).value();
    client._process = process;
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_TRUE(process->is_alive());
    ASSERT_TRUE(client.close().ok());
    EXPECT_EQ(0, pool->active_clients);
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_FALSE(process->is_alive());
}

TEST_F(PythonServerTest, AutoSuspendInitializationFailureReleasesLease) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/missing_python");
    auto meta = test_meta(version);
    meta.auto_suspend = 0;
    std::shared_ptr<PythonUDFClient> client;
    EXPECT_FALSE(mgr.get_client(meta, version, &client).ok());
    auto pool = mgr._get_or_create_process_pool(meta, version, nullptr).value();
    EXPECT_EQ(0, pool->active_clients);
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now());
    EXPECT_TRUE(mgr.process_pool_snapshot_for_test(meta.id).empty());
}

TEST_F(PythonServerTest, AutoSuspendProtectsClientInitialization) {
    setup_doris_home();
    auto python_path = create_fake_python_with_socket_creation();
    PythonVersion version("3.9.16", test_dir_, python_path);
    PythonServerManager mgr;
    auto meta = test_meta(version);
    meta.auto_suspend = 0;
    std::promise<void> started;
    std::promise<void> resume;
    auto resumed = resume.get_future().share();
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("PythonServerManager::fork:before_process_start", [&](auto&&) {
        started.set_value();
        resumed.wait();
    });
    sync_point->enable_processing();
    Defer cleanup {[&]() {
        sync_point->disable_processing();
        sync_point->clear_call_back("PythonServerManager::fork:before_process_start");
    }};
    auto opening = std::async(std::launch::async, [&]() {
        std::shared_ptr<PythonUDFClient> client;
        return mgr.get_client(meta, version, &client);
    });
    EXPECT_EQ(std::future_status::ready, started.get_future().wait_for(std::chrono::seconds(5)));
    auto pool = mgr._get_or_create_process_pool(meta, version, nullptr).value();
    mgr._reclaim_idle_pools(std::chrono::steady_clock::now() + std::chrono::hours(24));
    EXPECT_EQ(1, pool->active_clients);
    EXPECT_NE(PythonServerManager::PoolState::STOPPED, pool->state);
    resume.set_value();
    // Either an initialization error or client destruction must release usage.
    (void)opening.get();
    EXPECT_EQ(0, pool->active_clients);
}

TEST_F(PythonServerTest, AutoSuspendMonitorTerminatesAnIdleProcess) {
    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/unused_python");
    auto meta = test_meta(version);
    meta.auto_suspend = 0;
    auto process = create_sleep_process();
    mgr.set_process_pool_for_test(meta, version, {process});
    // A zero timeout becomes eligible immediately, but reclamation waits for the monitor scan.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_TRUE(process->is_alive());
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(40);
    while (process->is_alive() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(process->is_alive());
    EXPECT_TRUE(mgr.process_pool_snapshot_for_test(meta.id).empty());
}

TEST_F(PythonServerTest, CheckAndRecreateProcessesRecreatesDeadProcess) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 3;

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

    mgr.set_process_pool_for_test(test_meta(version), version,
                                  {alive_process, dead_process, nullptr});

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
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

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(test_meta(version), version, {dead_process});
    auto pool_result = mgr._ensure_pool_initialized(test_meta(version), version);
    ASSERT_TRUE(pool_result.has_value()) << pool_result.error().to_string();
    {
        std::lock_guard<std::mutex> lock(pool_result.value()->mutex);
        pool_result.value()->repairing = true;
    }

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
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

    test_pool_size_ = 1;

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, python_path);

    ProcessPtr dead_process;
    ASSERT_TRUE(mgr.fork(version, &dead_process).ok());
    ASSERT_NE(dead_process, nullptr);
    pid_t dead_pid = dead_process->get_child_pid();
    dead_process->shutdown();
    ASSERT_FALSE(dead_process->is_alive());

    mgr.set_process_pool_for_test(test_meta(version), version, {dead_process}, false);

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(test_meta(version).id);
    ASSERT_EQ(pool_snapshot.size(), 1);
    ASSERT_NE(pool_snapshot[0], nullptr);
    EXPECT_FALSE(pool_snapshot[0]->is_alive());
    EXPECT_EQ(pool_snapshot[0]->get_child_pid(), dead_pid);

    mgr.shutdown();
}

TEST_F(PythonServerTest, CheckAndRecreateProcessesKeepsDeadSlotsWhenRecreateFails) {
    setup_doris_home();
    std::string python_path = create_fake_python_with_socket_creation("3.9.16");

    test_pool_size_ = 2;

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
    mgr.set_process_pool_for_test(test_meta(invalid_version), invalid_version,
                                  {dead_process_1, dead_process_2});

    mgr.check_and_recreate_processes_for_test();

    auto pool_snapshot = mgr.process_pool_snapshot_for_test(test_meta(invalid_version).id);
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
