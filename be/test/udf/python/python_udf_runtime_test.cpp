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

#include "udf/python/python_udf_runtime.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/process.hpp>
#include <filesystem>
#include <optional>
#include <string>

#include "util/defer_op.h"

namespace doris {

namespace fs = std::filesystem;
namespace bp = boost::process;

class PythonUDFRuntimeTest : public ::testing::Test {
protected:
    std::string test_dir_;

    void SetUp() override {
        test_dir_ = fs::temp_directory_path().string() + "/python_runtime_test_" +
                    std::to_string(getpid()) + "_" + std::to_string(rand());
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        if (!test_dir_.empty() && fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    // Helper to create a unix socket file for testing
    bool create_unix_socket(const std::string& path) {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) return false;

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

        unlink(path.c_str()); // Remove if exists
        int ret = bind(fd, (struct sockaddr*)&addr, sizeof(addr));
        close(fd);
        return ret == 0;
    }
};

// ============================================================================
// Helper function tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, UnixSocketPathsMatchFlightContract) {
    EXPECT_EQ(get_base_unix_socket_path(), "grpc+unix:///tmp/doris_python_udf");
    EXPECT_EQ(get_unix_socket_path(12345), "grpc+unix:///tmp/doris_python_udf_12345.sock");
    EXPECT_EQ(get_unix_socket_file_path(12345), "/tmp/doris_python_udf_12345.sock");
}

TEST_F(PythonUDFRuntimeTest, GetFightServerPath) {
    // Save original DORIS_HOME
    std::optional<std::string> original_doris_home;
    if (const char* doris_home = std::getenv("DORIS_HOME")) {
        original_doris_home = doris_home;
    }

    // Set test DORIS_HOME
    setenv("DORIS_HOME", "/test/doris/home", 1);

    std::string path = get_fight_server_path();
    EXPECT_EQ(path, "/test/doris/home/plugins/python_udf/python_server.py");

    // Restore original DORIS_HOME
    if (original_doris_home) {
        setenv("DORIS_HOME", original_doris_home->c_str(), 1);
    } else {
        unsetenv("DORIS_HOME");
    }
}

TEST_F(PythonUDFRuntimeTest, WaitChildExitReturnsExitedForExitedChild) {
    bp::ipstream output;
    bp::child child("/bin/bash", "-c", "exit 7", bp::std_out > output);
    ASSERT_TRUE(child.valid());

    int exit_status = 0;
    auto result = PythonUDFProcess::wait_child_exit(child.id(), std::chrono::milliseconds(1000),
                                                    &exit_status);
    child.detach();

    EXPECT_TRUE(result == PythonUDFProcess::ChildExitWaitResult::EXITED ||
                result == PythonUDFProcess::ChildExitWaitResult::ALREADY_REAPED);
    if (result == PythonUDFProcess::ChildExitWaitResult::EXITED) {
        EXPECT_TRUE(WIFEXITED(exit_status));
        EXPECT_EQ(WEXITSTATUS(exit_status), 7);
    }
}

TEST_F(PythonUDFRuntimeTest, WaitChildExitReturnsTimeoutForRunningChild) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);
    ASSERT_TRUE(child.valid());
    ASSERT_TRUE(child.running());

    int exit_status = 0;
    auto result = PythonUDFProcess::wait_child_exit(child.id(), std::chrono::milliseconds(20),
                                                    &exit_status);

    EXPECT_EQ(result, PythonUDFProcess::ChildExitWaitResult::TIMEOUT);

    child.terminate();
    child.wait();
}

TEST_F(PythonUDFRuntimeTest, WaitChildExitReturnsAlreadyReapedForReapedChild) {
    bp::ipstream output;
    bp::child child("/bin/true", bp::std_out > output);
    ASSERT_TRUE(child.valid());
    pid_t child_pid = child.id();
    child.wait();

    int exit_status = 0;
    auto result = PythonUDFProcess::wait_child_exit(child_pid, std::chrono::milliseconds(0),
                                                    &exit_status);

    EXPECT_EQ(result, PythonUDFProcess::ChildExitWaitResult::ALREADY_REAPED);
}

TEST_F(PythonUDFRuntimeTest, BackgroundReaperReapsQueuedChild) {
    bp::ipstream output;
    bp::child child("/bin/bash", "-c", "sleep 0.1; exit 0", bp::std_out > output);
    ASSERT_TRUE(child.valid());
    pid_t child_pid = child.id();

    // Do not try to force the real "SIGKILLed but still not reapable" case in UT. That usually
    // needs kernel-level uninterruptible sleep. The behavior we must guarantee is that once such a
    // pid is handed off, the background reaper keeps waitpid ownership until the child exits.
    child.detach();
    PythonUDFProcess::enqueue_child_for_reap(child_pid);

    bool reaped = PythonUDFProcess::wait_background_reaped_for_test(
            child_pid, std::chrono::milliseconds(5000));
    EXPECT_TRUE(reaped);

    int exit_status = 0;
    auto result = PythonUDFProcess::wait_child_exit(child_pid, std::chrono::milliseconds(0),
                                                    &exit_status);
    EXPECT_EQ(result, PythonUDFProcess::ChildExitWaitResult::ALREADY_REAPED);
}

TEST_F(PythonUDFRuntimeTest, SocketPathLength) {
    // Unix socket paths have a maximum length (usually 107 chars)
    // Verify generated paths are within reasonable limits
    pid_t max_pid = 4194304; // Max PID on Linux (2^22)
    std::string path = get_unix_socket_file_path(max_pid);

    // Should be well under 107 characters
    EXPECT_LT(path.length(), 100);
}

// ============================================================================
// PythonUDFProcess shutdown() tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, ShutdownTerminatesProcess) {
    // Use sleep instead of a stdin-driven command. In CI, stdin may be closed and
    // commands like cat can exit before running() is checked.
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);

    ASSERT_TRUE(child.valid());
    ASSERT_TRUE(child.running());

    pid_t child_pid = child.id();

    PythonUDFProcess process(std::move(child), std::move(output));

    EXPECT_FALSE(process.is_shutdown());
    EXPECT_TRUE(process.is_alive());
    EXPECT_EQ(process.get_child_pid(), child_pid);

    // Shutdown should terminate the process
    process.shutdown();

    EXPECT_TRUE(process.is_shutdown());
    EXPECT_FALSE(process.is_alive());
}

TEST_F(PythonUDFRuntimeTest, ShutdownIdempotent) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);

    PythonUDFProcess process(std::move(child), std::move(output));

    // Multiple shutdown calls should be safe
    process.shutdown();
    EXPECT_TRUE(process.is_shutdown());

    process.shutdown(); // Should not crash
    EXPECT_TRUE(process.is_shutdown());

    process.shutdown(); // Should not crash
    EXPECT_TRUE(process.is_shutdown());
}

TEST_F(PythonUDFRuntimeTest, ShutdownWithStubbornProcess) {
    // Create a process that ignores SIGTERM - tests the SIGKILL fallback path
    bp::ipstream output;
    bp::child child("/bin/bash", "-c", "trap '' TERM; exec sleep 60", bp::std_out > output);

    PythonUDFProcess process(std::move(child), std::move(output));
    EXPECT_TRUE(process.is_alive());

    // Shutdown should still work (will use SIGKILL after retries)
    process.shutdown();

    EXPECT_TRUE(process.is_shutdown());
    EXPECT_FALSE(process.is_alive());
}

TEST_F(PythonUDFRuntimeTest, ShutdownEnqueuesBackgroundReapWhenSigkillWaitTimesOut) {
    bp::ipstream output;
    bp::child child("/bin/bash", "-c", "trap '' TERM; exec sleep 60", bp::std_out > output);
    ASSERT_TRUE(child.valid());
    pid_t child_pid = child.id();

    PythonUDFProcess process(std::move(child), std::move(output));
    ASSERT_TRUE(process.is_alive());

    // SIGKILL not becoming reapable inside a short bounded wait is rare and depends on kernel
    // state, so force only the wait results here. This covers the shutdown handoff contract:
    // a pid that was killed but not reaped synchronously must be owned by the background reaper.
    PythonUDFProcess::force_child_exit_timeouts_for_test(2);
    process.shutdown();
    PythonUDFProcess::force_child_exit_timeouts_for_test(0);

    EXPECT_TRUE(process.is_shutdown());
    EXPECT_TRUE(PythonUDFProcess::wait_background_reaped_for_test(child_pid,
                                                                  std::chrono::milliseconds(5000)));
}

// ============================================================================
// PythonUDFProcess remove_unix_socket() tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, RemoveUnixSocketExistingFile) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    // Create a socket file at the expected path
    std::string socket_path = get_unix_socket_file_path(child_pid);
    ASSERT_TRUE(create_unix_socket(socket_path));
    ASSERT_TRUE(fs::exists(socket_path));

    // Shutdown calls remove_unix_socket internally
    process.shutdown();

    // Socket file should be removed
    EXPECT_FALSE(fs::exists(socket_path));
}

TEST_F(PythonUDFRuntimeTest, ShutdownPreservesUnexpectedSocketDirectory) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);
    ASSERT_TRUE(child.valid());
    ASSERT_TRUE(child.running());

    PythonUDFProcess process(std::move(child), std::move(output));
    const std::string socket_path = process.get_socket_file_path();
    fs::remove_all(socket_path);
    ASSERT_TRUE(fs::create_directory(socket_path));
    Defer cleanup {[&]() { fs::remove_all(socket_path); }};

    // A directory at the derived socket path makes unlink() fail with EISDIR. Shutdown must still
    // finish, and it must not recursively delete an unexpected filesystem object.
    process.shutdown();

    EXPECT_TRUE(process.is_shutdown());
    EXPECT_TRUE(fs::is_directory(socket_path));
}

// ============================================================================
// PythonUDFProcess getter tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, ProcessUsesPidDerivedSocketPaths) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    EXPECT_EQ(process.get_uri(), get_unix_socket_path(child_pid));
    EXPECT_EQ(process.get_socket_file_path(), get_unix_socket_file_path(child_pid));

    process.shutdown();
}

// ============================================================================
// PythonUDFProcess destructor tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, DestructorCallsShutdown) {
    pid_t child_pid;
    {
        bp::ipstream output;
        bp::child child("/bin/sleep", "60", bp::std_out > output);
        child_pid = child.id();

        PythonUDFProcess process(std::move(child), std::move(output));
        EXPECT_TRUE(process.is_alive());
        // process goes out of scope, destructor should call shutdown
    }

    // Verify process is terminated - waitpid should return immediately
    int status;
    pid_t result = waitpid(child_pid, &status, WNOHANG);
    // Either already reaped by shutdown (-1 with ECHILD) or process not found
    EXPECT_NE(result, 0);
}

// ============================================================================
// PythonUDFProcess is_alive tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, IsAliveReflectsState) {
    bp::ipstream output;
    bp::child child("/bin/sleep", "60", bp::std_out > output);

    PythonUDFProcess process(std::move(child), std::move(output));

    // Initially alive
    EXPECT_TRUE(process.is_alive());
    EXPECT_FALSE(process.is_shutdown());

    // After shutdown, not alive
    process.shutdown();
    EXPECT_FALSE(process.is_alive());
    EXPECT_TRUE(process.is_shutdown());
}

} // namespace doris
