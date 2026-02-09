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
#include <unistd.h>

#include <boost/process.hpp>
#include <filesystem>
#include <string>

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

TEST_F(PythonUDFRuntimeTest, GetBaseUnixSocketPath) {
    std::string path = get_base_unix_socket_path();
    EXPECT_TRUE(path.find("grpc+unix://") != std::string::npos);
    EXPECT_TRUE(path.find("/tmp/doris_python_udf") != std::string::npos);
}

TEST_F(PythonUDFRuntimeTest, GetUnixSocketPath) {
    pid_t test_pid = 12345;
    std::string path = get_unix_socket_path(test_pid);
    EXPECT_TRUE(path.find("grpc+unix://") != std::string::npos);
    EXPECT_TRUE(path.find("12345") != std::string::npos);
    EXPECT_TRUE(path.find(".sock") != std::string::npos);
}

TEST_F(PythonUDFRuntimeTest, GetUnixSocketFilePathFormat) {
    pid_t test_pid = 99999;
    std::string path = get_unix_socket_file_path(test_pid);
    EXPECT_TRUE(path.find("/tmp/doris_python_udf") != std::string::npos);
    EXPECT_TRUE(path.find("99999") != std::string::npos);
    EXPECT_TRUE(path.find(".sock") != std::string::npos);
    // Should NOT have grpc+unix:// prefix
    EXPECT_TRUE(path.find("grpc+unix://") == std::string::npos);
}

TEST_F(PythonUDFRuntimeTest, GetUnixSocketPathDifferentPids) {
    std::string path1 = get_unix_socket_path(1000);
    std::string path2 = get_unix_socket_path(2000);
    EXPECT_NE(path1, path2);
    EXPECT_TRUE(path1.find("1000") != std::string::npos);
    EXPECT_TRUE(path2.find("2000") != std::string::npos);
}

TEST_F(PythonUDFRuntimeTest, GetFightServerPath) {
    // Save original DORIS_HOME
    const char* original_doris_home = std::getenv("DORIS_HOME");

    // Set test DORIS_HOME
    setenv("DORIS_HOME", "/test/doris/home", 1);

    std::string path = get_fight_server_path();
    EXPECT_EQ(path, "/test/doris/home/plugins/python_udf/python_server.py");

    // Restore original DORIS_HOME
    if (original_doris_home) {
        setenv("DORIS_HOME", original_doris_home, 1);
    } else {
        unsetenv("DORIS_HOME");
    }
}

// ============================================================================
// PythonUDFProcess tests (without actually spawning a process)
// ============================================================================

// Note: Most PythonUDFProcess tests require actually spawning Python processes,
// which is environment-dependent. Here we test what we can without real processes.

TEST_F(PythonUDFRuntimeTest, ProcessPtrIsSharedPtr) {
    // Verify ProcessPtr is a shared_ptr
    ProcessPtr ptr = nullptr;
    EXPECT_EQ(ptr, nullptr);
    EXPECT_FALSE(ptr);
}

// Test socket file path generation for various PIDs
TEST_F(PythonUDFRuntimeTest, SocketPathGenerationEdgeCases) {
    // Minimum PID
    std::string path1 = get_unix_socket_file_path(1);
    EXPECT_TRUE(path1.find("_1.sock") != std::string::npos);

    // Large PID
    std::string path2 = get_unix_socket_file_path(999999);
    EXPECT_TRUE(path2.find("_999999.sock") != std::string::npos);
}

TEST_F(PythonUDFRuntimeTest, SocketPathConsistency) {
    pid_t pid = 54321;
    // Multiple calls should return same result
    std::string path1 = get_unix_socket_path(pid);
    std::string path2 = get_unix_socket_path(pid);
    EXPECT_EQ(path1, path2);

    std::string file_path1 = get_unix_socket_file_path(pid);
    std::string file_path2 = get_unix_socket_file_path(pid);
    EXPECT_EQ(file_path1, file_path2);
}

TEST_F(PythonUDFRuntimeTest, UnixSocketPathRelationship) {
    pid_t pid = 12345;
    std::string socket_path = get_unix_socket_path(pid);
    std::string file_path = get_unix_socket_file_path(pid);

    // Socket path should contain the prefix + file path structure
    EXPECT_TRUE(socket_path.find(UNIX_SOCKET_PREFIX) == 0);

    // File path should not contain the prefix
    EXPECT_TRUE(file_path.find(UNIX_SOCKET_PREFIX) == std::string::npos);
}

// ============================================================================
// Socket path security tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, SocketPathInTmpDirectory) {
    // All socket files should be in /tmp to avoid path length issues
    pid_t pid = 12345;
    std::string file_path = get_unix_socket_file_path(pid);
    EXPECT_TRUE(file_path.find("/tmp/") == 0);
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
// Flight server path tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, FlightServerPathTemplate) {
    // Verify template includes necessary components
    std::string tmpl = FLIGHT_SERVER_PATH_TEMPLATE;
    EXPECT_TRUE(tmpl.find("{}") != std::string::npos); // Has placeholder
    EXPECT_TRUE(tmpl.find("plugins") != std::string::npos);
    EXPECT_TRUE(tmpl.find("python_udf") != std::string::npos);
}

// ============================================================================
// PythonUDFProcess shutdown() tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, ShutdownTerminatesProcess) {
    // Use /bin/cat which blocks waiting for stdin - reliable and fast
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

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
    bp::child child("/bin/cat", bp::std_out > output);

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
    bp::child child("/bin/bash", "-c", "trap '' TERM; cat", bp::std_out > output);

    PythonUDFProcess process(std::move(child), std::move(output));
    EXPECT_TRUE(process.is_alive());

    // Shutdown should still work (will use SIGKILL after retries)
    process.shutdown();

    EXPECT_TRUE(process.is_shutdown());
    EXPECT_FALSE(process.is_alive());
}

// ============================================================================
// PythonUDFProcess remove_unix_socket() tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, RemoveUnixSocketExistingFile) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

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

TEST_F(PythonUDFRuntimeTest, RemoveUnixSocketNonExistent) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    // Don't create socket file - it doesn't exist
    std::string socket_path = get_unix_socket_file_path(child_pid);
    ASSERT_FALSE(fs::exists(socket_path));

    // Shutdown should not crash even if socket doesn't exist (ENOENT case)
    process.shutdown();
    EXPECT_TRUE(process.is_shutdown());
}

TEST_F(PythonUDFRuntimeTest, RemoveUnixSocketIsDirectory) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

    pid_t child_pid = child.id();

    // Create a directory at the socket path location (instead of a file)
    // This will cause unlink() to fail with EISDIR
    std::string socket_path = get_unix_socket_file_path(child_pid);
    fs::create_directories(socket_path);
    ASSERT_TRUE(fs::is_directory(socket_path));

    PythonUDFProcess process(std::move(child), std::move(output));

    // Shutdown should handle EISDIR error gracefully (logs warning but doesn't crash)
    process.shutdown();
    EXPECT_TRUE(process.is_shutdown());

    // Cleanup - remove the directory we created
    fs::remove_all(socket_path);
}

// ============================================================================
// PythonUDFProcess to_string() tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, ToStringFormat) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    std::string str = process.to_string();

    // Verify to_string contains expected fields
    EXPECT_TRUE(str.find("PythonUDFProcess") != std::string::npos);
    EXPECT_TRUE(str.find("child_pid") != std::string::npos);
    EXPECT_TRUE(str.find(std::to_string(child_pid)) != std::string::npos);
    EXPECT_TRUE(str.find("uri") != std::string::npos);
    EXPECT_TRUE(str.find("unix_socket_file_path") != std::string::npos);
    EXPECT_TRUE(str.find("is_shutdown") != std::string::npos);

    process.shutdown();
}

// ============================================================================
// PythonUDFProcess getter tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, GetUri) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    std::string uri = process.get_uri();

    // URI should contain the grpc+unix prefix and pid
    EXPECT_TRUE(uri.find("grpc+unix://") != std::string::npos);
    EXPECT_TRUE(uri.find(std::to_string(child_pid)) != std::string::npos);
    EXPECT_TRUE(uri.find(".sock") != std::string::npos);

    process.shutdown();
}

TEST_F(PythonUDFRuntimeTest, GetSocketFilePath) {
    bp::ipstream output;
    bp::child child("/bin/cat", bp::std_out > output);

    pid_t child_pid = child.id();
    PythonUDFProcess process(std::move(child), std::move(output));

    const std::string& path = process.get_socket_file_path();

    // File path should NOT have grpc+unix prefix
    EXPECT_TRUE(path.find("grpc+unix://") == std::string::npos);
    EXPECT_TRUE(path.find(std::to_string(child_pid)) != std::string::npos);
    EXPECT_TRUE(path.find(".sock") != std::string::npos);
    EXPECT_TRUE(path.find("/tmp/") == 0);

    process.shutdown();
}

// ============================================================================
// PythonUDFProcess equality tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, ProcessEquality) {
    bp::ipstream output1, output2;
    bp::child child1("/bin/cat", bp::std_out > output1);
    bp::child child2("/bin/cat", bp::std_out > output2);

    PythonUDFProcess process1(std::move(child1), std::move(output1));
    PythonUDFProcess process2(std::move(child2), std::move(output2));

    // Different processes should not be equal (different PIDs)
    EXPECT_NE(process1, process2);

    process1.shutdown();
    process2.shutdown();
}

// ============================================================================
// PythonUDFProcess destructor tests
// ============================================================================

TEST_F(PythonUDFRuntimeTest, DestructorCallsShutdown) {
    pid_t child_pid;
    {
        bp::ipstream output;
        bp::child child("/bin/cat", bp::std_out > output);
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
    bp::child child("/bin/cat", bp::std_out > output);

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
