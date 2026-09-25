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

#include "runtime/cdc_client_mgr.h"

#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>

#include "common/config.h"
#include "common/status.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {

class CdcClientMgrTest : public testing::Test {
public:
    void SetUp() override {
        // Save original environment variables
        _original_doris_home = getenv("DORIS_HOME");
        _original_log_dir = getenv("LOG_DIR");
        _original_java_home = getenv("JAVA_HOME");

        // Use existing DORIS_HOME
        const char* doris_home = std::getenv("DORIS_HOME");
        if (doris_home) {
            _doris_home = doris_home;
            _lib_dir = _doris_home + "/lib/cdc_client";
            _jar_path = _lib_dir + "/cdc-client.jar";

            // Create lib directory and jar file if they don't exist
            [[maybe_unused]] int ret1 = system(("mkdir -p " + _lib_dir).c_str());
            // Create a dummy jar file for testing if it doesn't exist
            if (access(_jar_path.c_str(), F_OK) != 0) {
                [[maybe_unused]] int ret2 = system(("touch " + _jar_path).c_str());
                _jar_created = true;
            }
        }

        // Use existing LOG_DIR or set a default
        const char* log_dir = std::getenv("LOG_DIR");
        if (!log_dir) {
            _log_dir = "/tmp/doris_test_log";
            setenv("LOG_DIR", _log_dir.c_str(), 1);
            _log_dir_set = true;
        }

        auto* env = ExecEnv::GetInstance();
        _cluster_info = std::make_unique<ClusterInfo>();
        _cluster_info->token = "test_token";
        env->set_cluster_info(_cluster_info.get());
    }

    void TearDown() override {
        // Restore original environment variables
        if (_original_doris_home) {
            setenv("DORIS_HOME", _original_doris_home, 1);
        } else {
            unsetenv("DORIS_HOME");
        }

        if (_original_log_dir) {
            setenv("LOG_DIR", _original_log_dir, 1);
        } else if (_log_dir_set) {
            unsetenv("LOG_DIR");
        }

        if (_original_java_home) {
            setenv("JAVA_HOME", _original_java_home, 1);
        } else {
            unsetenv("JAVA_HOME");
        }

        // Clean up created jar file if we created it
        if (_jar_created && !_jar_path.empty()) {
            [[maybe_unused]] int cleanup_ret = system(("rm -f " + _jar_path).c_str());
        }

        ExecEnv::GetInstance()->set_cluster_info(nullptr);
    }

protected:
    std::string _doris_home;
    std::string _log_dir;
    std::string _lib_dir;
    std::string _jar_path;
    const char* _original_doris_home = nullptr;
    const char* _original_log_dir = nullptr;
    const char* _original_java_home = nullptr;
    bool _jar_created = false;
    bool _log_dir_set = false;
    std::unique_ptr<ClusterInfo> _cluster_info;
};

// Test stop method when there's no child process
TEST_F(CdcClientMgrTest, StopWithoutChild) {
    CdcClientMgr mgr;
    // Should not crash
    mgr.stop();
}

// Test stop when the published process is already absent.
TEST_F(CdcClientMgrTest, StopWhenProcessDead) {
    CdcClientMgr mgr;

    // Start CDC client (sets PID to 99999 in BE_TEST mode)
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0);

    // The generation-qualified cleanup observes ECHILD and revokes ownership without signalling.
    mgr.stop();

    // PID should be reset to 0
    EXPECT_EQ(mgr.get_child_pid(), 0);
}

// Test stop with real process that exits gracefully (covers lines 98-111: graceful shutdown)
TEST_F(CdcClientMgrTest, StopWithRealProcessGraceful) {
    CdcClientMgr mgr;

    // Use popen to start a background sleep process and get its PID
    // This avoids fork() which conflicts with gcov/coverage tools
    FILE* pipe = popen("sleep 10 & echo $!", "r");
    if (pipe) {
        char buffer[128];
        if (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            pid_t real_pid = std::atoi(buffer);
            pclose(pipe);

            if (real_pid > 0) {
                // Set the PID in the manager
                mgr.set_child_pid_for_test(real_pid);

                // Call stop - process will respond to SIGTERM and exit
                // This covers the graceful shutdown path
                mgr.stop();

                // Verify PID is reset
                EXPECT_EQ(mgr.get_child_pid(), 0);

                // Clean up: make sure child is dead
                kill(real_pid, SIGKILL);
                waitpid(real_pid, nullptr, WNOHANG);
            }
        } else {
            pclose(pipe);
        }
    }
}

// Test stop with a direct child that requires force kill.
TEST_F(CdcClientMgrTest, StopWithRealProcessForceKill) {
    CdcClientMgr mgr;

    int ready_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    Defer close_pipe {[&]() {
        close(ready_pipe[0]);
        if (ready_pipe[1] >= 0) {
            close(ready_pipe[1]);
        }
    }};

    posix_spawn_file_actions_t actions;
    ASSERT_EQ(posix_spawn_file_actions_init(&actions), 0);
    Defer destroy_actions {[&]() { posix_spawn_file_actions_destroy(&actions); }};
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, ready_pipe[1], STDOUT_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, ready_pipe[0]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, ready_pipe[1]), 0);

    // The shell reports readiness only after ignoring SIGTERM. exec preserves ignored signals,
    // leaving sleep as this test process's direct child with the same PID.
    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("trap '' TERM; printf R; exec sleep 3600"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", &actions, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);
    bool child_reaped = false;
    Defer cleanup_child {[&]() {
        if (!child_reaped) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }};
    close(ready_pipe[1]);
    ready_pipe[1] = -1;
    char ready = 0;
    ASSERT_EQ(read(ready_pipe[0], &ready, 1), 1);
    ASSERT_EQ(ready, 'R');

    mgr.set_child_pid_for_test(pid);
    mgr.stop();
    EXPECT_EQ(mgr.get_child_pid(), 0);

    errno = 0;
    const pid_t wait_result = waitpid(pid, nullptr, WNOHANG);
    const int wait_error = errno;
    child_reaped = wait_result == pid || (wait_result < 0 && wait_error == ECHILD);
    EXPECT_EQ(wait_result, -1) << "stop() did not collect the forced-kill child";
    EXPECT_EQ(wait_error, ECHILD);
}

// Test start_cdc_client with missing jar file
TEST_F(CdcClientMgrTest, StartCdcClientMissingJar) {
    [[maybe_unused]] int rm_ret = system(("rm -f " + _jar_path).c_str());

    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("cdc-client.jar") != std::string::npos);
    EXPECT_TRUE(result.has_status());

    [[maybe_unused]] int touch_ret = system(("touch " + _jar_path).c_str());
}

// Test start_cdc_client with missing JAVA_HOME
TEST_F(CdcClientMgrTest, StartCdcClientMissingJavaHome) {
    unsetenv("JAVA_HOME");

    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("JAVA_HOME") != std::string::npos);
    EXPECT_TRUE(result.has_status());
}

// Test multiple calls to start_cdc_client
TEST_F(CdcClientMgrTest, StartCdcClientMultipleTimes) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result1;
    PRequestCdcClientResult result2;

    // Initially no child process
    EXPECT_EQ(mgr.get_child_pid(), 0);

    // First call - should start CDC client
    Status status1 = mgr.start_cdc_client(&result1);
    EXPECT_TRUE(status1.ok());
    pid_t pid_after_first = mgr.get_child_pid();
    EXPECT_GT(pid_after_first, 0); // PID should be set

    // Second call - should detect already started and not restart
    Status status2 = mgr.start_cdc_client(&result2);
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(mgr.get_child_pid(), pid_after_first); // PID should not change!
}

// Test destructor calls stop
TEST_F(CdcClientMgrTest, DestructorCallsStop) {
    {
        CdcClientMgr mgr;
        PRequestCdcClientResult result;
        [[maybe_unused]] Status status = mgr.start_cdc_client(&result);
        // Destructor will be called when mgr goes out of scope
    }
    // If no crash, test passes
    SUCCEED();
}

// Test send_request_to_cdc_client with explicitly empty params (covers params_body.empty() branch)
TEST_F(CdcClientMgrTest, SendRequestExplicitlyEmptyParams) {
    CdcClientMgr mgr;
    std::string response;

    // Explicitly test empty params_body path (line 309)
    std::string empty_params = "";
    Status status = mgr.send_request_to_cdc_client("/health", empty_params, &response);

    // Expected to fail but should not crash
    EXPECT_FALSE(status.ok());
}

// Test request_cdc_client_impl when start fails (covers lines 288-291)
TEST_F(CdcClientMgrTest, RequestCdcClientImplStartFailed) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api("/test/api");
    request.set_params("{}");

    // Remove JAVA_HOME to make start_cdc_client fail
    unsetenv("JAVA_HOME");

    struct TestClosure : public google::protobuf::Closure {
        void Run() override { called = true; }
        bool called = false;
    };
    TestClosure closure;

    mgr.request_cdc_client_impl(&request, &result, &closure);

    // Should have error status in result
    EXPECT_TRUE(result.has_status());
    EXPECT_NE(result.status().status_code(), 0);
    EXPECT_TRUE(closure.called);

    // Restore JAVA_HOME
    if (_original_java_home) {
        setenv("JAVA_HOME", _original_java_home, 1);
    }
}

// Test start_cdc_client with result
TEST_F(CdcClientMgrTest, StartCdcClientWithResult) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;

    EXPECT_EQ(mgr.get_child_pid(), 0);

    Status status = mgr.start_cdc_client(&result);

    // Should succeed
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0); // PID should be set
}

// Scenario: starting the cdc client installs a process-wide SIGCHLD handler, and a process-wide
// handler sees every child of the BE, not just the cdc client. BE also runs an embedded JVM, which
// forks children of its own for Runtime.exec() and reads their exit status from its process-reaper
// thread. Reaping one of those here makes that thread find the child already gone, and
// java.lang.ProcessHandleImpl turns the resulting ECHILD into exit code 0 whatever the child
// really returned - Java code inside BE that branches on an exit status then takes the wrong
// branch silently. The handler must wait on the cdc client's pid alone.
TEST_F(CdcClientMgrTest, SigchldHandlerDoesNotReapOtherChildren) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    ASSERT_TRUE(mgr.start_cdc_client(&result).ok());

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 0.2; exit 7"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);

    // Be somewhere other than waitpid() when the child exits, so its SIGCHLD reaches the handler
    // rather than a waiter that is already blocked on this pid.
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    int child_status = 0;
    const pid_t reaped = waitpid(pid, &child_status, 0);
    ASSERT_EQ(reaped, pid) << "the cdc SIGCHLD handler consumed a child that is not the cdc client";
    ASSERT_TRUE(WIFEXITED(child_status));
    EXPECT_EQ(WEXITSTATUS(child_status), 7);

    mgr.stop();
}

TEST_F(CdcClientMgrTest, SigchldHandlerReapsOwnedChildAndPreservesErrno) {
    sigset_t blocked;
    sigset_t old_mask;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGCHLD);
    ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &blocked, &old_mask), 0);
    Defer restore_mask {[&]() { pthread_sigmask(SIG_SETMASK, &old_mask, nullptr); }};

    // Earlier cases install the production SIGCHLD handler process-wide. Temporarily restore the
    // default disposition so only the deterministic direct invocation below can collect this child;
    // blocking SIGCHLD on this thread alone cannot stop another test/runtime thread receiving it.
    struct sigaction old_action {};
    ASSERT_EQ(sigaction(SIGCHLD, nullptr, &old_action), 0);
    struct sigaction default_action {};
    default_action.sa_handler = SIG_DFL;
    sigemptyset(&default_action.sa_mask);
    ASSERT_EQ(sigaction(SIGCHLD, &default_action, nullptr), 0);
    Defer restore_action {[&]() { sigaction(SIGCHLD, &old_action, nullptr); }};

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("exit 7"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);

    CdcClientMgr mgr;
    mgr.set_child_pid_for_test(pid);

    siginfo_t child_info {};
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(waitid(P_PID, pid, &child_info, WEXITED | WNOHANG | WNOWAIT), 0);
        if (child_info.si_pid == pid) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(child_info.si_pid, pid);

    errno = EBUSY;
    CdcClientMgr::invoke_sigchld_handler_for_test();
    EXPECT_EQ(errno, EBUSY);
    EXPECT_EQ(mgr.get_child_pid(), 0)
            << "reaping the owned child must also revoke the manager's ownership";

    int status = 0;
    errno = 0;
    EXPECT_EQ(waitpid(pid, &status, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD) << "the handler must collect the cdc child itself";

    // Once ownership is revoked, stop() must not act on another live child of the same BE. This
    // covers the dangerous same-parent case: waitpid() would accept that child, unlike a reused PID
    // owned by another process.
    pid_t unrelated_pid = 0;
    char* const unrelated_argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                                    const_cast<char*>("sleep 10"), nullptr};
    ASSERT_EQ(posix_spawn(&unrelated_pid, "/bin/sh", nullptr, nullptr, unrelated_argv, envp), 0);
    ASSERT_GT(unrelated_pid, 0);
    Defer cleanup_unrelated {[&]() {
        kill(unrelated_pid, SIGKILL);
        waitpid(unrelated_pid, nullptr, 0);
    }};

    mgr.stop();
    EXPECT_EQ(kill(unrelated_pid, 0), 0)
            << "stop() signalled a child after the CDC ownership had been revoked";
}

TEST_F(CdcClientMgrTest, StopWaitsForAHandlerHoldingTheOldChildIdentity) {
    sigset_t blocked;
    sigset_t old_mask;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGCHLD);
    ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &blocked, &old_mask), 0);
    Defer restore_mask {[&]() { pthread_sigmask(SIG_SETMASK, &old_mask, nullptr); }};

    struct sigaction old_action {};
    ASSERT_EQ(sigaction(SIGCHLD, nullptr, &old_action), 0);
    struct sigaction default_action {};
    default_action.sa_handler = SIG_DFL;
    sigemptyset(&default_action.sa_mask);
    ASSERT_EQ(sigaction(SIGCHLD, &default_action, nullptr), 0);
    Defer restore_action {[&]() { sigaction(SIGCHLD, &old_action, nullptr); }};

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 10"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);

    CdcClientMgr mgr;
    mgr.set_child_pid_for_test(pid);
    CdcClientMgr::pause_sigchld_handler_for_test(true);
    std::thread handler([]() { CdcClientMgr::invoke_sigchld_handler_for_test(); });

    for (int i = 0; i < 100 && !CdcClientMgr::sigchld_handler_paused_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!CdcClientMgr::sigchld_handler_paused_for_test()) {
        CdcClientMgr::pause_sigchld_handler_for_test(false);
        handler.join();
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        FAIL() << "the deterministic handler did not reach its pause point";
    }

    CdcClientMgr::reset_child_claim_failed_for_test();
    std::atomic<bool> stop_finished {false};
    std::thread stopper([&]() {
        mgr.stop();
        stop_finished.store(true);
    });
    // The handler keeps the identity published while it owns the process-operation claim. That
    // prevents a replacement generation from publishing the same numeric pid until the handler's
    // final syscall has completed.
    EXPECT_EQ(mgr.get_child_pid(), pid);
    for (int i = 0; i < 5000 && !CdcClientMgr::child_claim_failed_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(CdcClientMgr::child_claim_failed_for_test())
            << "stop did not attempt to claim the child while the handler was paused";
    EXPECT_FALSE(stop_finished.load())
            << "stop returned while a signal handler could still operate the old numeric pid";

    CdcClientMgr::pause_sigchld_handler_for_test(false);
    handler.join();
    stopper.join();
    EXPECT_TRUE(stop_finished.load());
    EXPECT_EQ(mgr.get_child_pid(), 0);

    errno = 0;
    EXPECT_EQ(waitpid(pid, nullptr, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

TEST_F(CdcClientMgrTest, StaleGenerationCannotTerminateAReusedNumericPid) {
    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 10"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);
    // Declare the manager before the cleanup guard so that a fatal assertion destroys the guard
    // first: it kills and reaps this direct child, and the manager's stop() then observes ECHILD
    // instead of signalling a numeric pid whose ownership it has already given up.
    CdcClientMgr mgr;
    bool child_needs_cleanup = true;
    Defer cleanup {[&]() {
        if (child_needs_cleanup) {
            kill(pid, SIGKILL);
            waitpid(pid, nullptr, 0);
        }
    }};

    const uint64_t old_identity = mgr.set_child_pid_for_test(pid);
    // Republish the same numeric pid under a new generation. This deterministically models the
    // kernel reusing a reaped CDC pid for another same-parent child without depending on PID churn.
    const uint64_t replacement_identity = mgr.set_child_pid_for_test(pid);
    ASSERT_NE(old_identity, replacement_identity);
    ASSERT_EQ(mgr.get_child_identity_for_test(), replacement_identity);

    EXPECT_FALSE(mgr.terminate_child_identity_for_test(old_identity));
    EXPECT_EQ(kill(pid, 0), 0)
            << "cleanup retained a stale raw pid and signalled its replacement generation";

    mgr.stop();
    child_needs_cleanup = false;
    EXPECT_EQ(mgr.get_child_identity_for_test(), 0);
}

TEST_F(CdcClientMgrTest, ConcurrentHandlersHaveOneExclusiveProcessOperator) {
    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 10"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);

    CdcClientMgr mgr;
    mgr.set_child_pid_for_test(pid);
    CdcClientMgr::pause_sigchld_handler_for_test(true);
    Defer resume_handler {[]() { CdcClientMgr::pause_sigchld_handler_for_test(false); }};
    std::thread first([]() { CdcClientMgr::invoke_sigchld_handler_for_test(); });

    for (int i = 0; i < 100 && !CdcClientMgr::sigchld_handler_paused_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!CdcClientMgr::sigchld_handler_paused_for_test()) {
        CdcClientMgr::pause_sigchld_handler_for_test(false);
        first.join();
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        FAIL() << "the first handler did not acquire and pause its process operation";
    }

    std::atomic<bool> second_finished {false};
    std::thread second([&]() {
        CdcClientMgr::invoke_sigchld_handler_for_test();
        second_finished.store(true);
    });
    second.join();
    EXPECT_TRUE(second_finished.load());
    EXPECT_EQ(kill(pid, 0), 0);

    CdcClientMgr::pause_sigchld_handler_for_test(false);
    first.join();
    mgr.stop();
}

TEST_F(CdcClientMgrTest, SigchldDuringRunningInspectionIsHandedBackForReap) {
    sigset_t blocked;
    sigset_t old_mask;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGCHLD);
    ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &blocked, &old_mask), 0);
    Defer restore_mask {[&]() { pthread_sigmask(SIG_SETMASK, &old_mask, nullptr); }};

    // Keep delivery deterministic: the test invokes the production handler only after waitid proves
    // the child is waitable, while the inspecting thread still holds the operation claim.
    struct sigaction old_action {};
    ASSERT_EQ(sigaction(SIGCHLD, nullptr, &old_action), 0);
    struct sigaction default_action {};
    default_action.sa_handler = SIG_DFL;
    sigemptyset(&default_action.sa_mask);
    ASSERT_EQ(sigaction(SIGCHLD, &default_action, nullptr), 0);
    Defer restore_action {[&]() { sigaction(SIGCHLD, &old_action, nullptr); }};

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 10"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);

    CdcClientMgr mgr;
    const uint64_t identity = mgr.set_child_pid_for_test(pid);
    CdcClientMgr::pause_child_inspection_after_running_for_test(true);
    std::atomic<bool> inspector_saw_running {true};
    std::thread inspector(
            [&]() { inspector_saw_running.store(mgr.inspect_child_identity_for_test(identity)); });
    Defer resume_and_join_inspector {[&]() {
        CdcClientMgr::pause_child_inspection_after_running_for_test(false);
        if (inspector.joinable()) {
            inspector.join();
        }
    }};

    for (int i = 0; i < 100 && !CdcClientMgr::child_inspection_paused_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!CdcClientMgr::child_inspection_paused_for_test()) {
        FAIL() << "the inspector did not pause after observing WNOHANG=0";
    }

    ASSERT_EQ(kill(pid, SIGKILL), 0);
    siginfo_t child_info {};
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(waitid(P_PID, pid, &child_info, WEXITED | WNOHANG | WNOWAIT), 0);
        if (child_info.si_pid == pid) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(child_info.si_pid, pid);

    // The handler cannot claim while inspect owns it. It must attach a pending request rather than
    // consume the only notification and return. No later signal and no explicit stop() drive cleanup.
    CdcClientMgr::invoke_sigchld_handler_for_test();
    EXPECT_EQ(mgr.get_child_identity_for_test(), identity);
    CdcClientMgr::pause_child_inspection_after_running_for_test(false);
    inspector.join();

    EXPECT_FALSE(inspector_saw_running.load());
    EXPECT_EQ(mgr.get_child_identity_for_test(), 0);
    errno = 0;
    EXPECT_EQ(waitpid(pid, nullptr, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD) << "the pending-reap handoff did not collect the exited child";
}

// Test start_cdc_client when environment is missing
TEST_F(CdcClientMgrTest, StartCdcClientMissingEnv) {
    unsetenv("JAVA_HOME");

    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("JAVA_HOME") != std::string::npos);
    EXPECT_TRUE(result.has_status());

    // Restore JAVA_HOME
    if (_original_java_home) {
        setenv("JAVA_HOME", _original_java_home, 1);
    }
}

// Test concurrent start attempts
TEST_F(CdcClientMgrTest, ConcurrentStartAttempts) {
    CdcClientMgr mgr;
    std::vector<std::thread> threads;
    std::atomic<int> success_count {0};

    // Initially no child process
    EXPECT_EQ(mgr.get_child_pid(), 0);

    // Launch multiple threads trying to start CDC client
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&mgr, &success_count]() {
            PRequestCdcClientResult result;
            Status status = mgr.start_cdc_client(&result);
            if (status.ok()) {
                success_count++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All attempts should succeed (subsequent ones detect already started)
    EXPECT_EQ(success_count.load(), 5);

    // CDC client should be started exactly once (PID is set)
    EXPECT_GT(mgr.get_child_pid(), 0);
}

// Test send_request with various HTTP methods implicitly
TEST_F(CdcClientMgrTest, SendRequestVariousEndpoints) {
    CdcClientMgr mgr;
    std::string response;

    // Test different API endpoints (reduced from 4 to 2 for speed)
    std::vector<std::string> apis = {"/actuator/health", "/api/submit"};

    for (const auto& api : apis) {
        Status status = mgr.send_request_to_cdc_client(api, "{}", &response);
        // Expected to fail (no real CDC client running) but should not crash
        EXPECT_FALSE(status.ok());
    }
}

// Test that stop is idempotent
TEST_F(CdcClientMgrTest, StopIdempotent) {
    CdcClientMgr mgr;

    // Multiple stops should not cause issues
    mgr.stop();
    mgr.stop();
    mgr.stop();

    // Should still be able to start after multiple stops
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());
}

// Test rapid start/stop cycles
TEST_F(CdcClientMgrTest, RapidStartStopCycles) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(mgr.get_child_pid(), 0); // Should be stopped

        Status status = mgr.start_cdc_client(&result);
        EXPECT_TRUE(status.ok());
        EXPECT_GT(mgr.get_child_pid(), 0); // Should be started

        mgr.stop();
        EXPECT_EQ(mgr.get_child_pid(), 0); // Should be stopped again
    }
}

// Test that multiple request_cdc_client_impl calls work
TEST_F(CdcClientMgrTest, MultipleRequestCalls) {
    CdcClientMgr mgr;

    // Reduced from 5 to 2 iterations for speed
    for (int i = 0; i < 2; ++i) {
        PRequestCdcClientRequest request;
        PRequestCdcClientResult result;
        struct TestClosure : public google::protobuf::Closure {
            void Run() override {}
        };
        TestClosure closure;

        request.set_api("/test");
        request.set_params("{\"index\":" + std::to_string(i) + "}");

        mgr.request_cdc_client_impl(&request, &result, &closure);
        EXPECT_TRUE(result.has_status());
    }
}

// Test behavior when DORIS_HOME has trailing slash
TEST_F(CdcClientMgrTest, DorisHomeWithTrailingSlash) {
    if (_original_doris_home) {
        std::string doris_home_with_slash = std::string(_original_doris_home) + "/";
        setenv("DORIS_HOME", doris_home_with_slash.c_str(), 1);

        CdcClientMgr mgr;
        PRequestCdcClientResult result;
        Status status = mgr.start_cdc_client(&result);

        // Should still work fine
        EXPECT_TRUE(status.ok());

        // Restore original
        setenv("DORIS_HOME", _original_doris_home, 1);
    }
}

// Test send_request_to_cdc_client with very long API path
TEST_F(CdcClientMgrTest, SendRequestLongApiPath) {
    CdcClientMgr mgr;
    std::string response;

    std::string long_api = "/api/v1/very/long/path/to/endpoint/with/many/segments";
    Status status = mgr.send_request_to_cdc_client(long_api, "{\"data\":\"test\"}", &response);

    EXPECT_FALSE(status.ok());
}

// Test concurrent stop calls
TEST_F(CdcClientMgrTest, ConcurrentStopCalls) {
    CdcClientMgr mgr;

    // Start CDC client first
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());

    std::vector<std::thread> threads;
    // Launch multiple threads trying to stop simultaneously
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&mgr]() { mgr.stop(); });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Should be stopped cleanly
    EXPECT_EQ(mgr.get_child_pid(), 0);
}

// Test concurrent requests
TEST_F(CdcClientMgrTest, ConcurrentRequests) {
    CdcClientMgr mgr;
    std::vector<std::thread> threads;
    std::atomic<int> completed {0};

    // Launch multiple threads making requests
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&mgr, &completed, i]() {
            PRequestCdcClientRequest request;
            PRequestCdcClientResult result;

            request.set_api("/test/" + std::to_string(i));
            request.set_params("{\"id\":" + std::to_string(i) + "}");

            struct TestClosure : public google::protobuf::Closure {
                void Run() override {}
            };
            TestClosure closure;

            mgr.request_cdc_client_impl(&request, &result, &closure);
            completed++;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(completed.load(), 3);
}

// Test start after failed start attempt
TEST_F(CdcClientMgrTest, StartAfterFailedStart) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result1;

    // First attempt: remove JAVA_HOME to fail
    unsetenv("JAVA_HOME");
    Status status1 = mgr.start_cdc_client(&result1);
    EXPECT_FALSE(status1.ok());

    // Restore JAVA_HOME
    if (_original_java_home) {
        setenv("JAVA_HOME", _original_java_home, 1);
    }

    // Second attempt: should succeed now
    PRequestCdcClientResult result2;
    Status status2 = mgr.start_cdc_client(&result2);
    EXPECT_TRUE(status2.ok());
}

// Test send_request with special characters in params
TEST_F(CdcClientMgrTest, SendRequestSpecialCharsInParams) {
    CdcClientMgr mgr;
    std::string response;

    std::string special_params = "{\"data\":\"test\\n\\t\\r\\\"\"}";
    Status status = mgr.send_request_to_cdc_client("/test", special_params, &response);

    EXPECT_FALSE(status.ok());
}

// Test request_cdc_client_impl with long params
TEST_F(CdcClientMgrTest, RequestWithLongParams) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api("/test");
    // Create a long params string
    std::string long_params = "{\"data\":\"";
    for (int i = 0; i < 1000; ++i) {
        long_params += "x";
    }
    long_params += "\"}";
    request.set_params(long_params);

    struct TestClosure : public google::protobuf::Closure {
        void Run() override {}
    };
    TestClosure closure;

    mgr.request_cdc_client_impl(&request, &result, &closure);
    EXPECT_TRUE(result.has_status());
}

// Test multiple managers with concurrent operations
TEST_F(CdcClientMgrTest, MultipleManagersConcurrent) {
    std::vector<std::unique_ptr<CdcClientMgr>> managers;
    std::vector<std::thread> threads;

    // Create 3 managers
    for (int i = 0; i < 3; ++i) {
        managers.push_back(std::make_unique<CdcClientMgr>());
    }

    // Each manager tries to start CDC client concurrently
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&managers, i]() {
            PRequestCdcClientResult result;
            Status status = managers[i]->start_cdc_client(&result);
            EXPECT_TRUE(status.ok());
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All should have succeeded
    for (auto& mgr : managers) {
        EXPECT_GT(mgr->get_child_pid(), 0);
    }
}

// Test start_cdc_client with result having pre-existing status
TEST_F(CdcClientMgrTest, StartWithPreExistingResultStatus) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;

    // Pre-populate result with some status
    auto* status_pb = result.mutable_status();
    status_pb->set_status_code(999);
    status_pb->add_error_msgs("pre-existing error");

    Status status = mgr.start_cdc_client(&result);

    // Should succeed
    EXPECT_TRUE(status.ok());
    // Note: start_cdc_client only updates result status on error, not on success
    // So the pre-existing status (999) will remain unchanged
    EXPECT_EQ(result.status().status_code(), 999);
}

// Verify _adopted_external defaults to false and start_cdc_client (BE_TEST
// short-circuit) does not alter it.
TEST_F(CdcClientMgrTest, AdoptedExternalDefaultFalse) {
    CdcClientMgr mgr;
    EXPECT_FALSE(mgr.get_adopted_external_for_test());

    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(mgr.get_adopted_external_for_test());
}

// Verify the _adopted_external flag round-trips through the setter/getter.
TEST_F(CdcClientMgrTest, AdoptedExternalSetterRoundTrip) {
    CdcClientMgr mgr;
    EXPECT_FALSE(mgr.get_adopted_external_for_test());

    mgr.set_adopted_external_for_test(true);
    EXPECT_TRUE(mgr.get_adopted_external_for_test());

    mgr.set_adopted_external_for_test(false);
    EXPECT_FALSE(mgr.get_adopted_external_for_test());
}

// Test send_request_to_cdc_client with empty API
TEST_F(CdcClientMgrTest, SendRequestEmptyApi) {
    CdcClientMgr mgr;
    std::string response;

    Status status = mgr.send_request_to_cdc_client("", "{\"test\":\"data\"}", &response);

    EXPECT_FALSE(status.ok());
}

} // namespace doris
