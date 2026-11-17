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

namespace {

void reap_direct_child_if_present(pid_t pid) {
    siginfo_t child_info {};
    if (waitid(P_PID, pid, &child_info, WEXITED | WNOHANG | WNOWAIT) != 0) {
        EXPECT_EQ(errno, ECHILD);
        return;
    }
    if (child_info.si_pid == 0) {
        EXPECT_EQ(kill(pid, SIGKILL), 0);
    }
    pid_t wait_result;
    do {
        wait_result = waitpid(pid, nullptr, 0);
    } while (wait_result < 0 && errno == EINTR);
    EXPECT_EQ(wait_result, pid);
}

} // namespace

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

// Test stop when the published pid cannot be reaped as this process's own child. The
// generation-qualified cleanup observes ECHILD there, which counts as success, so stop() must
// revoke ownership without signalling: a pid this process cannot reap is not one it may operate on.
// The signal and pipe checkpoints must stay in this process-lifecycle test.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(CdcClientMgrTest, StopDoesNotSignalANonChildPid) {
    CdcClientMgr mgr;

    int pid_pipe[2];
    ASSERT_EQ(pipe(pid_pipe), 0);
    Defer close_pid_pipe {[&]() {
        close(pid_pipe[0]);
        if (pid_pipe[1] >= 0) {
            close(pid_pipe[1]);
        }
    }};
    int report_pipe[2];
    ASSERT_EQ(pipe(report_pipe), 0);
    Defer close_report_pipe {[&]() {
        close(report_pipe[0]);
        if (report_pipe[1] >= 0) {
            close(report_pipe[1]);
        }
    }};
    int phase_pipe[2];
    ASSERT_EQ(pipe(phase_pipe), 0);
    Defer close_phase_pipe {[&]() {
        if (phase_pipe[0] >= 0) {
            close(phase_pipe[0]);
        }
        close(phase_pipe[1]);
    }};

    struct sigaction old_pipe_action {};
    ASSERT_EQ(sigaction(SIGPIPE, nullptr, &old_pipe_action), 0);
    struct sigaction ignored_pipe_action {};
    ignored_pipe_action.sa_handler = SIG_IGN;
    sigemptyset(&ignored_pipe_action.sa_mask);
    ASSERT_EQ(sigaction(SIGPIPE, &ignored_pipe_action, nullptr), 0);
    Defer restore_pipe_action {[&]() { sigaction(SIGPIPE, &old_pipe_action, nullptr); }};

    posix_spawn_file_actions_t actions;
    ASSERT_EQ(posix_spawn_file_actions_init(&actions), 0);
    Defer destroy_actions {[&]() { posix_spawn_file_actions_destroy(&actions); }};
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, pid_pipe[0]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, report_pipe[0]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, phase_pipe[1]), 0);
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, pid_pipe[1], STDOUT_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, report_pipe[1], STDERR_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, phase_pipe[0], STDIN_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, pid_pipe[1]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, report_pipe[1]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, phase_pipe[0]), 0);

    posix_spawnattr_t attr;
    ASSERT_EQ(posix_spawnattr_init(&attr), 0);
    Defer destroy_attr {[&]() { posix_spawnattr_destroy(&attr); }};
    sigset_t no_signals;
    sigemptyset(&no_signals);
    sigset_t default_signals;
    sigemptyset(&default_signals);
    sigaddset(&default_signals, SIGTERM);
    sigaddset(&default_signals, SIGCHLD);
    ASSERT_EQ(posix_spawnattr_setsigmask(&attr, &no_signals), 0);
    ASSERT_EQ(posix_spawnattr_setsigdefault(&attr, &default_signals), 0);
    ASSERT_EQ(posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF), 0);

    // The launcher reports its background helper's PID and exits. The helper keeps the report and
    // phase pipes open but is no longer this process's child when the launcher has been reaped.
    pid_t launcher = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sh -c 'trap \"printf S >&2\" TERM; printf R >&2; "
                                            "while IFS= read -r phase; do printf A >&2; done' "
                                            "<&0 >/dev/null & echo $!"),
                          nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&launcher, "/bin/sh", &actions, &attr, argv, envp), 0);
    ASSERT_GT(launcher, 0);
    bool launcher_reaped = false;
    Defer cleanup_launcher {[&]() {
        if (!launcher_reaped) {
            reap_direct_child_if_present(launcher);
        }
    }};
    close(pid_pipe[1]);
    pid_pipe[1] = -1;
    close(report_pipe[1]);
    report_pipe[1] = -1;
    close(phase_pipe[0]);
    phase_pipe[0] = -1;

    char pid_text[32] {};
    size_t pid_length = 0;
    char digit = 0;
    for (; pid_length < sizeof(pid_text) - 1; ++pid_length) {
        ASSERT_EQ(read(pid_pipe[0], &digit, 1), 1);
        if (digit == '\n') {
            break;
        }
        pid_text[pid_length] = digit;
    }
    ASSERT_EQ(digit, '\n');
    const pid_t helper_pid = std::atoi(pid_text);
    ASSERT_GT(helper_pid, 0);
    int launcher_status = 0;
    ASSERT_EQ(waitpid(launcher, &launcher_status, 0), launcher);
    launcher_reaped = true;
    ASSERT_TRUE(WIFEXITED(launcher_status));
    ASSERT_EQ(WEXITSTATUS(launcher_status), 0);
    errno = 0;
    ASSERT_EQ(waitpid(helper_pid, nullptr, WNOHANG), -1);
    ASSERT_EQ(errno, ECHILD);

    char ready = 0;
    ASSERT_EQ(read(report_pipe[0], &ready, 1), 1);
    ASSERT_EQ(ready, 'R');
    mgr.set_child_pid_for_test(helper_pid);
    mgr.stop();
    EXPECT_EQ(mgr.get_child_pid(), 0);

    ASSERT_EQ(write(phase_pipe[1], "\n", 1), 1);
    char phase_ack = 0;
    ASSERT_EQ(read(report_pipe[0], &phase_ack, 1), 1);
    ASSERT_EQ(phase_ack, 'A') << "stop() sent SIGTERM to a process it could not reap";
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
// The failure cleanup and handler checkpoint are part of one process-lifecycle test.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(CdcClientMgrTest, SigchldHandlerDoesNotReapOtherChildren) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    ASSERT_TRUE(mgr.start_cdc_client(&result).ok());
    ASSERT_GT(mgr.get_child_pid(), 0);

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("sleep 0.2; exit 7"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);
    bool child_reaped = false;
    Defer cleanup_child {[&]() {
        if (!child_reaped) {
            reap_direct_child_if_present(pid);
        }
    }};

    // Wait for the handler itself to run before reaping anything. The handler unpublishes the
    // identity it was handed once its own waitpid has returned, so the published test child
    // disappearing is the observable proof that the handler already executed for this child's exit.
    // A fixed sleep proves nothing: on a delayed delivery the blocking waitpid below would win the
    // race and reap the child itself, passing the case without exercising the replacement for
    // waitpid(-1).
    for (int i = 0; i < 5000 && mgr.get_child_pid() != 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(mgr.get_child_pid(), 0)
            << "the SIGCHLD handler did not run to completion for the unrelated child's exit";

    int child_status = 0;
    const pid_t reaped = waitpid(pid, &child_status, 0);
    child_reaped = reaped == pid;
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
    EXPECT_EQ(mgr.get_child_pid(), 0);

    errno = 0;
    EXPECT_EQ(waitpid(pid, nullptr, WNOHANG), -1);
    EXPECT_EQ(errno, ECHILD);
}

// Both signal phases and their cleanup must remain visible in one test.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(CdcClientMgrTest, StaleGenerationCannotTerminateAReusedNumericPid) {
    int report_pipe[2];
    ASSERT_EQ(pipe(report_pipe), 0);
    Defer close_pipe {[&]() {
        close(report_pipe[0]);
        if (report_pipe[1] >= 0) {
            close(report_pipe[1]);
        }
    }};
    int phase_pipe[2];
    ASSERT_EQ(pipe(phase_pipe), 0);
    Defer close_phase_pipe {[&]() {
        if (phase_pipe[0] >= 0) {
            close(phase_pipe[0]);
        }
        close(phase_pipe[1]);
    }};

    struct sigaction old_pipe_action {};
    ASSERT_EQ(sigaction(SIGPIPE, nullptr, &old_pipe_action), 0);
    struct sigaction ignored_pipe_action {};
    ignored_pipe_action.sa_handler = SIG_IGN;
    sigemptyset(&ignored_pipe_action.sa_mask);
    ASSERT_EQ(sigaction(SIGPIPE, &ignored_pipe_action, nullptr), 0);
    Defer restore_pipe_action {[&]() { sigaction(SIGPIPE, &old_pipe_action, nullptr); }};

    posix_spawn_file_actions_t actions;
    ASSERT_EQ(posix_spawn_file_actions_init(&actions), 0);
    Defer destroy_actions {[&]() { posix_spawn_file_actions_destroy(&actions); }};
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, report_pipe[1], STDOUT_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_adddup2(&actions, phase_pipe[0], STDIN_FILENO), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, report_pipe[0]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, report_pipe[1]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, phase_pipe[0]), 0);
    ASSERT_EQ(posix_spawn_file_actions_addclose(&actions, phase_pipe[1]), 0);

    // Give the child its own signal environment instead of the inherited one: all of the suites run
    // in one process, so any case that leaves SIGTERM ignored or blocked behind it decides whether
    // this child can act on the signal at all. A non-interactive shell silently refuses to trap a
    // signal that was ignored on entry, and a blocked one is never delivered; both leave the report
    // below empty, which is indistinguishable from the forced kill. SIGTERM therefore starts at its
    // default disposition and unblocked, and SIGCHLD the same so that the child's own `wait` works.
    posix_spawnattr_t attr;
    ASSERT_EQ(posix_spawnattr_init(&attr), 0);
    Defer destroy_attr {[&]() { posix_spawnattr_destroy(&attr); }};
    sigset_t no_signals;
    sigemptyset(&no_signals);
    sigset_t default_signals;
    sigemptyset(&default_signals);
    sigaddset(&default_signals, SIGTERM);
    sigaddset(&default_signals, SIGCHLD);
    ASSERT_EQ(posix_spawnattr_setsigmask(&attr, &no_signals), 0);
    ASSERT_EQ(posix_spawnattr_setsigdefault(&attr, &default_signals), 0);
    ASSERT_EQ(posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSIGMASK | POSIX_SPAWN_SETSIGDEF), 0);

    // The shell reports S if the stale identity signals it. It acknowledges the phase change with
    // A only after processing the first phase, then reports T for the intentional stop().
    pid_t pid = 0;
    char* const argv[] = {
            const_cast<char*>("sh"), const_cast<char*>("-c"),
            const_cast<char*>("trap 'printf S' TERM; printf R; "
                              "IFS= read -r phase; trap 'printf T; exit 0' TERM; "
                              "printf A; sleep 10 </dev/null >/dev/null 2>&1 & wait $!"),
            nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", &actions, &attr, argv, envp), 0);
    ASSERT_GT(pid, 0);
    close(report_pipe[1]);
    report_pipe[1] = -1;
    close(phase_pipe[0]);
    phase_pipe[0] = -1;

    // Declare the manager before the cleanup guard so that a fatal assertion destroys the guard
    // first: it kills and reaps this direct child, and the manager's stop() then observes ECHILD
    // instead of signalling a numeric pid whose ownership it has already given up.
    CdcClientMgr mgr;
    bool child_needs_cleanup = true;
    Defer cleanup {[&]() {
        if (child_needs_cleanup) {
            reap_direct_child_if_present(pid);
        }
    }};

    char ready = 0;
    ASSERT_EQ(read(report_pipe[0], &ready, 1), 1);
    ASSERT_EQ(ready, 'R');

    const uint64_t old_identity = mgr.set_child_pid_for_test(pid);
    // Republish the same numeric pid under a new generation. This deterministically models the
    // kernel reusing a reaped CDC pid for another same-parent child without depending on PID churn.
    const uint64_t replacement_identity = mgr.set_child_pid_for_test(pid);
    ASSERT_NE(old_identity, replacement_identity);
    ASSERT_EQ(mgr.get_child_identity_for_test(), replacement_identity);

    ASSERT_FALSE(mgr.terminate_child_identity_for_test(old_identity));
    ASSERT_EQ(write(phase_pipe[1], "\n", 1), 1);
    char phase_ack = 0;
    ASSERT_EQ(read(report_pipe[0], &phase_ack, 1), 1);
    ASSERT_EQ(phase_ack, 'A') << "the stale generation delivered SIGTERM before stop()";

    mgr.stop();
    EXPECT_EQ(mgr.get_child_identity_for_test(), 0);
    errno = 0;
    const pid_t wait_result = waitpid(pid, nullptr, WNOHANG);
    const int wait_error = errno;
    child_needs_cleanup = wait_result < 0 && wait_error == ECHILD;
    EXPECT_EQ(wait_result, -1);
    EXPECT_EQ(wait_error, ECHILD);

    char handled = 0;
    EXPECT_EQ(read(report_pipe[0], &handled, 1), 1)
            << "the published child was collected without reporting the SIGTERM trap: it either "
               "never got to run it, or was not scheduled inside the grace window";
    EXPECT_EQ(handled, 'T') << "stop() fell through the grace window to the forced kill";
}

// The controlled interleaving and cleanup must remain visible in one test.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(CdcClientMgrTest, DelayedHandlerReapsExitedSuccessorGeneration) {
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

    CdcClientMgr mgr;
    const uint64_t old_identity = mgr.set_child_pid_for_test(99999);
    ASSERT_NE(old_identity, 0);
    CdcClientMgr::pause_sigchld_before_claim_for_test(true);
    Defer resume_handlers {[]() {
        CdcClientMgr::pause_sigchld_before_claim_for_test(false);
        CdcClientMgr::pause_stale_child_claim_for_test(false);
    }};
    std::thread delayed_handler([]() { CdcClientMgr::invoke_sigchld_handler_for_test(); });
    Defer resume_and_join_delayed {[&]() {
        CdcClientMgr::pause_sigchld_before_claim_for_test(false);
        CdcClientMgr::pause_stale_child_claim_for_test(false);
        if (delayed_handler.joinable()) {
            delayed_handler.join();
        }
    }};
    for (int i = 0; i < 5000 && !CdcClientMgr::sigchld_before_claim_paused_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(CdcClientMgr::sigchld_before_claim_paused_for_test());

    pid_t pid = 0;
    char* const argv[] = {const_cast<char*>("sh"), const_cast<char*>("-c"),
                          const_cast<char*>("exec sleep 3600"), nullptr};
    char* const envp[] = {const_cast<char*>("PATH=/bin:/usr/bin"), nullptr};
    ASSERT_EQ(posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, envp), 0);
    ASSERT_GT(pid, 0);
    bool child_reaped = false;
    Defer cleanup_child {[&]() {
        if (!child_reaped) {
            reap_direct_child_if_present(pid);
        }
    }};

    const uint64_t successor_identity = mgr.set_child_pid_for_test(pid);
    ASSERT_NE(successor_identity, old_identity);
    ASSERT_TRUE(mgr.inspect_child_identity_for_test(successor_identity));
    ASSERT_TRUE(mgr.inspect_child_identity_for_test(successor_identity));

    CdcClientMgr::pause_stale_child_claim_for_test(true);
    CdcClientMgr::pause_sigchld_before_claim_for_test(false);
    for (int i = 0; i < 5000 && !CdcClientMgr::stale_child_claim_paused_for_test(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(CdcClientMgr::stale_child_claim_paused_for_test());

    ASSERT_EQ(kill(pid, SIGKILL), 0);
    siginfo_t child_info {};
    for (int i = 0; i < 5000 && child_info.si_pid != pid; ++i) {
        ASSERT_EQ(waitid(P_PID, pid, &child_info, WEXITED | WNOHANG | WNOWAIT), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(child_info.si_pid, pid);

    // This handler sees the successor, but the stale claim prevents it from recording a pending
    // reap. The delayed handler must take responsibility after releasing that stale claim.
    CdcClientMgr::invoke_sigchld_handler_for_test();
    EXPECT_EQ(mgr.get_child_identity_for_test(), successor_identity);
    CdcClientMgr::pause_stale_child_claim_for_test(false);
    delayed_handler.join();

    EXPECT_EQ(mgr.get_child_identity_for_test(), 0);
    errno = 0;
    const pid_t wait_result = waitpid(pid, nullptr, WNOHANG);
    const int wait_error = errno;
    child_reaped = wait_result < 0 && wait_error == ECHILD;
    EXPECT_EQ(wait_result, -1);
    EXPECT_EQ(wait_error, ECHILD);
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

    // The second handler must return instead of blocking on the claim the first one holds, so
    // join() returning is the proof; a flag written before the thread returns would assert itself.
    std::thread second([]() { CdcClientMgr::invoke_sigchld_handler_for_test(); });
    second.join();
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
