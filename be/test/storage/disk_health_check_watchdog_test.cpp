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

#include "storage/disk_health_check_watchdog.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <limits>
#include <string>
#include <thread>

#include "common/status.h"

namespace doris {

using namespace std::chrono_literals;

TEST(DiskHealthCheckWatchdogTest, DisabledCheckNeverArms) {
    DiskHealthCheckWatchdog watchdog;
    ASSERT_TRUE(watchdog.start(0ms).ok());
    {
        DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
        EXPECT_EQ(0, watchdog._deadline.load());
        EXPECT_FALSE(watchdog._claim_timeout(0, std::numeric_limits<int64_t>::max()));
    }
    EXPECT_EQ(0, watchdog._deadline.load());
}

TEST(DiskHealthCheckWatchdogTest, ActiveCheckDoesNotExpireBeforeDeadline) {
    DiskHealthCheckWatchdog watchdog;
    // Drive the production state transitions without a polling thread or wall-clock waits.
    watchdog._timeout_ms = 1000;
    DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
    const int64_t deadline = watchdog._deadline.load();
    ASSERT_GT(deadline, 0);
    EXPECT_FALSE(watchdog._claim_timeout(deadline, deadline - 1));
    EXPECT_EQ(deadline, watchdog._deadline.load());
}

TEST(DiskHealthCheckWatchdogTest, CompletionCancelsPreviouslyObservedDeadline) {
    DiskHealthCheckWatchdog watchdog;
    watchdog._timeout_ms = 1000;
    int64_t observed_deadline;
    {
        DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
        observed_deadline = watchdog._deadline.load();
    }
    // The poller observed an active check, but its producer completed before the claim.
    EXPECT_FALSE(watchdog._claim_timeout(observed_deadline, observed_deadline + 1));
    EXPECT_EQ(0, watchdog._deadline.load());
}

TEST(DiskHealthCheckWatchdogTest, StaleObservationCannotExpireNextCheck) {
    DiskHealthCheckWatchdog watchdog;
    watchdog._timeout_ms = 1000;
    int64_t observed_deadline;
    {
        DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
        observed_deadline = watchdog._deadline.load();
    }
    // Give the next check a distinct deadline without depending on clock resolution.
    watchdog._timeout_ms = 2000;
    DiskHealthCheckWatchdog::ScopedCheck next_check(watchdog);
    const int64_t next_deadline = watchdog._deadline.load();
    ASSERT_GT(next_deadline, observed_deadline);
    // Even a late poll using the old observation must not claim the new check.
    EXPECT_FALSE(watchdog._claim_timeout(observed_deadline, next_deadline + 1));
    EXPECT_EQ(next_deadline, watchdog._deadline.load());
    EXPECT_TRUE(watchdog._claim_timeout(next_deadline, next_deadline));
}

TEST(DiskHealthCheckWatchdogTest, CompletionCannotUndoClaimedTimeout) {
    DiskHealthCheckWatchdog watchdog;
    watchdog._timeout_ms = 1000;
    int64_t deadline;
    {
        DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
        deadline = watchdog._deadline.load();
        ASSERT_TRUE(watchdog._claim_timeout(deadline, deadline));
    }
    // The producer returned after the timeout won; its guard must not cancel the exit.
    EXPECT_EQ(DiskHealthCheckWatchdog::kTimedOut, watchdog._deadline.load());
    EXPECT_FALSE(watchdog._claim_timeout(deadline, deadline + 1));
}

TEST(DiskHealthCheckWatchdogTest, RejectsInvalidTimeouts) {
    DiskHealthCheckWatchdog watchdog;
    EXPECT_FALSE(watchdog.start(-1ms).ok());
    EXPECT_FALSE(watchdog.start(std::chrono::milliseconds::max()).ok());
}

TEST(DiskHealthCheckWatchdogTest, IdleAndCompletedChecksDoNotExit) {
    // This normal lifecycle must run in the parent: _exit() in a death-test child
    // bypasses profile flushing and loses coverage of thread start, polling and stop.
    DiskHealthCheckWatchdog watchdog;
    auto status = watchdog.start(1s);
    ASSERT_TRUE(status.ok()) << status;
    for (int i = 0; i < 3; ++i) {
        DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
    }
    // Stay idle beyond the completed checks' deadlines. A stale deadline would
    // terminate the test process instead of allowing stop() to return normally.
    std::this_thread::sleep_for(1250ms);
    watchdog.stop();
    watchdog.stop();
}

#if !defined(THREAD_SANITIZER)

class DiskHealthCheckWatchdogDeathTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_style = ::testing::FLAGS_gtest_death_test_style;
        // Re-exec instead of forking an already multithreaded BE unit-test process.
        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }

    void TearDown() override { ::testing::FLAGS_gtest_death_test_style = _saved_style; }

private:
    std::string _saved_style;
};

TEST_F(DiskHealthCheckWatchdogDeathTest, ExitsWhenCheckHangs) {
    ASSERT_EXIT(
            {
                signal(SIGALRM, SIG_DFL);
                alarm(10);
                // A normal exit() would run this and fail the expected exit-status check.
                if (atexit([] { _exit(75); }) != 0) {
                    _exit(2);
                }
                DiskHealthCheckWatchdog watchdog;
                if (!watchdog.start(100ms).ok()) {
                    _exit(2);
                }
                DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
                while (true) {
                    pause();
                }
            },
            ::testing::ExitedWithCode(74), "");
}

TEST_F(DiskHealthCheckWatchdogDeathTest, NewCheckPreservesClaimedTimeoutExit) {
    ASSERT_EXIT(
            {
                signal(SIGALRM, SIG_DFL);
                alarm(10);
                if (atexit([] { _exit(75); }) != 0) {
                    _exit(2);
                }
                DiskHealthCheckWatchdog watchdog;
                watchdog._timeout_ms = 1000;
                {
                    DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
                    const int64_t deadline = watchdog._deadline.load();
                    if (!watchdog._claim_timeout(deadline, deadline)) {
                        _exit(2);
                    }
                }
                // The poller claimed the timeout but was descheduled before _exit().
                // A producer starting its next check must preserve the timeout exit,
                // without entering a logging or exception path for the terminal state.
                DiskHealthCheckWatchdog::ScopedCheck next_check(watchdog);
                _exit(3);
            },
            ::testing::ExitedWithCode(74), "");
}

TEST_F(DiskHealthCheckWatchdogDeathTest, ExitsWhileJoiningBlockedProducer) {
    ASSERT_EXIT(
            {
                signal(SIGALRM, SIG_DFL);
                alarm(10);
                DiskHealthCheckWatchdog watchdog;
                if (!watchdog.start(100ms).ok()) {
                    _exit(2);
                }
                pthread_t producer;
                int err = pthread_create(
                        &producer, nullptr,
                        [](void* arg) -> void* {
                            auto& instance = *static_cast<DiskHealthCheckWatchdog*>(arg);
                            DiskHealthCheckWatchdog::ScopedCheck check(instance);
                            while (true) {
                                pause();
                            }
                        },
                        &watchdog);
                if (err != 0) {
                    _exit(2);
                }
                // Model shutdown waiting for its producer; supervision must stay alive.
                pthread_join(producer, nullptr);
                _exit(3);
            },
            ::testing::ExitedWithCode(74), "");
}

TEST_F(DiskHealthCheckWatchdogDeathTest, ExitsWithFullBlockingStderrPipe) {
    ASSERT_EXIT(
            {
                signal(SIGALRM, SIG_DFL);
                alarm(10);
                int pipe_fds[2];
                if (pipe(pipe_fds) != 0) {
                    _exit(2);
                }
                int flags = fcntl(pipe_fds[1], F_GETFL);
                if (flags < 0 || fcntl(pipe_fds[1], F_SETFL, flags | O_NONBLOCK) != 0) {
                    _exit(2);
                }
                const char byte = 'x';
                while (true) {
                    // Single-byte writes leave no spare capacity for even a short log line.
                    ssize_t written = write(pipe_fds[1], &byte, 1);
                    if (written > 0 || (written < 0 && errno == EINTR)) {
                        continue;
                    }
                    if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    }
                    _exit(2);
                }
                if (fcntl(pipe_fds[1], F_SETFL, flags) != 0 ||
                    dup2(pipe_fds[1], STDERR_FILENO) < 0) {
                    _exit(2);
                }
                close(pipe_fds[1]);
                // Keep the read end open without consuming it: stderr writes would block.
                DiskHealthCheckWatchdog watchdog;
                if (!watchdog.start(100ms).ok()) {
                    _exit(2);
                }
                DiskHealthCheckWatchdog::ScopedCheck check(watchdog);
                while (true) {
                    pause();
                }
            },
            ::testing::ExitedWithCode(74), "");
}

#endif

} // namespace doris
