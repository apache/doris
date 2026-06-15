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

#include "service/http/action/be_thread_stack_action.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#endif

#include "common/config.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_client.h"
#include "service/http/http_method.h"

namespace doris {

#ifdef __linux__
namespace {

class ParkedMarkerThread {
public:
    void start() {
        _thread = std::thread([this] { run(); });
        std::unique_lock<std::mutex> lock(_mu);
        _ready_cv.wait(lock, [this] { return _tid.load() != 0; });
    }

    void stop() {
        _stop.store(true);
        if (_thread.joinable()) {
            _thread.join();
        }
    }

    ~ParkedMarkerThread() { stop(); }

    pid_t tid() const { return _tid.load(); }

private:
    __attribute__((noinline)) void run() {
        _tid.store(static_cast<pid_t>(::syscall(SYS_gettid)));
        {
            std::lock_guard<std::mutex> lock(_mu);
            _ready_cv.notify_all();
        }
        spin_until_stopped();
    }

    __attribute__((noinline)) void spin_until_stopped() {
        while (!_stop.load()) {
            std::atomic_signal_fence(std::memory_order_seq_cst);
        }
    }

    std::thread _thread;
    std::atomic<pid_t> _tid {0};
    std::atomic<bool> _stop {false};
    std::mutex _mu;
    std::condition_variable _ready_cv;
};

EvHttpServer* s_server = nullptr;
BeThreadStackAction* s_action = nullptr;
int s_real_port = 0;
std::string s_hostname;

Status do_get(const std::string& path, long* http_status, std::string* body) {
    HttpClient client;
    RETURN_IF_ERROR(client.init(s_hostname + path, /*set_fail_on_error=*/false));
    client.set_method(GET);
    client.set_timeout_ms(5000);
    RETURN_IF_ERROR(client.execute(body));
    *http_status = client.get_http_status();
    return Status::OK();
}

std::string thread_header(pid_t tid) {
    return "----- thread " + std::to_string(tid) + " (";
}

int count_thread_headers(const std::string& body) {
    int count = 0;
    size_t pos = 0;
    while ((pos = body.find("----- thread ", pos)) != std::string::npos) {
        ++count;
        pos += strlen("----- thread ");
    }
    return count;
}

} // namespace

class BeThreadStackActionTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        config::enable_all_http_auth = false;
        s_server = new EvHttpServer(0);
        s_action = new BeThreadStackAction(nullptr);
        s_server->register_handler(GET, "/api/stack_trace", s_action);
        s_server->start();
        s_real_port = s_server->get_real_port();
        ASSERT_NE(0, s_real_port);
        s_hostname = "http://127.0.0.1:" + std::to_string(s_real_port);
    }

    static void TearDownTestSuite() {
        delete s_server;
        s_server = nullptr;
        delete s_action;
        s_action = nullptr;
        config::enable_all_http_auth = false;
    }
};

TEST_F(BeThreadStackActionTest, ThreadIdSelectorSupportsSingleAndMultipleIds) {
    ParkedMarkerThread first;
    ParkedMarkerThread second;
    first.start();
    second.start();

    long http_status = 0;
    std::string body;
    ASSERT_TRUE(do_get("/api/stack_trace?thread_id=" + std::to_string(first.tid()) + "," +
                               std::to_string(second.tid()) + "&mode=disabled",
                       &http_status, &body)
                        .ok());
    ASSERT_EQ(200, http_status);
    EXPECT_THAT(body, testing::HasSubstr("BE thread stack traces\n"));
    EXPECT_THAT(body, testing::HasSubstr("service_signal: "));
    EXPECT_THAT(body, testing::HasSubstr("thread_count: 2\n"));
    EXPECT_THAT(body, testing::HasSubstr("max_signal_threads: unlimited_for_thread_id_filter\n"));
    EXPECT_THAT(body, testing::HasSubstr("dwarf_location_info_mode: disabled\n"));
    EXPECT_THAT(body, testing::HasSubstr("signal_handler_unwinder: frame_pointer_only\n"));
    EXPECT_THAT(body, testing::HasSubstr("capture_method=frame_pointer"));
    EXPECT_THAT(body, testing::HasSubstr(thread_header(first.tid())));
    EXPECT_THAT(body, testing::HasSubstr(thread_header(second.tid())));
    EXPECT_EQ(2, count_thread_headers(body));

    first.stop();
    second.stop();
}

TEST_F(BeThreadStackActionTest, TidAliasRemainsSupported) {
    ParkedMarkerThread marker;
    marker.start();

    long http_status = 0;
    std::string body;
    ASSERT_TRUE(do_get("/api/stack_trace?tid=" + std::to_string(marker.tid()) + "&mode=disabled",
                       &http_status, &body)
                        .ok());
    ASSERT_EQ(200, http_status);
    EXPECT_THAT(body, testing::HasSubstr("thread_count: 1\n"));
    EXPECT_THAT(body, testing::HasSubstr(thread_header(marker.tid())));
    EXPECT_THAT(body, testing::HasSubstr("capture_method=frame_pointer"));
    EXPECT_THAT(body, testing::HasSubstr("fp_status="));
    EXPECT_EQ(1, count_thread_headers(body));

    marker.stop();
}

TEST_F(BeThreadStackActionTest, ExplicitExitedTidIsReported) {
    long http_status = 0;
    std::string body;
    ASSERT_TRUE(
            do_get("/api/stack_trace?thread_id=999999&mode=disabled", &http_status, &body).ok());
    ASSERT_EQ(200, http_status);
    EXPECT_THAT(body, testing::HasSubstr("thread_count: 1\n"));
    EXPECT_THAT(body, testing::HasSubstr("----- thread 999999 (?"));
    EXPECT_THAT(body, testing::HasSubstr("status=thread_exited"));
}

TEST_F(BeThreadStackActionTest, InvalidParamsReturnBadRequest) {
    struct InvalidCase {
        std::string path;
        std::string message;
    };

    const std::vector<InvalidCase> cases = {
            {"/api/stack_trace?thread_id=abc", "invalid thread_id: abc"},
            {"/api/stack_trace?thread_id=-1", "invalid thread_id: -1"},
            {"/api/stack_trace?thread_id=1,,2", "invalid thread_id: empty token"},
            {"/api/stack_trace?thread_id=,1", "invalid thread_id: empty token"},
            {"/api/stack_trace?thread_id=1,", "invalid thread_id: empty token"},
            {"/api/stack_trace?thread_id=0", "invalid thread_id: 0"},
            {"/api/stack_trace?thread_id=2147483648", "invalid thread_id: 2147483648"},
            {"/api/stack_trace?tid=1&thread_id=2", "tid and thread_id are mutually exclusive"},
            {"/api/stack_trace?timeout_ms=0", "invalid timeout_ms: 0"},
            {"/api/stack_trace?timeout_ms=10001", "invalid timeout_ms: 10001"},
            {"/api/stack_trace?max_signal_threads=-1", "invalid max_signal_threads: -1"},
            {"/api/stack_trace?max_signal_threads=1025", "invalid max_signal_threads: 1025"},
            {"/api/stack_trace?mode=unknown", "invalid dwarf_location_info_mode: unknown"},
    };

    for (const auto& c : cases) {
        SCOPED_TRACE(c.path);
        long http_status = 0;
        std::string body;
        ASSERT_TRUE(do_get(c.path, &http_status, &body).ok());
        EXPECT_EQ(400, http_status);
        EXPECT_THAT(body, testing::HasSubstr(c.message));
    }
}

TEST_F(BeThreadStackActionTest, BestEffortSymbolizedFrameObserved) {
    ParkedMarkerThread marker;
    marker.start();

    bool found = false;
    for (int attempt = 0; attempt < 100 && !found; ++attempt) {
        long http_status = 0;
        std::string body;
        ASSERT_TRUE(do_get("/api/stack_trace?thread_id=" + std::to_string(marker.tid()) +
                                   "&mode=FAST&timeout_ms=1000",
                           &http_status, &body)
                            .ok());
        ASSERT_EQ(200, http_status);
        ASSERT_THAT(body, testing::HasSubstr(thread_header(marker.tid())));
        if (body.find("ParkedMarkerThread") != std::string::npos ||
            body.find("spin_until_stopped") != std::string::npos ||
            body.find("be_thread_stack_action_test") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "no symbolized marker frame observed in 100 attempts";

    marker.stop();
}

#else

TEST(BeThreadStackActionTest, LinuxOnlyPlaceholder) {
    GTEST_SKIP() << "BE stack trace HTTP action is Linux-only";
}

#endif

} // namespace doris
