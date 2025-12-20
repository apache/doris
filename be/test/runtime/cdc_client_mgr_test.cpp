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
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

#include "common/config.h"
#include "common/status.h"

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
};

// Test constructor and destructor
TEST_F(CdcClientMgrTest, ConstructorDestructor) {
    CdcClientMgr* mgr = new CdcClientMgr();
    EXPECT_NE(mgr, nullptr);
    delete mgr;
}

// Test stop method when there's no child process
TEST_F(CdcClientMgrTest, StopWithoutChild) {
    CdcClientMgr mgr;
    // Should not crash
    mgr.stop();
}

// Test stop when child process is already dead (covers lines 124-127)
TEST_F(CdcClientMgrTest, StopWhenProcessDead) {
    CdcClientMgr mgr;

    // Start CDC client (sets PID to 99999 in BE_TEST mode)
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0);

    // Stop - since PID 99999 doesn't exist, kill(99999, 0) will fail
    // This should trigger the "Process already dead" branch (lines 124-127)
    mgr.stop();

    // PID should be reset to 0
    EXPECT_EQ(mgr.get_child_pid(), 0);
}

// Test start_cdc_client with missing DORIS_HOME
TEST_F(CdcClientMgrTest, StartCdcClientMissingDorisHome) {
    unsetenv("DORIS_HOME");

    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("DORIS_HOME") != std::string::npos);
    EXPECT_TRUE(result.has_status());
    EXPECT_FALSE(result.status().status_code() == 0);

    // Restore DORIS_HOME
    if (_original_doris_home) {
        setenv("DORIS_HOME", _original_doris_home, 1);
    }
}

// Test start_cdc_client with missing LOG_DIR
TEST_F(CdcClientMgrTest, StartCdcClientMissingLogDir) {
    unsetenv("LOG_DIR");

    CdcClientMgr mgr;
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("LOG_DIR") != std::string::npos);
    EXPECT_TRUE(result.has_status());
    EXPECT_FALSE(result.status().status_code() == 0);

    setenv("LOG_DIR", _log_dir.c_str(), 1);
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
    EXPECT_TRUE(status.to_string().find("java home") != std::string::npos);
    EXPECT_TRUE(result.has_status());
}

// Test start_cdc_client in BE_TEST mode (should succeed)
TEST_F(CdcClientMgrTest, StartCdcClientBeTest) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;

    // Initially no child process
    EXPECT_EQ(mgr.get_child_pid(), 0);

    Status status = mgr.start_cdc_client(&result);

    // In BE_TEST mode, it should succeed by setting a dummy PID
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0); // PID should be set
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

// Test stop then start
TEST_F(CdcClientMgrTest, StopThenStart) {
    CdcClientMgr mgr;

    // Stop (no process running)
    mgr.stop();

    // Then try to start
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);

    EXPECT_TRUE(status.ok());
}

// Test stop immediately after start (covers stop logic with existing PID)
TEST_F(CdcClientMgrTest, StopAfterStart) {
    CdcClientMgr mgr;

    // Start CDC client
    PRequestCdcClientResult result;
    Status status = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0);

    // Immediately stop (will test process dead branch since PID 99999 doesn't exist)
    mgr.stop();
    EXPECT_EQ(mgr.get_child_pid(), 0);

    // Should be able to start again
    Status status2 = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status2.ok());
    EXPECT_GT(mgr.get_child_pid(), 0);
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

// Test send_request_to_cdc_client
TEST_F(CdcClientMgrTest, SendRequestToCdcClient) {
    CdcClientMgr mgr;
    std::string response;

    // This will fail because CDC client is not really running
    // But it will test the code path
    Status status = mgr.send_request_to_cdc_client("/test/api", "{\"key\":\"value\"}", &response);

    // Expected to fail but should not crash
    EXPECT_FALSE(status.ok());
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

// Test request_cdc_client_impl
TEST_F(CdcClientMgrTest, RequestCdcClientImpl) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api("/test/api");
    request.set_params("{\"test\":\"data\"}");

    // Create a simple closure for testing
    struct TestClosure : public google::protobuf::Closure {
        void Run() override { called = true; }
        bool called = false;
    };
    TestClosure closure;

    mgr.request_cdc_client_impl(&request, &result, &closure);

    // Check that status is set in result
    EXPECT_TRUE(result.has_status());
    // Closure should be called
    EXPECT_TRUE(closure.called);
}

// Test request_cdc_client_impl when start fails (covers lines 288-291)
TEST_F(CdcClientMgrTest, RequestCdcClientImplStartFailed) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api("/test/api");
    request.set_params("{}");

    // Remove DORIS_HOME to make start_cdc_client fail
    unsetenv("DORIS_HOME");

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

    // Restore DORIS_HOME
    if (_original_doris_home) {
        setenv("DORIS_HOME", _original_doris_home, 1);
    }
}

// Test start_cdc_client with nullptr result
TEST_F(CdcClientMgrTest, StartCdcClientNullResult) {
    CdcClientMgr mgr;

    EXPECT_EQ(mgr.get_child_pid(), 0);

    Status status = mgr.start_cdc_client(nullptr);

    // Should handle nullptr result gracefully
    EXPECT_TRUE(status.ok());
    EXPECT_GT(mgr.get_child_pid(), 0); // PID should be set
}

// Test start_cdc_client when environment is missing (nullptr result)
TEST_F(CdcClientMgrTest, StartCdcClientMissingEnvNullResult) {
    unsetenv("DORIS_HOME");

    CdcClientMgr mgr;
    Status status = mgr.start_cdc_client(nullptr);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("DORIS_HOME") != std::string::npos);

    // Restore DORIS_HOME
    if (_original_doris_home) {
        setenv("DORIS_HOME", _original_doris_home, 1);
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

// Test stop and restart cycle
TEST_F(CdcClientMgrTest, StopRestartCycle) {
    CdcClientMgr mgr;
    PRequestCdcClientResult result;

    // Start
    Status status1 = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status1.ok());

    // Stop
    mgr.stop();

    // Restart
    Status status2 = mgr.start_cdc_client(&result);
    EXPECT_TRUE(status2.ok());

    // Stop again
    mgr.stop();
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

// Test multiple managers simultaneously
TEST_F(CdcClientMgrTest, MultipleManagers) {
    CdcClientMgr mgr1;
    CdcClientMgr mgr2;
    CdcClientMgr mgr3;

    PRequestCdcClientResult result1, result2, result3;

    // All should be able to detect the shared CDC client (or all succeed in BE_TEST)
    Status status1 = mgr1.start_cdc_client(&result1);
    Status status2 = mgr2.start_cdc_client(&result2);
    Status status3 = mgr3.start_cdc_client(&result3);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_TRUE(status3.ok());
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

// Test with different result object each time
TEST_F(CdcClientMgrTest, StartWithDifferentResults) {
    CdcClientMgr mgr;

    {
        PRequestCdcClientResult result1;
        Status status1 = mgr.start_cdc_client(&result1);
        EXPECT_TRUE(status1.ok());
    }

    {
        PRequestCdcClientResult result2;
        Status status2 = mgr.start_cdc_client(&result2);
        EXPECT_TRUE(status2.ok());
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

// Test behavior when LOG_DIR has trailing slash
TEST_F(CdcClientMgrTest, LogDirWithTrailingSlash) {
    const char* log_dir = std::getenv("LOG_DIR");
    if (log_dir) {
        std::string log_dir_with_slash = std::string(log_dir) + "/";
        setenv("LOG_DIR", log_dir_with_slash.c_str(), 1);

        CdcClientMgr mgr;
        PRequestCdcClientResult result;
        Status status = mgr.start_cdc_client(&result);

        // Should still work fine
        EXPECT_TRUE(status.ok());

        // Restore original
        setenv("LOG_DIR", log_dir, 1);
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

// Test send_request_to_cdc_client with nullptr response
TEST_F(CdcClientMgrTest, SendRequestNullResponse) {
    CdcClientMgr mgr;

    // Should handle nullptr response gracefully (though it will likely crash in real scenario)
    // This tests the code path through send_request_to_cdc_client
    Status status = mgr.send_request_to_cdc_client("/test", "{}", nullptr);

    // Expected to fail because no CDC client is running
    EXPECT_FALSE(status.ok());
}

// Test send_request_to_cdc_client with very long API path
TEST_F(CdcClientMgrTest, SendRequestLongApiPath) {
    CdcClientMgr mgr;
    std::string response;

    std::string long_api = "/api/v1/very/long/path/to/endpoint/with/many/segments";
    Status status = mgr.send_request_to_cdc_client(long_api, "{\"data\":\"test\"}", &response);

    EXPECT_FALSE(status.ok());
}

// Test request_cdc_client_impl with empty API string
TEST_F(CdcClientMgrTest, RequestCdcClientImplEmptyApiStr) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api(""); // Empty API
    request.set_params("{}");

    struct TestClosure : public google::protobuf::Closure {
        void Run() override { called = true; }
        bool called = false;
    };
    TestClosure closure;

    mgr.request_cdc_client_impl(&request, &result, &closure);

    EXPECT_TRUE(result.has_status());
    EXPECT_TRUE(closure.called);
}

// Test request_cdc_client_impl with no params set
TEST_F(CdcClientMgrTest, RequestCdcClientImplNoParams) {
    CdcClientMgr mgr;
    PRequestCdcClientRequest request;
    PRequestCdcClientResult result;

    request.set_api("/test");
    // Don't set params at all

    struct TestClosure : public google::protobuf::Closure {
        void Run() override { called = true; }
        bool called = false;
    };
    TestClosure closure;

    mgr.request_cdc_client_impl(&request, &result, &closure);

    EXPECT_TRUE(result.has_status());
    EXPECT_TRUE(closure.called);
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

    // First attempt: remove DORIS_HOME to fail
    unsetenv("DORIS_HOME");
    Status status1 = mgr.start_cdc_client(&result1);
    EXPECT_FALSE(status1.ok());

    // Restore DORIS_HOME
    if (_original_doris_home) {
        setenv("DORIS_HOME", _original_doris_home, 1);
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

    // Should succeed and update the status
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result.status().status_code(), 0); // Should be OK now
}

// Test send_request_to_cdc_client with empty API
TEST_F(CdcClientMgrTest, SendRequestEmptyApi) {
    CdcClientMgr mgr;
    std::string response;

    Status status = mgr.send_request_to_cdc_client("", "{\"test\":\"data\"}", &response);

    EXPECT_FALSE(status.ok());
}

// Test destructor during active operation (simulated)
TEST_F(CdcClientMgrTest, DestructorDuringOperation) {
    CdcClientMgr* mgr = new CdcClientMgr();
    PRequestCdcClientResult result;

    // Start CDC client
    [[maybe_unused]] Status status = mgr->start_cdc_client(&result);

    // Immediately destroy (tests destructor calling stop)
    delete mgr;

    SUCCEED();
}

// Test alternating start and stop
TEST_F(CdcClientMgrTest, AlternatingStartStop) {
    CdcClientMgr mgr;

    for (int i = 0; i < 3; ++i) {
        PRequestCdcClientResult result;

        // Start
        Status start_status = mgr.start_cdc_client(&result);
        EXPECT_TRUE(start_status.ok());
        EXPECT_GT(mgr.get_child_pid(), 0);

        // Stop
        mgr.stop();
        EXPECT_EQ(mgr.get_child_pid(), 0);
    }
}

} // namespace doris
