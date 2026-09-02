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

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "udf/python/python_env.h"
#include "udf/python/python_udaf_client.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udtf_client.h"
#include "util/defer_op.h"

namespace doris {

namespace fs = std::filesystem;
namespace bp = boost::process;

class ActionResultFlightServer final : public arrow::flight::FlightServerBase {
public:
    explicit ActionResultFlightServer(std::vector<std::string> results)
            : _results(std::move(results)) {}

    arrow::Status start() {
        auto location = arrow::flight::Location::ForGrpcTcp("localhost", 0);
        if (!location.ok()) {
            return location.status();
        }
        return Init(arrow::flight::FlightServerOptions(*location));
    }

    ~ActionResultFlightServer() override { static_cast<void>(Shutdown()); }

    arrow::Status DoAction(const arrow::flight::ServerCallContext&, const arrow::flight::Action&,
                           std::unique_ptr<arrow::flight::ResultStream>* result) override {
        std::vector<arrow::flight::Result> flight_results;
        flight_results.reserve(_results.size());
        for (const auto& body : _results) {
            flight_results.emplace_back(arrow::Buffer::FromString(body));
        }
        *result = std::make_unique<arrow::flight::SimpleResultStream>(std::move(flight_results));
        return arrow::Status::OK();
    }

private:
    std::vector<std::string> _results;
};

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

    Status install_real_python_server() {
        fs::path source_root = fs::current_path();
        fs::path server_script;
        while (!source_root.empty()) {
            server_script = source_root / "be/src/udf/python/python_server.py";
            if (fs::exists(server_script) || source_root == source_root.parent_path()) {
                break;
            }
            source_root = source_root.parent_path();
        }
        if (!fs::exists(server_script)) {
            return Status::InternalError("Python server script not found: {}",
                                         server_script.string());
        }

        setenv("DORIS_HOME", test_dir_.c_str(), 1);
        fs::path plugin_dir = fs::path(test_dir_) / "plugins/python_udf";
        fs::create_directories(plugin_dir);
        fs::copy_file(server_script, plugin_dir / "python_server.py",
                      fs::copy_options::overwrite_existing);
        return Status::OK();
    }

    ProcessPtr create_sleep_process() {
        bp::ipstream output_stream;
        std::string sleep_path = fs::exists("/bin/sleep") ? "/bin/sleep" : "/usr/bin/sleep";
        bp::child child(sleep_path, "60", bp::std_out > output_stream, bp::std_err > bp::null);
        return std::make_shared<PythonUDFProcess>(std::move(child), std::move(output_stream));
    }

    std::optional<std::string> find_python_udf_interpreter() {
        std::vector<std::string> candidates;
        if (const char* configured = std::getenv("DORIS_PYTHON_UDF_TEST_PYTHON")) {
            candidates.emplace_back(configured);
        }
        if (const char* path_env = std::getenv("PATH")) {
            std::stringstream paths(path_env);
            std::string path;
            while (std::getline(paths, path, ':')) {
                fs::path python3 = fs::path(path) / "python3";
                if (fs::exists(python3)) {
                    candidates.emplace_back(python3.string());
                }
            }
        }

        for (const auto& candidate : candidates) {
            if (!fs::exists(candidate)) {
                continue;
            }
            bp::child check(candidate, "-c", "import pandas, pyarrow", bp::std_out > bp::null,
                            bp::std_err > bp::null);
            check.wait();
            if (check.exit_code() == 0) {
                return candidate;
            }
        }
        return std::nullopt;
    }

    Status start_python_udf_server(const std::string& python, ProcessPtr* process) {
        bp::ipstream output_stream;
        try {
            bp::child child(python, "-u", get_fight_server_path(), get_base_unix_socket_path(),
                            bp::std_out > output_stream);
            auto candidate =
                    std::make_shared<PythonUDFProcess>(std::move(child), std::move(output_stream));
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (std::chrono::steady_clock::now() < deadline) {
                if (fs::exists(candidate->get_socket_file_path())) {
                    *process = std::move(candidate);
                    return Status::OK();
                }
                if (!candidate->is_alive()) {
                    return Status::InternalError("Python UDF server exited before becoming ready");
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            return Status::InternalError("Timed out waiting for Python UDF server at {}",
                                         candidate->get_socket_file_path());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to start Python UDF server: {}", e.what());
        }
    }

    Status evaluate_int_udf_batch(const PythonUDFClientPtr& client,
                                  const std::vector<int32_t>& inputs,
                                  std::vector<int32_t>* results) {
        arrow::Int32Builder builder;
        RETURN_DORIS_STATUS_IF_ERROR(builder.AppendValues(inputs));
        std::shared_ptr<arrow::Array> input_array;
        RETURN_DORIS_STATUS_IF_ERROR(builder.Finish(&input_array));
        auto input_batch = arrow::RecordBatch::Make(
                arrow::schema({arrow::field("arg0", arrow::int32(), false)}), inputs.size(),
                {input_array});

        std::shared_ptr<arrow::RecordBatch> output_batch;
        Status evaluate_status = client->evaluate(*input_batch, &output_batch);
        RETURN_IF_ERROR(evaluate_status);
        if (!output_batch || output_batch->num_columns() != 1 ||
            output_batch->num_rows() != inputs.size() ||
            output_batch->column(0)->type_id() != arrow::Type::INT32) {
            return Status::InternalError("Unexpected Python UDF output batch");
        }

        auto result_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(0));
        results->clear();
        results->reserve(inputs.size());
        for (int64_t i = 0; i < result_array->length(); ++i) {
            results->push_back(result_array->Value(i));
        }
        return Status::OK();
    }

    Status evaluate_int_module_udf_batch(const PythonUDFMeta& meta, const ProcessPtr& process,
                                         const std::vector<int32_t>& inputs,
                                         std::vector<int32_t>* results) {
        PythonUDFClientPtr client;
        RETURN_IF_ERROR(PythonUDFClient::create(meta, process, &client));
        Status evaluate_status = evaluate_int_udf_batch(client, inputs, results);
        static_cast<void>(client->close());
        return evaluate_status;
    }

    Status evaluate_int_module_udf(const PythonUDFMeta& meta, const ProcessPtr& process,
                                   int32_t input, int32_t* result) {
        std::vector<int32_t> results;
        RETURN_IF_ERROR(evaluate_int_module_udf_batch(meta, process, {input}, &results));
        *result = results[0];
        return Status::OK();
    }

    PythonUDFMeta make_int_module_meta(int64_t id, const fs::path& location,
                                       const std::string& symbol,
                                       PythonClientType client_type = PythonClientType::UDF) {
        PythonUDFMeta meta;
        meta.id = id;
        meta.name = symbol;
        meta.symbol = symbol;
        meta.location = location.string();
        meta.checksum = "test-checksum";
        meta.runtime_version = "test-runtime";
        meta.input_types = {std::make_shared<DataTypeInt32>()};
        meta.return_type = std::make_shared<DataTypeInt32>();
        meta.type = PythonUDFLoadType::MODULE;
        meta.client_type = client_type;
        return meta;
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

TEST_F(PythonServerTest, ClearModuleCacheReloadsModuleOnNextUdfExecution) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path module_dir = fs::path(test_dir_) / "module_cache";
    fs::create_directories(module_dir);
    auto write_module = [&module_dir](int offset) {
        std::ofstream module(module_dir / "cache_reload_udf.py", std::ios::trunc);
        module << "def evaluate(value):\n"
               << "    return value + " << offset << "\n";
    };

    write_module(1);
    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    PythonUDFMeta meta;
    meta.id = 1;
    meta.name = "cache_reload_udf";
    meta.symbol = "cache_reload_udf.evaluate";
    meta.location = module_dir.string();
    meta.checksum = "test-checksum";
    meta.runtime_version = version.full_version;
    meta.input_types = {std::make_shared<DataTypeInt32>()};
    meta.return_type = std::make_shared<DataTypeInt32>();
    meta.type = PythonUDFLoadType::MODULE;
    meta.client_type = PythonClientType::UDF;

    int32_t result = 0;
    Status evaluate_status = evaluate_int_module_udf(meta, process, 10, &result);
    ASSERT_TRUE(evaluate_status.ok()) << evaluate_status.to_string();
    ASSERT_EQ(result, 11);

    // DROP clears the Python module before UserFunctionCache deletes its extracted directory.
    ASSERT_TRUE(mgr.clear_module_cache(meta.location).ok());
    fs::remove_all(module_dir);
    fs::create_directories(module_dir);
    write_module(100);

    evaluate_status = evaluate_int_module_udf(meta, process, 10, &result);
    ASSERT_TRUE(evaluate_status.ok()) << evaluate_status.to_string();
    EXPECT_EQ(result, 110);
    mgr.shutdown();
}

TEST_F(PythonServerTest, ConcurrentModuleImportsIsolateSameNamedDependencies) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path module_a_dir = fs::path(test_dir_) / "module_a";
    fs::path module_b_dir = fs::path(test_dir_) / "module_b";
    fs::create_directories(module_a_dir);
    fs::create_directories(module_b_dir);
    fs::path import_started = fs::path(test_dir_) / "import_started";
    fs::path second_import_started = fs::path(test_dir_) / "second_import_started";
    fs::path second_import_completed = fs::path(test_dir_) / "second_import_completed";
    fs::path allow_import = fs::path(test_dir_) / "allow_import";
    {
        std::ofstream dependency(module_a_dir / "pd.py");
        dependency << "import pathlib\n"
                   << "import time\n"
                   << "OFFSET = 1\n"
                   << "pathlib.Path(r'" << import_started.string() << "').touch()\n"
                   << "allowed = pathlib.Path(r'" << allow_import.string() << "')\n"
                   << "deadline = time.monotonic() + 10\n"
                   << "while not allowed.exists() and time.monotonic() < deadline:\n"
                   << "    time.sleep(0.01)\n";
        std::ofstream module(module_a_dir / "first_udf.py");
        module << "def evaluate(value):\n"
               << "    import pd\n"
               << "    return value + pd.OFFSET\n";
    }
    {
        std::ofstream dependency(module_b_dir / "pd.py");
        dependency << "OFFSET = 100\n";
        std::ofstream module(module_b_dir / "second_udf.py");
        module << "import pathlib\n"
               << "def evaluate(value):\n"
               << "    if value == 0:\n"
               << "        return 0\n"
               << "    pathlib.Path(r'" << second_import_started.string() << "').touch()\n"
               << "    import pd\n"
               << "    pathlib.Path(r'" << second_import_completed.string() << "').touch()\n"
               << "    return value + pd.OFFSET\n";
    }

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    auto first_meta = make_int_module_meta(1, module_a_dir, "first_udf.evaluate");
    auto second_meta = make_int_module_meta(2, module_b_dir, "second_udf.evaluate");

    // Keep B's Flight exchange open after loading its entry module. Sending the
    // second batch then reaches evaluate() directly instead of loading B again.
    PythonUDFClientPtr second_client;
    ASSERT_TRUE(PythonUDFClient::create(second_meta, process, &second_client).ok());
    int32_t warmup_result = -1;
    std::vector<int32_t> warmup_results;
    ASSERT_TRUE(evaluate_int_udf_batch(second_client, {0}, &warmup_results).ok());
    ASSERT_EQ(warmup_results.size(), 1);
    warmup_result = warmup_results[0];
    ASSERT_EQ(warmup_result, 0);

    int32_t first_result = 0;
    auto first_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(first_meta, process, 10, &first_result);
    });
    Defer release_first_import {[&] { std::ofstream(allow_import).close(); }};
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(import_started) && std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(import_started));

    int32_t second_result = 0;
    auto second_status = std::async(std::launch::async, [&] {
        std::vector<int32_t> results;
        RETURN_IF_ERROR(evaluate_int_udf_batch(second_client, {10}, &results));
        second_result = results[0];
        return Status::OK();
    });

    // "pd" used to be treated as a server alias. A live sys.modules lookup
    // would return A's pd.py here instead of waiting to restore B's context.
    marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(second_import_started) &&
           std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(second_import_started));

    // A broken lock-free lookup returns A's live pd module and creates this
    // marker. The isolated path cannot finish the import until A releases it.
    auto completion_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!fs::exists(second_import_completed) &&
           std::chrono::steady_clock::now() < completion_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(fs::exists(second_import_completed));
    std::ofstream(allow_import).close();

    ASSERT_TRUE(first_status.get().ok());
    ASSERT_TRUE(second_status.get().ok());
    EXPECT_EQ(first_result, 11);
    EXPECT_EQ(second_result, 110);
    EXPECT_TRUE(fs::exists(second_import_completed));
    static_cast<void>(second_client->close());
    mgr.shutdown();
}

TEST_F(PythonServerTest, UdafAndUdtfModulesAreIsolatedInTheSameProcess) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    auto write_udaf = [](const fs::path& location, int offset) {
        fs::create_directories(location);
        std::ofstream(location / "shared_udaf_dependency.py") << "OFFSET = " << offset << "\n";
        std::ofstream(location / "udaf_entry.py")
                << "from shared_udaf_dependency import OFFSET\n"
                << "class SumAgg:\n"
                << "    def __init__(self): self.total = 0\n"
                << "    @property\n"
                << "    def aggregate_state(self):\n"
                << "        return {'total': complex(self.total), 'tag': bytearray(b'x')}\n"
                << "    def accumulate(self, value): self.total += value + OFFSET\n"
                << "    def merge(self, other): self.total += int(other['total'].real)\n"
                << "    def finish(self): return self.total\n";
    };
    auto write_udtf = [](const fs::path& location, int offset) {
        fs::create_directories(location);
        std::ofstream(location / "shared_udtf_dependency.py") << "OFFSET = " << offset << "\n";
        std::ofstream(location / "udtf_entry.py") << "from shared_udtf_dependency import OFFSET\n"
                                                  << "def evaluate(value):\n"
                                                  << "    yield value + OFFSET\n";
    };

    fs::path udaf_a_dir = fs::path(test_dir_) / "same_process_udaf_a";
    fs::path udaf_b_dir = fs::path(test_dir_) / "same_process_udaf_b";
    fs::path udtf_a_dir = fs::path(test_dir_) / "same_process_udtf_a";
    fs::path udtf_b_dir = fs::path(test_dir_) / "same_process_udtf_b";
    fs::path blocking_dir = fs::path(test_dir_) / "blocking_module_import";
    fs::path import_started = fs::path(test_dir_) / "blocking_import_started";
    fs::path allow_import = fs::path(test_dir_) / "allow_blocking_import";
    fs::path object_udaf_dir = fs::path(test_dir_) / "object_state_udaf";
    write_udaf(udaf_a_dir, 1);
    write_udaf(udaf_b_dir, 100);
    write_udtf(udtf_a_dir, 2);
    write_udtf(udtf_b_dir, 200);
    fs::create_directories(object_udaf_dir);
    std::ofstream(object_udaf_dir / "object_state_dependency.py")
            << "class AggregateValue:\n"
            << "    def __init__(self, total): self.total = total\n";
    std::ofstream(object_udaf_dir / "object_udaf_entry.py")
            << "from object_state_dependency import AggregateValue\n"
            << "class ObjectStateAgg:\n"
            << "    def __init__(self): self.total = 0\n"
            << "    @property\n"
            << "    def aggregate_state(self): return AggregateValue(self.total)\n"
            << "    def accumulate(self, value): self.total += value\n"
            << "    def merge(self, other): self.total += other.total\n"
            << "    def finish(self): return self.total\n";
    fs::create_directories(blocking_dir);
    std::ofstream(blocking_dir / "blocking_udf.py")
            << "import pathlib\n"
            << "import time\n"
            << "started = pathlib.Path(r'" << import_started.string() << "')\n"
            << "allowed = pathlib.Path(r'" << allow_import.string() << "')\n"
            << "started.touch()\n"
            << "deadline = time.monotonic() + 10\n"
            << "while not allowed.exists() and time.monotonic() < deadline:\n"
            << "    time.sleep(0.01)\n"
            << "def evaluate(value): return value\n";

    ProcessPtr process;
    ASSERT_TRUE(start_python_udf_server(*python, &process).ok());
    ASSERT_NE(process, nullptr);

    arrow::Int32Builder input_builder;
    ASSERT_TRUE(input_builder.Append(10).ok());
    std::shared_ptr<arrow::Array> input_array;
    ASSERT_TRUE(input_builder.Finish(&input_array).ok());
    auto input_schema = arrow::schema({arrow::field("arg0", arrow::int32(), false)});
    auto input_batch = arrow::RecordBatch::Make(input_schema, 1, {input_array});

    auto udaf_schema = arrow::schema({arrow::field("arg0", arrow::int32(), false),
                                      arrow::field("places", arrow::int64()),
                                      arrow::field("binary_data", arrow::binary())});
    PythonUDAFClientPtr udaf_a;
    PythonUDAFClientPtr udaf_b;
    ASSERT_TRUE(PythonUDAFClient::create(make_int_module_meta(1, udaf_a_dir, "udaf_entry.SumAgg",
                                                              PythonClientType::UDAF),
                                         process, udaf_schema, &udaf_a)
                        .ok());
    ASSERT_TRUE(PythonUDAFClient::create(make_int_module_meta(2, udaf_b_dir, "udaf_entry.SumAgg",
                                                              PythonClientType::UDAF),
                                         process, udaf_schema, &udaf_b)
                        .ok());
    ASSERT_TRUE(udaf_a->create(101).ok());
    ASSERT_TRUE(udaf_b->create(102).ok());
    ASSERT_TRUE(udaf_a->accumulate(101, true, *input_batch, 0, 1).ok());
    ASSERT_TRUE(udaf_b->accumulate(102, true, *input_batch, 0, 1).ok());

    auto finalize_int = [](const PythonUDAFClientPtr& client, int64_t place_id) {
        std::shared_ptr<arrow::RecordBatch> output;
        EXPECT_TRUE(client->finalize(place_id, &output).ok());
        EXPECT_NE(output, nullptr);
        if (!output || output->num_columns() != 1 || output->num_rows() != 1) {
            return int32_t {0};
        }
        return std::static_pointer_cast<arrow::Int32Array>(output->column(0))->Value(0);
    };
    EXPECT_EQ(finalize_int(udaf_a, 101), 11);
    EXPECT_EQ(finalize_int(udaf_b, 102), 110);

    // A user-defined aggregate-state object needs its defining module visible
    // for both directions. User serialization code must run exactly once.
    PythonUDAFClientPtr object_udaf;
    ASSERT_TRUE(PythonUDAFClient::create(
                        make_int_module_meta(6, object_udaf_dir, "object_udaf_entry.ObjectStateAgg",
                                             PythonClientType::UDAF),
                        process, udaf_schema, &object_udaf)
                        .ok());
    ASSERT_TRUE(object_udaf->create(103).ok());
    ASSERT_TRUE(object_udaf->create(104).ok());
    ASSERT_TRUE(object_udaf->accumulate(103, true, *input_batch, 0, 1).ok());
    std::shared_ptr<arrow::Buffer> object_state;
    ASSERT_TRUE(object_udaf->serialize(103, &object_state).ok());
    ASSERT_TRUE(object_udaf->merge(104, object_state).ok());
    EXPECT_EQ(finalize_int(object_udaf, 104), 10);

    // A built-in-only aggregate state, including complex and bytearray values,
    // does not need UDF modules restored. SERIALIZE and MERGE must complete
    // while another module import owns the process-wide writer lock.
    auto blocking_meta = make_int_module_meta(5, blocking_dir, "blocking_udf.evaluate");
    int32_t blocking_result = 0;
    auto blocking_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(blocking_meta, process, 10, &blocking_result);
    });
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(import_started) && std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(import_started));

    std::shared_ptr<arrow::Buffer> serialized_state;
    auto serialize_status = std::async(std::launch::async,
                                       [&] { return udaf_a->serialize(101, &serialized_state); });
    auto serialize_wait = serialize_status.wait_for(std::chrono::seconds(5));
    std::optional<Status> serialize_result;
    std::optional<std::future<Status>> merge_status;
    std::optional<std::future_status> merge_wait;
    if (serialize_wait == std::future_status::ready) {
        serialize_result = serialize_status.get();
        if (serialize_result->ok()) {
            merge_status.emplace(std::async(std::launch::async,
                                            [&] { return udaf_a->merge(101, serialized_state); }));
            merge_wait = merge_status->wait_for(std::chrono::seconds(5));
        }
    }
    std::ofstream(allow_import).close();

    EXPECT_EQ(serialize_wait, std::future_status::ready);
    if (serialize_result) {
        EXPECT_TRUE(serialize_result->ok()) << serialize_result->to_string();
    }
    if (merge_wait) {
        EXPECT_EQ(*merge_wait, std::future_status::ready);
    }
    if (merge_status) {
        auto merge_result = merge_status->get();
        EXPECT_TRUE(merge_result.ok()) << merge_result.to_string();
    }
    if (serialize_wait != std::future_status::ready) {
        auto delayed_serialize_result = serialize_status.get();
        ADD_FAILURE() << "primitive UDAF serialization waited for the module import: "
                      << delayed_serialize_result.to_string();
    }
    ASSERT_TRUE(blocking_status.get().ok());
    EXPECT_EQ(blocking_result, 10);
    if (merge_wait && *merge_wait == std::future_status::ready) {
        EXPECT_EQ(finalize_int(udaf_a, 101), 22);
    }

    PythonUDTFClientPtr udtf_a;
    PythonUDTFClientPtr udtf_b;
    ASSERT_TRUE(PythonUDTFClient::create(make_int_module_meta(3, udtf_a_dir, "udtf_entry.evaluate",
                                                              PythonClientType::UDTF),
                                         process, &udtf_a)
                        .ok());
    ASSERT_TRUE(PythonUDTFClient::create(make_int_module_meta(4, udtf_b_dir, "udtf_entry.evaluate",
                                                              PythonClientType::UDTF),
                                         process, &udtf_b)
                        .ok());
    auto evaluate_udtf = [&](const PythonUDTFClientPtr& client) {
        std::shared_ptr<arrow::ListArray> output;
        EXPECT_TRUE(client->evaluate(*input_batch, &output).ok());
        EXPECT_NE(output, nullptr);
        if (!output || output->length() != 1 || output->value_length(0) != 1) {
            return int32_t {0};
        }
        auto values = std::static_pointer_cast<arrow::Int32Array>(output->values());
        return values->Value(output->value_offset(0));
    };
    EXPECT_EQ(evaluate_udtf(udtf_a), 12);
    EXPECT_EQ(evaluate_udtf(udtf_b), 210);

    static_cast<void>(udaf_a->close());
    static_cast<void>(udaf_b->close());
    static_cast<void>(object_udaf->close());
    static_cast<void>(udtf_a->close());
    static_cast<void>(udtf_b->close());
    process->shutdown();
}

TEST_F(PythonServerTest, InlineImportWaitsForModuleImportEnvironment) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path global_module_dir = fs::path(test_dir_) / "global_modules";
    fs::path udf_module_dir = fs::path(test_dir_) / "module_with_shadow";
    fs::create_directories(global_module_dir);
    fs::create_directories(udf_module_dir);
    fs::path module_import_started = fs::path(test_dir_) / "module_import_started";
    fs::path inline_import_started = fs::path(test_dir_) / "inline_import_started";
    {
        std::ofstream dependency(global_module_dir / "shared_inline_dependency.py");
        dependency << "OFFSET = 1\n";
    }
    {
        std::ofstream dependency(udf_module_dir / "shared_inline_dependency.py");
        dependency << "OFFSET = 100\n";
    }
    {
        std::ofstream module(udf_module_dir / "slow_module_udf.py");
        module << "import pathlib\n"
               << "import time\n"
               << "started = pathlib.Path(r'" << module_import_started.string() << "')\n"
               << "inline_started = pathlib.Path(r'" << inline_import_started.string() << "')\n"
               << "started.touch()\n"
               << "deadline = time.monotonic() + 5\n"
               << "while not inline_started.exists() and time.monotonic() < deadline:\n"
               << "    time.sleep(0.01)\n"
               << "if not inline_started.exists():\n"
               << "    raise RuntimeError('inline import did not start')\n"
               << "time.sleep(1)\n"
               << "def evaluate(value):\n"
               << "    return value\n";
    }

    const char* original_python_path = std::getenv("PYTHONPATH");
    std::optional<std::string> saved_python_path;
    if (original_python_path) {
        saved_python_path = original_python_path;
    }
    setenv("PYTHONPATH", global_module_dir.c_str(), 1);

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    if (saved_python_path) {
        setenv("PYTHONPATH", saved_python_path->c_str(), 1);
    } else {
        unsetenv("PYTHONPATH");
    }
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    PythonUDFMeta module_meta;
    module_meta.id = 1;
    module_meta.name = "slow_module_udf";
    module_meta.symbol = "slow_module_udf.evaluate";
    module_meta.location = udf_module_dir.string();
    module_meta.checksum = "test-checksum";
    module_meta.runtime_version = version.full_version;
    module_meta.input_types = {std::make_shared<DataTypeInt32>()};
    module_meta.return_type = std::make_shared<DataTypeInt32>();
    module_meta.type = PythonUDFLoadType::MODULE;
    module_meta.client_type = PythonClientType::UDF;

    PythonUDFMeta inline_meta;
    inline_meta.id = 2;
    inline_meta.name = "inline_import_udf";
    inline_meta.symbol = "evaluate";
    inline_meta.runtime_version = version.full_version;
    inline_meta.inline_code = "open(r'" + inline_import_started.string() +
                              "', 'w').close()\n"
                              "import shared_inline_dependency\n"
                              "def evaluate(value):\n"
                              "    return value + shared_inline_dependency.OFFSET\n";
    inline_meta.input_types = {std::make_shared<DataTypeInt32>()};
    inline_meta.return_type = std::make_shared<DataTypeInt32>();
    inline_meta.type = PythonUDFLoadType::INLINE;
    inline_meta.client_type = PythonClientType::UDF;

    PythonUDFClientPtr inline_client;
    ASSERT_TRUE(PythonUDFClient::create(inline_meta, process, &inline_client).ok());

    int32_t module_result = 0;
    auto module_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(module_meta, process, 10, &module_result);
    });
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(module_import_started) &&
           std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(module_import_started));

    std::vector<int32_t> inline_results;
    auto inline_status = std::async(std::launch::async, [&] {
        return evaluate_int_udf_batch(inline_client, {10}, &inline_results);
    });

    ASSERT_TRUE(module_status.get().ok());
    ASSERT_TRUE(inline_status.get().ok());
    EXPECT_EQ(module_result, 10);
    EXPECT_EQ(inline_results, std::vector<int32_t>({11}));
    static_cast<void>(inline_client->close());
    mgr.shutdown();
}

TEST_F(PythonServerTest, ModuleImportWaitsForInlineImport) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path global_module_dir = fs::path(test_dir_) / "slow_global_modules";
    fs::path udf_module_dir = fs::path(test_dir_) / "module_during_inline_import";
    fs::create_directories(global_module_dir);
    fs::create_directories(udf_module_dir);
    fs::path inline_import_started = fs::path(test_dir_) / "slow_inline_import_started";
    fs::path allow_child_import = fs::path(test_dir_) / "allow_child_import";
    fs::path module_import_started = fs::path(test_dir_) / "waiting_module_import_started";
    {
        std::ofstream dependency(global_module_dir / "nested_global_dependency.py");
        dependency << "OFFSET = 2\n";
    }
    {
        std::ofstream dependency(global_module_dir / "slow_global_dependency.py");
        dependency << "import pathlib\n"
                   << "import threading\n"
                   << "import time\n"
                   << "started = pathlib.Path(r'" << inline_import_started.string() << "')\n"
                   << "release = pathlib.Path(r'" << allow_child_import.string() << "')\n"
                   << "started.touch()\n"
                   << "deadline = time.monotonic() + 5\n"
                   << "while not release.exists() and time.monotonic() < deadline:\n"
                   << "    time.sleep(0.01)\n"
                   << "if not release.exists():\n"
                   << "    raise RuntimeError('timed out waiting to start child import')\n"
                   << "results = []\n"
                   << "def import_from_child():\n"
                   << "    import nested_global_dependency\n"
                   << "    results.append(nested_global_dependency.OFFSET)\n"
                   << "thread = threading.Thread(target=import_from_child)\n"
                   << "thread.start()\n"
                   << "thread.join(5)\n"
                   << "if thread.is_alive():\n"
                   << "    raise RuntimeError('child reader waited behind module writer')\n"
                   << "OFFSET = results[0]\n";
    }
    {
        std::ofstream module(udf_module_dir / "waiting_module_udf.py");
        module << "open(r'" << module_import_started.string() << "', 'w').close()\n"
               << "def evaluate(value):\n"
               << "    return value\n";
    }

    const char* original_python_path = std::getenv("PYTHONPATH");
    std::optional<std::string> saved_python_path;
    if (original_python_path) {
        saved_python_path = original_python_path;
    }
    setenv("PYTHONPATH", global_module_dir.c_str(), 1);

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    if (saved_python_path) {
        setenv("PYTHONPATH", saved_python_path->c_str(), 1);
    } else {
        unsetenv("PYTHONPATH");
    }
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    PythonUDFMeta inline_meta;
    inline_meta.id = 1;
    inline_meta.name = "slow_inline_import_udf";
    inline_meta.symbol = "evaluate";
    inline_meta.runtime_version = version.full_version;
    inline_meta.inline_code =
            "import slow_global_dependency\n"
            "def evaluate(value):\n"
            "    return value + slow_global_dependency.OFFSET\n";
    inline_meta.input_types = {std::make_shared<DataTypeInt32>()};
    inline_meta.return_type = std::make_shared<DataTypeInt32>();
    inline_meta.type = PythonUDFLoadType::INLINE;
    inline_meta.client_type = PythonClientType::UDF;

    PythonUDFMeta module_meta;
    module_meta.id = 2;
    module_meta.name = "waiting_module_udf";
    module_meta.symbol = "waiting_module_udf.evaluate";
    module_meta.location = udf_module_dir.string();
    module_meta.checksum = "test-checksum";
    module_meta.runtime_version = version.full_version;
    module_meta.input_types = {std::make_shared<DataTypeInt32>()};
    module_meta.return_type = std::make_shared<DataTypeInt32>();
    module_meta.type = PythonUDFLoadType::MODULE;
    module_meta.client_type = PythonClientType::UDF;

    PythonUDFClientPtr inline_client;
    ASSERT_TRUE(PythonUDFClient::create(inline_meta, process, &inline_client).ok());
    std::vector<int32_t> inline_results;
    auto inline_status = std::async(std::launch::async, [&] {
        return evaluate_int_udf_batch(inline_client, {10}, &inline_results);
    });
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(inline_import_started) &&
           std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(inline_import_started));

    int32_t module_result = 0;
    auto module_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(module_meta, process, 10, &module_result);
    });
    Defer release_child_import {[&] { std::ofstream(allow_child_import).close(); }};

    EXPECT_EQ(module_status.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);
    EXPECT_FALSE(fs::exists(module_import_started));
    std::ofstream(allow_child_import).close();

    ASSERT_TRUE(inline_status.get().ok());
    ASSERT_TRUE(module_status.get().ok());
    EXPECT_EQ(inline_results, std::vector<int32_t>({12}));
    EXPECT_EQ(module_result, 10);
    static_cast<void>(inline_client->close());
    mgr.shutdown();
}

TEST_F(PythonServerTest, CachedRuntimeImportsDoNotWaitForAnotherModuleImport) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path cached_module_dir = fs::path(test_dir_) / "cached_runtime_import";
    fs::path blocking_module_dir = fs::path(test_dir_) / "blocking_module_import";
    fs::create_directories(cached_module_dir / "cached_import_pkg");
    fs::create_directories(blocking_module_dir);
    fs::path import_started = fs::path(test_dir_) / "blocking_import_started";
    fs::path cached_batch_completed = fs::path(test_dir_) / "cached_batch_completed";
    fs::path watchdog_expired = fs::path(test_dir_) / "watchdog_expired";
    {
        std::ofstream init(cached_module_dir / "cached_import_pkg/__init__.py");
        std::ofstream dependency(cached_module_dir / "cached_import_pkg/cached_dependency.py");
        dependency << "OFFSET = 1\n";
        std::ofstream module(cached_module_dir / "cached_import_pkg/entry.py");
        module << "import importlib\n"
               << "import pathlib\n"
               << "completed = pathlib.Path(r'" << cached_batch_completed.string() << "')\n"
               << "def evaluate(value):\n"
               << "    dependency = importlib.import_module(\n"
               << "        '.cached_dependency', __package__)\n"
               << "    from .cached_dependency import OFFSET\n"
               << "    builtin_dependency = __import__(\n"
               << "        'cached_dependency', globals(), locals(), (), 1)\n"
               << "    if value == -1:\n"
               << "        try:\n"
               << "            __import__(\n"
               << "                'cached_import_pkg.cached_dependency',\n"
               << "                globals(), locals(), (), -1)\n"
               << "        except ValueError:\n"
               << "            pass\n"
               << "        else:\n"
               << "            raise RuntimeError('negative import level was accepted')\n"
               << "    if value == 99:\n"
               << "        completed.touch()\n"
               << "    return (value + dependency.OFFSET + OFFSET\n"
               << "            + builtin_dependency.OFFSET)\n";
    }
    {
        std::ofstream module(blocking_module_dir / "blocking_import_udf.py");
        module << "import pathlib\n"
               << "import time\n"
               << "started = pathlib.Path(r'" << import_started.string() << "')\n"
               << "completed = pathlib.Path(r'" << cached_batch_completed.string() << "')\n"
               << "watchdog = pathlib.Path(r'" << watchdog_expired.string() << "')\n"
               << "deadline = time.monotonic() + 15\n"
               << "started.touch()\n"
               << "while not completed.exists() and time.monotonic() < deadline:\n"
               << "    time.sleep(0.01)\n"
               << "if not completed.exists():\n"
               << "    watchdog.touch()\n"
               << "def evaluate(value):\n"
               << "    return value\n";
    }

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    auto cached_meta =
            make_int_module_meta(1, cached_module_dir, "cached_import_pkg.entry.evaluate");
    auto blocking_meta =
            make_int_module_meta(2, blocking_module_dir, "blocking_import_udf.evaluate");

    PythonUDFClientPtr cached_client;
    ASSERT_TRUE(PythonUDFClient::create(cached_meta, process, &cached_client).ok());
    std::vector<int32_t> warmup_results;
    ASSERT_TRUE(evaluate_int_udf_batch(cached_client, {-1}, &warmup_results).ok());
    ASSERT_EQ(warmup_results, std::vector<int32_t>({2}));

    int32_t blocking_result = 0;
    auto blocking_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(blocking_meta, process, 10, &blocking_result);
    });
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(import_started) && std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(import_started));

    std::vector<int32_t> inputs(100);
    std::iota(inputs.begin(), inputs.end(), 0);
    std::vector<int32_t> results;
    auto cached_status = std::async(std::launch::async, [&] {
        return evaluate_int_udf_batch(cached_client, inputs, &results);
    });

    ASSERT_TRUE(cached_status.get().ok());
    ASSERT_FALSE(fs::exists(watchdog_expired))
            << "cached function-body imports waited for the process-wide import lock";
    ASSERT_TRUE(fs::exists(cached_batch_completed));
    ASSERT_EQ(results.size(), inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
        EXPECT_EQ(results[i], inputs[i] + 3);
    }
    ASSERT_TRUE(blocking_status.get().ok());
    EXPECT_EQ(blocking_result, 10);
    static_cast<void>(cached_client->close());
    mgr.shutdown();
}

TEST_F(PythonServerTest, ModuleUdfChildThreadsUseModuleContext) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path module_dir = fs::path(test_dir_) / "module_thread_context";
    fs::path package_dir = module_dir / "thread_context_pkg";
    fs::path direct_import_completed = fs::path(test_dir_) / "direct_import_completed";
    fs::path runtime_import_completed = fs::path(test_dir_) / "runtime_import_completed";
    fs::path callable_import_completed = fs::path(test_dir_) / "callable_import_completed";
    fs::create_directories(package_dir);
    std::ofstream(package_dir / "__init__.py").close();
    {
        std::ofstream dependency(package_dir / "builtin_dependency.py");
        dependency << "OFFSET = 2\n";
    }
    {
        std::ofstream dependency(package_dir / "importlib_dependency.py");
        dependency << "OFFSET = 3\n";
    }
    {
        std::ofstream dependency(package_dir / "nested_dependency.py");
        dependency << "OFFSET = 5\n";
    }
    {
        std::ofstream dependency(package_dir / "initial_dependency.py");
        dependency << "OFFSET = 7\n";
    }
    {
        std::ofstream dependency(package_dir / "direct_dependency.py");
        dependency << "import pathlib\n"
                   << "pathlib.Path(r'" << direct_import_completed.string() << "').touch()\n"
                   << "OFFSET = 11\n";
    }
    {
        std::ofstream dependency(package_dir / "runtime_dependency.py");
        dependency << "import pathlib\n"
                   << "pathlib.Path(r'" << runtime_import_completed.string() << "').touch()\n"
                   << "OFFSET = 13\n";
    }
    {
        std::ofstream dependency(package_dir / "callable_dependency.py");
        dependency << "import pathlib\n"
                   << "pathlib.Path(r'" << callable_import_completed.string() << "').touch()\n"
                   << "OFFSET = 17\n";
    }
    {
        std::ofstream dependency(package_dir / "parent_dependency.py");
        dependency << "import threading\n"
                   << "results = []\n"
                   << "def import_from_child_during_parent_import():\n"
                   << "    from .nested_dependency import OFFSET\n"
                   << "    results.append(OFFSET)\n"
                   << "thread = threading.Thread(target=import_from_child_during_parent_import)\n"
                   << "thread.start()\n"
                   << "thread.join(5)\n"
                   << "if thread.is_alive():\n"
                   << "    raise RuntimeError('nested child import waited for parent import')\n"
                   << "OFFSET = results[0]\n";
    }
    {
        std::ofstream module(package_dir / "entry.py");
        module << "import importlib\n"
               << "import pathlib\n"
               << "import threading\n"
               << "initial_results = []\n"
               << "def import_during_initialization():\n"
               << "    from .initial_dependency import OFFSET\n"
               << "    initial_results.append(OFFSET)\n"
               << "initial_thread = threading.Thread(target=import_during_initialization)\n"
               << "initial_thread.start()\n"
               << "initial_thread.join(5)\n"
               << "if initial_thread.is_alive():\n"
               << "    raise RuntimeError('child import waited for initial module import')\n"
               << "INITIAL_OFFSET = initial_results[0]\n"
               << "direct_thread = threading.Thread(\n"
               << "    target=importlib.import_module,\n"
               << "    args=('.direct_dependency', __package__))\n"
               << "direct_thread.start()\n"
               << "direct_thread.join(5)\n"
               << "if direct_thread.is_alive():\n"
               << "    raise RuntimeError('direct child import waited for initial module import')\n"
               << "if not pathlib.Path(r'" << direct_import_completed.string() << "').exists():\n"
               << "    raise RuntimeError('direct child import did not run')\n"
               << "from .direct_dependency import OFFSET as DIRECT_OFFSET\n"
               << "def evaluate(value):\n"
               << "    runtime_thread = threading.Thread(\n"
               << "        target=importlib.import_module,\n"
               << "        args=('.runtime_dependency', __package__))\n"
               << "    runtime_thread.start()\n"
               << "    runtime_thread.join(5)\n"
               << "    if runtime_thread.is_alive():\n"
               << "        raise RuntimeError('direct runtime import did not finish')\n"
               << "    if not pathlib.Path(r'" << runtime_import_completed.string()
               << "').exists():\n"
               << "        raise RuntimeError('direct runtime import did not run')\n"
               << "    from .runtime_dependency import OFFSET as RUNTIME_OFFSET\n"
               << "    results = []\n"
               << "    errors = []\n"
               << "    class ImportTarget:\n"
               << "        def __eq__(self, other):\n"
               << "            raise RuntimeError('thread target equality must not run')\n"
               << "        def __call__(self):\n"
               << "            try:\n"
               << "                dependency = importlib.import_module(\n"
               << "                    '.callable_dependency', __package__)\n"
               << "                results.append(dependency.OFFSET)\n"
               << "            except Exception as exc:\n"
               << "                errors.append(str(exc))\n"
               << "    callable_thread = threading.Thread(target=ImportTarget())\n"
               << "    callable_thread.start()\n"
               << "    callable_thread.join(5)\n"
               << "    def import_with_statement():\n"
               << "        try:\n"
               << "            from .builtin_dependency import OFFSET\n"
               << "            results.append(OFFSET)\n"
               << "        except Exception as error:\n"
               << "            errors.append(str(error))\n"
               << "    def import_with_importlib():\n"
               << "        try:\n"
               << "            dependency = importlib.import_module(\n"
               << "                '.importlib_dependency', __package__)\n"
               << "            results.append(dependency.OFFSET)\n"
               << "        except Exception as error:\n"
               << "            errors.append(str(error))\n"
               << "    for target in (import_with_statement, import_with_importlib):\n"
               << "        thread = threading.Thread(target=target)\n"
               << "        thread.start()\n"
               << "        thread.join()\n"
               << "    if errors:\n"
               << "        raise RuntimeError('; '.join(errors))\n"
               << "    from .parent_dependency import OFFSET\n"
               << "    return (value + INITIAL_OFFSET + DIRECT_OFFSET + RUNTIME_OFFSET\n"
               << "            + sum(results) + OFFSET)\n";
    }

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    PythonUDFMeta meta;
    meta.id = 1;
    meta.name = "thread_context_udf";
    meta.symbol = "thread_context_pkg.entry.evaluate";
    meta.location = module_dir.string();
    meta.checksum = "test-checksum";
    meta.runtime_version = version.full_version;
    meta.input_types = {std::make_shared<DataTypeInt32>()};
    meta.return_type = std::make_shared<DataTypeInt32>();
    meta.type = PythonUDFLoadType::MODULE;
    meta.client_type = PythonClientType::UDF;

    int32_t result = 0;
    Status evaluate_status = evaluate_int_module_udf(meta, process, 10, &result);
    ASSERT_TRUE(evaluate_status.ok()) << evaluate_status.to_string();
    EXPECT_EQ(result, 68);
    mgr.shutdown();
}

TEST_F(PythonServerTest, LongLivedWorkerUsesCallingModuleContext) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path global_module_dir = fs::path(test_dir_) / "global_thread_pool";
    fs::path first_module_dir = fs::path(test_dir_) / "thread_pool_udf_a";
    fs::path second_module_dir = fs::path(test_dir_) / "thread_pool_udf_b";
    fs::create_directories(global_module_dir);
    fs::create_directories(first_module_dir / "shared_thread_pkg");
    fs::create_directories(second_module_dir / "shared_thread_pkg");
    {
        std::ofstream pool(global_module_dir / "shared_thread_pool.py");
        pool << "from concurrent.futures import ThreadPoolExecutor\n"
             << "executor = ThreadPoolExecutor(max_workers=1)\n";
    }
    auto write_udf = [](const fs::path& module_dir, int offset) {
        fs::path package_dir = module_dir / "shared_thread_pkg";
        std::ofstream(package_dir / "__init__.py").close();
        {
            std::ofstream dependency(package_dir / "dependency.py");
            dependency << "OFFSET = " << offset << "\n";
        }
        {
            std::ofstream entry(package_dir / "entry.py");
            entry << "import shared_thread_pool\n"
                  << "def import_dependency(value):\n"
                  << "    from .dependency import OFFSET\n"
                  << "    return value + OFFSET\n"
                  << "def evaluate(value):\n"
                  << "    return shared_thread_pool.executor.submit(\n"
                  << "        import_dependency, value).result(5)\n";
        }
    };
    write_udf(first_module_dir, 1);
    write_udf(second_module_dir, 100);

    const char* original_python_path = std::getenv("PYTHONPATH");
    std::optional<std::string> saved_python_path;
    if (original_python_path) {
        saved_python_path = original_python_path;
    }
    setenv("PYTHONPATH", global_module_dir.c_str(), 1);

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    if (saved_python_path) {
        setenv("PYTHONPATH", saved_python_path->c_str(), 1);
    } else {
        unsetenv("PYTHONPATH");
    }
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    int32_t first_result = 0;
    int32_t second_result = 0;
    ASSERT_TRUE(evaluate_int_module_udf(make_int_module_meta(1, first_module_dir,
                                                             "shared_thread_pkg.entry.evaluate"),
                                        process, 10, &first_result)
                        .ok());
    ASSERT_TRUE(evaluate_int_module_udf(make_int_module_meta(2, second_module_dir,
                                                             "shared_thread_pkg.entry.evaluate"),
                                        process, 10, &second_result)
                        .ok());
    EXPECT_EQ(first_result, 11);
    EXPECT_EQ(second_result, 110);
    mgr.shutdown();
}

TEST_F(PythonServerTest, ClearModuleCacheWaitsForModuleImport) {
    auto python = find_python_udf_interpreter();
    if (!python) {
        GTEST_SKIP() << "Python with pandas and pyarrow is required";
    }

    ASSERT_TRUE(install_real_python_server().ok());

    fs::path module_dir = fs::path(test_dir_) / "module_clear_during_import";
    fs::create_directories(module_dir);
    fs::path import_started = fs::path(test_dir_) / "clear_import_started";
    fs::path allow_import = fs::path(test_dir_) / "allow_import";
    auto write_module = [&](int offset, bool wait_for_release) {
        std::ofstream module(module_dir / "clear_during_import_udf.py", std::ios::trunc);
        if (wait_for_release) {
            module << "import pathlib\n"
                   << "import time\n"
                   << "started = pathlib.Path(r'" << import_started.string() << "')\n"
                   << "release = pathlib.Path(r'" << allow_import.string() << "')\n"
                   << "deadline = time.monotonic() + 10\n"
                   << "started.touch()\n"
                   << "while not release.exists() and time.monotonic() < deadline:\n"
                   << "    time.sleep(0.01)\n"
                   << "if not release.exists():\n"
                   << "    raise RuntimeError('timed out waiting to finish import')\n";
        }
        module << "def evaluate(value):\n"
               << "    return value + " << offset << "\n";
    };
    write_module(1, true);

    PythonVersion version("test-runtime", fs::path(*python).parent_path().parent_path().string(),
                          *python);
    PythonServerManager mgr;
    ProcessPtr process;
    Status fork_status = start_python_udf_server(*python, &process);
    ASSERT_TRUE(fork_status.ok()) << fork_status.to_string();
    ASSERT_NE(process, nullptr);
    mgr.set_process_pool_for_test(version, {process});

    PythonUDFMeta meta;
    meta.id = 1;
    meta.name = "clear_during_import_udf";
    meta.symbol = "clear_during_import_udf.evaluate";
    meta.location = module_dir.string();
    meta.checksum = "test-checksum";
    meta.runtime_version = version.full_version;
    meta.input_types = {std::make_shared<DataTypeInt32>()};
    meta.return_type = std::make_shared<DataTypeInt32>();
    meta.type = PythonUDFLoadType::MODULE;
    meta.client_type = PythonClientType::UDF;

    int32_t initial_result = 0;
    auto evaluate_status = std::async(std::launch::async, [&] {
        return evaluate_int_module_udf(meta, process, 10, &initial_result);
    });
    Defer release_import {[&] { std::ofstream(allow_import).close(); }};
    auto marker_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!fs::exists(import_started) && std::chrono::steady_clock::now() < marker_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(import_started));

    auto clear_status =
            std::async(std::launch::async, [&] { return mgr.clear_module_cache(meta.location); });
    EXPECT_EQ(clear_status.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);
    std::ofstream(allow_import).close();

    ASSERT_TRUE(evaluate_status.get().ok());
    ASSERT_EQ(initial_result, 11);
    auto clear_result = clear_status.get();
    ASSERT_TRUE(clear_result.ok()) << clear_result.to_string();

    write_module(100, false);
    int32_t reloaded_result = 0;
    ASSERT_TRUE(evaluate_int_module_udf(meta, process, 10, &reloaded_result).ok());
    EXPECT_EQ(reloaded_result, 110);
    mgr.shutdown();
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

TEST_F(PythonServerTest, BroadcastActionReportsFailedFlightResults) {
    ActionResultFlightServer success_server({R"({"success": true})"});
    ASSERT_TRUE(success_server.start().ok());
    ActionResultFlightServer failed_server(
            {R"({"success": false, "error": "cache clear failed"})"});
    ASSERT_TRUE(failed_server.start().ok());

    PythonServerManager mgr;
    PythonVersion version("3.9.16", test_dir_, test_dir_ + "/bin/python3");
    ProcessPtr success_process = create_sleep_process();
    ASSERT_NE(success_process, nullptr);
    ASSERT_TRUE(success_process->is_alive());
    success_process->set_uri_for_test(success_server.location().ToString());
    ProcessPtr failed_process = create_sleep_process();
    ASSERT_NE(failed_process, nullptr);
    ASSERT_TRUE(failed_process->is_alive());
    failed_process->set_uri_for_test(failed_server.location().ToString());

    mgr.set_process_pool_for_test(version, {success_process, failed_process});
    auto status = mgr.clear_module_cache("/tmp/test_udf");

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("success=1, failed=1"), std::string::npos);
    EXPECT_NE(status.to_string().find("cache clear failed"), std::string::npos);

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
