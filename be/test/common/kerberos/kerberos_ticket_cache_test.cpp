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

#include "common/kerberos/kerberos_ticket_cache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

namespace doris::kerberos {

using testing::_;

// Mock class
class MockKrb5Interface : public Krb5Interface {
public:
    MOCK_METHOD(Status, init_context, (krb5_context*), (override));
    MOCK_METHOD(Status, parse_name, (krb5_context, const char*, krb5_principal*), (override));
    MOCK_METHOD(Status, kt_resolve, (krb5_context, const char*, krb5_keytab*), (override));
    MOCK_METHOD(Status, cc_resolve, (krb5_context, const char*, krb5_ccache*), (override));
    MOCK_METHOD(Status, get_init_creds_opt_alloc, (krb5_context, krb5_get_init_creds_opt**),
                (override));
    MOCK_METHOD(Status, get_init_creds_keytab,
                (krb5_context, krb5_creds*, krb5_principal, krb5_keytab, krb5_deltat, const char*,
                 krb5_get_init_creds_opt*),
                (override));
    MOCK_METHOD(Status, cc_initialize, (krb5_context, krb5_ccache, krb5_principal), (override));
    MOCK_METHOD(Status, cc_store_cred, (krb5_context, krb5_ccache, krb5_creds*), (override));
    MOCK_METHOD(Status, timeofday, (krb5_context, krb5_timestamp*), (override));
    MOCK_METHOD(Status, cc_start_seq_get, (krb5_context, krb5_ccache, krb5_cc_cursor*), (override));
    MOCK_METHOD(Status, cc_next_cred, (krb5_context, krb5_ccache, krb5_cc_cursor*, krb5_creds*),
                (override));
    MOCK_METHOD(void, cc_end_seq_get, (krb5_context, krb5_ccache, krb5_cc_cursor*), (override));

    MOCK_METHOD(void, free_principal, (krb5_context, krb5_principal), (override));
    MOCK_METHOD(void, free_cred_contents, (krb5_context, krb5_creds*), (override));
    MOCK_METHOD(void, get_init_creds_opt_free, (krb5_context, krb5_get_init_creds_opt*),
                (override));
    MOCK_METHOD(void, kt_close, (krb5_context, krb5_keytab), (override));
    MOCK_METHOD(void, cc_close, (krb5_context, krb5_ccache), (override));
    MOCK_METHOD(void, free_context, (krb5_context), (override));
    MOCK_METHOD(const char*, get_error_message, (krb5_context, krb5_error_code), (override));
    MOCK_METHOD(void, free_error_message, (krb5_context, const char*), (override));
    MOCK_METHOD(Status, unparse_name, (krb5_context, krb5_principal, char**), (override));
    MOCK_METHOD(void, free_unparsed_name, (krb5_context, char*), (override));
};

class KerberosTicketCacheTest : public testing::Test {
protected:
    void SetUp() override {
        _mock_krb5 = std::make_unique<testing::StrictMock<MockKrb5Interface>>();
        _mock_krb5_ptr = _mock_krb5.get();

        // Create a temporary directory for testing
        _test_dir = std::filesystem::temp_directory_path() / "kerberos_test";
        std::filesystem::create_directories(_test_dir);

        _config.set_principal_and_keytab("test_principal", "/path/to/keytab");
        _config.set_krb5_conf_path("/etc/krb5.conf");
        _config.set_refresh_interval(2);
        _config.set_min_time_before_refresh(600);

        _cache = std::make_unique<KerberosTicketCache>(_config, _test_dir.string(),
                                                       std::move(_mock_krb5));
        _cache->set_refresh_thread_sleep_time(std::chrono::milliseconds(1));
    }

    void TearDown() override {
        _cache.reset();
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    // Helper function to set up basic expectations for initialization
    void SetupBasicInitExpectations() {
        EXPECT_CALL(*_mock_krb5_ptr, init_context(_)).WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, parse_name(_, _, _)).WillOnce(testing::Return(Status::OK()));
    }

    // Helper function to simulate ticket cache file creation/update
    void SimulateTicketCacheFileUpdate(const std::string& cache_path) {
        std::ofstream cache_file(cache_path);
        cache_file << "mock ticket cache content";
        cache_file.close();
        // Sleep a bit to ensure file timestamps are different
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    void ExpectForLogin(const std::string& cache_path) {
        // Setup expectations for login
        EXPECT_CALL(*_mock_krb5_ptr, kt_resolve(_, _, _)).WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, cc_resolve(_, _, _)).WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_opt_alloc(_, _))
                .WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_keytab(_, _, _, _, _, _, _))
                .WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, cc_initialize(_, _, _))
                .WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_mock_krb5_ptr, cc_store_cred(_, _, _))
                .WillOnce(testing::DoAll(
                        testing::Invoke([this, cache_path](krb5_context, krb5_ccache, krb5_creds*) {
                            SimulateTicketCacheFileUpdate(cache_path);
                            return Status::OK();
                        })));

        // Cleanup calls
        EXPECT_CALL(*_mock_krb5_ptr, free_cred_contents(_, _)).Times(1);
        EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_opt_free(_, _)).Times(1);
        EXPECT_CALL(*_mock_krb5_ptr, kt_close(_, _)).Times(1);
    }

    // Helper function to get file last write time
    std::filesystem::file_time_type GetFileLastWriteTime(const std::string& path) {
        return std::filesystem::last_write_time(path);
    }

    void printFileTime(const std::filesystem::file_time_type& ftime) {
        auto sctp = std::chrono::time_point_cast<std::chrono::system_clock::duration>(
                ftime - std::filesystem::file_time_type::clock::now() +
                std::chrono::system_clock::now());
        std::time_t cftime = std::chrono::system_clock::to_time_t(sctp);
        std::cout << std::put_time(std::localtime(&cftime), "%Y-%m-%d %H:%M:%S") << '\n';
    }

protected:
    KerberosConfig _config;
    std::unique_ptr<KerberosTicketCache> _cache;
    MockKrb5Interface* _mock_krb5_ptr;
    std::unique_ptr<MockKrb5Interface> _mock_krb5;
    std::filesystem::path _test_dir;
};

TEST_F(KerberosTicketCacheTest, Initialize) {
    SetupBasicInitExpectations();
    ASSERT_TRUE(_cache->initialize().ok());

    // Verify that the cache file is created in the test directory
    std::string cache_path = _cache->get_ticket_cache_path();
    ASSERT_TRUE(cache_path.find(_test_dir.string()) == 0);
}

TEST_F(KerberosTicketCacheTest, LoginSuccess) {
    SetupBasicInitExpectations();
    ASSERT_TRUE(_cache->initialize().ok());

    std::string cache_path = _cache->get_ticket_cache_path();
    ExpectForLogin(cache_path);
    ASSERT_TRUE(_cache->login().ok());
    ASSERT_TRUE(std::filesystem::exists(cache_path));
}

TEST_F(KerberosTicketCacheTest, RefreshTickets) {
    SetupBasicInitExpectations();
    ASSERT_TRUE(_cache->initialize().ok());

    std::string cache_path = _cache->get_ticket_cache_path();

    // Create initial ticket cache file
    SimulateTicketCacheFileUpdate(cache_path);
    auto initial_write_time = GetFileLastWriteTime(cache_path);

    // Setup expectations for refresh (login)
    EXPECT_CALL(*_mock_krb5_ptr, kt_resolve(_, _, _)).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*_mock_krb5_ptr, cc_resolve(_, _, _)).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_opt_alloc(_, _))
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_keytab(_, _, _, _, _, _, _))
            .WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*_mock_krb5_ptr, cc_initialize(_, _, _)).WillOnce(testing::Return(Status::OK()));
    EXPECT_CALL(*_mock_krb5_ptr, cc_store_cred(_, _, _))
            .WillOnce(testing::DoAll(
                    testing::Invoke([this, cache_path](krb5_context, krb5_ccache, krb5_creds*) {
                        SimulateTicketCacheFileUpdate(cache_path);
                        return Status::OK();
                    })));

    // Cleanup calls
    // Because we forcbly refresh the ticket, the _need_refresh() is not called,
    // so it only call free_cred_contents once.
    EXPECT_CALL(*_mock_krb5_ptr, free_cred_contents(_, _)).Times(1);
    EXPECT_CALL(*_mock_krb5_ptr, get_init_creds_opt_free(_, _)).Times(1);
    EXPECT_CALL(*_mock_krb5_ptr, kt_close(_, _)).Times(1);

    ASSERT_TRUE(_cache->refresh_tickets().ok());

    // Verify that the cache file has been updated
    auto new_write_time = GetFileLastWriteTime(cache_path);
    ASSERT_GT(new_write_time, initial_write_time);
}

TEST_F(KerberosTicketCacheTest, PeriodicRefresh) {
    SetupBasicInitExpectations();
    ASSERT_TRUE(_cache->initialize().ok());

    std::string cache_path = _cache->get_ticket_cache_path();
    SimulateTicketCacheFileUpdate(cache_path);
    auto initial_write_time = GetFileLastWriteTime(cache_path);
    printFileTime(initial_write_time);

    ExpectForLogin(cache_path);

    // Start periodic refresh
    _cache->start_periodic_refresh();

    // Wait for a short time to allow some refresh attempts
    // Because the refresh interval is 2s, need larger than 2s
    std::this_thread::sleep_for(std::chrono::milliseconds(4000));

    // Stop periodic refresh
    _cache->stop_periodic_refresh();

    // Verify that the test directory still exists
    ASSERT_TRUE(std::filesystem::exists(_test_dir));

    // Verify that the cache file still exists and has been updated
    ASSERT_TRUE(std::filesystem::exists(cache_path));
    auto final_write_time = GetFileLastWriteTime(cache_path);
    printFileTime(final_write_time);
    ASSERT_GT(final_write_time, initial_write_time);
}

} // namespace doris::kerberos
