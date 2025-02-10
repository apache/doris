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

#include "common/kerberos/kerberos_ticket_mgr.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

namespace doris::kerberos {

// Mock KerberosTicketCache class
class MockKerberosTicketCache : public KerberosTicketCache {
public:
    explicit MockKerberosTicketCache(const KerberosConfig& config, const std::string& root_path)
            : KerberosTicketCache(config, root_path) {}

    MOCK_METHOD(Status, initialize, (), (override));
    MOCK_METHOD(Status, login, (), (override));
    MOCK_METHOD(Status, write_ticket_cache, (), (override));
    MOCK_METHOD(void, start_periodic_refresh, (), (override));
};

class MockKerberosTicketMgr : public KerberosTicketMgr {
public:
    MockKerberosTicketMgr(const std::string& root_path) : KerberosTicketMgr(root_path) {}

    // by calling this, will first set _mock_cache_path in MockKerberosTicketMgr,
    // and _mock_cache_path will pass to KerberosTicketCache to set cache's ticket_cache_path.
    void set_mock_cache_path(const std::string& cache_path) { _mock_cache_path = cache_path; }
    void set_should_succeed(bool val) { _should_succeed = val; }

protected:
    std::shared_ptr<KerberosTicketCache> _make_new_ticket_cache(
            const KerberosConfig& config) override {
        auto mock_cache =
                std::make_shared<testing::StrictMock<MockKerberosTicketCache>>(config, _root_path);

        if (_should_succeed) {
            EXPECT_CALL(*mock_cache, initialize()).WillOnce(testing::Return(Status::OK()));
            EXPECT_CALL(*mock_cache, login()).WillOnce(testing::Return(Status::OK()));
            EXPECT_CALL(*mock_cache, write_ticket_cache()).WillOnce(testing::Return(Status::OK()));
            EXPECT_CALL(*mock_cache, start_periodic_refresh()).Times(1);
            mock_cache->set_ticket_cache_path(_mock_cache_path);
        } else {
            EXPECT_CALL(*mock_cache, initialize())
                    .WillOnce(testing::Return(Status::InternalError("Init failed")));
        }

        return mock_cache;
    }

private:
    std::string _mock_cache_path;
    bool _should_succeed = true;
};

class KerberosTicketMgrTest : public testing::Test {
protected:
    void SetUp() override {
        // Create a temporary directory for testing
        _test_dir = std::filesystem::temp_directory_path() / "kerberos_mgr_test";
        std::filesystem::create_directories(_test_dir);
        _mgr = std::make_unique<MockKerberosTicketMgr>(_test_dir.string());
    }

    void TearDown() override {
        _mgr.reset();
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    // Helper function to create a config
    KerberosConfig create_config(const std::string& principal, const std::string& keytab) {
        KerberosConfig config;
        config.set_principal_and_keytab(principal, keytab);
        config.set_refresh_interval(300);
        config.set_min_time_before_refresh(600);
        return config;
    }

protected:
    std::unique_ptr<MockKerberosTicketMgr> _mgr;
    std::filesystem::path _test_dir;
};

TEST_F(KerberosTicketMgrTest, GetOrSetNewCache) {
    KerberosConfig config = create_config("test_principal", "/path/to/keytab");
    std::string expected_cache_path = (_test_dir / "test_cache").string();
    _mgr->set_should_succeed(true);
    _mgr->set_mock_cache_path(expected_cache_path);

    std::shared_ptr<KerberosTicketCache> ticket_cache;
    doris::Status st = _mgr->get_or_set_ticket_cache(config, &ticket_cache);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(ticket_cache->get_ticket_cache_path(), expected_cache_path);

    // Second call should return the same cache path
    std::shared_ptr<KerberosTicketCache> second_ticket_cache;
    ASSERT_TRUE(_mgr->get_or_set_ticket_cache(config, &second_ticket_cache).ok());
    ASSERT_EQ(second_ticket_cache->get_ticket_cache_path(), expected_cache_path);
}

TEST_F(KerberosTicketMgrTest, GetOrSetCacheFailure) {
    KerberosConfig config = create_config("test_principal", "/path/to/keytab");

    // Test initialization failure
    {
        _mgr->set_should_succeed(false);
        std::shared_ptr<KerberosTicketCache> ticket_cache;
        auto status = _mgr->get_or_set_ticket_cache(config, &ticket_cache);
        ASSERT_FALSE(status.ok());
        ASSERT_TRUE(status.to_string().find("Init failed") != std::string::npos);
    }

    // Verify no cache was added
    ASSERT_TRUE(_mgr->get_ticket_cache(config.get_principal(), config.get_keytab_path()) ==
                nullptr);
}

} // namespace doris::kerberos
