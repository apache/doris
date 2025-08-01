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

#include "io/fs/hdfs/hdfs_mgr.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/kerberos/kerberos_ticket_cache.h"
#include "common/kerberos/kerberos_ticket_mgr.h"
#include "common/status.h"
#include "io/fs/hdfs.h"

namespace doris::io {

using testing::_;
using testing::Return;
using testing::NiceMock;
using testing::DoAll;
using testing::SetArgPointee;

// Mock KerberosTicketCache class to track its lifecycle
class MockKerberosTicketCache : public kerberos::KerberosTicketCache {
public:
    MockKerberosTicketCache(const kerberos::KerberosConfig& config, const std::string& root_path)
            : KerberosTicketCache(config, root_path) {
        ++instance_count;
    }

    ~MockKerberosTicketCache() override { --instance_count; }

    static int get_instance_count() { return instance_count; }
    static void reset_instance_count() { instance_count = 0; }

    // Mock methods that would normally interact with the real Kerberos system
    Status initialize() override { return Status::OK(); }
    Status login() override { return Status::OK(); }
    Status write_ticket_cache() override { return Status::OK(); }

private:
    static int instance_count;
};

int MockKerberosTicketCache::instance_count = 0;

// Mock HdfsMgr class to override the actual HDFS operations
class MockHdfsMgr : public HdfsMgr {
public:
    MockHdfsMgr() : HdfsMgr() {}

    MOCK_METHOD(Status, _create_hdfs_fs_impl,
                (const THdfsParams& hdfs_params, const std::string& fs_name,
                 std::shared_ptr<HdfsHandler>* fs_handler),
                (override));
};

// Mock KerberosTicketMgr class to use MockKerberosTicketCache
class MockKerberosTicketMgr : public kerberos::KerberosTicketMgr {
public:
    explicit MockKerberosTicketMgr(const std::string& root_path) : KerberosTicketMgr(root_path) {}

protected:
    std::shared_ptr<kerberos::KerberosTicketCache> _make_new_ticket_cache(
            const kerberos::KerberosConfig& config) override {
        return std::make_shared<MockKerberosTicketCache>(config, _root_path);
    }
};

class HdfsMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _hdfs_mgr = std::make_unique<NiceMock<MockHdfsMgr>>();

        // Set shorter timeout for testing
        _hdfs_mgr->set_instance_timeout_seconds(2);
        _hdfs_mgr->set_cleanup_interval_seconds(1);

        // Reset MockKerberosTicketCache instance count
        MockKerberosTicketCache::reset_instance_count();

        // Setup default mock behavior
        ON_CALL(*_hdfs_mgr, _create_hdfs_fs_impl(_, _, _))
                .WillByDefault([](const THdfsParams& params, const std::string& fs_name,
                                  std::shared_ptr<HdfsHandler>* fs_handler) {
                    *fs_handler = std::make_shared<HdfsHandler>(nullptr, false, "", "", fs_name);
                    return Status::OK();
                });
    }

    void TearDown() override { _hdfs_mgr.reset(); }

    THdfsParams create_test_params(const std::string& user = "", const std::string& principal = "",
                                   const std::string& keytab = "") {
        THdfsParams params;
        if (!user.empty()) {
            params.__set_user(user);
        }
        if (!principal.empty()) {
            params.__set_hdfs_kerberos_principal(principal);
        }
        if (!keytab.empty()) {
            params.__set_hdfs_kerberos_keytab(keytab);
        }
        return params;
    }

protected:
    std::unique_ptr<MockHdfsMgr> _hdfs_mgr;
};

// Test get_or_create_fs with successful creation
TEST_F(HdfsMgrTest, GetOrCreateFsSuccess) {
    THdfsParams params = create_test_params("test_user");
    std::shared_ptr<HdfsHandler> handler;

    // First call should create new handler
    ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler).ok());
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 1);
    ASSERT_TRUE(handler != nullptr);

    // Second call with same params should reuse handler
    std::shared_ptr<HdfsHandler> handler2;
    ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler2).ok());
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 1);
    ASSERT_EQ(handler, handler2);
}

// Test get_or_create_fs with creation failure
TEST_F(HdfsMgrTest, GetOrCreateFsFailure) {
    THdfsParams params = create_test_params("test_user");
    std::shared_ptr<HdfsHandler> handler;

    // Setup mock to return error
    ON_CALL(*_hdfs_mgr, _create_hdfs_fs_impl(_, _, _))
            .WillByDefault(Return(Status::InternalError("Mock error")));

    // Call should fail
    ASSERT_FALSE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler).ok());
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 0);
    ASSERT_TRUE(handler == nullptr);
}

// Test _hdfs_hash_code with different inputs
TEST_F(HdfsMgrTest, HdfsHashCode) {
    // Same inputs should produce same hash
    THdfsParams params1 = create_test_params("user1", "principal1", "keytab1");
    uint64_t hash1 = _hdfs_mgr->_hdfs_hash_code(params1, "fs1");
    uint64_t hash2 = _hdfs_mgr->_hdfs_hash_code(params1, "fs1");
    ASSERT_EQ(hash1, hash2);

    // Different inputs should produce different hashes
    THdfsParams params2 = create_test_params("user2", "principal1", "keytab1");
    THdfsParams params3 = create_test_params("user1", "principal2", "keytab1");
    THdfsParams params4 = create_test_params("user1", "principal1", "keytab2");

    uint64_t hash3 = _hdfs_mgr->_hdfs_hash_code(params2, "fs1");
    uint64_t hash4 = _hdfs_mgr->_hdfs_hash_code(params3, "fs1");
    uint64_t hash5 = _hdfs_mgr->_hdfs_hash_code(params4, "fs1");
    uint64_t hash6 = _hdfs_mgr->_hdfs_hash_code(params1, "fs2");

    ASSERT_NE(hash1, hash3);
    ASSERT_NE(hash1, hash4);
    ASSERT_NE(hash1, hash5);
    ASSERT_NE(hash1, hash6);
}

// Test cleanup of expired handlers
TEST_F(HdfsMgrTest, CleanupExpiredHandlers) {
    THdfsParams params = create_test_params("test_user");
    std::shared_ptr<HdfsHandler> handler;

    // Create handler
    ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler).ok());
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 1);

    // Wait for cleanup
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // Handler should be removed
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 0);

    // Creating new handler should work
    ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler).ok());
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 1);
}

// Test concurrent access to get_or_create_fs
TEST_F(HdfsMgrTest, ConcurrentAccess) {
    const int NUM_THREADS = 10;
    std::vector<std::thread> threads;
    std::vector<std::shared_ptr<HdfsHandler>> handlers(NUM_THREADS);

    THdfsParams params = create_test_params("test_user");

    // Create multiple threads accessing get_or_create_fs simultaneously
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([this, &params, &handlers, i]() {
            ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handlers[i]).ok());
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads got the same handler
    ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 1);
    for (int i = 1; i < NUM_THREADS; i++) {
        ASSERT_EQ(handlers[0], handlers[i]);
    }
}

// Test sharing of KerberosTicketCache between handlers
// TEST_F(HdfsMgrTest, SharedKerberosTicketCache) {
//     // Create handlers with same Kerberos credentials
//     THdfsParams params = create_test_params("user1", "principal1", "keytab1");
//     std::shared_ptr<HdfsHandler> handler1;
//     std::shared_ptr<HdfsHandler> handler2;
//
//     // Create a shared ticket cache that will be used by both handlers
//     auto shared_ticket_mgr = std::make_shared<NiceMock<MockKerberosTicketMgr>>("/tmp/kerberos");
//     // Set cleanup interval to 1 second for testing
//     shared_ticket_mgr->set_cleanup_interval(std::chrono::seconds(1));
//
//     // Setup mock to create handlers with Kerberos
//     ON_CALL(*_hdfs_mgr, _create_hdfs_fs_impl(_, _, _))
//             .WillByDefault([shared_ticket_mgr](const THdfsParams& params,
//                                                const std::string& fs_name,
//                                                std::shared_ptr<HdfsHandler>* fs_handler) {
//                 kerberos::KerberosConfig config;
//                 config.set_principal_and_keytab(params.hdfs_kerberos_principal,
//                                                 params.hdfs_kerberos_keytab);
//                 std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache;
//                 RETURN_IF_ERROR(shared_ticket_mgr->get_or_set_ticket_cache(config, &ticket_cache));
//                 *fs_handler = std::make_shared<HdfsHandler>(
//                         nullptr, true, params.hdfs_kerberos_principal, params.hdfs_kerberos_keytab,
//                         fs_name, ticket_cache);
//                 return Status::OK();
//             });
//
//     // Create first handler
//     ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs1", &handler1).ok());
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 1);
//
//     // Create second handler with same credentials
//     ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs2", &handler2).ok());
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 1);
//
//     // Verify both handlers share the same ticket cache
//     ASSERT_EQ(handler1->ticket_cache, handler2->ticket_cache);
//     // ASSERT_EQ(handler1->ticket_cache, shared_ticket_cache);
// }
//
// // Test cleanup of KerberosTicketCache when handlers are destroyed
// TEST_F(HdfsMgrTest, KerberosTicketCacheCleanup) {
//     THdfsParams params = create_test_params("user1", "principal1", "keytab1");
//
//     // Create a ticket manager that will be used by the handler
//     auto ticket_mgr = std::make_shared<NiceMock<MockKerberosTicketMgr>>("/tmp/kerberos");
//     // Set cleanup interval to 1 second for testing
//     ticket_mgr->set_cleanup_interval(std::chrono::seconds(1));
//
//     // Setup mock to create handler with Kerberos
//     ON_CALL(*_hdfs_mgr, _create_hdfs_fs_impl(_, _, _))
//             .WillByDefault([ticket_mgr](const THdfsParams& params, const std::string& fs_name,
//                                         std::shared_ptr<HdfsHandler>* fs_handler) {
//                 kerberos::KerberosConfig config;
//                 config.set_principal_and_keytab(params.hdfs_kerberos_principal,
//                                                 params.hdfs_kerberos_keytab);
//                 std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache;
//
//                 RETURN_IF_ERROR(ticket_mgr->get_or_set_ticket_cache(config, &ticket_cache));
//                 *fs_handler = std::make_shared<HdfsHandler>(
//                         nullptr, true, params.hdfs_kerberos_principal, params.hdfs_kerberos_keytab,
//                         fs_name, ticket_cache);
//                 return Status::OK();
//             });
//
//     // Create handler
//     std::shared_ptr<HdfsHandler> handler;
//     ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params, "test_fs", &handler).ok());
//     std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache_holder = handler->ticket_cache;
//     handler.reset();
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 1);
//
//     // Wait for cleanup
//     ticket_cache_holder.reset();
//     std::this_thread::sleep_for(std::chrono::seconds(6));
//
//     // Verify handler and ticket cache are cleaned up
//     ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 0);
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 0);
// }
//
// // Test multiple handlers with different Kerberos credentials
// TEST_F(HdfsMgrTest, DifferentKerberosCredentials) {
//     // Create a ticket manager that will be used by both handlers
//     auto ticket_mgr = std::make_shared<NiceMock<MockKerberosTicketMgr>>("/tmp/kerberos");
//     // Set cleanup interval to 1 second for testing
//     ticket_mgr->set_cleanup_interval(std::chrono::seconds(1));
//
//     // Setup mock to create handlers with Kerberos
//     ON_CALL(*_hdfs_mgr, _create_hdfs_fs_impl(_, _, _))
//             .WillByDefault([ticket_mgr](const THdfsParams& params, const std::string& fs_name,
//                                         std::shared_ptr<HdfsHandler>* fs_handler) {
//                 kerberos::KerberosConfig config;
//                 config.set_principal_and_keytab(params.hdfs_kerberos_principal,
//                                                 params.hdfs_kerberos_keytab);
//                 std::shared_ptr<kerberos::KerberosTicketCache> ticket_cache;
//
//                 RETURN_IF_ERROR(ticket_mgr->get_or_set_ticket_cache(config, &ticket_cache));
//                 *fs_handler = std::make_shared<HdfsHandler>(
//                         nullptr, true, params.hdfs_kerberos_principal, params.hdfs_kerberos_keytab,
//                         fs_name, ticket_cache);
//                 return Status::OK();
//             });
//
//     // Create handlers with different credentials
//     // std::cout << "xxx 6 MockKerberosTicketCache::get_instance_count(): " << MockKerberosTicketCache::get_instance_count() << std::endl;
//     THdfsParams params1 = create_test_params("user1", "principal1", "keytab1");
//     THdfsParams params2 = create_test_params("user2", "principal2", "keytab2");
//     std::shared_ptr<HdfsHandler> handler1;
//     std::shared_ptr<HdfsHandler> handler2;
//     ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params1, "test_fs1", &handler1).ok());
//     ASSERT_TRUE(_hdfs_mgr->get_or_create_fs(params2, "test_fs2", &handler2).ok());
//
//     // Verify each handler has its own ticket cache
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 2);
//     ASSERT_NE(handler1->ticket_cache, handler2->ticket_cache);
//
//     // Wait for cleanup
//     // Also need to reset this 2 temp references
//     handler1.reset();
//     handler2.reset();
//     std::this_thread::sleep_for(std::chrono::seconds(6));
//
//     // Verify all handlers and ticket caches are cleaned up
//     ASSERT_EQ(_hdfs_mgr->get_fs_handlers_size(), 0);
//
//     ASSERT_EQ(MockKerberosTicketCache::get_instance_count(), 0);
// }

} // namespace doris::io
