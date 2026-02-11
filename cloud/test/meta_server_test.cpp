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

#include "meta-service/meta_server.h"

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <bvar/window.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <random>
#include <thread>

#include "common/config.h"
#include "common/defer.h"
#include "common/logging.h"
#include "common/stats.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    // Clear the secondary package name for testing, to avoid brpc::Server::~Server() SEGV
    config::secondary_package_name = "";
    if (!cloud::init_glog("meta_server_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {
void notify_refresh_instance(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                             KVStats* stats, bool include_self);
} // namespace doris::cloud

TEST(MetaServerTest, FQDNRefreshInstance) {
    class MockMetaService : public cloud::MetaServiceImpl {
    public:
        MockMetaService(std::shared_ptr<TxnKv> txn_kv,
                        std::shared_ptr<ResourceManager> resource_mgr,
                        std::shared_ptr<RateLimiter> rate_controller,
                        std::shared_ptr<SnapshotManager> snapshot_mgr)
                : MetaServiceImpl(txn_kv, resource_mgr, rate_controller, snapshot_mgr) {}
        ~MockMetaService() override = default;

        void alter_instance(google::protobuf::RpcController* controller,
                            const ::doris::cloud::AlterInstanceRequest* request,
                            ::doris::cloud::AlterInstanceResponse* response,
                            ::google::protobuf::Closure* done) override {
            (void)controller;
            response->mutable_status()->set_code(cloud::MetaServiceCode::OK);
            LOG(INFO) << "alter instance " << request->DebugString();
            if (request->op() == cloud::AlterInstanceRequest::REFRESH) {
                std::unique_lock<std::mutex> lock(mu_);
                LOG(INFO) << "refresh instance " << request->instance_id();
                refreshed_instances_.insert(request->instance_id());
            }
            done->Run();
        }

        bool is_instance_refreshed(std::string instance_id) {
            std::unique_lock<std::mutex> lock(mu_);
            return refreshed_instances_.count(instance_id) > 0;
        }

        std::mutex mu_;
        std::unordered_set<std::string> refreshed_instances_;
    };

    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_shared<cloud::MemTxnKv>();
    auto resource_mgr = std::make_shared<MockResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<cloud::RateLimiter>();
    auto snapshot_mgr = std::make_shared<cloud::SnapshotManager>(txn_kv);
    auto mock_service =
            std::make_unique<MockMetaService>(txn_kv, resource_mgr, rate_limiter, snapshot_mgr);
    MockMetaService* mock_service_ptr = mock_service.get();
    MetaServiceProxy meta_service(std::move(mock_service));

    brpc::ServerOptions options;
    options.num_threads = 1;
    brpc::Server server;
    ASSERT_EQ(server.AddService(&meta_service, brpc::ServiceOwnership::SERVER_DOESNT_OWN_SERVICE),
              0);
    ASSERT_EQ(server.Start(0, &options), 0);
    auto addr = server.listen_address();

    config::hostname = butil::my_hostname();
    config::brpc_listen_port = addr.port;
    config::meta_server_register_interval_ms = 1;

    // Register meta service.
    cloud::MetaServerRegister server_register(txn_kv);
    ASSERT_EQ(server_register.start(), 0);

    while (true) {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto system_key = cloud::system_meta_service_registry_key();
        std::string value;
        if (txn->get(system_key, &value) == TxnErrorCode::TXN_OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    server_register.stop();

    // Refresh instance with FQDN endpoint.
    config::hostname = "";
    notify_refresh_instance(txn_kv, "fqdn_instance_id", nullptr, false);

    bool refreshed = false;
    for (size_t i = 0; i < 100; ++i) {
        if (mock_service_ptr->is_instance_refreshed("fqdn_instance_id")) {
            refreshed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_TRUE(refreshed);

    server.Stop(1);
    server.Join();
}

TEST(MetaServerTest, InstanceWatcherStartAndStop) {
    class MockResourceManagerWithTracking : public MockResourceManager {
    public:
        MockResourceManagerWithTracking(std::shared_ptr<TxnKv> txn_kv)
                : MockResourceManager(txn_kv) {}

        std::pair<MetaServiceCode, std::string> refresh_instance(
                const std::string& instance_id) override {
            std::unique_lock<std::mutex> lock(mu_);
            refreshed_instances_.insert(instance_id);
            refresh_count_++;
            cv_.notify_all();
            return std::make_pair(MetaServiceCode::OK, "");
        }

        bool wait_for_refresh(const std::string& instance_id, int timeout_ms = 5000) {
            std::unique_lock<std::mutex> lock(mu_);
            return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this, &instance_id] {
                return refreshed_instances_.count(instance_id) > 0;
            });
        }

        int get_refresh_count() {
            std::unique_lock<std::mutex> lock(mu_);
            return refresh_count_;
        }

        std::mutex mu_;
        std::condition_variable cv_;
        std::unordered_set<std::string> refreshed_instances_;
        int refresh_count_ = 0;
    };

    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_shared<cloud::MemTxnKv>();
    auto resource_mgr = std::make_shared<MockResourceManagerWithTracking>(txn_kv);

    // Test basic start and stop
    {
        auto watcher = std::make_unique<cloud::MetaServerInstanceWatcher>(txn_kv, resource_mgr);
        ASSERT_EQ(watcher->start(), 0);
        // Try to start again should fail
        ASSERT_EQ(watcher->start(), -2);
        watcher->stop();
    }

    // Test nullptr txn_kv
    {
        auto watcher = std::make_unique<cloud::MetaServerInstanceWatcher>(nullptr, resource_mgr);
        ASSERT_EQ(watcher->start(), -1);
    }
}

TEST(MetaServerTest, InstanceWatcherDetectInstanceUpdate) {
    class MockResourceManagerWithTracking : public MockResourceManager {
    public:
        MockResourceManagerWithTracking(std::shared_ptr<TxnKv> txn_kv)
                : MockResourceManager(txn_kv) {}

        std::pair<MetaServiceCode, std::string> refresh_instance(
                const std::string& instance_id) override {
            std::unique_lock<std::mutex> lock(mu_);
            refreshed_instances_.insert(instance_id);
            refresh_count_++;
            cv_.notify_all();
            LOG(INFO) << "refresh_instance called for instance_id=" << instance_id
                      << " total_count=" << refresh_count_;
            return std::make_pair(MetaServiceCode::OK, "");
        }

        bool wait_for_refresh(const std::string& instance_id, int timeout_ms = 5000) {
            std::unique_lock<std::mutex> lock(mu_);
            return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this, &instance_id] {
                return refreshed_instances_.count(instance_id) > 0;
            });
        }

        bool wait_for_refresh_count(int count, int timeout_ms = 5000) {
            std::unique_lock<std::mutex> lock(mu_);
            return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                [this, count] { return refresh_count_ >= count; });
        }

        int get_refresh_count() {
            std::unique_lock<std::mutex> lock(mu_);
            return refresh_count_;
        }

        std::mutex mu_;
        std::condition_variable cv_;
        std::unordered_set<std::string> refreshed_instances_;
        int refresh_count_ = 0;
    };

    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_shared<cloud::MemTxnKv>();
    auto resource_mgr = std::make_shared<MockResourceManagerWithTracking>(txn_kv);

    auto watcher = std::make_unique<cloud::MetaServerInstanceWatcher>(txn_kv, resource_mgr);
    ASSERT_EQ(watcher->start(), 0);

    // Create instance in kv store
    std::string instance_id = "test_instance_123";
    cloud::InstanceKeyInfo key_info {instance_id};
    std::string instance_key = cloud::instance_key(key_info);

    cloud::InstanceInfoPB instance_pb;
    instance_pb.set_instance_id(instance_id);
    instance_pb.set_name("test_instance");
    std::string instance_val = instance_pb.SerializeAsString();

    {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key, instance_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Trigger instance update
    {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_add(cloud::system_meta_service_instance_update_key(), 1);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Wait for watcher to detect and refresh
    ASSERT_TRUE(resource_mgr->wait_for_refresh(instance_id, 5000))
            << "Failed to detect instance update";

    // Update instance again
    instance_pb.set_name("test_instance_updated");
    instance_val = instance_pb.SerializeAsString();
    {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key, instance_val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Trigger instance update again
    {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_add(cloud::system_meta_service_instance_update_key(), 1);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Should detect the second update
    ASSERT_TRUE(resource_mgr->wait_for_refresh_count(2, 5000))
            << "Failed to detect second instance update";

    watcher->stop();
}

TEST(MetaServerTest, InstanceWatcherMultipleInstances) {
    class MockResourceManagerWithTracking : public MockResourceManager {
    public:
        MockResourceManagerWithTracking(std::shared_ptr<TxnKv> txn_kv)
                : MockResourceManager(txn_kv) {}

        std::pair<MetaServiceCode, std::string> refresh_instance(
                const std::string& instance_id) override {
            std::unique_lock<std::mutex> lock(mu_);
            refreshed_instances_.insert(instance_id);
            refresh_count_[instance_id]++;
            cv_.notify_all();
            LOG(INFO) << "refresh_instance called for instance_id=" << instance_id;
            return std::make_pair(MetaServiceCode::OK, "");
        }

        bool wait_for_instances(const std::vector<std::string>& instance_ids,
                                int timeout_ms = 5000) {
            std::unique_lock<std::mutex> lock(mu_);
            return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this, &instance_ids] {
                for (const auto& id : instance_ids) {
                    if (refreshed_instances_.count(id) == 0) {
                        return false;
                    }
                }
                return true;
            });
        }

        std::mutex mu_;
        std::condition_variable cv_;
        std::unordered_set<std::string> refreshed_instances_;
        std::unordered_map<std::string, int> refresh_count_;
    };

    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_shared<cloud::MemTxnKv>();
    auto resource_mgr = std::make_shared<MockResourceManagerWithTracking>(txn_kv);

    auto watcher = std::make_unique<cloud::MetaServerInstanceWatcher>(txn_kv, resource_mgr);
    ASSERT_EQ(watcher->start(), 0);

    // Create multiple instances
    std::vector<std::string> instance_ids = {"instance_1", "instance_2", "instance_3"};

    for (const auto& instance_id : instance_ids) {
        cloud::InstanceKeyInfo key_info {instance_id};
        std::string instance_key = cloud::instance_key(key_info);

        cloud::InstanceInfoPB instance_pb;
        instance_pb.set_instance_id(instance_id);
        instance_pb.set_name("instance_" + instance_id);

        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(instance_key, instance_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Trigger instance update
    {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_add(cloud::system_meta_service_instance_update_key(), 1);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Wait for all instances to be refreshed
    ASSERT_TRUE(resource_mgr->wait_for_instances(instance_ids, 5000))
            << "Failed to detect all instance updates";

    watcher->stop();
}

TEST(MetaServerTest, StartAndStop) {
    std::shared_ptr<cloud::TxnKv> txn_kv = std::make_shared<cloud::MemTxnKv>();
    auto server = std::make_unique<MetaServer>(txn_kv);
    auto resource_mgr = std::make_shared<MockResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<cloud::RateLimiter>();

    brpc::ServerOptions options;
    options.num_threads = 1;
    brpc::Server brpc_server;

    auto sp = SyncPoint::get_instance();

    std::array<std::string, 2> sps {"MetaServer::start:1", "MetaServer::start:2"};
    // use structured binding for point alias (avoid multi lines of declaration)
    auto [meta_server_start_1, meta_server_start_2] = sps;
    sp->enable_processing();
    DORIS_CLOUD_DEFER {
        for (auto& i : sps) {
            sp->clear_call_back(i);
        } // redundant
        sp->disable_processing();
    };

    auto foo = [](auto&& args) {
        auto* ret = try_any_cast<int*>(args[0]);
        *ret = 1;
    };

    // failed to init resource mgr
    sp->set_call_back(meta_server_start_1, foo);
    ASSERT_EQ(server->start(&brpc_server), 1);
    sp->clear_call_back(meta_server_start_1);

    // failed to start registry
    sp->set_call_back(meta_server_start_2, foo);
    ASSERT_EQ(server->start(&brpc_server), -1);
    sp->clear_call_back(meta_server_start_2);

    ASSERT_EQ(server->start(&brpc_server), 0);
    ASSERT_EQ(brpc_server.Start(0, &options), 0);
    auto addr = brpc_server.listen_address();

    config::hostname = butil::my_hostname();
    config::brpc_listen_port = addr.port;
    config::meta_server_register_interval_ms = 1;

    while (true) {
        std::unique_ptr<cloud::Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto system_key = cloud::system_meta_service_registry_key();
        std::string value;
        if (txn->get(system_key, &value) == TxnErrorCode::TXN_OK) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    server->stop();
    brpc_server.Stop(1);
    brpc_server.Join();
}
