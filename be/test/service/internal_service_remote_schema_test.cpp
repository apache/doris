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

#include <brpc/callback.h>
#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <bthread/id.h>
#include <butil/endpoint.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <new>
#include <optional>
#include <utility>
#include <vector>

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/load_stream_mgr.h"
#include "service/internal_service.h"
#include "util/brpc_client_cache.h"
#include "util/debug/leak_annotations.h"
#include "util/defer_op.h"

namespace doris {
namespace {

using namespace std::chrono_literals;

int start_schema_worker(brpc::Server& server) {
    // bRPC 1.4 retains its keytable pool after Join() for any remaining bthreads.
    debug::ScopedLSANDisabler ignore_brpc_bootstrap;
    return server.Start("127.0.0.1:0", nullptr);
}

template <typename T>
class ScopedConfigValue {
public:
    ScopedConfigValue(T& setting, T value)
            : _setting(setting), _old(std::exchange(setting, value)) {}
    ~ScopedConfigValue() { _setting = _old; }

private:
    T& _setting;
    T _old;
};

class HeldSchemaWorker final : public PBackendService {
public:
    void fetch_remote_tablet_schema(google::protobuf::RpcController*,
                                    const PFetchRemoteSchemaRequest* request,
                                    PFetchRemoteSchemaResponse* response,
                                    google::protobuf::Closure* done) override {
        EXPECT_FALSE(request->is_coordinator());
        EXPECT_EQ(1, request->tablet_location_size());
        std::vector<int64_t> tablet_ids;
        for (const auto& location : request->tablet_location()) {
            tablet_ids.insert(tablet_ids.end(), location.tablet_id().begin(),
                              location.tablet_id().end());
        }
        EXPECT_EQ((std::vector<int64_t> {1, 2}), tablet_ids);
        Status::OK().to_protobuf(response->mutable_status());
        response->mutable_merged_schema();
        reply.set_value(done);
    }

    std::promise<google::protobuf::Closure*> reply;
};

class SchemaDispatchChannel final : public FailureDetectChannel {
public:
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done) override {
        if (++_calls == 2) {
            // Model allocation failure before FailureDetectChannel enters BRPC.
            auto* cntl = static_cast<brpc::Controller*>(controller);
            failed_id.set_value(cntl->call_id());
            throw std::bad_alloc();
        }
        FailureDetectChannel::CallMethod(
                method, controller, request, response,
                brpc::NewCallback(&SchemaDispatchChannel::complete_first, done, &first_done));
    }

    std::promise<brpc::CallId> failed_id;
    std::promise<void> first_done;

private:
    static void complete_first(google::protobuf::Closure* done, std::promise<void>* completed) {
        done->Run();
        completed->set_value();
    }

    int _calls = 0;
};

class SchemaReplyDone final : public google::protobuf::Closure {
public:
    explicit SchemaReplyDone(const std::shared_future<void>& first_completed)
            : _first_completed(first_completed) {}

    void Run() override {
        completion.set_value(_first_completed.wait_for(0s) == std::future_status::ready);
    }

    std::promise<bool> completion;

private:
    const std::shared_future<void>& _first_completed;
};

class ScopedSchemaClientCache {
public:
    ScopedSchemaClientCache()
            : _env(ExecEnv::GetInstance()), _previous(_env->brpc_internal_client_cache()) {
        if (_previous == nullptr) {
            _env->_internal_client_cache = &_owned.emplace();
        }
    }

    ~ScopedSchemaClientCache() { _env->_internal_client_cache = _previous; }

private:
    ExecEnv* _env;
    BrpcClientCache<PBackendService_Stub>* _previous;
    std::optional<BrpcClientCache<PBackendService_Stub>> _owned;
};

enum class ReplicaReply { COVERED, MISSING, LEGACY, IO_ERROR };

class CoverageSchemaWorker final : public PBackendService {
public:
    explicit CoverageSchemaWorker(ReplicaReply replica) : _replica(replica) {}

    void fetch_remote_tablet_schema(google::protobuf::RpcController*,
                                    const PFetchRemoteSchemaRequest* request,
                                    PFetchRemoteSchemaResponse* response,
                                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard reply(done);
        EXPECT_TRUE(request->report_tablet_coverage());
        ASSERT_EQ(1, request->tablet_location_size());
        const auto& location = request->tablet_location(0);
        if (location.tablet_visible_version_size() != 0 &&
            (location.tablet_id_size() == 1 || _replica != ReplicaReply::LEGACY)) {
            EXPECT_EQ(location.tablet_id_size(), location.tablet_visible_version_size());
            response->set_tablet_versions_verified(true);
        }
        if (location.tablet_id_size() == 1) {
            if (_replica == ReplicaReply::IO_ERROR) {
                Status::IOError("injected segment read failure")
                        .to_protobuf(response->mutable_status());
                return;
            }
            response->mutable_tablet_coverage()->add_missing_tablet_id(101);
        } else {
            // This worker always reads tablet 102. It may also cover tablet 101.
            response->mutable_merged_schema();
            if (_replica != ReplicaReply::LEGACY) {
                auto* coverage = response->mutable_tablet_coverage();
                if (_replica == ReplicaReply::MISSING) {
                    coverage->add_missing_tablet_id(101);
                }
            }
        }
        Status::OK().to_protobuf(response->mutable_status());
    }

private:
    ReplicaReply _replica;
};

void signal_schema_response(std::promise<void>* finished) {
    finished->set_value();
}

void check_replica_coverage(ReplicaReply replica, int expected_status,
                            bool require_versions = false) {
    ScopedConfigValue heavy_threads(config::brpc_heavy_work_pool_threads, 1);
    ScopedConfigValue light_threads(config::brpc_light_work_pool_threads, 1);
    ScopedConfigValue flight_threads(config::brpc_arrow_flight_work_pool_threads, 1);
    ScopedConfigValue rpc_timeout(config::fetch_remote_schema_rpc_timeout_ms, int64_t {5000});
    ScopedSchemaClientCache schema_cache;
    CoverageSchemaWorker worker(replica);
    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&worker, brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, start_schema_worker(server));
    Defer stop_server {[&] {
        server.Stop(0);
        server.Join();
        ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(server.listen_address());
    }};

    PFetchRemoteSchemaRequest request;
    request.set_is_coordinator(true);
    for (int i = 0; i < 2; ++i) {
        auto* location = request.add_tablet_location();
        location->set_host("127.0.0.1");
        location->set_brpc_port(server.listen_address().port);
        location->add_tablet_id(101);
    }
    request.mutable_tablet_location(1)->add_tablet_id(102);
    if (require_versions) {
        request.mutable_tablet_location(0)->add_tablet_visible_version(2);
        request.mutable_tablet_location(1)->add_tablet_visible_version(2);
        request.mutable_tablet_location(1)->add_tablet_visible_version(3);
    }

    std::promise<void> finished;
    auto reply = finished.get_future();
    PFetchRemoteSchemaResponse response;
    ExecEnv coordinator_env;
    coordinator_env._load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
    PInternalService coordinator(&coordinator_env);
    Defer drain {[&] {
        coordinator._heavy_work_pool.shutdown();
        coordinator._heavy_work_pool.join();
        coordinator_env.load_stream_mgr()->set_heavy_work_pool(nullptr);
    }};
    coordinator.fetch_remote_tablet_schema(nullptr, &request, &response,
                                           brpc::NewCallback(signal_schema_response, &finished));
    ASSERT_EQ(std::future_status::ready, reply.wait_for(10s));
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(expected_status, response.status().status_code());
    EXPECT_EQ(expected_status == 0, response.has_merged_schema());
    EXPECT_EQ(expected_status == 0 && require_versions, response.tablet_versions_verified());
}

void release_pending_schema_rpcs(std::future<google::protobuf::Closure*>& first_reply,
                                 const std::shared_future<brpc::CallId>& failed_id,
                                 const std::shared_future<bool>& completed) {
    if (first_reply.valid() && first_reply.wait_for(5s) == std::future_status::ready) {
        first_reply.get()->Run();
    }
    if (completed.wait_for(0s) == std::future_status::ready ||
        failed_id.wait_for(5s) != std::future_status::ready) {
        return;
    }
    // Destroy the unused ID and wake Join if the coordinator still waits on it.
    const auto id = failed_id.get();
    if (bthread_id_trylock(id, nullptr) == 0) {
        bthread_id_unlock_and_destroy(id);
    }
}

void expect_schema_allocation_error(const std::shared_future<bool>& completed,
                                    const PFetchRemoteSchemaResponse& response) {
    ASSERT_EQ(completed.wait_for(2s), std::future_status::ready)
            << "First RPC finished but the coordinator is still waiting on the undispatched ID";
    EXPECT_TRUE(completed.get()) << "The coordinator replied before the first RPC completed";
    ASSERT_TRUE(response.has_status());
    EXPECT_EQ(ErrorCode::MEM_LIMIT_EXCEEDED, response.status().status_code());
    EXPECT_FALSE(response.has_merged_schema());
}

} // namespace

TEST(InternalServiceRemoteSchemaTest, DrainsFirstRpcWhenSecondFailsBeforeDispatch) {
    ScopedConfigValue heavy_threads(config::brpc_heavy_work_pool_threads, 1);
    ScopedConfigValue light_threads(config::brpc_light_work_pool_threads, 1);
    ScopedConfigValue flight_threads(config::brpc_arrow_flight_work_pool_threads, 1);
    ScopedConfigValue rpc_timeout(config::fetch_remote_schema_rpc_timeout_ms, int64_t {15000});

    HeldSchemaWorker worker;
    brpc::Server server;
    ASSERT_EQ(server.AddService(&worker, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(start_schema_worker(server), 0);
    Defer stop_server {[&] {
        server.Stop(0);
        server.Join();
    }};

    SchemaDispatchChannel channel;
    ASSERT_EQ(channel.Init(server.listen_address(), nullptr), 0);
    auto stub = std::make_shared<PBackendService_Stub>(&channel);
    ScopedSchemaClientCache schema_cache;
    auto* cache = ExecEnv::GetInstance()->brpc_internal_client_cache();
    const std::string endpoint = butil::endpoint2str(server.listen_address()).c_str();
    ASSERT_TRUE(cache->_stub_map
                        .try_emplace(endpoint, StubEntry<PBackendService_Stub> {"127.0.0.1", stub})
                        .second);
    Defer erase_stub {[&] { cache->erase(endpoint); }};

    ExecEnv coordinator_env;
    coordinator_env._load_stream_mgr = std::make_unique<LoadStreamMgr>(1);
    PInternalService coordinator(&coordinator_env);
    PFetchRemoteSchemaRequest request;
    request.set_is_coordinator(true);
    const std::vector<std::vector<int64_t>> assignments {{1, 2}, {3}};
    for (const auto& tablet_ids : assignments) {
        auto* location = request.add_tablet_location();
        location->set_host("127.0.0.1");
        location->set_brpc_port(server.listen_address().port);
        location->mutable_tablet_id()->Assign(tablet_ids.begin(), tablet_ids.end());
    }
    PFetchRemoteSchemaResponse response;
    auto first_reply = worker.reply.get_future();
    auto first_completed = channel.first_done.get_future().share();
    auto failed_id = channel.failed_id.get_future().share();
    SchemaReplyDone done(first_completed);
    auto completed = done.completion.get_future().share();

    Defer drain {[&] {
        release_pending_schema_rpcs(first_reply, failed_id, completed);
        coordinator._heavy_work_pool.shutdown();
        coordinator._heavy_work_pool.join();
        coordinator_env.load_stream_mgr()->set_heavy_work_pool(nullptr);
    }};

    coordinator.fetch_remote_tablet_schema(nullptr, &request, &response, &done);
    ASSERT_EQ(first_reply.wait_for(5s), std::future_status::ready);
    ASSERT_EQ(failed_id.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(completed.wait_for(100ms), std::future_status::timeout)
            << "Coordinator returned before the first in-flight RPC finished";

    first_reply.get()->Run();
    ASSERT_EQ(first_completed.wait_for(5s), std::future_status::ready);
    expect_schema_allocation_error(completed, response);
}

TEST(InternalServiceRemoteSchemaTest, RecoversOnlyWhenAnotherWorkerProvesCoverage) {
    check_replica_coverage(ReplicaReply::COVERED, 0);
    check_replica_coverage(ReplicaReply::MISSING, ErrorCode::TABLE_NOT_FOUND);
    check_replica_coverage(ReplicaReply::LEGACY, ErrorCode::TABLE_NOT_FOUND);
    check_replica_coverage(ReplicaReply::IO_ERROR, ErrorCode::IO_ERROR);
    check_replica_coverage(ReplicaReply::COVERED, 0, true);
    check_replica_coverage(ReplicaReply::LEGACY, ErrorCode::TABLE_NOT_FOUND, true);
}

} // namespace doris
