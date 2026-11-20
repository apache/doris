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

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "exec/sink/writer/vtablet_writer.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/time.h"

namespace doris {

class VTabletWriterOpenTest : public testing::Test {
protected:
    class OpenService : public PBackendService {
    public:
        std::atomic<int> network_failures {1};
        bool business_failure = false;
        std::mutex mutex;
        std::vector<std::string> requests;

        void tablet_writer_open(google::protobuf::RpcController* controller,
                                const PTabletWriterOpenRequest* request,
                                PTabletWriterOpenResult* response,
                                google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            int attempt;
            {
                std::lock_guard lock(mutex);
                requests.push_back(request->SerializeAsString());
                attempt = requests.size();
            }
            Status::OK().to_protobuf(response->mutable_status());
            if (attempt <= network_failures.load()) {
                static_cast<brpc::Controller*>(controller)->SetFailed(EHOSTDOWN, "peer recovering");
            } else if (business_failure) {
                Status::InternalError("invalid tablet schema")
                        .to_protobuf(response->mutable_status());
            }
        }
    };

    void SetUp() override {
        _previous_cache = ExecEnv::GetInstance()->_internal_client_cache;
        ASSERT_EQ(0, _server.AddService(&_service, brpc::SERVER_DOESNT_OWN_SERVICE));
        debug::ScopedLeakCheckDisabler disable_lsan;
        ASSERT_EQ(0, _server.Start("127.0.0.1:0", nullptr));
        ExecEnv::GetInstance()->_internal_client_cache = &_cache;
        _index = std::make_unique<IndexChannel>(nullptr, 1, nullptr);
        _channel = std::make_unique<VNodeChannel>(nullptr, _index.get(), 1);
        _channel->_state = &_state;
        _channel->_node_info.host = "127.0.0.1";
        _channel->_node_info.brpc_port = _server.listen_address().port;
        _channel->_stub = _cache.get_client("127.0.0.1", _server.listen_address().port, false);
        ASSERT_NE(nullptr, _channel->_stub);
    }

    void TearDown() override {
        _channel.reset();
        _index.reset();
        ExecEnv::GetInstance()->_internal_client_cache = _previous_cache;
        _server.Stop(0);
        _server.Join();
    }

    void start_open(int64_t budget_ms) {
        auto request = std::make_shared<PTabletWriterOpenRequest>();
        request->mutable_id()->set_hi(7);
        request->mutable_id()->set_lo(8);
        request->set_index_id(1);
        request->set_txn_id(123);
        auto* schema = request->mutable_schema();
        schema->set_db_id(1);
        schema->set_table_id(2);
        schema->set_version(3);
        schema->mutable_tuple_desc()->set_id(0);
        schema->mutable_tuple_desc()->set_byte_size(0);
        schema->mutable_tuple_desc()->set_num_null_bytes(0);
        request->add_tablets()->set_tablet_id(456);
        request->mutable_tablets(0)->set_partition_id(789);
        request->set_num_senders(1);
        request->set_sender_id(0);
        request->set_need_gen_rollup(false);
        request->set_is_incremental(false);
        ASSERT_TRUE(request->IsInitialized());
        auto callback = DummyBrpcCallback<PTabletWriterOpenResult>::create_shared();
        callback->cntl_->set_timeout_ms(1000);
        callback->cntl_->set_max_retry(0);
        auto closure = AutoReleaseClosure<
                PTabletWriterOpenRequest,
                DummyBrpcCallback<PTabletWriterOpenResult>>::create_unique(request, callback);
        _channel->_stub->tablet_writer_open(closure->cntl_.get(), closure->request_.get(),
                                            closure->response_.get(), closure.get());
        closure.release();
        _channel->_open_rpcs.push_back({callback, request, MonotonicMillis() + budget_ms});
    }

    OpenService _service;
    brpc::Server _server;
    BrpcClientCache<PBackendService_Stub> _cache;
    BrpcClientCache<PBackendService_Stub>* _previous_cache = nullptr;
    RuntimeState _state;
    std::unique_ptr<IndexChannel> _index;
    std::unique_ptr<VNodeChannel> _channel;
};

TEST_F(VTabletWriterOpenTest, RebuildsFailedConnectionAndReplaysIdenticalRequest) {
    start_open(5000);
    auto original_stub = _channel->_stub;
    ASSERT_TRUE(_channel->open_wait().ok());
    EXPECT_FALSE(_channel->_cancelled);
    EXPECT_NE(original_stub, _channel->_stub);
    EXPECT_EQ(nullptr, _channel->_open_rpcs.front().retry_request);
    std::lock_guard lock(_service.mutex);
    ASSERT_EQ(2, _service.requests.size());
    EXPECT_EQ(_service.requests[0], _service.requests[1]);
}

TEST_F(VTabletWriterOpenTest, DoesNotRetryBusinessErrors) {
    _service.network_failures = 0;
    _service.business_failure = true;
    start_open(5000);
    EXPECT_FALSE(_channel->open_wait().ok());
    EXPECT_TRUE(_channel->_cancelled);
    std::lock_guard lock(_service.mutex);
    EXPECT_EQ(1, _service.requests.size());
}

TEST_F(VTabletWriterOpenTest, DoesNotRetryAfterDeadline) {
    start_open(-1);
    EXPECT_FALSE(_channel->open_wait().ok());
    EXPECT_TRUE(_channel->_cancelled);
    std::lock_guard lock(_service.mutex);
    EXPECT_EQ(1, _service.requests.size());
}

TEST_F(VTabletWriterOpenTest, PersistentNetworkFailureStopsWithinBudget) {
    _service.network_failures = 1000;
    start_open(1500);
    auto start_ms = MonotonicMillis();
    EXPECT_FALSE(_channel->open_wait().ok());
    EXPECT_LT(MonotonicMillis() - start_ms, 3000);
    EXPECT_TRUE(_channel->_cancelled);
    std::lock_guard lock(_service.mutex);
    EXPECT_LE(_service.requests.size(), 2);
}

TEST_F(VTabletWriterOpenTest, CancellationStopsRecovery) {
    start_open(5000);
    _state.cancel(Status::Cancelled("test cancellation"));
    auto status = _channel->open_wait();
    EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
    std::lock_guard lock(_service.mutex);
    EXPECT_EQ(1, _service.requests.size());
}

} // namespace doris
