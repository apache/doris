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

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "gen_cpp/internal_service.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/service.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "testutil/desc_tbl_builder.h"
#include "util/brpc_client_cache.h"
#include "util/proto_util.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {

class LocalMockBackendService : public PBackendService {
public:
    void transmit_block(::google::protobuf::RpcController* controller,
                        const ::doris::PTransmitDataParams* request,
                        ::doris::PTransmitDataResult* response, ::google::protobuf::Closure* done) {
        // stream_mgr->transmit_block(request, &done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
        attachment_transfer_request_block<PTransmitDataParams>(request, cntl);
        // The response is accessed when done->Run is called in transmit_block(),
        // give response a default value to avoid null pointers in high concurrency.
        Status st;
        st.to_protobuf(response->mutable_status());
        st = stream_mgr->transmit_block(request, &done);
        if (!st.ok()) {
            LOG(WARNING) << "transmit_block failed, message=" << st.get_error_msg()
                         << ", fragment_instance_id=" << print_id(request->finst_id())
                         << ", node=" << request->node_id();
        }
    }

private:
    VDataStreamMgr* stream_mgr;
};

class MockChannel : public google::protobuf::RpcChannel {
public:
    MockChannel(PBackendService* service) : _service(service) {}

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done) {
        auto call_id = ((brpc::Controller*)controller)->call_id();

        bthread_id_lock_and_reset_range(call_id, NULL, 2 + 3);
        _service->transmit_block(controller, (PTransmitDataParams*)request,
                                 (PTransmitDataResult*)response, done);
        // brpc::StartCancel(call_id);
        // LOG(INFO) << bthread_id_cancel(call_id);
        // void * data = nullptr;
        // bthread_id_trylock(call_id, &data);
        bthread_id_unlock_and_destroy(call_id);
    }

private:
    std::unique_ptr<PBackendService> _service;
};

template <class T>
class MockBrpcClientCache : public BrpcClientCache<T> {
public:
    MockBrpcClientCache(google::protobuf::RpcChannel* channel) {
        _channel.reset(channel);
        _stub.reset(new T(channel));
    }
    virtual ~MockBrpcClientCache() = default;
    virtual std::shared_ptr<T> get_client(const TNetworkAddress&) { return _stub; }

private:
    std::unique_ptr<google::protobuf::RpcChannel> _channel;
    std::shared_ptr<T> _stub;
};

class VDataStreamTest : public testing::Test {
    virtual void SetUp() override {}
    virtual void TearDown() override {}

private:
    VDataStreamMgr _instance;
    ObjectPool _object_pool;
};

TEST_F(VDataStreamTest, BasicTest) {
    doris::DescriptorTblBuilder builder(&_object_pool);
    builder.declare_tuple() << doris::TYPE_INT << doris::TYPE_DOUBLE;
    doris::DescriptorTbl* desc_tbl = builder.build();
    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);

    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_mem_trackers();
    runtime_stat.set_desc_tbl(desc_tbl);
    runtime_stat.set_be_number(1);
    runtime_stat._exec_env = _object_pool.add(new ExecEnv);

    // prepare mock some method
    LocalMockBackendService* mock_service = new LocalMockBackendService;
    mock_service->stream_mgr = &_instance;
    MockChannel* channel = new MockChannel(std::move(mock_service));

    runtime_stat._exec_env->_internal_client_cache =
            _object_pool.add(new MockBrpcClientCache<PBackendService_Stub>(std::move(channel)));

    TUniqueId uid;
    PlanNodeId nid = 1;
    int num_senders = 1;
    int buffer_size = 1024 * 1024;
    RuntimeProfile profile("profile");
    bool is_merge = false;
    std::shared_ptr<QueryStatisticsRecvr> statistics = std::make_shared<QueryStatisticsRecvr>();
    auto recv = _instance.create_recvr(&runtime_stat, row_desc, uid, nid, num_senders, buffer_size,
                                       &profile, is_merge, statistics);

    // Test Sender
    int sender_id = 1;
    TDataSink tsink;
    {
        tsink.stream_sink.output_partition.type = TPartitionType::UNPARTITIONED;
        tsink.stream_sink.dest_node_id = 1;
    }
    std::vector<TPlanFragmentDestination> dests;
    {
        TPlanFragmentDestination dest;
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(8888);

        dest.__set_brpc_server(addr);
        dest.__set_fragment_instance_id(uid);
        dest.__set_server(addr);
        dests.push_back(dest);
    }
    int per_channel_buffer_size = 1024 * 1024;
    bool send_query_statistics_with_every_batch = false;
    VDataStreamSender sender(&runtime_stat, &_object_pool, sender_id, row_desc, tsink.stream_sink,
                             dests, per_channel_buffer_size,
                             send_query_statistics_with_every_batch);
    sender.set_query_statistics(std::make_shared<QueryStatistics>());
    sender.init(tsink);
    sender.prepare(&runtime_stat);
    sender.open(&runtime_stat);

    auto vec = vectorized::ColumnVector<Int32>::create();
    auto& data = vec->get_data();
    for (int i = 0; i < 1024; ++i) {
        data.push_back(i);
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
    vectorized::Block block({type_and_name});
    sender.send(&runtime_stat, &block);

    Block block_2;
    bool eos;
    recv->get_next(&block_2, &eos);

    EXPECT_EQ(block_2.rows(), 1024);

    Status exec_status;
    sender.close(&runtime_stat, exec_status);
    recv->close();
}
} // namespace doris::vectorized
