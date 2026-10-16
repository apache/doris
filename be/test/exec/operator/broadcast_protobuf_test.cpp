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

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/protocol.h>
#include <brpc/serialized_request.h>
#include <brpc/server.h>
#include <bthread/countdown_event.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "exec/exchange/vdata_stream_sender.h"
#include "exec/operator/exchange_sink_buffer.h"
#include "exec/operator/exchange_sink_operator.h"
#include "exec/pipeline/dependency.h"
#include "runtime/thread_context.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/defer_op.h"
#include "util/proto_util.h"

namespace doris {
namespace {

PTransmitDataParams make_header(int64_t destination = 1, int64_t sequence = 1, bool eos = false) {
    PTransmitDataParams request;
    request.mutable_finst_id()->set_hi(11);
    request.mutable_finst_id()->set_lo(destination);
    request.mutable_query_id()->set_hi(21);
    request.mutable_query_id()->set_lo(22);
    request.set_node_id(3);
    request.set_sender_id(4);
    request.set_be_number(5);
    request.set_packet_seq(sequence);
    request.set_eos(eos);
    request.mutable_query_statistics()->set_scan_rows(42);
    request.mutable_exec_status()->set_status_code(0);
    request.set_transfer_by_attachment(false);
    return request;
}

std::shared_ptr<BroadcastPBlockHolder> make_holder(char value = 'a') {
    auto holder = BroadcastPBlockHolder::create_shared();
    auto* block = holder->get_block();
    auto* meta = block->add_column_metas();
    meta->set_name("values");
    meta->set_type(PGenericType::STRING);
    meta->set_is_nullable(true);
    block->set_column_values(std::string(64 * 1024, value));
    block->set_compressed(true);
    block->set_compression_type(segment_v2::LZ4);
    block->set_uncompressed_size(128 * 1024);
    block->set_be_exec_version(7);
    return holder;
}

PTransmitDataParams parse(const butil::IOBuf& data) {
    PTransmitDataParams parsed;
    butil::IOBufAsZeroCopyInputStream stream(data);
    EXPECT_TRUE(parsed.ParseFromZeroCopyStream(&stream));
    return parsed;
}

class BroadcastProtobufTest : public testing::TestWithParam<bool> {};

class CapturingRpcChannel : public google::protobuf::RpcChannel {
public:
    ~CapturingRpcChannel() override {
        if (completion != nullptr) {
            completion->Run();
        }
    }

    butil::IOBuf wire;
    google::protobuf::Closure* completion = nullptr;

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done) override {
        EXPECT_EQ(method, PBackendService::descriptor()->FindMethodByName("transmit_block"));
        EXPECT_EQ(request->GetDescriptor(), brpc::SerializedRequest::descriptor());
        brpc::SerializeRequestDefault(&wire, static_cast<brpc::Controller*>(controller), request);
        completion = done;
    }
};

void expect_broadcast_packet(const PTransmitDataParams& packet, const RpcInstance& instance,
                             bool multi, int sent, bool eos) {
    EXPECT_EQ(packet.packet_seq(), multi ? 3 : sent);
    EXPECT_EQ(packet.eos(), eos);
    EXPECT_EQ(packet.finst_id().lo(), instance.id);
    ASSERT_EQ(packet.blocks_size(), multi ? 2 : 0);
    const auto& block = multi ? packet.blocks(0) : packet.block();
    EXPECT_EQ(block.column_values(), std::string(64 * 1024, multi ? 'a' : 'a' + sent - 1));
    EXPECT_FALSE(instance.request->has_block());
    EXPECT_EQ(instance.request->blocks_size(), 0);
}

TEST_P(BroadcastProtobufTest, SendRpcPreservesBatchSequenceEosAndRequestTemplate) {
    auto state = std::make_shared<MockRuntimeState>();
    ExchangeSinkBuffer buffer(state.get(), 1);
    buffer._query_id = make_header().query_id();
    buffer._dest_node_id = 3;
    buffer._send_multi_blocks = GetParam();
    buffer._send_multi_blocks_byte_size = 1024 * 1024;
    TUniqueId destination;
    destination.__set_hi(11);
    destination.__set_lo(1);
    buffer.construct_request(destination);
    auto sink = ExchangeSinkLocalState::create_shared(state.get());
    CapturingRpcChannel transport;
    Channel channel(sink.get(), TNetworkAddress {}, destination, 0);
    channel._brpc_stub = std::make_shared<PBackendService_Stub>(&transport);
    auto& instance = *buffer._rpc_instances.at(destination.lo);
    auto& queue = instance.broadcast_package_queue[&channel];
    for (int i = 0; i < 2; ++i) {
        auto holder = make_holder('a' + i);
        ASSERT_TRUE(holder->prepare_serialized_block(GetParam(), 2).ok());
        queue.push({.block_holder = holder, .eos = !GetParam() && i == 1});
    }
    if (GetParam()) {
        queue.push({.block_holder = BroadcastPBlockHolder::create_shared(), .eos = true});
    }
    int sent = 0;
    while (!queue.empty()) {
        ASSERT_TRUE(buffer._send_rpc(instance).ok());
        ASSERT_NE(transport.completion, nullptr);
        ++sent;
        expect_broadcast_packet(parse(transport.wire), instance, GetParam(), sent, queue.empty());
        // We drive the next send explicitly; expire the callback before completing this RPC.
        channel._send_callback.reset();
        transport.completion->Run();
        transport.completion = nullptr;
        transport.wire.clear();
    }
}

void expect_shared_backing_blocks(const PTransmitDataParams& header,
                                  const brpc::SerializedRequest& request,
                                  const BroadcastPBlockHolder& holder) {
    brpc::Controller controller;
    butil::IOBuf wire;
    brpc::SerializeRequestDefault(&wire, &controller, &request);
    ASSERT_FALSE(controller.Failed());
    wire.pop_front(header.ByteSizeLong());
    ASSERT_EQ(wire.backing_block_num(), holder.serialized_block().backing_block_num());
    for (size_t i = 0; i < wire.backing_block_num(); ++i) {
        EXPECT_EQ(wire.backing_block(i).data(), holder.serialized_block().backing_block(i).data());
        EXPECT_EQ(wire.backing_block(i).size(), holder.serialized_block().backing_block(i).size());
    }
}

TEST_P(BroadcastProtobufTest, MatchesExistingWireMessageAndSharesBackingBlocks) {
    auto holder = make_holder();
    const auto original = holder->get_block()->SerializeAsString();
    ASSERT_TRUE(holder->prepare_serialized_block(GetParam(), 8).ok());
    ASSERT_FALSE(holder->serialized_block().empty());

    for (int destination = 1; destination <= 8; ++destination) {
        auto header = make_header(destination, destination + 100, destination == 8);
        brpc::SerializedRequest request;
        ASSERT_TRUE(ExchangeSinkBuffer::_serialize_broadcast_request(
                            header, {{.block_holder = holder, .eos = false}}, &request)
                            .ok());
        auto expected = header;
        auto* block = GetParam() ? expected.add_blocks() : expected.mutable_block();
        block->CopyFrom(*holder->get_block());
        EXPECT_EQ(parse(request.serialized_data()).SerializeAsString(),
                  expected.SerializeAsString());

        // Exercise the actual brpc serializer: it must retain the same backing data,
        // rather than encoding the payload again for this destination.
        expect_shared_backing_blocks(header, request, *holder);
    }
    EXPECT_EQ(holder->get_block()->SerializeAsString(), original);
}

TEST_P(BroadcastProtobufTest, ConcurrentDestinationsAndHolderRelease) {
    auto holder = make_holder();
    ASSERT_TRUE(holder->prepare_serialized_block(GetParam(), 8).ok());
    std::weak_ptr<BroadcastPBlockHolder> weak_holder = holder;
    std::vector<brpc::SerializedRequest> requests(8);
    std::vector<std::thread> senders;
    for (int i = 0; i < 8; ++i) {
        senders.emplace_back([&, i, shared_holder = holder] {
            SCOPED_INIT_THREAD_CONTEXT();
            EXPECT_TRUE(ExchangeSinkBuffer::_serialize_broadcast_request(
                                make_header(i + 1, i + 10),
                                {{.block_holder = shared_holder, .eos = false}}, &requests[i])
                                .ok());
        });
    }
    for (auto& sender : senders) {
        sender.join();
    }
    holder.reset();
    ASSERT_TRUE(weak_holder.expired());
    for (int i = 0; i < 8; ++i) {
        auto parsed = parse(requests[i].serialized_data());
        EXPECT_EQ(parsed.finst_id().lo(), i + 1);
        EXPECT_EQ(parsed.packet_seq(), i + 10);
        const auto& block = GetParam() ? parsed.blocks(0) : parsed.block();
        EXPECT_EQ(block.column_values(), std::string(64 * 1024, 'a'));
    }
}

TEST(BroadcastProtobufTest, RepeatedBlockPreservesLegacyDefaultFieldPresence) {
    auto holder = make_holder();
    holder->get_block()->clear_compressed();
    holder->get_block()->clear_uncompressed_size();
    holder->get_block()->clear_compression_type();
    ASSERT_TRUE(holder->prepare_serialized_block(true, 2).ok());
    brpc::SerializedRequest request;
    ASSERT_TRUE(ExchangeSinkBuffer::_serialize_broadcast_request(
                        make_header(), {{.block_holder = holder, .eos = false}}, &request)
                        .ok());
    const auto parsed = parse(request.serialized_data());
    ASSERT_EQ(parsed.blocks_size(), 1);
    EXPECT_TRUE(parsed.blocks(0).has_compressed());
    EXPECT_FALSE(parsed.blocks(0).compressed());
    EXPECT_TRUE(parsed.blocks(0).has_uncompressed_size());
    EXPECT_EQ(parsed.blocks(0).uncompressed_size(), holder->get_block()->uncompressed_size());
    EXPECT_TRUE(parsed.blocks(0).has_compression_type());
    EXPECT_EQ(parsed.blocks(0).compression_type(), holder->get_block()->compression_type());
    EXPECT_FALSE(holder->get_block()->has_compressed());
    EXPECT_FALSE(holder->get_block()->has_uncompressed_size());
    EXPECT_FALSE(holder->get_block()->has_compression_type());
}

TEST(BroadcastProtobufTest, DifferentBatchBoundariesAndEmptyEos) {
    auto first = make_holder('a');
    auto second = make_holder('b');
    auto eos = BroadcastPBlockHolder::create_shared();
    for (const auto& holder : {first, second, eos}) {
        ASSERT_TRUE(holder->prepare_serialized_block(true, 2).ok());
    }
    ASSERT_TRUE(eos->serialized_block().empty());

    const std::vector<std::vector<BroadcastTransmitInfo>> batches = {
            {{.block_holder = first, .eos = false},
             {.block_holder = second, .eos = false},
             {.block_holder = eos, .eos = true}},
            {{.block_holder = first, .eos = false}},
            {{.block_holder = second, .eos = false}, {.block_holder = eos, .eos = true}},
            {{.block_holder = eos, .eos = true}}};
    const int64_t sequences[] = {3, 1, 3, 4};
    const int expected_blocks[] = {2, 1, 1, 0};
    for (size_t i = 0; i < batches.size(); ++i) {
        brpc::SerializedRequest request;
        auto header = make_header(i + 1, sequences[i], batches[i].back().eos);
        ASSERT_TRUE(ExchangeSinkBuffer::_serialize_broadcast_request(header, batches[i], &request)
                            .ok());
        auto parsed = parse(request.serialized_data());
        EXPECT_EQ(parsed.packet_seq(), sequences[i]);
        EXPECT_EQ(parsed.eos(), batches[i].back().eos);
        ASSERT_EQ(parsed.blocks_size(), expected_blocks[i]);
        for (int j = 0; j < parsed.blocks_size(); ++j) {
            EXPECT_EQ(parsed.blocks(j).SerializeAsString(),
                      batches[i][j].block_holder->get_block()->SerializeAsString());
        }
    }
}

TEST(BroadcastProtobufTest, SingleReceiverKeepsOriginalPath) {
    for (bool multi : {false, true}) {
        auto holder = make_holder();
        const auto original = holder->get_block()->SerializeAsString();
        ASSERT_TRUE(holder->prepare_serialized_block(multi, 1).ok());
        EXPECT_TRUE(holder->serialized_block().empty());
        EXPECT_EQ(holder->get_block()->SerializeAsString(), original);
    }
}

TEST(BroadcastProtobufTest, RejectsMissingRequiredFieldsBeforeSerializedRequestBypassesValidation) {
    auto valid_holder = make_holder();
    ASSERT_TRUE(valid_holder->prepare_serialized_block(false, 2).ok());
    auto header = make_header();
    header.clear_packet_seq();
    brpc::SerializedRequest request;
    EXPECT_FALSE(ExchangeSinkBuffer::_serialize_broadcast_request(
                         header, {{.block_holder = valid_holder, .eos = false}}, &request)
                         .ok());
    EXPECT_TRUE(request.serialized_data().empty());
}

TEST(BroadcastProtobufTest, QueueAccountsSharedCacheOnceAndReleasesAdmissionCharge) {
    auto dependency = Dependency::create_shared(1, 1, "broadcast", true);
    auto limiter = BroadcastPBlockHolderMemLimiter::create_shared(dependency);
    auto holder = make_holder();
    ASSERT_TRUE(holder->prepare_serialized_block(false, 8).ok());
    const auto charge =
            holder->get_block()->column_values().size() + holder->serialized_block().size();
    limiter->_total_queue_buffer_size_limit = charge;
    limiter->acquire(*holder);
    EXPECT_EQ(limiter->_total_queue_buffer_size, charge);
    EXPECT_EQ(limiter->_total_queue_blocks_count, 1);
    EXPECT_EQ(dependency->is_blocked_by(nullptr), dependency.get());

    std::vector<std::shared_ptr<BroadcastPBlockHolder>> channels(8, holder);
    holder.reset();
    EXPECT_EQ(limiter->_total_queue_buffer_size, charge);
    // Release must subtract the admission charge even if the original HTTP path
    // moves column_values out of the PBlock before the holder is destroyed.
    channels.front()->get_block()->clear_column_values();
    channels.clear();
    EXPECT_EQ(limiter->_total_queue_buffer_size, 0);
    EXPECT_EQ(limiter->_total_queue_blocks_count, 0);
    EXPECT_EQ(dependency->is_blocked_by(nullptr), nullptr);
}

TEST(BroadcastProtobufTest, RejectsOversizedBatchBeforeBrpcBypassesProtobufSizeCheck) {
    auto holder = BroadcastPBlockHolder::create_shared();
    ASSERT_EQ(holder->_serialized_block.append(std::string(1024 * 1024, 'x')), 0);
    // Grow references, not a 2 GiB allocation. Only the size check reads this buffer.
    while (holder->_serialized_block.size() < MIN_HTTP_BRPC_SIZE) {
        auto copy = holder->_serialized_block;
        holder->_serialized_block.append(copy);
    }
    brpc::SerializedRequest request;
    auto status = ExchangeSinkBuffer::_serialize_broadcast_request(
            make_header(), {{.block_holder = holder, .eos = false}}, &request);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(request.serialized_data().empty());
}

class BroadcastService : public PBackendService {
public:
    bthread::CountdownEvent release {1};
    std::atomic<int> received {0};

    void release_requests() {
        if (!_released) {
            release.signal();
            _released = true;
        }
    }

    void transmit_block(google::protobuf::RpcController* controller,
                        const PTransmitDataParams* request, PTransmitDataResult* response,
                        google::protobuf::Closure* done) override {
        SCOPED_INIT_THREAD_CONTEXT();
        brpc::ClosureGuard guard(done);
        release.wait();
        const auto& block = request->has_block() ? request->block() : request->blocks(0);
        EXPECT_EQ(block.column_values(), std::string(64 * 1024, 'a'));
        EXPECT_TRUE(block.compressed());
        EXPECT_EQ(request->packet_seq(), request->finst_id().lo() + 10);
        received.fetch_add(1);
        if (request->finst_id().lo() == 1) {
            controller->SetFailed("injected transport failure");
        } else if (request->finst_id().lo() == 2) {
            Status::EndOfFile("receiver finished").to_protobuf(response->mutable_status());
        } else {
            Status::OK().to_protobuf(response->mutable_status());
        }
    }

private:
    bool _released = false;
};

auto make_counting_callback(std::atomic<int>& succeeded, std::atomic<int>& failed,
                            std::atomic<int>& eof) {
    auto callback = ExchangeSendCallback<PTransmitDataResult>::create_shared();
    callback->init(nullptr, false);
    callback->start_rpc_time = 0;
    callback->addFailedHandler([&](auto*, const auto&) { failed.fetch_add(1); });
    callback->addSuccessHandler([&](auto*, const auto&, const auto& result, const auto&) {
        if (Status::create(result.status()).template is<ErrorCode::END_OF_FILE>()) {
            eof.fetch_add(1);
        } else {
            EXPECT_TRUE(Status::create(result.status()).ok());
            succeeded.fetch_add(1);
        }
    });
    return callback;
}

TEST_P(BroadcastProtobufTest, AsyncRpcPreservesPayloadAndReleasesCancelledCallbackRequest) {
    BroadcastService service;
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    {
        // brpc registers process-lifetime built-in services on first startup.
        debug::ScopedLeakCheckDisabler disable_lsan;
        ASSERT_EQ(server.Start("127.0.0.1:0", nullptr), 0);
    }
    Defer stop([&] {
        server.Stop(0);
        server.Join();
    });
    Defer unblock([&] { service.release_requests(); });
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.max_retry = 0;
    options.timeout_ms = 10000;
    ASSERT_EQ(channel.Init(server.listen_address(), &options), 0);
    PBackendService_Stub stub(&channel);
    std::atomic<int> succeeded {0};
    std::atomic<int> failed {0};
    std::atomic<int> eof {0};
    std::vector<std::shared_ptr<ExchangeSendCallback<PTransmitDataResult>>> callbacks;
    std::vector<brpc::CallId> ids;
    std::vector<std::weak_ptr<brpc::SerializedRequest>> weak_requests;
    auto holder = make_holder();
    ASSERT_TRUE(holder->prepare_serialized_block(GetParam(), 4).ok());
    for (int i = 1; i <= 4; ++i) {
        auto request = std::make_shared<brpc::SerializedRequest>();
        ASSERT_TRUE(ExchangeSinkBuffer::_serialize_broadcast_request(
                            make_header(i, i + 10), {{.block_holder = holder, .eos = false}},
                            request.get())
                            .ok());
        auto callback = make_counting_callback(succeeded, failed, eof);
        ids.push_back(callback->cntl_->call_id());
        weak_requests.push_back(request);
        auto closure = AutoReleaseClosure<
                brpc::SerializedRequest,
                ExchangeSendCallback<PTransmitDataResult>>::create_unique(request, callback);
        transmit_serialized_block(&stub, std::move(closure));
        // Simulate query cancellation before the fourth destination replies.
        if (i != 4) {
            callbacks.push_back(std::move(callback));
        }
    }
    // No queue/producer owns the source block while these RPCs are outstanding.
    holder.reset();
    service.release_requests();
    for (auto id : ids) {
        brpc::Join(id);
    }
    EXPECT_EQ(service.received, 4);
    EXPECT_EQ(succeeded, 1);
    EXPECT_EQ(failed, 1);
    EXPECT_EQ(eof, 1);
    for (const auto& request : weak_requests) {
        EXPECT_TRUE(request.expired());
    }
}

INSTANTIATE_TEST_SUITE_P(BlockField, BroadcastProtobufTest, testing::Bool());

} // namespace
} // namespace doris
