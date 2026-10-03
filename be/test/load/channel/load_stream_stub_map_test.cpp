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

#include <thread>

#include "cpp/sync_point.h"
#include "exec/sink/load_stream_map_pool.h"
#include "exec/sink/load_stream_stub.h"
#include "load/channel/load_stream.h"
#include "runtime/workload_management/resource_context.h"
#include "util/defer_op.h"

namespace doris {

class LoadStreamMapPoolTest : public testing::Test {
public:
    LoadStreamMapPoolTest() = default;
    virtual ~LoadStreamMapPoolTest() = default;
};

TEST_F(LoadStreamMapPoolTest, test) {
    LoadStreamMapPool pool;
    int64_t src_id = 100;
    PUniqueId load_id;
    load_id.set_lo(1);
    load_id.set_hi(2);
    PUniqueId load_id2;
    load_id2.set_lo(2);
    load_id2.set_hi(1);
    auto streams_for_node1 = pool.get_or_create(load_id, src_id, 5, 2);
    auto streams_for_node2 = pool.get_or_create(load_id, src_id, 5, 2);
    EXPECT_EQ(1, pool.size());
    auto streams_for_node3 = pool.get_or_create(load_id2, src_id, 8, 1);
    EXPECT_EQ(2, pool.size());
    EXPECT_EQ(streams_for_node1, streams_for_node2);
    EXPECT_NE(streams_for_node1, streams_for_node3);

    EXPECT_EQ(5, streams_for_node1->get_or_create(101)->size());
    EXPECT_EQ(5, streams_for_node2->get_or_create(102)->size());
    EXPECT_EQ(8, streams_for_node3->get_or_create(101)->size());

    EXPECT_TRUE(streams_for_node3->release());
    EXPECT_EQ(1, pool.size());
    EXPECT_FALSE(streams_for_node1->release());
    EXPECT_EQ(1, pool.size());
    EXPECT_TRUE(streams_for_node2->release());
    EXPECT_EQ(0, pool.size());
}

TEST_F(LoadStreamMapPoolTest, AccumulateIndependentWriterSegmentCounts) {
    LoadStreamMap map(UniqueId(1, 2), 10, 1, 2, nullptr);
    std::thread first([&] { map.save_segments_for_tablet({{100, 2}, {200, 1}}); });
    std::thread second([&] { map.save_segments_for_tablet({{100, 3}, {300, 4}}); });
    first.join();
    second.join();
    EXPECT_EQ(5, map._segments_for_tablet.at(100));
    EXPECT_EQ(1, map._segments_for_tablet.at(200));
    EXPECT_EQ(4, map._segments_for_tablet.at(300));
}

TEST_F(LoadStreamMapPoolTest, ReceiveMetadataWithLoadMemoryContext) {
    auto resource = ResourceContext::create_shared();
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD,
                                                    "ReceiveMetadataWithLoadMemoryContext");
    resource->memory_context()->set_mem_tracker(tracker);
    std::shared_ptr<LoadStreamStub> stub;
    {
        SCOPED_ATTACH_TASK(resource);
        stub = std::make_shared<LoadStreamStub>(UniqueId(1, 2), 10,
                                                std::make_shared<IndexToTabletSchema>(),
                                                std::make_shared<IndexToEnableMoW>());
    }
    auto* sp = SyncPoint::get_instance();
    Defer reset([&] {
        sp->disable_processing();
        sp->clear_all_call_backs();
    });
    int parsed = 0;
    auto check_context = [&](auto&&) {
        EXPECT_EQ(thread_context()->resource_ctx()->memory_context()->mem_tracker(), tracker);
        ++parsed;
    };
    sp->set_call_back("LoadStreamReplyHandler::before_parse", check_context);
    sp->set_call_back("LoadStream::before_parse", check_context);
    sp->enable_processing();

    PLoadStreamResponse response;
    Status::OK().to_protobuf(response.mutable_status());
    response.mutable_write_context()->set_writer_id("writer");
    butil::IOBuf reply;
    reply.append(response.SerializeAsString());
    butil::IOBuf* messages[] = {&reply};
    LoadStreamReplyHandler handler(UniqueId(1, 2).to_proto(), 20, stub);
    EXPECT_EQ(handler.on_received_messages(brpc::INVALID_STREAM_ID, messages, 1), 0);
    EXPECT_EQ(parsed, 1);
    EXPECT_FALSE(thread_context()->is_attach_task());

    LoadStream stream(UniqueId(1, 2).to_proto(), nullptr, false);
    stream._resource_ctx = resource;
    PStreamHeader header;
    *header.mutable_load_id() = UniqueId(3, 4).to_proto();
    header.set_opcode(PStreamHeader::ADD_PARTIAL_ROWSET);
    // An unknown load is rejected after parsing, without requiring tablet writers.
    auto encoded = header.SerializeAsString();
    size_t header_size = encoded.size();
    size_t data_size = 0;
    butil::IOBuf request;
    request.append(&header_size, sizeof(header_size));
    request.append(encoded);
    request.append(&data_size, sizeof(data_size));
    messages[0] = &request;
    EXPECT_EQ(stream.on_received_messages(brpc::INVALID_STREAM_ID, messages, 1), 0);
    EXPECT_EQ(parsed, 2);
    EXPECT_FALSE(thread_context()->is_attach_task());
}

} // namespace doris
