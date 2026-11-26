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

#include "exec/operator/exchange_sink_buffer.h"

#include <brpc/controller.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>

#include "util/defer_op.h"
#include "util/proto_util.h"

namespace doris {
namespace {

PBlock make_block(const std::string& column_values) {
    PBlock block;

    auto* int_meta = block.add_column_metas();
    int_meta->set_name("int_col");
    int_meta->set_type(PGenericType::INT32);
    int_meta->set_is_nullable(false);
    int_meta->set_be_exec_version(12);

    auto* struct_meta = block.add_column_metas();
    struct_meta->set_name("struct_col");
    struct_meta->set_type(PGenericType::STRUCT);
    struct_meta->set_is_nullable(true);
    struct_meta->set_result_is_nullable(true);
    struct_meta->set_function_name("named_struct");
    auto* child_meta = struct_meta->add_children();
    child_meta->set_name("child_string");
    child_meta->set_type(PGenericType::STRING);
    child_meta->set_is_nullable(true);

    block.set_column_values(column_values);
    block.set_be_exec_version(37);
    block.set_compressed(true);
    block.set_compression_type(segment_v2::CompressionTypePB::LZ4);
    block.set_uncompressed_size(4096);
    return block;
}

// Minimal closure shape accepted by the attachment helpers: they only touch cntl_.
struct ExchangeSinkBufferTestClosure {
    std::shared_ptr<brpc::Controller> cntl_ = std::make_shared<brpc::Controller>();
};

bool equal_except_column_values(const PBlock& expected, const PBlock& actual) {
    google::protobuf::util::MessageDifferencer differencer;
    differencer.IgnoreField(PBlock::descriptor()->FindFieldByName("column_values"));
    return differencer.Compare(expected, actual);
}

} // namespace

// borrow_block() copies every PBlock field by hand except column_values. Adding a field to
// PBlock must extend it, which this test pins down with the exact field set.
TEST(ExchangeSinkBufferTest, BorrowBlockCoversEveryPBlockField) {
    std::set<std::string> fields;
    for (int i = 0; i < PBlock::descriptor()->field_count(); ++i) {
        fields.insert(PBlock::descriptor()->field(i)->name());
    }
    EXPECT_EQ(fields,
              (std::set<std::string> {"column_metas", "column_values", "compressed",
                                      "uncompressed_size", "compression_type", "be_exec_version"}));
}

TEST(ExchangeSinkBufferTest, BorrowBlockCopiesMetadataAndBorrowsPayload) {
    const std::string column_values = "large-column-values";
    auto owner = make_block(column_values);
    const std::string* owner_payload = &owner.column_values();

    PTransmitDataParams request;
    // Return the borrows on every exit, so a failing assertion cannot turn into a double free.
    Defer give_back([&]() { release_borrowed_blocks(&request); });
    borrow_block(&owner, request.mutable_block());

    EXPECT_TRUE(equal_except_column_values(owner, request.block()));
    EXPECT_EQ(&request.block().column_values(), owner_payload);

    release_borrowed_blocks(&request);
    EXPECT_FALSE(request.has_block());
    EXPECT_EQ(request.blocks_size(), 0);
    EXPECT_EQ(&owner.column_values(), owner_payload);
    EXPECT_EQ(owner.column_values(), column_values);
    EXPECT_TRUE(equal_except_column_values(make_block(column_values), owner));
}

// column_metas is copied regardless of whether the block is unicast-owned or broadcast-shared,
// so the owner keeps its own metas untouched; only column_values is borrowed.
TEST(ExchangeSinkBufferTest, BorrowBlockCopiesMetadataWithoutMutatingOwner) {
    const std::string column_values = "owned-column-values";
    auto owner = make_block(column_values);
    const std::string* owner_payload = &owner.column_values();

    PTransmitDataParams request;
    Defer give_back([&]() { release_borrowed_blocks(&request); });
    borrow_block(&owner, request.add_blocks());

    EXPECT_TRUE(equal_except_column_values(make_block(column_values), request.blocks(0)));
    EXPECT_EQ(owner.column_metas_size(), 2);
    EXPECT_EQ(&request.blocks(0).column_values(), owner_payload);

    release_borrowed_blocks(&request);
    EXPECT_EQ(request.blocks_size(), 0);
    EXPECT_EQ(&owner.column_values(), owner_payload);
    EXPECT_EQ(owner.column_values(), column_values);

    // The request is reused: a second block's metas simply overwrite the pooled ones.
    auto next_owner = make_block("next");
    borrow_block(&next_owner, request.add_blocks());
    EXPECT_EQ(request.blocks(0).column_metas_size(), 2);
    EXPECT_EQ(request.blocks(0).column_metas(1).children_size(), 1);
    EXPECT_EQ(next_owner.column_metas_size(), 2);
    release_borrowed_blocks(&request);
}

TEST(ExchangeSinkBufferTest, ReleaseBorrowedBlocksReturnsEveryPayload) {
    auto first = make_block("first");
    auto second = make_block("second");

    PTransmitDataParams request;
    Defer give_back([&]() { release_borrowed_blocks(&request); });
    borrow_block(&first, request.add_blocks());
    borrow_block(&second, request.add_blocks());
    ASSERT_EQ(request.blocks_size(), 2);
    EXPECT_EQ(&request.blocks(0).column_values(), &first.column_values());
    EXPECT_EQ(&request.blocks(1).column_values(), &second.column_values());

    release_borrowed_blocks(&request);
    EXPECT_EQ(request.blocks_size(), 0);
    EXPECT_EQ(first.column_values(), "first");
    EXPECT_EQ(second.column_values(), "second");

    // The request is reused for the next rpc; borrowing again must start from a clean slate.
    borrow_block(&first, request.add_blocks());
    ASSERT_EQ(request.blocks_size(), 1);
    EXPECT_EQ(request.blocks(0).column_metas_size(), 2);
    release_borrowed_blocks(&request);
}

// Mirrors a broadcast http send: two channels borrow the same shared block, one of them goes
// through the attachment path, and the shared block as well as the other channel's request are
// untouched. The receiver then extracts the payload the way transmit_block_by_http does, and the
// PBlock::CopyFrom of VDataStreamMgr keeps it.
TEST(ExchangeSinkBufferTest, HttpSendDoesNotMutateSharedBlock) {
    const std::string column_values = "broadcast-column-values";
    auto shared = make_block(column_values);

    PTransmitDataParams http_request;
    PTransmitDataParams other_request;
    Defer give_back([&]() {
        release_borrowed_blocks(&http_request);
        release_borrowed_blocks(&other_request);
    });
    for (auto* request : {&http_request, &other_request}) {
        request->mutable_finst_id()->set_hi(1);
        request->mutable_finst_id()->set_lo(2);
        request->set_node_id(3);
        request->set_sender_id(4);
        request->set_be_number(5);
        request->set_eos(false);
        request->set_packet_seq(6);
    }
    borrow_block(&shared, http_request.mutable_block());
    borrow_block(&shared, other_request.mutable_block());

    auto closure = std::make_unique<ExchangeSinkBufferTestClosure>();
    auto status = request_embed_attachment_borrowed_blocksv2(&http_request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(shared.column_values(), column_values);
    EXPECT_EQ(&http_request.block().column_values(), &shared.column_values());
    EXPECT_EQ(&other_request.block().column_values(), &shared.column_values());
    release_borrowed_blocks(&http_request);
    release_borrowed_blocks(&other_request);
    EXPECT_EQ(shared.column_values(), column_values);

    PTransmitDataParams extracted;
    status = attachment_extract_request_contain_block(&extracted, closure->cntl_.get());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(extracted.packet_seq(), 6);
    ASSERT_TRUE(extracted.has_block());
    EXPECT_TRUE(equal_except_column_values(shared, extracted.block()));
    EXPECT_EQ(extracted.block().column_values(), column_values);

    PBlock copied;
    copied.CopyFrom(extracted.block());
    EXPECT_EQ(copied.column_values(), column_values);
}

} // namespace doris
