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

#include "util/proto_util.h"

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {
namespace {

struct ProtoUtilTestClosure {
    std::shared_ptr<brpc::Controller> cntl_ = std::make_shared<brpc::Controller>();
};

void fill_transmit_request_header(PTransmitDataParams* request) {
    request->mutable_finst_id()->set_hi(1);
    request->mutable_finst_id()->set_lo(2);
    request->set_node_id(3);
    request->set_sender_id(4);
    request->set_be_number(5);
    request->set_eos(false);
    request->set_packet_seq(6);
}

void fill_block(PBlock* block, const std::string& name, const std::string& column_values) {
    auto* meta = block->add_column_metas();
    meta->set_name(name);
    meta->set_type(PGenericType::INT32);
    block->set_column_values(column_values);
    block->set_uncompressed_size(column_values.size());
}

PTransmitDataParams extract_request_from_attachment(brpc::Controller* cntl) {
    PTransmitDataParams extracted;
    auto status = attachment_extract_request_contain_block(&extracted, cntl);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return extracted;
}

// Decode only the request section of the attachment, the way a receiver that takes the payload
// slot before parsing sees it: the wire contract for those receivers is that every payload block
// still carries column_values (present but empty).
PTransmitDataParams parse_wire_request(brpc::Controller* cntl) {
    const butil::IOBuf& io_buf = cntl->request_attachment();
    int64_t req_str_size = 0;
    io_buf.copy_to(&req_str_size, sizeof(req_str_size), 0);
    std::string req_str;
    io_buf.copy_to(&req_str, req_str_size, sizeof(req_str_size));
    PTransmitDataParams wire_request;
    EXPECT_TRUE(wire_request.ParseFromString(req_str));
    return wire_request;
}

// VDataStreamMgr::transmit_block() / SenderQueue::add_blocks() copy the extracted blocks with
// PBlock::CopyFrom, which only carries proto2 fields whose presence bit is set.
std::string column_values_after_copy_from(const PBlock& block) {
    PBlock copied;
    copied.CopyFrom(block);
    return copied.column_values();
}

} // namespace

// The tablet writer owns its request: the payload is moved into the attachment.
TEST(ProtoUtilTest, OwnedBlockPayloadIsMovedIntoAttachment) {
    const std::string column_values = "owned-column-values";
    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    fill_block(request.mutable_block(), "c0", column_values);
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachment_contain_blockv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_TRUE(request.block().has_column_values());
    EXPECT_TRUE(request.block().column_values().empty());
    EXPECT_TRUE(parse_wire_request(closure->cntl_.get()).block().has_column_values());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_EQ(extracted.packet_seq(), 6);
    ASSERT_TRUE(extracted.has_block());
    EXPECT_EQ(extracted.block().column_metas(0).name(), "c0");
    EXPECT_EQ(extracted.block().column_values(), column_values);
    EXPECT_EQ(column_values_after_copy_from(extracted.block()), column_values);
}

// The exchange sink only borrows the payload of its singular `block`: the owner is untouched by
// the http send and the request still borrows the very same string afterwards.
TEST(ProtoUtilTest, BorrowedSingleBlockPayloadStaysWithOwner) {
    const std::string column_values = "borrowed-column-values";
    PBlock owner;
    fill_block(&owner, "c0", column_values);
    const std::string* owner_payload = &owner.column_values();

    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    request.mutable_block()->mutable_column_metas()->CopyFrom(owner.column_metas());
    request.mutable_block()->set_allocated_column_values(owner.mutable_column_values());
    // Return the borrow on every exit, so a failing assertion cannot turn into a double free.
    Defer give_back([&]() { static_cast<void>(request.mutable_block()->release_column_values()); });
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachment_borrowed_blocksv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(&owner.column_values(), owner_payload);
    EXPECT_EQ(owner.column_values(), column_values);
    EXPECT_EQ(&request.block().column_values(), owner_payload);
    EXPECT_TRUE(parse_wire_request(closure->cntl_.get()).block().has_column_values());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_EQ(extracted.packet_seq(), 6);
    ASSERT_TRUE(extracted.has_block());
    EXPECT_EQ(extracted.blocks_size(), 0);
    EXPECT_EQ(extracted.block().column_values(), column_values);
    EXPECT_EQ(column_values_after_copy_from(extracted.block()), column_values);
}

// Multi-block exchange requests carry one attachment payload per element of `blocks`.
TEST(ProtoUtilTest, BorrowedMultiBlockPayloadsTravelInBlockOrder) {
    const std::string first_values = "first-column-values";
    const std::string second_values = "second-column-values";
    PBlock first_owner;
    PBlock second_owner;
    fill_block(&first_owner, "c0", first_values);
    fill_block(&second_owner, "c1", second_values);

    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    for (auto* owner : {&first_owner, &second_owner}) {
        auto* block = request.add_blocks();
        block->mutable_column_metas()->CopyFrom(owner->column_metas());
        block->set_allocated_column_values(owner->mutable_column_values());
    }
    Defer give_back([&]() {
        for (auto& block : *request.mutable_blocks()) {
            static_cast<void>(block.release_column_values());
        }
    });
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachment_borrowed_blocksv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(first_owner.column_values(), first_values);
    EXPECT_EQ(second_owner.column_values(), second_values);
    EXPECT_EQ(&request.blocks(0).column_values(), &first_owner.column_values());
    EXPECT_EQ(&request.blocks(1).column_values(), &second_owner.column_values());
    auto wire_request = parse_wire_request(closure->cntl_.get());
    ASSERT_EQ(wire_request.blocks_size(), 2);
    EXPECT_TRUE(wire_request.blocks(0).has_column_values());
    EXPECT_TRUE(wire_request.blocks(1).has_column_values());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_FALSE(extracted.has_block());
    ASSERT_EQ(extracted.blocks_size(), 2);
    EXPECT_EQ(extracted.blocks(0).column_metas(0).name(), "c0");
    EXPECT_EQ(extracted.blocks(0).column_values(), first_values);
    EXPECT_EQ(extracted.blocks(1).column_metas(0).name(), "c1");
    EXPECT_EQ(extracted.blocks(1).column_values(), second_values);
    EXPECT_EQ(column_values_after_copy_from(extracted.blocks(0)), first_values);
    EXPECT_EQ(column_values_after_copy_from(extracted.blocks(1)), second_values);
}

// The receiver marks the payload present itself, so the serialized request does not have to
// carry an empty column_values for the receiver-side PBlock::CopyFrom to keep the payload.
TEST(ProtoUtilTest, ReceiverMarksPayloadPresentWithoutWireField) {
    const std::string column_values = "payload-without-presence";
    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    fill_block(request.mutable_block(), "c0", column_values);
    request.mutable_block()->clear_column_values();
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachmentv2(&request, column_values, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_FALSE(request.block().has_column_values());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_TRUE(extracted.block().has_column_values());
    EXPECT_EQ(extracted.block().column_values(), column_values);
    EXPECT_EQ(column_values_after_copy_from(extracted.block()), column_values);
}

// The tablet writer request only has the singular `block`, so it goes through the generic
// payload_blocks() template on both sides.
TEST(ProtoUtilTest, TabletWriterRequestRoundTripsThroughGenericPayloadBlocks) {
    const std::string column_values = "tablet-writer-column-values";
    PTabletWriterAddBlockRequest request;
    request.mutable_id()->set_hi(1);
    request.mutable_id()->set_lo(2);
    request.set_index_id(3);
    request.set_sender_id(4);
    fill_block(request.mutable_block(), "c0", column_values);
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachment_contain_blockv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(request.block().column_values().empty());

    PTabletWriterAddBlockRequest extracted;
    status = attachment_extract_request_contain_block(&extracted, closure->cntl_.get());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(extracted.index_id(), 3);
    EXPECT_EQ(extracted.block().column_metas(0).name(), "c0");
    EXPECT_EQ(extracted.block().column_values(), column_values);
}

// A truncated attachment is rejected at the framing level instead of being accepted with a short
// or empty payload.
TEST(ProtoUtilTest, TruncatedAttachmentIsRejected) {
    const std::string column_values = "payload-that-gets-truncated";
    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    fill_block(request.mutable_block(), "c0", column_values);
    auto closure = std::make_unique<ProtoUtilTestClosure>();
    auto status = request_embed_attachment_contain_blockv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    butil::IOBuf full;
    full.append(closure->cntl_->request_attachment());
    const size_t full_size = full.size();

    // Payload section cut short.
    for (size_t keep :
         {full_size - 1, full_size - column_values.size() - 4, sizeof(int64_t) + 1, size_t(3)}) {
        butil::IOBuf truncated;
        full.append_to(&truncated, keep);
        closure->cntl_->request_attachment().swap(truncated);
        PTransmitDataParams extracted;
        status = attachment_extract_request_contain_block(&extracted, closure->cntl_.get());
        EXPECT_FALSE(status.ok()) << "kept " << keep << " of " << full_size << " bytes";
    }

    // The complete attachment still extracts.
    butil::IOBuf complete;
    complete.append(full);
    closure->cntl_->request_attachment().swap(complete);
    PTransmitDataParams extracted;
    status = attachment_extract_request_contain_block(&extracted, closure->cntl_.get());
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(extracted.block().column_values(), column_values);
}

TEST(ProtoUtilTest, EnableHttpSendBlockNeedsAPayloadBlock) {
    config::transfer_large_data_by_brpc = true;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("proto_util.enable_http_send_block.always_http");

    PTransmitDataParams request;
    fill_transmit_request_header(&request);
    // An eos-only request has nothing to put in an attachment.
    EXPECT_FALSE(enable_http_send_block(request));

    fill_block(request.add_blocks(), "c0", "small");
    EXPECT_TRUE(enable_http_send_block(request));
    request.clear_blocks();
    fill_block(request.mutable_block(), "c0", "small");
    EXPECT_TRUE(enable_http_send_block(request));

    DebugPoints::instance()->remove("proto_util.enable_http_send_block.always_http");
    EXPECT_FALSE(enable_http_send_block(request));
    config::enable_debug_points = false;
}

} // namespace doris
