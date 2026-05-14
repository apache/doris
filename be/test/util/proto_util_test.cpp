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

namespace doris {
namespace {

struct ProtoUtilTestClosure {
    std::shared_ptr<brpc::Controller> cntl_ = std::make_shared<brpc::Controller>();
};

PTransmitDataParams make_transmit_request(const std::string& column_values) {
    PTransmitDataParams request;
    request.mutable_finst_id()->set_hi(1);
    request.mutable_finst_id()->set_lo(2);
    request.set_node_id(3);
    request.set_sender_id(4);
    request.set_be_number(5);
    request.set_eos(false);
    request.set_packet_seq(6);
    request.mutable_block()->set_column_values(column_values);
    return request;
}

PTransmitDataParams extract_request_from_attachment(brpc::Controller* cntl) {
    PTransmitDataParams extracted;
    auto status = attachment_extract_request_contain_block(&extracted, cntl);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return extracted;
}

} // namespace

TEST(ProtoUtilTest, EmbedAttachmentMovesOwnedColumnValuesByDefault) {
    const std::string column_values = "owned-column-values";
    auto request = make_transmit_request(column_values);
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status = request_embed_attachment_contain_blockv2(&request, closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_TRUE(request.block().column_values().empty());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_EQ(extracted.block().column_values(), column_values);
}

TEST(ProtoUtilTest, EmbedAttachmentUsesBorrowedColumnValuesWithoutMutation) {
    const std::string column_values = "borrowed-column-values";
    auto borrowed_owner = make_transmit_request(column_values);
    auto request = make_transmit_request(column_values);
    request.mutable_block()->clear_column_values();
    auto closure = std::make_unique<ProtoUtilTestClosure>();

    auto status =
            request_embed_attachmentv2(&request, borrowed_owner.block().column_values(), closure);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(borrowed_owner.block().column_values(), column_values);
    EXPECT_FALSE(request.block().has_column_values());

    auto extracted = extract_request_from_attachment(closure->cntl_.get());
    EXPECT_EQ(extracted.block().column_values(), column_values);
}

} // namespace doris
