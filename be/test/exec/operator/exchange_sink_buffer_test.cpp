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

#include <gtest/gtest.h>

#include <string>

namespace doris {

namespace exchange_sink_buffer::detail {

void copy_block_metadata_without_column_values(const PBlock& src, PBlock* dst);
std::shared_ptr<PTransmitDataParams> make_http_request_without_column_values(
        const PTransmitDataParams& src);

} // namespace exchange_sink_buffer::detail

namespace {

PBlock make_block_with_column_values(const std::string& column_values) {
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

PTransmitDataParams make_transmit_data_params(const std::string& column_values) {
    PTransmitDataParams request;
    request.mutable_finst_id()->set_hi(1);
    request.mutable_finst_id()->set_lo(2);
    request.set_node_id(3);
    request.set_sender_id(4);
    request.set_be_number(5);
    request.set_eos(true);
    request.set_packet_seq(6);

    auto* query_statistics = request.mutable_query_statistics();
    query_statistics->set_scan_rows(7);
    query_statistics->set_scan_bytes(8);
    auto* node_statistics = query_statistics->add_nodes_statistics();
    node_statistics->set_node_id(9);
    node_statistics->set_peak_memory_bytes(10);

    request.set_transfer_by_attachment(false);
    request.mutable_query_id()->set_hi(11);
    request.mutable_query_id()->set_lo(12);
    request.mutable_exec_status()->set_status_code(13);
    request.mutable_exec_status()->add_error_msgs("exec status");
    request.mutable_block()->CopyFrom(make_block_with_column_values(column_values));
    return request;
}

} // namespace

TEST(ExchangeSinkBufferTest, CopyBlockMetadataWithoutColumnValues) {
    const std::string column_values = "large-column-values";
    auto src = make_block_with_column_values(column_values);
    PBlock dst;

    exchange_sink_buffer::detail::copy_block_metadata_without_column_values(src, &dst);

    EXPECT_EQ(dst.column_metas_size(), 2);
    EXPECT_EQ(dst.column_metas(0).name(), "int_col");
    EXPECT_EQ(dst.column_metas(0).type(), PGenericType::INT32);
    EXPECT_FALSE(dst.column_metas(0).is_nullable());
    EXPECT_EQ(dst.column_metas(0).be_exec_version(), 12);

    EXPECT_EQ(dst.column_metas(1).name(), "struct_col");
    EXPECT_EQ(dst.column_metas(1).type(), PGenericType::STRUCT);
    EXPECT_TRUE(dst.column_metas(1).is_nullable());
    EXPECT_TRUE(dst.column_metas(1).result_is_nullable());
    EXPECT_EQ(dst.column_metas(1).function_name(), "named_struct");
    ASSERT_EQ(dst.column_metas(1).children_size(), 1);
    EXPECT_EQ(dst.column_metas(1).children(0).name(), "child_string");
    EXPECT_EQ(dst.column_metas(1).children(0).type(), PGenericType::STRING);

    EXPECT_FALSE(dst.has_column_values());
    EXPECT_TRUE(src.has_column_values());
    EXPECT_EQ(src.column_values(), column_values);
    EXPECT_EQ(dst.be_exec_version(), 37);
    EXPECT_TRUE(dst.compressed());
    EXPECT_EQ(dst.compression_type(), segment_v2::CompressionTypePB::LZ4);
    EXPECT_EQ(dst.uncompressed_size(), 4096);
}

TEST(ExchangeSinkBufferTest, MakeHttpRequestWithoutColumnValues) {
    const std::string column_values = "borrowed-column-values";
    auto src = make_transmit_data_params(column_values);

    auto dst = exchange_sink_buffer::detail::make_http_request_without_column_values(src);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(dst->finst_id().hi(), 1);
    EXPECT_EQ(dst->finst_id().lo(), 2);
    EXPECT_EQ(dst->node_id(), 3);
    EXPECT_EQ(dst->sender_id(), 4);
    EXPECT_EQ(dst->be_number(), 5);
    EXPECT_TRUE(dst->eos());
    EXPECT_EQ(dst->packet_seq(), 6);

    ASSERT_TRUE(dst->has_query_statistics());
    EXPECT_EQ(dst->query_statistics().scan_rows(), 7);
    EXPECT_EQ(dst->query_statistics().scan_bytes(), 8);
    ASSERT_EQ(dst->query_statistics().nodes_statistics_size(), 1);
    EXPECT_EQ(dst->query_statistics().nodes_statistics(0).node_id(), 9);
    EXPECT_EQ(dst->query_statistics().nodes_statistics(0).peak_memory_bytes(), 10);

    EXPECT_TRUE(dst->has_transfer_by_attachment());
    EXPECT_FALSE(dst->transfer_by_attachment());
    ASSERT_TRUE(dst->has_query_id());
    EXPECT_EQ(dst->query_id().hi(), 11);
    EXPECT_EQ(dst->query_id().lo(), 12);
    ASSERT_TRUE(dst->has_exec_status());
    EXPECT_EQ(dst->exec_status().status_code(), 13);
    ASSERT_EQ(dst->exec_status().error_msgs_size(), 1);
    EXPECT_EQ(dst->exec_status().error_msgs(0), "exec status");

    ASSERT_TRUE(dst->has_block());
    EXPECT_EQ(dst->block().column_metas_size(), 2);
    EXPECT_FALSE(dst->block().has_column_values());
    EXPECT_EQ(dst->block().be_exec_version(), 37);
    EXPECT_TRUE(dst->block().compressed());
    EXPECT_EQ(dst->block().compression_type(), segment_v2::CompressionTypePB::LZ4);
    EXPECT_EQ(dst->block().uncompressed_size(), 4096);

    EXPECT_EQ(src.block().column_values(), column_values);
    EXPECT_EQ(dst->blocks_size(), 0);
    EXPECT_FALSE(dst->has_row_batch());
}

} // namespace doris
