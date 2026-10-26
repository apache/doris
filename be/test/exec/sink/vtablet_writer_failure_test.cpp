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

#include <gen_cpp/DataSinks_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/sink/writer/vtablet_writer.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

std::unique_ptr<VTabletWriter> create_writer() {
    TDataSink sink;
    sink.__isset.olap_table_sink = true;
    sink.olap_table_sink.num_replicas = 3;
    auto writer = std::make_unique<VTabletWriter>(sink, VExprContextSPtrs {}, nullptr, nullptr);
    writer->_num_replicas = 3;
    return writer;
}

TEST(VTabletWriterFailureTest, FailureAllowanceUsesSelectedReplicas) {
    auto writer = create_writer();
    IndexChannel channel(writer.get(), 10, nullptr);
    struct TestCase {
        int total;
        int required;
        int selected;
        int max_failed;
    };
    std::vector<TestCase> cases {{.total = 3, .required = 1, .selected = 1, .max_failed = 0},
                                 {.total = 3, .required = 1, .selected = 2, .max_failed = 1},
                                 {.total = 3, .required = 1, .selected = 3, .max_failed = 2},
                                 {.total = 3, .required = 2, .selected = 2, .max_failed = 0},
                                 {.total = 3, .required = 2, .selected = 3, .max_failed = 1},
                                 {.total = 5, .required = 2, .selected = 3, .max_failed = 1}};
    int64_t tablet_id = 100;
    for (const auto& test_case : cases) {
        writer->_tablet_replica_info.emplace(
                tablet_id,
                VTabletWriter::TabletReplicaInfo {.total_replicas_num = test_case.total,
                                                  .load_required_replicas_num = test_case.required,
                                                  .selected_replicas_num = test_case.selected});
        EXPECT_EQ(test_case.max_failed, channel._max_failed_replicas(tablet_id));
        ++tablet_id;
    }
}

TEST(VTabletWriterFailureTest, RepeatedFailureDoesNotConsumeAnotherDestination) {
    auto writer = create_writer();
    writer->_tablet_replica_info.emplace(
            100, VTabletWriter::TabletReplicaInfo {.total_replicas_num = 3,
                                                   .load_required_replicas_num = 1,
                                                   .selected_replicas_num = 2});
    IndexChannel channel(writer.get(), 10, nullptr);
    VNodeChannel node1(writer.get(), &channel, 1);
    VNodeChannel node2(writer.get(), &channel, 2);
    channel._tablets_by_channel[1].insert(100);
    channel._tablets_by_channel[2].insert(100);

    channel.mark_as_failed(&node1, "first failure", 100);
    EXPECT_TRUE(channel.check_intolerable_failure().ok());
    channel.mark_as_failed(&node1, "repeated failure", 100);
    EXPECT_TRUE(channel.check_intolerable_failure().ok());
    channel.mark_as_failed(&node2, "last selected replica failed", 100);
    EXPECT_FALSE(channel.check_intolerable_failure().ok());
}

TEST(VTabletWriterFailureTest, RejectsFewerSelectedReplicasThanRequired) {
    auto writer = create_writer();
    TOlapTableLocationParam thrift_location;
    TTabletLocation tablet_location;
    tablet_location.tablet_id = 100;
    tablet_location.node_ids = {1};
    thrift_location.tablets = {tablet_location};
    OlapTableLocationParam location(thrift_location);
    writer->_location = &location;
    writer->_tablet_replica_info.emplace(
            100, VTabletWriter::TabletReplicaInfo {.total_replicas_num = 3,
                                                   .load_required_replicas_num = 2,
                                                   .selected_replicas_num = 1});
    IndexChannel channel(writer.get(), 10, nullptr);
    TTabletWithPartition tablet;
    tablet.tablet_id = 100;
    tablet.partition_id = 20;

    auto status = channel.init(nullptr, {tablet});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("1 selected replicas, fewer than required 2"),
              std::string::npos);
}

TEST(VTabletWriterFailureTest, BootstrapTabletUsesSingleReplicaPolicy) {
    auto writer = create_writer();
    TOlapTableLocationParam thrift_location;
    TTabletLocation tablet_location;
    tablet_location.tablet_id = 0;
    tablet_location.node_ids = {1};
    thrift_location.tablets = {tablet_location};
    OlapTableLocationParam location(thrift_location);
    writer->_location = &location;

    ASSERT_TRUE(writer->_build_tablet_replica_info(0, nullptr).ok());
    const auto& replica_info = writer->_tablet_replica_info.at(0);
    EXPECT_EQ(1, replica_info.total_replicas_num);
    EXPECT_EQ(1, replica_info.load_required_replicas_num);
    EXPECT_EQ(1, replica_info.selected_replicas_num);
    IndexChannel channel(writer.get(), 10, nullptr);
    EXPECT_EQ(0, channel._max_failed_replicas(0));
}

TEST(VTabletWriterFailureTest, NonlocalTabletErrorIsReportedForGlobalValidation) {
    auto writer = create_writer();
    writer->_tablet_replica_info.emplace(
            200, VTabletWriter::TabletReplicaInfo {.total_replicas_num = 3,
                                                   .load_required_replicas_num = 2,
                                                   .selected_replicas_num = 3});
    IndexChannel channel(writer.get(), 10, nullptr);
    VNodeChannel node(writer.get(), &channel, 1);
    // This writer opened tablet 200, but the shared receiver returns tablet 100's late error.
    channel._tablets_by_channel[1].insert(200);
    channel.mark_as_failed(&node, "late error from another sender", 100);

    EXPECT_TRUE(channel.check_intolerable_failure().ok());
    MockRuntimeState state;
    channel.set_error_tablet_in_state(&state);
    auto error_infos = state.error_tablet_infos();
    ASSERT_EQ(1, error_infos.size());
    EXPECT_EQ(100, error_infos[0].tabletId);
}

TEST(VTabletWriterFailureTest, BootstrapOnlySenderPreservesRealTabletError) {
    auto writer = create_writer();
    IndexChannel channel(writer.get(), 10, nullptr);
    VNodeChannel node(writer.get(), &channel, 1);
    // Tablet 0 is excluded from backend bookkeeping, so this sender has no entry for BE1.
    channel.mark_as_failed(&node, "late error returned to bootstrap sender", 100);

    EXPECT_TRUE(channel.check_intolerable_failure().ok());
    MockRuntimeState state;
    channel.set_error_tablet_in_state(&state);
    auto error_infos = state.error_tablet_infos();
    ASSERT_EQ(1, error_infos.size());
    EXPECT_EQ(100, error_infos[0].tabletId);
}

TEST(VTabletWriterFailureTest, BootstrapOnlySenderRejectsOverallChannelFailure) {
    auto writer = create_writer();
    IndexChannel channel(writer.get(), 10, nullptr);
    VNodeChannel node(writer.get(), &channel, 1);
    channel.mark_as_failed(&node, "whole rpc failed", -1);

    auto status = channel.check_intolerable_failure();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("no local real tablet"), std::string::npos);
}

TEST(VTabletWriterFailureTest, OmittedReplicaPolicyUsesSelectedDestinations) {
    auto writer = create_writer();
    TOlapTableLocationParam thrift_location;
    TTabletLocation tablet_location;
    tablet_location.tablet_id = 100;
    tablet_location.node_ids = {1};
    thrift_location.tablets = {tablet_location};
    OlapTableLocationParam location(thrift_location);
    writer->_location = &location;
    ASSERT_TRUE(writer->_build_tablet_replica_info(100, nullptr).ok());
    const auto& replica_info = writer->_tablet_replica_info.at(100);
    EXPECT_EQ(3, replica_info.total_replicas_num);
    EXPECT_EQ(1, replica_info.load_required_replicas_num);
    EXPECT_EQ(1, replica_info.selected_replicas_num);
}

} // namespace
} // namespace doris
