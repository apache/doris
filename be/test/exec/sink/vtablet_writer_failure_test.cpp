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

#include "exec/sink/writer/vtablet_writer.h"

namespace doris {
namespace {

TEST(VTabletWriterFailureTest, LoadUnavailableReplicaFailsWithoutOpeningChannel) {
    TDataSink sink;
    sink.__isset.olap_table_sink = true;
    sink.olap_table_sink.num_replicas = 1;
    auto writer = std::make_unique<VTabletWriter>(sink, VExprContextSPtrs {}, nullptr, nullptr);
    writer->_num_replicas = 1;

    TTabletLocation tablet_location;
    tablet_location.__set_tablet_id(100);
    tablet_location.__set_node_ids({1});
    TOlapTableLocationParam thrift_location;
    thrift_location.tablets = {tablet_location};
    OlapTableLocationParam location(thrift_location);
    writer->_location = &location;
    writer->_tablet_replica_info[100] = std::make_pair(1, 1);

    TNodeInfo disabled_node;
    disabled_node.__set_id(1);
    disabled_node.__set_option(0);
    disabled_node.__set_host("load-disabled-host");
    disabled_node.__set_async_internal_port(8060);
    disabled_node.__set_is_load_available(false);
    TNodeInfo old_fe_node;
    old_fe_node.__set_id(2);
    old_fe_node.__set_option(0);
    old_fe_node.__set_host("old-fe-host");
    old_fe_node.__set_async_internal_port(8060);
    TPaloNodesInfo thrift_nodes;
    thrift_nodes.nodes = {disabled_node, old_fe_node};
    DorisNodesInfo nodes_info(thrift_nodes);
    writer->_nodes_info = &nodes_info;

    ASSERT_TRUE(nodes_info.find_node(2)->is_load_available);
    IndexChannel channel(writer.get(), 10, nullptr);
    TTabletWithPartition tablet;
    tablet.tablet_id = 100;
    tablet.partition_id = 20;

    auto status = channel.init(nullptr, {tablet});

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("backend is not available for load"), std::string::npos);
    EXPECT_EQ(0, channel.num_node_channels());
}

} // namespace
} // namespace doris
