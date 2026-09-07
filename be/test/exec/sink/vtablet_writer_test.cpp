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

#include "exec/sink/writer/vtablet_writer.h"

#include <gtest/gtest.h>

#include <string>

namespace doris {

class IndexChannelTestAccessor {
public:
    static void set_replica_info(VTabletWriter* writer, int64_t tablet_id, int num_replicas,
                                 int required_replicas) {
        writer->_num_replicas = num_replicas;
        writer->_tablet_replica_info[tablet_id] = {num_replicas, required_replicas};
    }

    static void add_tablet(IndexChannel* index_channel, int64_t node_id, int64_t tablet_id) {
        index_channel->_tablets_by_channel[node_id].insert(tablet_id);
    }
};

TEST(IndexChannelTest, preservesFirstFailureStatus) {
    TDataSink sink;
    VExprContextSPtrs output_exprs;
    VTabletWriter writer(sink, output_exprs, nullptr, nullptr);
    IndexChannelTestAccessor::set_replica_info(&writer, 1, 3, 2);

    IndexChannel index_channel(&writer, 1, nullptr);
    VNodeChannel first_channel(&writer, &index_channel, 1, false);
    VNodeChannel second_channel(&writer, &index_channel, 2, false);
    IndexChannelTestAccessor::add_tablet(&index_channel, 1, 1);
    IndexChannelTestAccessor::add_tablet(&index_channel, 2, 1);

    index_channel.mark_as_failed(
            &first_channel,
            Status::InvalidArgument("Cannot found origin partitions in auto detect overwriting"));
    ASSERT_TRUE(index_channel.check_intolerable_failure().ok());

    index_channel.mark_as_failed(&second_channel, Status::InternalError("later channel failure"));
    const auto failure_status = index_channel.check_intolerable_failure();

    EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, failure_status.code());
    EXPECT_NE(std::string::npos, failure_status.to_string().find("Cannot found origin partitions"));
    EXPECT_EQ(std::string::npos, failure_status.to_string().find("later channel failure"));
}

} // namespace doris
