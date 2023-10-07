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

#pragma once

#include <gen_cpp/Types_types.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <utility>

#include "common/global_types.h"
#include "common/status.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {
class RuntimeState;
class RowDescriptor;
class RuntimeProfile;
class QueryStatisticsRecvr;
class PTransmitDataParams;

namespace vectorized {
class VDataStreamRecvr;

class VDataStreamMgr {
public:
    VDataStreamMgr();
    ~VDataStreamMgr();

    std::shared_ptr<VDataStreamRecvr> create_recvr(
            RuntimeState* state, const RowDescriptor& row_desc,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
            RuntimeProfile* profile, bool is_merging,
            std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    std::shared_ptr<VDataStreamRecvr> find_recvr(const TUniqueId& fragment_instance_id,
                                                 PlanNodeId node_id, bool acquire_lock = true);

    Status deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    Status transmit_block(const PTransmitDataParams* request, ::google::protobuf::Closure** done);

    void cancel(const TUniqueId& fragment_instance_id, Status exec_status);

private:
    std::mutex _lock;
    using StreamMap = std::unordered_multimap<uint32_t, std::shared_ptr<VDataStreamRecvr>>;
    StreamMap _receiver_map;

    struct ComparisonOp {
        bool operator()(const std::pair<doris::TUniqueId, PlanNodeId>& a,
                        const std::pair<doris::TUniqueId, PlanNodeId>& b) const {
            if (a.first.hi < b.first.hi) {
                return true;
            } else if (a.first.hi > b.first.hi) {
                return false;
            } else if (a.first.lo < b.first.lo) {
                return true;
            } else if (a.first.lo > b.first.lo) {
                return false;
            }
            return a.second < b.second;
        }
    };
    using FragmentStreamSet = std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp>;
    FragmentStreamSet _fragment_stream_set;

    uint32_t get_hash_value(const TUniqueId& fragment_instance_id, PlanNodeId node_id);
};
} // namespace vectorized
} // namespace doris
