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

#include <parallel_hashmap/phmap.h>

#include <list>
#include <queue>
#include <shared_mutex>

#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace vectorized {
class PipChannel;
}

namespace pipeline {
using InstanceLoId = int64_t;
struct TransmitInfo {
    vectorized::PipChannel* channel;
    std::unique_ptr<PBlock> block;
    bool eos;
};

class PipelineFragmentContext;

// Each ExchangeSinkOperator have one ExchangeSinkBuffer
class ExchangeSinkBuffer {
public:
    ExchangeSinkBuffer(PUniqueId, int, PlanNodeId, int, PipelineFragmentContext*);
    ~ExchangeSinkBuffer();
    void register_sink(TUniqueId);
    Status add_block(TransmitInfo&& request);
    bool can_write() const;
    bool is_pending_finish() const;
    void close();

private:
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<std::mutex>>
            _instance_to_package_queue_mutex;
    phmap::flat_hash_map<InstanceLoId, std::queue<TransmitInfo, std::list<TransmitInfo>>>
            _instance_to_package_queue;
    using PackageSeq = int64_t;
    // must init zero
    phmap::flat_hash_map<InstanceLoId, PackageSeq> _instance_to_seq;
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<PTransmitDataParams>> _instance_to_request;
    phmap::flat_hash_map<InstanceLoId, PUniqueId> _instance_to_finst_id;
    phmap::flat_hash_map<InstanceLoId, bool> _instance_to_sending_by_pipeline;

    std::atomic<bool> _is_finishing;
    PUniqueId _query_id;
    PlanNodeId _dest_node_id;
    // Sender instance id, unique within a fragment. StreamSender save the variable
    int _sender_id;
    int _be_number;

    PipelineFragmentContext* _context;

private:
    Status _send_rpc(InstanceLoId);
    // must hold the _instance_to_package_queue_mutex[id] mutex to opera
    void _construct_request(InstanceLoId id);
    inline void _ended(InstanceLoId id);
    inline void _failed(InstanceLoId id, const std::string& err);
};

} // namespace pipeline
} // namespace doris