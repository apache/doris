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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/data-stream-mgr.h
// and modified by Doris

#ifndef DORIS_BE_SRC_RUNTIME_DATA_STREAM_MGR_H
#define DORIS_BE_SRC_RUNTIME_DATA_STREAM_MGR_H

#include <condition_variable>
#include <list>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h" // for PlanNodeId
#include "runtime/query_statistics.h"
#include "util/runtime_profile.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {

class DescriptorTbl;
class DataStreamRecvr;
class RowBatch;
class RuntimeState;
class PRowBatch;
class PUniqueId;

// Singleton class which manages all incoming data streams at a backend node. It
// provides both producer and consumer functionality for each data stream.
// - dorisBackend service threads use this to add incoming data to streams
//   in response to TransmitData rpcs (add_data()) or to signal end-of-stream conditions
//   (close_sender()).
// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
//   which is created with create_recvr().
//
// DataStreamMgr also allows asynchronous cancellation of streams via cancel()
// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
// of the cancelled fragment id.
//
// TODO: The recv buffers used in DataStreamRecvr should count against
// per-query memory limits.
class DataStreamMgr {
public:
    DataStreamMgr();
    ~DataStreamMgr();

    // Create a receiver for a specific fragment_instance_id/node_id destination;
    // If is_merging is true, the receiver maintains a separate queue of incoming row
    // batches for each sender and merges the sorted streams from each sender into a
    // single stream.
    // Ownership of the receiver is shared between this DataStream mgr instance and the
    // caller.
    std::shared_ptr<DataStreamRecvr> create_recvr(
            RuntimeState* state, const RowDescriptor& row_desc,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
            int buffer_size, RuntimeProfile* profile, bool is_merging,
            std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    Status transmit_data(const PTransmitDataParams* request, ::google::protobuf::Closure** done);

    // Closes all receivers registered for fragment_instance_id immediately.
    void cancel(const TUniqueId& fragment_instance_id);

private:
    friend class DataStreamRecvr;
    friend class DataStreamSender;

    // protects all fields below
    std::mutex _lock;

    // map from hash value of fragment instance id/node id pair to stream receivers;
    // Ownership of the stream revcr is shared between this instance and the caller of
    // create_recvr().
    // we don't want to create a map<pair<TUniqueId, PlanNodeId>, DataStreamRecvr*>,
    // because that requires a bunch of copying of ids for lookup
    typedef std::unordered_multimap<uint32_t, std::shared_ptr<DataStreamRecvr>> StreamMap;
    StreamMap _receiver_map;

    // less-than ordering for pair<TUniqueId, PlanNodeId>
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

    // ordered set of registered streams' fragment instance id/node id
    typedef std::set<std::pair<TUniqueId, PlanNodeId>, ComparisonOp> FragmentStreamSet;
    FragmentStreamSet _fragment_stream_set;

    // Return the receiver for given fragment_instance_id/node_id,
    // or nullptr if not found. If 'acquire_lock' is false, assumes _lock is already being
    // held and won't try to acquire it.
    std::shared_ptr<DataStreamRecvr> find_recvr(const TUniqueId& fragment_instance_id,
                                                PlanNodeId node_id, bool acquire_lock = true);

    // Remove receiver block for fragment_instance_id/node_id from the map.
    Status deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    uint32_t get_hash_value(const TUniqueId& fragment_instance_id, PlanNodeId node_id);
};

} // namespace doris

#endif
