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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/data-stream-mgr.cc
// and modified by Doris

#include "runtime/data_stream_mgr.h"

#include <iostream>
#include <thread>

#include "gen_cpp/BackendService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/types.pb.h" // PUniqueId
#include "runtime/data_stream_recvr.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(data_stream_receiver_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fragment_endpoint_count, MetricUnit::NOUNIT);

using std::mutex;
using std::shared_ptr;
using std::unique_lock;
using std::lock_guard;

DataStreamMgr::DataStreamMgr() {
    REGISTER_HOOK_METRIC(data_stream_receiver_count, [this]() {
        lock_guard<mutex> l(_lock);
        return _receiver_map.size();
    });
    REGISTER_HOOK_METRIC(fragment_endpoint_count, [this]() {
        lock_guard<mutex> l(_lock);
        return _fragment_stream_set.size();
    });
}

DataStreamMgr::~DataStreamMgr() {
    DEREGISTER_HOOK_METRIC(data_stream_receiver_count);
    DEREGISTER_HOOK_METRIC(fragment_endpoint_count);
}
inline uint32_t DataStreamMgr::get_hash_value(const TUniqueId& fragment_instance_id,
                                              PlanNodeId node_id) {
    uint32_t value = RawValue::get_hash_value(&fragment_instance_id.lo, TYPE_BIGINT, 0);
    value = RawValue::get_hash_value(&fragment_instance_id.hi, TYPE_BIGINT, value);
    value = RawValue::get_hash_value(&node_id, TYPE_INT, value);
    return value;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::create_recvr(
        RuntimeState* state, const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile,
        bool is_merging, std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr) {
    DCHECK(profile != nullptr);
    VLOG_FILE << "creating receiver for fragment=" << fragment_instance_id
              << ", node=" << dest_node_id;
    shared_ptr<DataStreamRecvr> recvr(
            new DataStreamRecvr(this, row_desc, fragment_instance_id, dest_node_id, num_senders,
                                is_merging, buffer_size, profile, sub_plan_query_statistics_recvr));
    uint32_t hash_value = get_hash_value(fragment_instance_id, dest_node_id);
    lock_guard<mutex> l(_lock);
    _fragment_stream_set.insert(std::make_pair(fragment_instance_id, dest_node_id));
    _receiver_map.insert(std::make_pair(hash_value, recvr));
    return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::find_recvr(const TUniqueId& fragment_instance_id,
                                                      PlanNodeId node_id, bool acquire_lock) {
    VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    if (acquire_lock) {
        _lock.lock();
    }
    std::pair<StreamMap::iterator, StreamMap::iterator> range =
            _receiver_map.equal_range(hash_value);
    while (range.first != range.second) {
        shared_ptr<DataStreamRecvr> recvr = range.first->second;
        if (recvr->fragment_instance_id() == fragment_instance_id &&
            recvr->dest_node_id() == node_id) {
            if (acquire_lock) {
                _lock.unlock();
            }
            return recvr;
        }
        ++range.first;
    }
    if (acquire_lock) {
        _lock.unlock();
    }
    return shared_ptr<DataStreamRecvr>();
}

Status DataStreamMgr::transmit_data(const PTransmitDataParams* request,
                                    ::google::protobuf::Closure** done) {
    const PUniqueId& finst_id = request->finst_id();
    TUniqueId t_finst_id;
    t_finst_id.hi = finst_id.hi();
    t_finst_id.lo = finst_id.lo();
    shared_ptr<DataStreamRecvr> recvr = find_recvr(t_finst_id, request->node_id());
    if (recvr == nullptr) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous nullptr if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK();
    }

    // request can only be used before calling recvr's add_batch or when request
    // is the last for the sender, because request maybe released after it's batch
    // is consumed by ExchangeNode.
    if (request->has_query_statistics()) {
        recvr->add_sub_plan_statistics(request->query_statistics(), request->sender_id());
    }

    bool eos = request->eos();
    if (request->has_row_batch()) {
        recvr->add_batch(request->row_batch(), request->sender_id(), request->be_number(),
                         request->packet_seq(), eos ? nullptr : done);
    }

    if (eos) {
        recvr->remove_sender(request->sender_id(), request->be_number());
    }
    return Status::OK();
}

Status DataStreamMgr::deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    std::shared_ptr<DataStreamRecvr> targert_recvr;
    VLOG_QUERY << "deregister_recvr(): fragment_instance_id=" << fragment_instance_id
               << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    {
        lock_guard<mutex> l(_lock);
        std::pair<StreamMap::iterator, StreamMap::iterator> range =
                _receiver_map.equal_range(hash_value);
        while (range.first != range.second) {
            const shared_ptr<DataStreamRecvr>& recvr = range.first->second;
            if (recvr->fragment_instance_id() == fragment_instance_id &&
                recvr->dest_node_id() == node_id) {
                targert_recvr = recvr;
                _fragment_stream_set.erase(
                        std::make_pair(recvr->fragment_instance_id(), recvr->dest_node_id()));
                _receiver_map.erase(range.first);
                break;
            }
            ++range.first;
        }
    }

    // Notify concurrent add_data() requests that the stream has been terminated.
    // cancel_stream maybe take a long time, so we handle it out of lock.
    if (targert_recvr) {
        targert_recvr->cancel_stream();
        return Status::OK();
    } else {
        std::stringstream err;
        err << "unknown row receiver id: fragment_instance_id=" << fragment_instance_id
            << " node_id=" << node_id;
        LOG(ERROR) << err.str();
        return Status::InternalError(err.str());
    }
}

void DataStreamMgr::cancel(const TUniqueId& fragment_instance_id) {
    VLOG_QUERY << "cancelling all streams for fragment=" << fragment_instance_id;
    std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
    {
        lock_guard<mutex> l(_lock);
        FragmentStreamSet::iterator i =
                _fragment_stream_set.lower_bound(std::make_pair(fragment_instance_id, 0));
        while (i != _fragment_stream_set.end() && i->first == fragment_instance_id) {
            shared_ptr<DataStreamRecvr> recvr = find_recvr(i->first, i->second, false);
            if (recvr == nullptr) {
                // keep going but at least log it
                std::stringstream err;
                err << "cancel(): missing in stream_map: fragment=" << i->first
                    << " node=" << i->second;
                LOG(ERROR) << err.str();
            } else {
                recvrs.push_back(recvr);
            }
            ++i;
        }
    }

    // cancel_stream maybe take a long time, so we handle it out of lock.
    for (auto& it : recvrs) {
        it->cancel_stream();
    }
}

} // namespace doris
