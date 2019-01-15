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

#include "runtime/data_stream_mgr.h"

#include <iostream>
#include <boost/functional/hash.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/row_batch.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

#include "gen_cpp/types.pb.h" // PUniqueId
#include "gen_cpp/BackendService.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

using boost::mutex;
using boost::shared_ptr;
using boost::unique_lock;
using boost::try_mutex;
using boost::lock_guard;

inline uint32_t DataStreamMgr::get_hash_value(
        const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    uint32_t value = RawValue::get_hash_value(&fragment_instance_id.lo, TYPE_BIGINT, 0);
    value = RawValue::get_hash_value(&fragment_instance_id.hi, TYPE_BIGINT, value);
    value = RawValue::get_hash_value(&node_id, TYPE_INT, value);
    return value;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::create_recvr(RuntimeState* state,
        const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, int buffer_size, RuntimeProfile* profile,
        bool is_merging, QueryStatistics* query_statistics) {
    DCHECK(profile != NULL);
    VLOG_FILE << "creating receiver for fragment="
            << fragment_instance_id << ", node=" << dest_node_id;
    shared_ptr<DataStreamRecvr> recvr(
            new DataStreamRecvr(this, state->instance_mem_tracker(), row_desc,
                fragment_instance_id, dest_node_id, num_senders, is_merging, buffer_size,
                profile, query_statistics));
    uint32_t hash_value = get_hash_value(fragment_instance_id, dest_node_id);
    lock_guard<mutex> l(_lock);
    _fragment_stream_set.insert(std::make_pair(fragment_instance_id, dest_node_id));
    _receiver_map.insert(std::make_pair(hash_value, recvr));
    return recvr;
}

shared_ptr<DataStreamRecvr> DataStreamMgr::find_recvr(
        const TUniqueId& fragment_instance_id, PlanNodeId node_id, bool acquire_lock) {
    VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id
            << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    if (acquire_lock) {
        _lock.lock();
    }
    std::pair<StreamMap::iterator, StreamMap::iterator> range =
            _receiver_map.equal_range(hash_value);
    while (range.first != range.second) {
        shared_ptr<DataStreamRecvr> recvr = range.first->second;
        if (recvr->fragment_instance_id() == fragment_instance_id
                && recvr->dest_node_id() == node_id) {
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

Status DataStreamMgr::add_data(
        const PUniqueId& finst_id, int32_t node_id,
        const PRowBatch& pb_batch, int32_t sender_id,
        int be_number, int64_t packet_seq,
        ::google::protobuf::Closure** done) {
    VLOG_ROW << "add_data(): fragment_instance_id=" << print_id(finst_id)
            << " node=" << node_id;
    TUniqueId t_finst_id;
    t_finst_id.hi = finst_id.hi();
    t_finst_id.lo = finst_id.lo();
    shared_ptr<DataStreamRecvr> recvr = find_recvr(t_finst_id, node_id);
    if (recvr == NULL) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous NULL if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK;
    }
    recvr->add_batch(pb_batch, sender_id, be_number, packet_seq, done);
    return Status::OK;
}

Status DataStreamMgr::update_query_statistics(const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                              int sender_id, const PQueryStatistics& query_statistics) {
    shared_ptr<DataStreamRecvr> recvr = find_recvr(fragment_instance_id, dest_node_id);
    if (recvr != NULL) {
        recvr->update_sub_plan_statistics(query_statistics, sender_id);
    }
    return Status::OK;
}

Status DataStreamMgr::close_sender(const TUniqueId& fragment_instance_id,
                                   PlanNodeId dest_node_id,
                                   int sender_id, 
                                   int be_number) {
    VLOG_FILE << "close_sender(): fragment_instance_id=" << fragment_instance_id
        << ", node=" << dest_node_id;
    shared_ptr<DataStreamRecvr> recvr = find_recvr(fragment_instance_id, dest_node_id);
    if (recvr == NULL) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous NULL if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK;
    }
    recvr->remove_sender(sender_id, be_number);
    return Status::OK;
}

Status DataStreamMgr::deregister_recvr(
        const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    VLOG_QUERY << "deregister_recvr(): fragment_instance_id=" << fragment_instance_id
        << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    lock_guard<mutex> l(_lock);
    std::pair<StreamMap::iterator, StreamMap::iterator> range =
        _receiver_map.equal_range(hash_value);
    while (range.first != range.second) {
        const shared_ptr<DataStreamRecvr>& recvr = range.first->second;
        if (recvr->fragment_instance_id() == fragment_instance_id
                && recvr->dest_node_id() == node_id) {
            // Notify concurrent add_data() requests that the stream has been terminated.
            recvr->cancel_stream();
            _fragment_stream_set.erase(std::make_pair(recvr->fragment_instance_id(),
                        recvr->dest_node_id()));
            _receiver_map.erase(range.first);
            return Status::OK;
        }
        ++range.first;
    }

    std::stringstream err;
    err << "unknown row receiver id: fragment_instance_id=" << fragment_instance_id
        << " node_id=" << node_id;
    LOG(ERROR) << err.str();
    return Status(err.str());
}

void DataStreamMgr::cancel(const TUniqueId& fragment_instance_id) {
    VLOG_QUERY << "cancelling all streams for fragment=" << fragment_instance_id;
    lock_guard<mutex> l(_lock);
    FragmentStreamSet::iterator i =
        _fragment_stream_set.lower_bound(std::make_pair(fragment_instance_id, 0));
    while (i != _fragment_stream_set.end() && i->first == fragment_instance_id) {
        shared_ptr<DataStreamRecvr> recvr = find_recvr(i->first, i->second, false);
        if (recvr == NULL) {
            // keep going but at least log it
            std::stringstream err;
            err << "cancel(): missing in stream_map: fragment=" << i->first
                << " node=" << i->second;
            LOG(ERROR) << err.str();
        } else {
            recvr->cancel_stream();
        }
        ++i;
    }
}

}
