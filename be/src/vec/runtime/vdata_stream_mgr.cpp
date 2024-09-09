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

#include "vec/runtime/vdata_stream_mgr.h"

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "util/hash_util.hpp"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris {
namespace vectorized {

VDataStreamMgr::VDataStreamMgr() {
    // TODO: metric
}

VDataStreamMgr::~VDataStreamMgr() {
    // Has to call close here, because receiver will check if the receiver is closed.
    // It will core during graceful stop.
    auto receivers = std::vector<std::shared_ptr<VDataStreamRecvr>>();
    auto receiver_iterator = _receiver_map.begin();
    while (receiver_iterator != _receiver_map.end()) {
        // Could not call close directly, because during close method, it will remove itself
        // from the map, and modify the map, it will core.
        receivers.push_back(receiver_iterator->second);
    }
    for (auto iter = receivers.begin(); iter != receivers.end(); ++iter) {
        (*iter)->close();
    }
}

inline uint32_t VDataStreamMgr::get_hash_value(const TUniqueId& fragment_instance_id,
                                               PlanNodeId node_id) {
    uint32_t value = HashUtil::hash(&fragment_instance_id.lo, 8, 0);
    value = HashUtil::hash(&fragment_instance_id.hi, 8, value);
    value = HashUtil::hash(&node_id, 4, value);
    return value;
}

std::shared_ptr<VDataStreamRecvr> VDataStreamMgr::create_recvr(
        RuntimeState* state, const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, RuntimeProfile* profile, bool is_merging) {
    DCHECK(profile != nullptr);
    VLOG_FILE << "creating receiver for fragment=" << print_id(fragment_instance_id)
              << ", node=" << dest_node_id;
    std::shared_ptr<VDataStreamRecvr> recvr(new VDataStreamRecvr(this, state, row_desc,
                                                                 fragment_instance_id, dest_node_id,
                                                                 num_senders, is_merging, profile));
    uint32_t hash_value = get_hash_value(fragment_instance_id, dest_node_id);
    std::lock_guard<std::mutex> l(_lock);
    _fragment_stream_set.insert(std::make_pair(fragment_instance_id, dest_node_id));
    _receiver_map.insert(std::make_pair(hash_value, recvr));
    return recvr;
}

Status VDataStreamMgr::find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id,
                                  std::shared_ptr<VDataStreamRecvr>* res, bool acquire_lock) {
    VLOG_ROW << "looking up fragment_instance_id=" << print_id(fragment_instance_id)
             << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    // Create lock guard and not own lock currently and will lock conditionally
    std::unique_lock recvr_lock(_lock, std::defer_lock);
    if (acquire_lock) {
        recvr_lock.lock();
    }
    std::pair<StreamMap::iterator, StreamMap::iterator> range =
            _receiver_map.equal_range(hash_value);
    while (range.first != range.second) {
        auto recvr = range.first->second;
        if (recvr->fragment_instance_id() == fragment_instance_id &&
            recvr->dest_node_id() == node_id) {
            *res = recvr;
            return Status::OK();
        }
        ++range.first;
    }
    return Status::InvalidArgument("Could not find local receiver for node {} with instance {}",
                                   node_id, print_id(fragment_instance_id));
}

Status VDataStreamMgr::transmit_block(const PTransmitDataParams* request,
                                      ::google::protobuf::Closure** done,
                                      const int64_t wait_for_worker) {
    const PUniqueId& finst_id = request->finst_id();
    TUniqueId t_finst_id;
    t_finst_id.hi = finst_id.hi();
    t_finst_id.lo = finst_id.lo();
    std::shared_ptr<VDataStreamRecvr> recvr = nullptr;
    ThreadCpuStopWatch cpu_time_stop_watch;
    cpu_time_stop_watch.start();
    static_cast<void>(find_recvr(t_finst_id, request->node_id(), &recvr));
    if (recvr == nullptr) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous NULL if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        //
        // e.g. for broadcast join build side, only one instance will build the hash table,
        // all other instances don't need build side data and will close the data stream receiver.
        //
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK(); // local data stream receiver closed
    }

    // Lock the fragment context to ensure the runtime state and other objects are not
    // deconstructed
    auto ctx_lock = recvr->task_exec_ctx();
    if (ctx_lock == nullptr) {
        // Do not return internal error, because when query finished, the downstream node
        // may finish before upstream node. And the object maybe deconstructed. If return error
        // then the upstream node may report error status to FE, the query is failed.
        return Status::OK(); // data stream receiver is deconstructed
    }

    bool eos = request->eos();
    if (request->has_block()) {
        RETURN_IF_ERROR(recvr->add_block(
                request->block(), request->sender_id(), request->be_number(), request->packet_seq(),
                eos ? nullptr : done, wait_for_worker, cpu_time_stop_watch.elapsed_time()));
    }

    if (eos) {
        Status exec_status =
                request->has_exec_status() ? Status::create(request->exec_status()) : Status::OK();
        recvr->remove_sender(request->sender_id(), request->be_number(), exec_status);
    }
    return Status::OK();
}

Status VDataStreamMgr::deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    std::shared_ptr<VDataStreamRecvr> targert_recvr;
    VLOG_QUERY << "deregister_recvr(): fragment_instance_id=" << print_id(fragment_instance_id)
               << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    {
        std::lock_guard<std::mutex> l(_lock);
        auto range = _receiver_map.equal_range(hash_value);
        while (range.first != range.second) {
            const std::shared_ptr<VDataStreamRecvr>& recvr = range.first->second;
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
        targert_recvr->cancel_stream(Status::OK());
        return Status::OK();
    } else {
        std::stringstream err;
        err << "unknown row receiver id: fragment_instance_id=" << print_id(fragment_instance_id)
            << " node_id=" << node_id;
        LOG(ERROR) << err.str();
        return Status::InternalError(err.str());
    }
}

void VDataStreamMgr::cancel(const TUniqueId& fragment_instance_id, Status exec_status) {
    VLOG_QUERY << "cancelling all streams for fragment=" << print_id(fragment_instance_id);
    std::vector<std::shared_ptr<VDataStreamRecvr>> recvrs;
    {
        std::lock_guard<std::mutex> l(_lock);
        FragmentStreamSet::iterator i =
                _fragment_stream_set.lower_bound(std::make_pair(fragment_instance_id, 0));
        while (i != _fragment_stream_set.end() && i->first == fragment_instance_id) {
            std::shared_ptr<VDataStreamRecvr> recvr;
            WARN_IF_ERROR(find_recvr(i->first, i->second, &recvr, false), "");
            if (recvr == nullptr) {
                // keep going but at least log it
                std::stringstream err;
                err << "cancel(): missing in stream_map: fragment=" << print_id(i->first)
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
        it->cancel_stream(exec_status);
    }
}

} // namespace vectorized
} // namespace doris