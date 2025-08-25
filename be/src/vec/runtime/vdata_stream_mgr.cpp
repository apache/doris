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
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "util/hash_util.hpp"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {

VDataStreamMgr::VDataStreamMgr() {
    // TODO: metric
}

VDataStreamMgr::~VDataStreamMgr() {
    // Has to call close here, because receiver will check if the receiver is closed.
    // It will core during graceful stop.
    auto receivers = std::vector<std::shared_ptr<VDataStreamRecvr>>();
    {
        std::shared_lock l(_lock);
        auto receiver_iterator = _receiver_map.begin();
        while (receiver_iterator != _receiver_map.end()) {
            // Could not call close directly, because during close method, it will remove itself
            // from the map, and modify the map, it will core.
            receivers.push_back(receiver_iterator->second);
            receiver_iterator++;
        }
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
        RuntimeState* state, RuntimeProfile::HighWaterMarkCounter* memory_used_counter,
        const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
        RuntimeProfile* profile, bool is_merging, size_t data_queue_capacity) {
    DCHECK(profile != nullptr);
    VLOG_FILE << "creating receiver for fragment=" << print_id(fragment_instance_id)
              << ", node=" << dest_node_id;
    std::shared_ptr<VDataStreamRecvr> recvr(new VDataStreamRecvr(
            this, memory_used_counter, state, fragment_instance_id, dest_node_id, num_senders,
            is_merging, profile, data_queue_capacity));
    uint32_t hash_value = get_hash_value(fragment_instance_id, dest_node_id);
    std::unique_lock l(_lock);
    _fragment_stream_set.insert(std::make_pair(fragment_instance_id, dest_node_id));
    _receiver_map.insert(std::make_pair(hash_value, recvr));
    return recvr;
}

Status VDataStreamMgr::find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id,
                                  std::shared_ptr<VDataStreamRecvr>* res, bool acquire_lock) {
    VLOG_ROW << "looking up fragment_instance_id=" << print_id(fragment_instance_id)
             << ", node=" << node_id;
    uint32_t hash_value = get_hash_value(fragment_instance_id, node_id);
    // Create lock guard and not own lock currently and will lock conditionally
    std::shared_lock recvr_lock(_lock, std::defer_lock);
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
        return Status::EndOfFile("data stream receiver closed");
    }

    // Lock the fragment context to ensure the runtime state and other objects are not
    // deconstructed
    auto ctx_lock = recvr->task_exec_ctx();
    if (ctx_lock == nullptr) {
        // Do not return internal error, because when query finished, the downstream node
        // may finish before upstream node. And the object maybe deconstructed. If return error
        // then the upstream node may report error status to FE, the query is failed.
        return Status::EndOfFile("data stream receiver is deconstructed");
    }

    bool eos = request->eos();
    if (!request->blocks().empty()) {
        for (int i = 0; i < request->blocks_size(); i++) {
            std::unique_ptr<PBlock> pblock_ptr = std::make_unique<PBlock>();
            pblock_ptr->Swap(const_cast<PBlock*>(&request->blocks(i)));
            auto pass_done = [&]() -> ::google::protobuf::Closure** {
                // If it is eos, no callback is needed, done can be nullptr
                if (eos) {
                    return nullptr;
                }
                // If it is the last block, a callback is needed, pass done
                if (i == request->blocks_size() - 1) {
                    return done;
                } else {
                    // If it is not the last block, the blocks in the request currently belong to the same queue,
                    // and the callback is handled by the done of the last block
                    return nullptr;
                }
            };
            RETURN_IF_ERROR(recvr->add_block(
                    std::move(pblock_ptr), request->sender_id(), request->be_number(),
                    request->packet_seq() - request->blocks_size() + i, pass_done(),
                    wait_for_worker, cpu_time_stop_watch.elapsed_time()));
        }
    }

    // old logic, for compatibility
    if (request->has_block()) {
        std::unique_ptr<PBlock> pblock_ptr {
                const_cast<PTransmitDataParams*>(request)->release_block()};
        RETURN_IF_ERROR(recvr->add_block(std::move(pblock_ptr), request->sender_id(),
                                         request->be_number(), request->packet_seq(),
                                         eos ? nullptr : done, wait_for_worker,
                                         cpu_time_stop_watch.elapsed_time()));
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
    uint32_t hash_value = get_hash_value(fragment_instance_id, node_id);
    {
        std::unique_lock l(_lock);
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
        return Status::InternalError("unknown row receiver id: fragment_instance_id={}, node_id={}",
                                     print_id(fragment_instance_id), node_id);
    }
}
} // namespace vectorized
} // namespace doris
