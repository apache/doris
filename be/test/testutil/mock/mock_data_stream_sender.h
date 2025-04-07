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

#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {
struct MockChannel : public Channel {
    MockChannel(pipeline::ExchangeSinkLocalState* parent, TUniqueId fragment_instance_id,
                bool is_local)
            : Channel(parent, TNetworkAddress {}, fragment_instance_id, 0) {
        _is_local = is_local;
    }

    Status _init_brpc_stub(RuntimeState* state) override { return Status::OK(); }
    Status _find_local_recvr(RuntimeState* state) override { return Status::OK(); }

    Status send_local_block(Block* block, bool eos, bool can_be_moved) override {
        Block nblock = *block;
        // local exchange should copy the block contented if use move == false
        // copy code from void VDataStreamRecvr::SenderQueue::add_block(Block* block, bool use_move)
        if (can_be_moved) {
            block->clear();
        } else {
            auto rows = block->rows();
            for (int i = 0; i < nblock.columns(); ++i) {
                nblock.get_by_position(i).column =
                        nblock.get_by_position(i).column->clone_resized(rows);
            }
        }
        if (!nblock.empty()) {
            RETURN_IF_ERROR(_send_block.merge(std::move(nblock)));
        }
        return Status::OK();
    }

    Status send_remote_block(std::unique_ptr<PBlock>&& block, bool eos = false) override {
        if (!block) {
            return Status::OK();
        }
        Block nblock;
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(nblock.deserialize(*_pblock));
        if (!nblock.empty()) {
            RETURN_IF_ERROR(_send_block.merge(std::move(nblock)));
        }

        return Status::OK();
    }
    Status send_broadcast_block(std::shared_ptr<BroadcastPBlockHolder>& block,
                                bool eos = false) override {
        Block nblock;
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(nblock.deserialize(*block->get_block()));
        if (!nblock.empty()) {
            RETURN_IF_ERROR(_send_block.merge(std::move(nblock)));
        }
        return Status::OK();
    }

    Block get_block() { return _send_block.to_block(); }

    MutableBlock _send_block;
};

} // namespace doris::vectorized