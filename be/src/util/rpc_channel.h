// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include <stdint.h>

#include <deque>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "rpc/comm.h"
#include "rpc/comm_address.h"
#include "rpc/comm_buf.h"
#include "rpc/connection_manager.h"
#include "rpc/dispatch_handler.h"
#include "rpc/event.h"

namespace palo {

// this class is NOT thread safe.
class RpcChannel : public DispatchHandler {
public:
    RpcChannel(Comm* comm, ConnectionManagerPtr conn_mgr, uint64_t command);
    virtual ~RpcChannel();

    Status init(const std::string& host, int port,
                uint32_t connect_timeout_ms,
                uint32_t rpc_timeout_ms);

    // DispatchHandler handle, used to handle request event
    void handle(EventPtr& event) override {
        {
            std::lock_guard<std::mutex> l(_lock);
            _events.push_back(event);
        }
        _cond.notify_one();
    }

    Status wait_last_sent() {
        return _wait_for_last_sent();
    }
    // get last send's response
    Status get_response(const uint8_t** data, uint32_t* size);

    // make sure
    Status send_message(const uint8_t* data, uint32_t size);

private:
    Status _wait_for_last_sent();
    Status _send_message(const uint8_t* data, uint32_t size);

private:
    Comm* _comm;
    ConnectionManagerPtr _conn_mgr;
    uint64_t _command;

    CommAddress _addr;
    uint32_t _connect_timeout_ms = 500;
    uint32_t _rpc_timeout_ms = 1000;

    bool _rpc_in_flight = false;
    Status _rpc_status;

    // only valid when _rpc_in_flight is true
    CommBufPtr _cbp;
    EventPtr _last_event;

    // lock, protect variables
    std::mutex _lock;
    std::condition_variable _cond;
    std::deque<EventPtr> _events;
};

using RpcChannelPtr = std::shared_ptr<RpcChannel>;

}
