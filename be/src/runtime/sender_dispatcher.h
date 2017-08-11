// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_RUNTIME_SENDER_DISPATCHER_H
#define BDG_PALO_BE_RUNTIME_SENDER_DISPATCHER_H

#include "common/logging.h"
#include "rpc/application_handler.h"
#include "rpc/application_queue.h"
#include "rpc/comm.h"
#include "rpc/comm_header.h"
#include "rpc/compat.h"
#include "rpc/dispatch_handler.h"
#include "rpc/error.h"
#include "rpc/event.h"
#include "rpc/inet_addr.h"
#include "rpc/io_handler.h"
#include "rpc/serialization.h"

namespace palo {

class ResponseHandler : public DispatchHandler {
public:
    ResponseHandler() {}

    virtual void handle(EventPtr &event_ptr) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (event_ptr->type == Event::ERROR) {
            _queue.push(event_ptr);
            _cond.notify_one();
        } else if (event_ptr->type == Event::MESSAGE) {
            _queue.push(event_ptr);
            _cond.notify_one();
        }
    }

    virtual void get_response(EventPtr &event_ptr) {
        std::unique_lock<std::mutex> lock(_mutex);
        while (_queue.empty()) {
            _cond.wait(lock);
        }
        event_ptr = _queue.front();
        _queue.pop();
    }

private:
    std::queue<EventPtr> _queue;
    std::mutex _mutex;
    std::condition_variable _cond;
};

}

#endif
