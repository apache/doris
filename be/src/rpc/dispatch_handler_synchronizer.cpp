// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "dispatch_handler_synchronizer.h"
#include "protocol.h"
#include "error.h"
#include "common/logging.h"

namespace palo {

DispatchHandlerSynchronizer::DispatchHandlerSynchronizer() {
    return;
}

void DispatchHandlerSynchronizer::handle(EventPtr &event_ptr) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_receive_queue.push(event_ptr);
    m_cond.notify_one();
}

bool DispatchHandlerSynchronizer::wait_for_reply(EventPtr &event_ptr) {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cond.wait(lock, [this]() {
        return !m_receive_queue.empty();
    });
    event_ptr = m_receive_queue.front();
    m_receive_queue.pop();
    if (event_ptr->type == Event::MESSAGE
            && Protocol::response_code(event_ptr.get()) == error::OK) {
        return true;
    }
    return false;
}

bool DispatchHandlerSynchronizer::wait_for_connection() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cond.wait(lock, [this]() {
        return !m_receive_queue.empty();
    });
    EventPtr event = m_receive_queue.front();
    m_receive_queue.pop();
    if (event->type == Event::ERROR)
        HT_THROW(event->error, "");
    if (event->type == Event::DISCONNECT)
        return false;
    assert(event->type == Event::CONNECTION_ESTABLISHED);
    return true;
}

} //namespace palo
