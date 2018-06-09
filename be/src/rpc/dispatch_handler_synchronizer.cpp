// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

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
