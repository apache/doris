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
#include "common/logging.h"
#include "io_handler_data.h"
#include "request_cache.h"

#include <cassert>

namespace palo {

void
RequestCache::insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                     ClockT::time_point &expire) {
    VLOG(3) << "Adding id %d" << id;
    IdHandlerMap::iterator iter = m_id_map.find(id);
    assert(iter == m_id_map.end());
    CacheNode *node = new CacheNode(id, handler, dh);
    node->expire = expire;
    if (m_head == 0) {
        node->next = node->prev = 0;
        m_head = m_tail = node;
    }
    else {
        node->next = m_tail;
        node->next->prev = node;
        node->prev = 0;
        m_tail = node;
    }
    m_id_map[id] = node;
}

bool RequestCache::remove(uint32_t id, DispatchHandler *&handler) {
    VLOG(3) << "remove request_id from request_cache. [request_id=" << id << "]";
    IdHandlerMap::iterator iter = m_id_map.find(id);
    if (iter == m_id_map.end()) {
        VLOG(3) << "request_id not found in request cache"
                << "[request_id=" << id << "]";
        return false;
    }
    CacheNode *node = (*iter).second;
    if (node->prev == 0) {
        m_tail = node->next;
    } else {
        node->prev->next = node->next;
    }

    if (node->next == 0) {
        m_head = node->prev;
    } else {
        node->next->prev = node->prev;
    }
    m_id_map.erase(iter);
    handler = node->dh;
    delete node;
    return true;
}

bool RequestCache::get_next_timeout(ClockT::time_point &now, IOHandler *&handlerp,
                                    DispatchHandler *&dh,
                                    ClockT::time_point *next_timeout, uint32_t* header_id) {
    bool handler_removed = false;
    while (m_head && !handler_removed && m_head->expire <= now) {
        IdHandlerMap::iterator iter = m_id_map.find(m_head->id);
        assert(iter != m_id_map.end());
        CacheNode *node = m_head;
        if (m_head->prev) {
            m_head = m_head->prev;
            m_head->next = 0;
        }
        else
            m_head = m_tail = 0;
        m_id_map.erase(iter);
        if (node->handler != 0) {
            handlerp = node->handler;
            dh = node->dh;
            *header_id = node->id;
            handler_removed = true;
        }
        delete node;
    }
    if (m_head) {
        *next_timeout = m_head->expire;
    } else {
        *next_timeout = ClockT::time_point();
    }
    return handler_removed;
}

void RequestCache::purge_requests(IOHandler *handler, int32_t error) {
    for (CacheNode *node = m_tail; node != 0; node = node->next) {
        if (node->handler == handler) {
            std::string proxy = handler->get_proxy();
            EventPtr event;
            VLOG(3) << "purging request id from request cache."
                    << "[request_id=" << node->id << "]";
            if (proxy.empty()) {
                event = std::make_shared<Event>(Event::ERROR, handler->get_address(), error);
            } else {
                event = std::make_shared<Event>(Event::ERROR, handler->get_address(), proxy, error);
            }
            handler->deliver_event(event, node->dh);
            node->handler = 0;  // mark for deletion
        }
    }
}

} //nemespace palo
