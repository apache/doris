// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "compat.h"
#include "io_handler_datagram.h"
#include "reactor_runner.h"
#include "error.h"
#include "file_utils.h"

#include <cassert>
#include <iostream>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
}

namespace palo {

bool IOHandlerDatagram::handle_event(struct epoll_event *event,
                                     ClockT::time_point arrival_time) {
    int error = 0;
    //DisplayEvent(event);
    if (event->events & EPOLLOUT) {
        if ((error = handle_write_readiness()) != error::OK) {
            EventPtr event_ptr = std::make_shared<Event>(Event::ERROR, m_addr, error);
            deliver_event(event_ptr);
            ReactorRunner::handler_map->decomission_handler(this);
            return true;
        }
    }
    if (event->events & EPOLLIN) {
        ssize_t nread = 0;
        ssize_t payload_len = 0;
        InetAddr addr;
        socklen_t fromlen = sizeof(struct sockaddr_in);
        while ((nread = FileUtils::recvfrom(m_sd, m_message, 65536,
                                            (struct sockaddr *)&addr, &fromlen)) != (ssize_t)-1) {
            EventPtr event_ptr = std::make_shared<Event>(Event::MESSAGE, addr, error::OK);
            try {
                event_ptr->load_message_header(m_message, (size_t)m_message[1]);
            }
            catch (palo::Exception &e) {
                LOG(ERROR) << e << " from " << addr.format();
                continue;
            }
            payload_len = nread - (ssize_t)event_ptr->header.header_len;
            event_ptr->payload_len = payload_len;
            event_ptr->payload = new uint8_t[payload_len];
            event_ptr->arrival_time = arrival_time;
            memcpy((void *)event_ptr->payload, m_message + event_ptr->header.header_len,
                   payload_len);
            deliver_event(event_ptr);
            fromlen = sizeof(struct sockaddr_in);
        }
        if (errno != EAGAIN) {
            LOG(ERROR) << "FileUtils::recvfrom(%d) failure : %s" << m_sd << strerror(errno);
            EventPtr event_ptr = std::make_shared<Event>(Event::ERROR, addr,
                                 error::COMM_RECEIVE_ERROR);
            deliver_event(event_ptr);
            ReactorRunner::handler_map->decomission_handler(this);
            return true;
        }
        return false;
    }
    if (event->events & EPOLLERR) {
        LOG(WARNING) << "Received EPOLLERR on descriptor " << m_sd << " ("
                     << m_addr.format() << ")" ;
        EventPtr event_ptr = std::make_shared<Event>(Event::ERROR, m_addr, error::COMM_POLL_ERROR);
        deliver_event(event_ptr);
        ReactorRunner::handler_map->decomission_handler(this);
        return true;
    }
    return false;
}

int IOHandlerDatagram::handle_write_readiness() {
    std::lock_guard<std::mutex> lock(m_mutex);
    int error = 0;
    if ((error = flush_send_queue()) != error::OK) {
        if (m_error == error::OK) {
            m_error = error;
        }
        return error;
    }
    // is this necessary?
    if (m_send_queue.empty()) {
        error = remove_epoll_interest(poll_event::WRITE);
    }
    if (error != error::OK && m_error == error::OK) {
        m_error = error;
    }
    return error;
}

int IOHandlerDatagram::send_message(const InetAddr &addr, CommBufPtr &cbp) {
    std::lock_guard<std::mutex> lock(m_mutex);
    int error = error::OK;
    bool initially_empty = m_send_queue.empty() ? true : false;
    //HT_LOG_ENTER;
    m_send_queue.push_back(SendRec(addr, cbp));
    if ((error = flush_send_queue()) != error::OK) {
        if (m_error == error::OK) {
            m_error = error;
        }
        return error;
    }
    if (initially_empty && !m_send_queue.empty()) {
        error = add_epoll_interest(poll_event::WRITE);
        //LOG(INFO) << "Adding Write interest";
    }
    else if (!initially_empty && m_send_queue.empty()) {
        error = remove_epoll_interest(poll_event::WRITE);
        //LOG(INFO) << "Removing Write interest";
    }
    if (error != error::OK && m_error == error::OK) {
        m_error = error;
    }
    return error;
}

int IOHandlerDatagram::flush_send_queue() {
    ssize_t nsent = 0;
    ssize_t tosend = 0;
    while (!m_send_queue.empty()) {
        SendRec &send_rec = m_send_queue.front();
        tosend = send_rec.second->data.size - (send_rec.second->data_ptr
                                               - send_rec.second->data.base);
        assert(tosend > 0);
        assert(send_rec.second->ext.base == 0);
        nsent = FileUtils::sendto(m_sd, send_rec.second->data_ptr, tosend,
                                  (sockaddr *)&send_rec.first,
                                  sizeof(struct sockaddr_in));
        if (nsent == (ssize_t)-1) {
            LOG(WARNING) << "FileUtils::sendto(%d, len=%d, addr=%s:%d) failed : %s" << m_sd <<
                         (int)tosend << inet_ntoa(send_rec.first.sin_addr) <<
                         ntohs(send_rec.first.sin_port) << strerror(errno);
            return error::COMM_SEND_ERROR;
        }
        else if (nsent < tosend) {
            LOG(WARNING) << "Only sent %d bytes" << (int)nsent;
            if (nsent == 0) {
                break;
            }
            send_rec.second->data_ptr += nsent;
            break;
        }
        // buffer written successfully, now remove from queue (destroys buffer)
        m_send_queue.pop_front();
    }
    return error::OK;
}

} //nemespace palo
