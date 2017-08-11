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
#include "io_handler_raw.h"
#include "poll_event.h"
#include "reactor_runner.h"
#include "error.h"
#include "file_utils.h"
#include "inet_addr.h"

#include <cassert>
#include <iostream>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
}

namespace palo {

bool
IOHandlerRaw::handle_event(struct epoll_event *event,
                           ClockT::time_point arrival_time) {
    //display_event(event);
    try {
        if (ReactorFactory::ms_epollet && event->events & EPOLLRDHUP) {
            VLOG(3) << "Received EPOLLRDHUP on descriptor %d (%s:%d)" << m_sd <<
                    inet_ntoa(m_addr.sin_addr) << ntohs(m_addr.sin_port);
            ReactorRunner::handler_map->decomission_handler(this);
            return true;
        }
        if (event->events & EPOLLERR) {
            LOG(INFO) << "Received EPOLLERR on descriptor %d (%s:%d)" << m_sd <<
                      inet_ntoa(m_addr.sin_addr) << ntohs(m_addr.sin_port);
            ReactorRunner::handler_map->decomission_handler(this);
            return true;
        }
        if (event->events & EPOLLHUP) {
            VLOG(3) << "Received EPOLLHUP on descriptor %d (%s:%d)" << m_sd <<
                    inet_ntoa(m_addr.sin_addr) << ntohs(m_addr.sin_port);
            ReactorRunner::handler_map->decomission_handler(this);
            return true;
        }
        int events {};
        if (event->events & EPOLLOUT)
            events |= poll_event::WRITE;
        if (event->events & EPOLLIN)
            events |= poll_event::READ;
        if (!m_handler->handle(m_sd, events))
            return true;
        update_poll_interest();
    }
    catch (palo::Exception &e) {
        LOG(ERROR)  << e ;
        ReactorRunner::handler_map->decomission_handler(this);
        return true;
    }
    return m_error != error::OK;
}

void IOHandlerRaw::update_poll_interest() {
    int error = 0;
    int new_interest = m_handler->poll_interest(m_sd);
    int changed = new_interest ^ m_epoll_interest;
    int turn_off = changed ^ new_interest;
    int turn_on  = changed ^ m_epoll_interest;
    int mask = ~(turn_off & turn_on);
    turn_off &= mask;
    turn_on &= mask;
    if (turn_off) {
        VLOG(3) << "Turning poll interest OFF:  0x%x" << turn_off;
        if ((error = remove_epoll_interest(turn_off)) != error::OK) {
            if (m_error == error::OK)
                m_error = error;
            return;
        }
    }
    if (turn_on) {
        VLOG(3) << "Turning poll interest ON:  0x%x" << turn_on;
        if ((error = add_epoll_interest(turn_on)) != error::OK) {
            if (m_error == error::OK)
                m_error = error;
            return;
        }
    }
}

} //namespace palo
