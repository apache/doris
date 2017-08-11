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
#include "io_handler_data.h"
#include "reactor.h"
#include "reactor_factory.h"
#include "reactor_runner.h"
#include "error.h"
#include "file_utils.h"
#include "common/logging.h"

#include <cassert>
#include <cstdio>
#include <iostream>
#include <set>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
}

namespace palo {

Reactor::Reactor() {
    struct sockaddr_in addr;
    if ((epoll_fd = epoll_create(256)) < 0) {
        perror("epoll_create");
        exit(EXIT_FAILURE);
    }
    while (true) {
        /**
         * The following logic creates a UDP socket that is used to
         * interrupt epoll_wait so that it can reset its timeout
         * value
         */
        if ((m_interrupt_sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
            LOG(ERROR) << "socket() failure: %s" << strerror(errno);
            exit(EXIT_FAILURE);
        }
        // Set to non-blocking (are we sure we should do this?)
        FileUtils::set_flags(m_interrupt_sd, O_NONBLOCK);
        // create address structure to bind to - any available port - any address
        memset(&addr, 0, sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        // Arbitray ephemeral port that won't conflict with our reserved ports
        uint16_t port =
            (uint16_t)(49152 + std::uniform_int_distribution<>(0, 16382)(ReactorFactory::rng));
        addr.sin_port = htons(port);
        // bind socket
        if ((::bind(m_interrupt_sd, (sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
            if (errno == EADDRINUSE) {
                ::close(m_interrupt_sd);
                continue;
            }
            LOG(ERROR) << "bind(%s) failure: %s" <<
                       InetAddr::format(addr).c_str() << strerror(errno);
        }
        break;
    }
    // Connect to ourself
    // NOTE: Here we assume that any error returned by connect implies
    //       that it will be carried out asynchronously
    if (connect(m_interrupt_sd, (sockaddr *)&addr, sizeof(addr)) < 0) {
        LOG(INFO) << "connect(interrupt_sd) to port %d failed - %s" <<
                  (int)ntohs(addr.sin_port) << strerror(errno);
    }
    if (ReactorFactory::ms_epollet) {
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        event.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
            LOG(ERROR) << "add events to epoll failed."
                       << "[epollfd=" << epoll_fd << ", "
                       << "socket=" << m_interrupt_sd << ", "
                       << "events=" << event.events
                       << "error=" <<  strerror(errno);
            exit(EXIT_FAILURE);
        }
    }
    else {
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
            LOG(ERROR) << "add events to epoll failed."
                       << "[epollfd=" << epoll_fd << ", "
                       << "socket=" << m_interrupt_sd << ", "
                       << "events=" << event.events
                       << "error=" <<  strerror(errno);
            exit(EXIT_FAILURE);
        }
    }
    m_next_wakeup = ClockT::time_point();
}

void Reactor::handle_timeouts(PollTimeout &next_timeout) {
    std::vector<ExpireTimer> expired_timers;
    EventPtr event;
    ClockT::time_point now;
    ClockT::time_point next_req_timeout;
    ExpireTimer timer;
    uint32_t header_id;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            IOHandler *handler = 0;
            DispatchHandler *dh = 0;
            now = ClockT::now();
            while (m_request_cache.get_next_timeout(now, handler, dh,
                                                    &next_req_timeout, &header_id)) {
                event = std::make_shared<Event>(
                            Event::ERROR,
                            ((IOHandlerData *)handler)->get_address(),
                            error::REQUEST_TIMEOUT);
                event->set_proxy(((IOHandlerData *)handler)->get_proxy());
                event->header.id = header_id;
                handler->deliver_event(event, dh);
            }
            if (next_req_timeout != ClockT::time_point()) {
                next_timeout.set(now, next_req_timeout);
                m_next_wakeup = next_req_timeout;
            } else {
                next_timeout.set_indefinite();
                m_next_wakeup = ClockT::time_point();
            }
            if (!m_timer_heap.empty()) {
                ExpireTimer timer;
                while (!m_timer_heap.empty()) {
                    timer = m_timer_heap.top();
                    if (timer.expire_time > now) {
                        if (next_req_timeout == ClockT::time_point() ||
                                timer.expire_time < next_req_timeout) {
                            next_timeout.set(now, timer.expire_time);
                            m_next_wakeup = timer.expire_time;
                        }
                        break;
                    }
                    expired_timers.push_back(timer);
                    m_timer_heap.pop();
                }
            }
        }
        /**
         * Deliver timer events
         */
        for (size_t i = 0; i < expired_timers.size(); i++) {
            event = std::make_shared<Event>(Event::TIMER, error::OK);
            if (expired_timers[i].handler) {
                expired_timers[i].handler->handle(event);
            }
        }

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_timer_heap.empty()) {
                timer = m_timer_heap.top();
                if (now > timer.expire_time) {
                    continue;
                }
                if (next_req_timeout == ClockT::time_point()
                        || timer.expire_time < next_req_timeout) {
                    next_timeout.set(now, timer.expire_time);
                    m_next_wakeup = timer.expire_time;
                }
            }
            poll_loop_continue();
        }
        break;
    }
}

int Reactor::poll_loop_interrupt() {
    m_interrupt_in_progress = true;
    if (ReactorFactory::ms_epollet) {
        char buf[4];
        ssize_t n = 0;
        // Send and receive 1 byte to ourself to cause epoll_wait to return
        if (FileUtils::send(m_interrupt_sd, "1", 1) < 0) {
            LOG(ERROR) << "send message to socket failed."
                       << "[socket=" << m_interrupt_sd << ", "
                       << "[error=" << strerror(errno) << "]";
            return error::COMM_SEND_ERROR;
        }
        if ((n = FileUtils::recv(m_interrupt_sd, buf, 1)) == -1) {
            LOG(ERROR) << "recv message from socket failed."
                       << "[socket=" << m_interrupt_sd << ", "
                       << "[error=" << strerror(errno) << "]";
            return error::COMM_RECEIVE_ERROR;
        }
    } else {
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        event.events = EPOLLOUT;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
            LOG(ERROR) << "modify socket in epoll failed."
                       << "[epoll_fd=" << epoll_fd << ","
                       << "socket=" << m_interrupt_sd << ", "
                       << "error=" << strerror(errno) << "]";
            return error::COMM_POLL_ERROR;
        }
    }
    return error::OK;
}

int Reactor::poll_loop_continue() {
    if (!m_interrupt_in_progress) {
        m_interrupt_in_progress = false;
        return error::OK;
    }
    if (!ReactorFactory::ms_epollet) {
        struct epoll_event event;
        char buf[8];
        // Receive message(s) we sent to ourself in poll_loop_interrupt()
        while (FileUtils::recv(m_interrupt_sd, buf, 8) > 0) {}
        memset(&event, 0, sizeof(struct epoll_event));
        event.events = EPOLLERR | EPOLLHUP;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
            LOG(ERROR) << "epoll_ctl(EPOLL_CTL_MOD, sd=%d) : %s" << m_interrupt_sd <<
                       strerror(errno);
            return error::COMM_POLL_ERROR;
        }
    }
    m_interrupt_in_progress = false;
    return error::OK;
}

} //namespace palo
