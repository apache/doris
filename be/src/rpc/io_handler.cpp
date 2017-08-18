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
#include "io_handler.h"
#include "reactor.h"
#include "reactor_runner.h"
#include "common/logging.h"

#include <cstdio>
#include <iostream>

extern "C" {
#include <errno.h>
}

namespace palo {

int IOHandler::start_epolling(int mode) {
    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    event.data.ptr = this;
    if (mode & poll_event::READ) {
        event.events |= EPOLLIN;
    }
    if (mode & poll_event::WRITE) {
        event.events |= EPOLLOUT;
    }
    if (ReactorFactory::ms_epollet) {
        event.events |= EPOLLRDHUP | EPOLLET;
    }
    m_epoll_interest = mode;
    if (epoll_ctl(m_reactor->epoll_fd, EPOLL_CTL_ADD, m_sd, &event) < 0) {
        LOG(ERROR) << "add events to epoll failed."
                   << "[epollfd=" << m_reactor->epoll_fd << ", "
                   << "socket=" << m_sd << ", "
                   << "events=" << event.events
                   << "error=" <<  strerror(errno);
        return error::COMM_POLL_ERROR;
    }
    return error::OK;
}

int IOHandler::add_epoll_interest(int mode) {
    m_epoll_interest |= mode;
    if (!ReactorFactory::ms_epollet) {
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        event.data.ptr = this;
        if (m_epoll_interest & poll_event::READ)
            event.events |= EPOLLIN;
        if (m_epoll_interest & poll_event::WRITE)
            event.events |= EPOLLOUT;
        if (epoll_ctl(m_reactor->epoll_fd, EPOLL_CTL_MOD, m_sd, &event) < 0) {
            LOG(ERROR) << "modify socket in epoll failed."
                       << "[epoll_fd=" << m_reactor->epoll_fd << ","
                       << "socket=" << m_sd << ", "
                       << "mode=" << mode << ","
                       << "m_epoll_interest" << m_epoll_interest << ","
                       << "error=" << strerror(errno) << "]";
            return error::COMM_POLL_ERROR;
        }
    }
    return error::OK;
}

int IOHandler::remove_epoll_interest(int mode) {
    m_epoll_interest &= ~mode;
    if (!ReactorFactory::ms_epollet) {
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        event.data.ptr = this;
        if (m_epoll_interest & poll_event::READ)
            event.events |= EPOLLIN;
        if (m_epoll_interest & poll_event::WRITE)
            event.events |= EPOLLOUT;
        if (epoll_ctl(m_reactor->epoll_fd, EPOLL_CTL_MOD, m_sd, &event) < 0) {
            LOG(ERROR) << "modify socket in epoll failed."
                       << "[epoll_fd=" << m_reactor->epoll_fd << ","
                       << "socket=" << m_sd << ", "
                       << "error=" << strerror(errno) << "]";
            return error::COMM_POLL_ERROR;
        }
    }
    return error::OK;
}

void IOHandler::display_event(struct epoll_event *event) {
    char buf[128];
    buf[0] = 0;
    if (event->events & EPOLLIN)
        strcat(buf, "EPOLLIN ");
    else if (event->events & EPOLLOUT)
        strcat(buf, "EPOLLOUT ");
    else if (event->events & EPOLLPRI)
        strcat(buf, "EPOLLPRI ");
    else if (event->events & EPOLLERR)
        strcat(buf, "EPOLLERR ");
    else if (event->events & EPOLLHUP)
        strcat(buf, "EPOLLHUP ");
    else if (ReactorFactory::ms_epollet && event->events & EPOLLRDHUP)
        strcat(buf, "EPOLLRDHUP ");
    else if (event->events & EPOLLET)
        strcat(buf, "EPOLLET ");
#if defined(EPOLLONESHOT)
    else if (event->events & EPOLLONESHOT)
        strcat(buf, "EPOLLONESHOT ");
#endif
    if (buf[0] == 0)
        sprintf(buf, "0x%x ", event->events);
    std::clog << "epoll events = " << buf << std::endl;
    return;
}

} //namespace palo
