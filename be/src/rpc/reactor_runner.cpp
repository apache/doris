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
#include "clock.h"
#include "handler_map.h"
#include "io_handler.h"
#include "io_handler_data.h"
#include "reactor_factory.h"
#include "reactor_runner.h"
#include "file_utils.h"

#include "common/logging.h"
#include <chrono>
#include <thread>

extern "C" {
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
}

namespace palo {

bool palo::ReactorRunner::shutdown = false;
bool palo::ReactorRunner::record_arrival_time = false;
HandlerMapPtr palo::ReactorRunner::handler_map;

void ReactorRunner::operator()() {
    int n = 0;
    IOHandler *handler = 0;
    std::set<IOHandler *> removed_handlers;
    PollTimeout timeout;
    bool did_delay = false;
    ClockT::time_point arrival_time;
    bool got_arrival_time = false;
    std::vector<IOHandler*> handlers;
    uint32_t dispatch_delay {};
    struct epoll_event events[256];
    while ((n = epoll_wait(m_reactor->epoll_fd, events, 256,
                           timeout.get_millis())) >= 0 || errno == EINTR) {
        if (record_arrival_time)
            got_arrival_time = false;
        if (dispatch_delay)
            did_delay = false;
        m_reactor->get_removed_handlers(removed_handlers);
        if (!shutdown) {
            VLOG(3) << "epoll_wait returned " << n << " events";
        }
        for (int i=0; i<n; i++) {
            handler = (IOHandler *)events[i].data.ptr;
            if (handler && removed_handlers.count(handler) == 0) {
                // dispatch delay for testing
                if (dispatch_delay && !did_delay && (events[i].events & EPOLLIN)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds((int)dispatch_delay));
                    did_delay = true;
                }
                if (record_arrival_time && !got_arrival_time
                        && (events[i].events & EPOLLIN)) {
                    arrival_time = ClockT::now();
                    got_arrival_time = true;
                }
                if (handler->handle_event(&events[i], arrival_time))
                    removed_handlers.insert(handler);
            }
        }
        if (!removed_handlers.empty()) {
            cleanup_and_remove_handlers(removed_handlers);
        }
        m_reactor->handle_timeouts(timeout);
        if (shutdown) {
            return;
        }
    }
    if (!shutdown) {
        LOG(ERROR) << "epoll_wait " << m_reactor->epoll_fd << ","
                   << "error: " << strerror(errno);
    }
}

void
ReactorRunner::cleanup_and_remove_handlers(std::set<IOHandler *> &handlers) {
    for (auto handler : handlers) {
        assert(handler);
        if (!handler_map->destroy_ok(handler))
            continue;
        m_reactor->cancel_requests(handler);
        struct epoll_event event;
        memset(&event, 0, sizeof(struct epoll_event));
        if (epoll_ctl(m_reactor->epoll_fd, EPOLL_CTL_DEL, handler->get_sd(), &event) < 0) {
            if (!shutdown) {
                LOG(ERROR) << "delete socket from epoll failed."
                           << "[epoll_fd=" << m_reactor->epoll_fd << ","
                           << "socket=" << handler->get_sd() << ", "
                           << "error=" << strerror(errno) << "]";
            }
        }
        handler_map->purge_handler(handler);
    }
}

} //namespace palo
