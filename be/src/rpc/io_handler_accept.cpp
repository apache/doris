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
#include "handler_map.h"
#include "io_handler_accept.h"
#include "io_handler_data.h"
#include "reactor_factory.h"
#include "reactor_runner.h"
#include "error.h"
#include "file_utils.h"
#include "common/logging.h"

#include <iostream>

extern "C" {
#include <errno.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

}
namespace palo {

bool IOHandlerAccept::handle_event(struct epoll_event *event,
                                   ClockT::time_point) {
    //DisplayEvent(event);
    return handle_incoming_connection();
}

bool IOHandlerAccept::handle_incoming_connection() {
    int sd = -1;
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(sockaddr_in);
    int one = 1;
    IOHandlerData *handler = 0;
    while (true) {
        if ((sd = accept(m_sd, (struct sockaddr *)&addr, &addr_len)) < 0) {
            if (errno == EAGAIN) {
                break;
            }
            LOG(ERROR) << "accept connection from client failed."
                       << "[error=" << strerror(errno) << "]";
            break;
        }
        VLOG(3) << "accepted incoming connection."
                << "[socket=" << m_sd << ", "
                << "client_addr=" << inet_ntoa(addr.sin_addr)
                << "client_port=" <<  ntohs(addr.sin_port) << "]";
        // Set to non-blocking
        FileUtils::set_flags(sd, O_NONBLOCK);
        if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
            LOG(ERROR) << "set socket TCP_NODELAY option failed."
                       << "[socket=" << sd << ", error=" << strerror(errno) << "]";
        }
        if (setsockopt(m_sd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) < 0) {
            LOG(ERROR) << "set socket SO_KEEPALIVE option failed."
                       << "[socket=" << sd << ", [error=" << strerror(errno) << "]";
        }
        int bufsize = 4*32768;
        if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
            LOG(ERROR) << "set socket SO_SNDBUF option failed. [error=" << strerror(errno) << "]";
        }
        if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize)) < 0) {
            LOG(ERROR) << "set sockopt SO_RCVBUF option failed. [error=" << strerror(errno) << "]";
        }
        DispatchHandlerPtr dhp;
        m_handler_factory->get_instance(dhp);
        handler = new IOHandlerData(sd, addr, dhp, true);
        m_handler_map->insert_handler(handler, true);
        int32_t error;
        if ((error = handler->start_epolling(poll_event::READ |
                                             poll_event::WRITE)) != error::OK) {
            LOG(ERROR) << "Problem starting polling on incoming connection - %s"
                       << error::get_text(error);
            ReactorRunner::handler_map->decrement_reference_count(handler);
            ReactorRunner::handler_map->decomission_handler(handler);
            return false;
        }
        if (ReactorFactory::proxy_master) {
            if ((error = ReactorRunner::handler_map->propagate_proxy_map(handler))
                    != error::OK) {
                LOG(ERROR) << "Problem sending proxy map to %s - %s"
                           << m_addr.format().c_str() << error::get_text(error);
                ReactorRunner::handler_map->decrement_reference_count(handler);
                return false;
            }
        }
        ReactorRunner::handler_map->decrement_reference_count(handler);
        EventPtr event = std::make_shared<Event>(Event::CONNECTION_ESTABLISHED, addr, error::OK);
        deliver_event(event);
    }
    return false;
}

} //namespace palo
