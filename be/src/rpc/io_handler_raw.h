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

#ifndef BDG_PALO_BE_SRC_RPC_IO_HANDLER_RAW_H
#define BDG_PALO_BE_SRC_RPC_IO_HANDLER_RAW_H

#include "comm_buf.h"
#include "io_handler.h"
#include "raw_socket_handler.h"
#include "error.h"

namespace palo {

/// I/O handler for raw sockets.
class IOHandlerRaw : public IOHandler {
public:

    /// Constructor.
    /// Initializes handler by setting #m_socket_internally_created to
    /// <i>false</i> and setting #m_addr to <code>addr</code>.
    /// @param sd Socket descriptor
    /// @param addr Connection address for identification purposes
    /// @param rhp Poiner to raw socket handler
    IOHandlerRaw(int sd, const InetAddr &addr, RawSocketHandler *rhp)
        : IOHandler(sd), m_handler(rhp) {
            m_socket_internally_created = false;
            memcpy(&m_addr, &addr, sizeof(InetAddr));
        }

    /// Destructor.
    /// Calls the RawSocketHandler::deregister() function of #m_handler.
    virtual ~IOHandlerRaw() {
        m_handler->deregister(m_sd);
    }

    /// Handle <code>epoll()</code> interface events.
    /// This method is called by its reactor thread to handle I/O events.
    /// It handles <code>EPOLLRDHUP</code>, <code>EPOLLERR</code>, and
    /// <code>EPOLLHUP</code> events by decomissing the handler and returning
    /// <i>true</i> causing the reactor runner to remove the socket from the
    /// event loop.  It handles <code>EPOLLIN</code> and <code>EPOLLOUT</code>
    /// events by passing them to the RawSocketHandler::handle() function of
    /// #m_handler and then calling update_poll_interest() to update the polling
    /// interest.
    /// @param event Pointer to <code>epoll_event</code> structure describing
    /// event
    /// @param arrival_time Time of event arrival
    /// @return <i>false</i> on success, <i>true</i> if error encountered and
    /// handler was decomissioned
    bool handle_event(struct epoll_event *event,
            ClockT::time_point arrival_time) override;

    /// Updates polling interest for socket.
    /// This function is called after the RawSocketHandler::handle() function is
    /// called to updated the polling interest of the socket.  It first calls
    /// RawSocketHandler::poll_interest() to obtain the new polling interest.
    /// Based on the difference between the existing polling interest registered
    /// for the socket and the new interest returned by
    /// RawSocketHandler::poll_interest(), calls remove_epoll_interest() and
    /// add_epoll_interest() to adjust the polling interest for the socket.
    void update_poll_interest();

private:

    /// Raw socket handler
    RawSocketHandler *m_handler;
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_IO_HANDLER_RAW_H
