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
