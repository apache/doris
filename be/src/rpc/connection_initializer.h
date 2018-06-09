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

#ifndef BDG_PALO_BE_SRC_RPC_CONNECTION_INITIALIZER_H
#define BDG_PALO_BE_SRC_RPC_CONNECTION_INITIALIZER_H

#include "comm_buf.h"

namespace palo {

class Event;

/** Driver interface for connection initialization handshake in ConnectionManager.
 */
class ConnectionInitializer {
public:

    /** Creates a connection initialization message.
     * @return Initialization message (freed by caller)
     */
    virtual CommBuf *create_initialization_request() = 0;

    /** Process response to initialization message.
     * @param event Pointer to event object holding response message
     * @return <i>true</i> on success, <i>false</i> on failure
     */
    virtual bool process_initialization_response(Event *event) = 0;

    /** Command code (see CommHeader::command) for initialization response
     * message.
     * This method is used by the ConnectionManager to determine if a received
     * message is part of the initialization handshake.
     * @return Initialization command code
     */
    virtual uint64_t initialization_command() = 0;
};

/// Smart pointer to ConnectionInitializer
typedef std::shared_ptr<ConnectionInitializer> ConnectionInitializerPtr;

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_CONNECTION_INITIALIZER_H
