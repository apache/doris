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

#ifndef BDG_PALO_BE_SRC_RPC_RAW_SOCKET_HANDLER_H
#define BDG_PALO_BE_SRC_RPC_RAW_SOCKET_HANDLER_H

#include <string>

namespace palo {

/// Abstract base class for application raw socket handlers registered with
/// AsyncComm.  Allows applications to register a socket with the AsyncComm
/// polling mechanism and perform raw event handling on the socket.
class RawSocketHandler {
public:

    /// Destructor
    virtual ~RawSocketHandler() { return; }

    /// Handle socket event.
    /// @param sd Socket descriptor
    /// @param events Bitmask of polling events
    /// @see poll_event::Flags
    virtual bool handle(int sd, int events) = 0;

    /// Deregister handler for a given socket.
    /// This method is called by the communication layer after the socket has
    /// been removed from the polling mechanism.
    /// @param sd Socket descripter that was deregistered
    virtual void deregister(int sd) = 0;

    /// Returns desired polling interest for a socket.
    /// This method returns the current polling interest for socket
    /// <code>sd</code>.  After calling handle(), the communication layer will
    /// call this method to obtain the polling interest for <code>sd</code> and
    /// will make the appropriate adjustments with the underlying polling
    /// mechanism.
    /// @param sd Socket descriptor 
    /// @return Bitmask of desired polling interest for <code>sd</code>.
    /// @see poll_event::Flags
    virtual int poll_interest(int sd) = 0;
};

} // namespace palo
#endif //BDG_PALO_BE_SRC_RPC_RAW_SOCKET_HANDLER_H
