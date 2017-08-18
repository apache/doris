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
