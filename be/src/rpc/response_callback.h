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

#ifndef BDG_PALO_BE_SRC_RPC_RESPONSE_CALLBACK_H
#define BDG_PALO_BE_SRC_RPC_RESPONSE_CALLBACK_H

#include "util.h"
#include "comm.h"
#include "event.h"

namespace palo {

/** This class is used to generate and deliver standard responses back to a
 * client.
 */
class ResponseCallback {
public:

    /** Constructor. Initializes a pointer to the Comm object and saves a
     * pointer to the event that triggered the request.
     *
     * @param comm Pointer to the Comm object
     * @param event Smart pointer to the event that generated the request
     */
    ResponseCallback(Comm *comm, EventPtr &event)
        : m_comm(comm), m_event(event) { return; }

    /** Default constructor.
    */
    ResponseCallback() : m_comm(0), m_event(0) { return; }

    /** Destructor */
    virtual ~ResponseCallback() { return; }

    /** Sends a standard error response back to the client.  The response message
     * that is generated and sent back has the following format:
     * <pre>
     *   [int32] error code
     *   [int16] error message length
     *   [chars] error message
     * </pre>
     * @param error %Error code
     * @param msg %Error message
     * @return error::OK on success or error code on failure
     */
    virtual int error(int error, const std::string &msg);

    /** Sends a a simple success response back to the client which is just
     * the 4-byte error code error::OK.  This can be used to signal success
     * for all methods that don't have return values.
     * @return error::OK on success or error code on failure
     */
    virtual int response_ok();

    /** Gets the remote address of the requesting client.
     * @param addr Reference to address structure to hold result
     */
    void get_address(struct sockaddr_in &addr) {
        memcpy(&addr, &m_event->addr, sizeof(addr));
    }

    /** Gets the remote address of the requesting client.
     * @return Remote address
     */
    const InetAddr get_address() const {
        return m_event->addr;
    }

    /** Get smart pointer to event object that triggered the request.
     * @return Smart pointer to event object that triggered the request.
     */
    EventPtr &event() { return m_event; }

protected:

    Comm     *m_comm; //!< Comm pointer
    EventPtr m_event; //!< Smart pointer to event object
};

} // namespace palo
#endif // BDG_PALO_BE_SRC_RPC_RESPONSE_CALLBACK_H
