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
