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

#ifndef BDG_PALO_BE_SRC_RPC_PROTOCOL_H
#define BDG_PALO_BE_SRC_RPC_PROTOCOL_H

#include "event.h"
#include "comm_header.h"
#include "comm_buf.h"

namespace palo {

class CommBuf;

/** Abstract base class for server protocol drivers.
 * This is the base class for all server protocol drivers.  A server protocol
 * driver is responsible for encoding and decoding messages sent and received
 * to and from a service.
 */
class Protocol {
public:

    /** Destructor. */
    virtual ~Protocol() { return; }

    /** Returns the response code from an event <code>event</code> generated
     * in response to a request message.
     * If <code>event</code> is of type ERROR, then <code>event->error</code>
     * is returned, otherwise the response code is decoded from the first four
     * bytes of the message payload.
     * @return Error or response code from <code>event</code> or
     * error::SERIALIZATION_INPUT_OVERRUN if payload of MESSAGE event is less
     * than 4 bytes.
     */
    static int32_t response_code(const Event *event);

    /** Returns the response code from an event <code>event</code> generated
     * in response to a request message.
     * If <code>event</code> is of type ERROR, then <code>event->error</code>
     * is returned, otherwise the response code is decoded from the first four
     * bytes of the message payload.
     * @return Error or response code from <code>event</code> or
     * error::SERIALIZATION_INPUT_OVERRUN if payload of MESSAGE event is less
     * than 4 bytes.
     */
    static int32_t response_code(const EventPtr &event) {
        return response_code(event.get());
    }

    /** Returns error message decoded standard error MESSAGE generated
     * in response to a request message.  When a request to a service method
     * results in an error, the error code an message are typically returned
     * in a response message encoded in the following format:
     * <pre>
     *   [int32] error code
     *   [int16] error message length
     *   [chars] error message
     * </pre>
     * This method extracts and returns the error message from a response
     * MESSAGE event.
     * @note The semantics of this method are wacky, it should be re-written
     * @param event Pointer to MESSAGE event received in response to a request
     * @return %Error message
     */
    static std::string string_format_message(const Event *event);

    /** Returns error message decoded from standard error MESSAGE generated
     * in response to a request message.  When a request to a service method
     * results in an error, the error code an message are typically returned
     * in a response message encoded in the following format:
     * <pre>
     *   [int32] error code
     *   [int16] error message length
     *   [chars] error message
     * </pre>
     * This method extracts and returns the error message from a response
     * MESSAGE event.
     * @note The semantics of this method are wacky, it should be re-written
     * @param event Pointer to MESSAGE event received in response to a request
     * @return %Error message
     */
    static std::string string_format_message(const EventPtr &event) {
        return string_format_message(event.get());
    }

    /** Creates a standard error message response.  This method creates a
     * standard error message response encoded in the following format:
     * <pre>
     *   [int32] error code
     *   [int16] error message length
     *   [chars] error message
     * </pre>
     * @param header Reference to header to use in response buffer
     * @param error %Error code
     * @param msg %Error message
     * @return Pointer to Commbuf message holding standard error response
     */
    static CommBufPtr
        create_error_message(CommHeader &header, int error, const char *msg);

    /** Returns the string representation of a command code.  Each protocol
     * defines a set of command codes that are sent in the CommHeader::command
     * field of a reqeust's message header to indicate which server command
     * (method) is to be executed.  This method returns a human readable
     * string mnemonic for the command.
     * @param command Command code
     * @return Pointer to human readable string mnemonic for
     * <code>command</code>.
     */
    virtual const char *command_text(uint64_t command) = 0;
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_PROTOCOL_H
