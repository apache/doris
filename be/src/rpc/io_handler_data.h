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

#ifndef BDG_PALO_BE_SRC_RPC_IO_HANDLER_DATA_H
#define BDG_PALO_BE_SRC_RPC_IO_HANDLER_DATA_H

#include "comm_buf.h"
#include "io_handler.h"
#include "error.h"

#include <list>

extern "C" {
#include <netdb.h>
#include <string.h>
}

namespace palo {

/** I/O handler for TCP sockets.
 */
class IOHandlerData : public IOHandler {
public:

    /** Constructor.
     * @param sd Socket descriptor
     * @param addr Address of remote end of connection
     * @param dhp Default dispatch handler for connection
     * @param connected Initial connection state for handler
     */
    IOHandlerData(int sd, const InetAddr& addr,
            const DispatchHandlerPtr& dhp, bool connected = false)
        : IOHandler(sd, dhp) {
            memcpy(&m_addr, &addr, sizeof(InetAddr));
            m_connected = connected;
            reset_incoming_message_state();
        }

    /** Destructor */
    virtual ~IOHandlerData() { }

    /** Disconnects handler by delivering Event::DISCONNECT via default dispatch
     * handler.
     */
    void disconnect() override {
        EventPtr event = std::make_shared<Event>(Event::DISCONNECT, m_addr, m_proxy, m_error);
        deliver_event(event);
    }

    /** Resets incoming message buffer state in preparation for next message.
    */
    void reset_incoming_message_state() {
        m_got_header = false;
        m_event.reset();
        m_message_header_ptr = m_message_header;
        m_message_header_remaining = CommHeader::FIXED_LENGTH;
        m_message = 0;
        m_message_ptr = 0;
        m_message_remaining = 0;
        m_message_aligned = false;
    }

    /// Frees the message buffer (#m_message).
    /// If #m_message was allocated with posix_memalign(), as indicated by
    /// #m_message_aligned, the free() function is used to deallocate the
    /// memory.  Otherwise, the buffer is deallocated with delete []
    void free_message_buffer() {
        if (m_message_aligned) {
            free(m_message);
        } else {
            delete [] m_message;
        }
        m_message = 0;
    }

    /** Sends message pointed to by <code>cbp</code> over socket associated
     * with this I/O handler.  If the message being sent is a request
     * message (has the CommHeader::FLAGS_BIT_REQUEST set) and
     * <code>disp_handler</code> is not 0, then an entry is added to the
     * reactor's request cache so that <code>disp_handler</code> will be
     * called to handle the response or receive a TIMEOUT event if the
     * response is not received within <code>timeout_ms</code> milliseconds.
     * @param cbp Reference to CommBufPtr pointing to message to send
     * @param timeout_ms Millisecond timeout used for request messages
     * @param disp_handler Dispatch handler used for request messages
     * @return error::OK on success, error::COMM_NOT_CONNECTED if handler has
     * been decomissioned, or error::COMM_BROKEN_CONNECTION if a write error
     * was encountered.
     */
    int send_message(CommBufPtr &cbp, uint32_t timeout_ms=0,
            DispatchHandler *disp_handler=nullptr);

    /** Flushes send queue.  When messages are sent, they are first added to a
     * send queue (#m_send_queue) and then the messages in the send queue are
     * written over the socket by calling this method.  This method attempts
     * to write all of the messages in the queue and stops under the following
     * conditions:
     *   - Send queue becomes empty
     *   - A write results in EAGAIN (socket buffer is full)
     *   - An error is encountered during a write
     * The send queue holds a list of CommBuf objects that contain <i>next
     * write</i> pointers that are updated by this method and allow it to
     * pick up where it left off in the event of EAGAIN.
     * @return error::OK on success or EAGAIN, or error::COMM_BROKEN_CONNECTION
     * if a write error was encountered.
     */
    int flush_send_queue();

    /** Handle <code>epoll()</code> interface events.
     * This method is called by its reactor thread to handle I/O events.
     * It handles <code>EPOLLOUT</code> events with a call to
     * #handle_write_readiness.  If #handle_write_readiness returns <i>true</i>
     * the handler is disconnected with a call to #handle_disconnect and
     * <i>true</i> is returned.  <code>EPOLLIN</code> events are handled by
     * reading message data off the socket.  First the message header is read
     * and decoded with #handle_message_header and then the message payload is
     * read and delivered to the application with #handle_message_body.  If a
     * read error is encountered, #m_error is set to the approprate error
     * code (if not already set) and the handler is disconnected with
     * a call to #handle_disconnect and <i>true</i> is returned.
     * <i>EOF</i>, <code>EPOLLERR</code> events, <code>EPOLLHUP</code>, and
     * <code>EPOLLRDHUP</code> events (level-triggered epoll only) are
     * handled by disconnecting the handler with a call to #handle_disconnect
     * and <i>true</i> is returned.  <code>arrival_time</code> is passed into
     * #handle_message_header to be delivered to the applicaiton via the
     * Event object.
     * @param event Pointer to <code>epoll_event</code> structure describing
     * event
     * @param arrival_time Time of event arrival
     * @return <i>false</i> on success, <i>true</i> if error encountered and
     * handler was decomissioned
     */
    bool handle_event(struct epoll_event *event,
            ClockT::time_point arrival_time) override;

    /** Handles write readiness by completing connection and flushing send
     * queue.  When a data handler is created after a call to
     * <code>connect</code> it is in the disconnected state.  Once the socket
     * becomes ready for writing, the connection request can be completed.
     * This method handles the completion of the connection when the handler
     * is in the disconnected state by doing the following:
     *   - Sets the socket send and receive buffer to <code>4*32768</code>
     *     bytes
     *   - Reads the local socket address and initializes #m_local_addr
     *   - Sets #m_connected to <i>true</i>
     *   - If <i>proxy master</i>, propagate proxy map over newly established
     *     connection.
     *   - Delivers Event::CONNECTION_ESTABLISHED event via the default
     *     dispatch handler
     * After completion has been handled (if needed) then this method
     * flushes the send queue with a call to #flush_send_queue.
     * @return <i>false</i> on success, <i>true</i> if error encountered
     */
    bool handle_write_readiness();

private:

     /**
     * Used to read data off a socket that is monotored with edge-triggered epoll.
     * When this function returns with *errnop set to EAGAIN, it is safe to call
     * epoll_wait on this socket.
     */
    ssize_t et_socket_read(int fd, void *vptr, size_t n, int *errnop, bool *eofp);

    ssize_t et_socket_writev(int fd, const iovec *vector, int count, int *errnop);
    /** Processes a message header.  This method is called when the fixed
     * length portion of a header has been completely received.  It first
     * checks to see if there is a variable portion of the header that has
     * not yet been read, if so, it adjusts #m_message_header_remaining and
     * returns.  If the header has been completely received, it allocates
     * a new Event object and sets #m_event pointing to it.  It initializes
     * the event object with the message header and <code>arrival_time</code>.
     * It then allocates the message payload buffer (#m_message), initialzes
     * the payload buffer pointers, and sets #m_got_header to <i>true</i>.
     */
    void handle_message_header(ClockT::time_point arrival_time);

    /** Processes a message body.  This method is called when a message
     * has been completely received (header + payload).  It first checks to
     * see if the message is a proxy update message and if so, it updates
     * its proxy map with a call to HandlerMap::update_proxy_map and returns.
     * Otherwise if it is a response message and the
     * CommHeader::FLAGS_BIT_IGNORE_RESPONSE bit is not set in the header
     * flags, the corresponding dispatch handler is removed form request queue
     * and the message is delivered to the applicaton using that handler.
     * Otherwise, the message is delivered to the application using the
     * default dispatch handler.  After the message has been delivered, the
     * message receive state is reset with a call to
     * #reset_incoming_message_state.
     */
    void handle_message_body();

    /** Decomissions the handler.
    */
    void handle_disconnect();

    /// Flag indicating if socket connection has been completed
    bool m_connected {};

    /// Flag indicating if message header has been completely received
    bool m_got_header {};

    /// Flag indicating if message buffer was allocated with posix_memalign()
    bool m_message_aligned {};

    /// Pointer to Event object holding message to deliver to application
    EventPtr m_event;

    /// Message header buffer
    uint8_t m_message_header[64];

    /// Pointer to next write position in #m_message_header
    uint8_t *m_message_header_ptr {};

    /// Amount of header remaining to be read
    size_t m_message_header_remaining;

    /// Poiner to message payload buffer
    uint8_t *m_message {};

    /// Pointer to next write position in #m_message
    uint8_t *m_message_ptr {};

    /// Amount of message payload remaining to be read
    size_t m_message_remaining {};

    /// Send queue
    std::list<CommBufPtr> m_send_queue;

};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_IO_HANDLER_DATA_H
