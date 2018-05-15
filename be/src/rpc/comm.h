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

#ifndef BDG_PALO_BE_SRC_RPC_COMM_H
#define BDG_PALO_BE_SRC_RPC_COMM_H

#include "clock.h"
#include "comm_address.h"
#include "comm_buf.h"
#include "connection_handler_factory.h"
#include "dispatch_handler.h"
#include "handler_map.h"
#include "raw_socket_handler.h"

#include <atomic>
#include <mutex>

namespace palo {

/** @defgroup AsyncComm AsyncComm
 * Network communication library.
 * The AsyncComm module is designed for maximally efficient network
 * programming by 1) providing an asynchronous API to facilitate
 * multiprogramming, and 2) using the most efficient polling mechanism for
 * each supported system (<code>epoll</code> on Linux, <code>kqueue</code>
 * on OSX and FreeBSD, and <code>port_associate</code> on Solaris).
 * @{
 */

/** Entry point to AsyncComm service.
 * There should be only one instance of this class per process and the static
 * method ReactorFactory#initialize must be called prior to constructing this
 * class in order to create the system-wide I/O reactor threads.
 */
class Comm {
public:

    /** Creates/returns singleton instance of the Comm class.
     * This method will construct a new instance of the Comm class if it has
     * not already been created.  All calls to this method return a pointer
     * to the same singleton instance of the Comm object between calls to
     * #destroy.  The static method ReactorFactory#initialize must be called
     * prior to calling this method for the first time, in order to create
     * system-wide I/O reactor threads.
     */
    static Comm *instance() {
        std::lock_guard<std::mutex> lock(ms_mutex);
        if (!ms_instance)
            ms_instance = new Comm();
        return ms_instance;
    }

    /** Destroys singleton instance of the Comm class.
     * This method deletes the singleton instance of the Comm class, setting
     * the ms_instance pointer to 0.
     */
    static void destroy();

    /// Registers an externally managed socket with comm event loop.
    /// This function allows an application to register a socket with
    /// the comm layer just for its the event loop.  The wire protocol
    /// is handled entirely by the <code>handler</code> object.
    /// @param sd Socket descriptor
    /// @param addr Address structure for connection identification purposes
    /// @param handler Raw connection handler
    /// @return error::OK on success, error::ALREADY_EXISTS if <code>addr</code>
    /// already registered with comm layer.
    /// @throws Exception on polling error
    int register_socket(int sd, const CommAddress &addr,
            RawSocketHandler *handler);

    /** Establishes a TCP connection and attaches a default dispatch handler.
     * This method establishes a TCP connection to <code>addr</code>
     * and associates it with <code>default_handler</code> as the default
     * dispatch handler for the connection.  The two types of events that
     * are delivered via the handler are CONNECTION_ESTABLISHED and DISCONNECT.
     * No ERROR events will be deliverd via the handler because any errors that
     * occur on the connection will result in the connection being closed,
     * resulting in a DISCONNECT event only.  If this method fails and returns
     * an error code, the connection will not have been setup and
     * <code>default_handler</code> will not have been installed.
     * The default dispatch handler, <code>default_handler</code>, will never be
     * called back via the calling thread.  It will be called back from a
     * reactor thread.  Because reactor threads are used to service I/O
     * events on many different sockets, the default dispatch handler should
     * return quickly from the callback.  When calling back into the default
     * dispatch handler, the calling reactor thread does not hold any locks
     * so the default dispatch handler callback may safely callback into the
     * Comm object (e.g. #send_response).  Upon successful completion,
     * <code>addr</code> can be used to subsequently refer to the connection.
     * @param addr Address to connect to
     * @param default_handler Smart pointer to default dispatch handler
     * @return error::OK on success, or error::COMM_ALREADY_CONNECTED,
     * error::COMM_SOCKET_ERROR, error::COMM_BIND_ERROR, or one of the
     * errors returned by #connect_socket on error.
     */
    int connect(const CommAddress &addr, const DispatchHandlerPtr &default_handler);

    /** Establishes a locally bound TCP connection and attaches a default
     * dispatch handler.  Establishes a TCP connection to
     * <code>addr</code> argument, binding the local side of the connection to
     * <code>local_addr</code>.  A default dispatch handler is associated with the
     * connection to receive CONNECTION_ESTABLISHED and DISCONNECT events.
     * No ERROR events will be deliverd via the handler because any errors that
     * occur on the connection will result in the connection being closed,
     * resulting in a DISCONNECT event only.  If this method fails and returns
     * an error code, the connection will not have been setup and
     * <code>default_handler</code> will not have been installed.
     * The default dispatch handler, <code>default_handler</code>, will never be
     * called back via the calling thread.  It will be called back from a
     * reactor thread.  Because reactor threads are used to service I/O
     * events on many different sockets, the default dispatch handler should
     * return quickly from the callback.  When calling back into the default
     * dispatch handler, the calling reactor thread does not hold any locks
     * so the default dispatch handler callback may safely callback into the
     * Comm object (e.g. #send_response).  Upon successful completion,
     * <code>addr</code> can be used to subsequently refer to the connection.
     * @param addr address to connect to
     * @param local_addr Local address to bind to
     * @param default_handler smart pointer to default dispatch handler
     * @return error::OK on success, or error::COMM_ALREADY_CONNECTED,
     * error::COMM_SOCKET_ERROR, error::COMM_BIND_ERROR, or one of the
     * errors returned by #connect_socket on error.
     */
    int connect(const CommAddress &addr, const CommAddress &local_addr,
            const DispatchHandlerPtr &default_handler);

    /** Sets an alias for a TCP connection.
     * RangeServers listen on a well-known port defined by the
     * <code>palo.RangeServer.Port</code> configuration property
     * (default = 15865).  However, RangeServers connect to the master using
     * an ephemeral port due to a bind conflict with its listen socket.  So that
     * the Master can refer to the RangeServer using the well-known port, an
     * alias address can be registered and subsequently used to reference the
     * connection.
     * @param addr Connection address (remote address)
     * @param alias Alias address
     * @return error::OK on success, or error::COMM_CONFLICTING_ADDRESS if
     * alias is already registered, or error::COMM_NOT_CONNECTED if
     * <code>addr</code> does not refer to an established connection.
     */
    int set_alias(const InetAddr &addr, const InetAddr &alias);

    /** Adds a proxy name for a TCP connection.
     * palo uses <i>proxy names</i> (e.g. "rs1") to refer to servers so
     * that the system can continue to operate properly even when servers are
     * reassigned IP addresses, such as starting and stopping palo running
     * on EBS volumes in AWS EC2.  This method adds a proxy name for the
     * connection identified by <code>addr</code> and pushes the new mapping
     * to the remote end of all active connections.
     * @note This method will assert if it is not called by the proxy master.
     * @param proxy Proxy name
     * @param hostname Hostname of remote machine
     * @param addr Connection address (remote address)
     * @return error::OK on success, or one of the errors returned by
     * HandlerMap::propagate_proxy_map
     */
    int add_proxy(const std::string &proxy, const std::string &hostname, const InetAddr &addr);

    /** Removes a proxy name for a TCP connection.
     * palo uses <i>proxy names</i> (e.g. "rs1") to refer to servers so
     * that the system can continue to operate properly even when servers are
     * reassigned IP addresses, such as starting and stopping palo running
     * on EBS volumes in AWS EC2.  This method removes the proxy name
     * <code>proxy</code> locally and from the remote end of all active
     * connections.
     * @note This method will assert if it is not called by the proxy master.
     * @param proxy Proxy name to remove
     */
    int remove_proxy(const std::string &proxy);

    /** Translates a proxy name to an IP address.
     * palo uses <i>proxy names</i> (e.g. "rs1") to refer to servers so
     * that the system can continue to operate properly even when servers are
     * reassigned IP addresses, such as starting and stopping palo running
     * on EBS volumes in AWS EC2.  This method translates <code>proxy</code>
     * to its associated address that was registered with a prior call to
     * add_proxy().  If <code>addr</code> is NULL, then the method just checks
     * to see if the proxy name has been registered and represents a valid
     * mapping.
     * @param proxy Proxy name to translate
     * @param addr Address of object to hold translated address
     * @return <i>true</i> if address was translated or could be translated,
     * <i>false</i> otherwise
     */
    bool translate_proxy(const std::string &proxy, InetAddr *addr);

    /** Returns the proxy map.
     * @param proxy_map Reference to return proxy map
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    /** Waits until a CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE message is
     * received from the proxy master
     *
     * @param timer Expiration timer
     * @return <i>true</i> if successful, <i>false</i> if timer expired
     */
    bool wait_for_proxy_load(Timer &timer);

    /** Creates listen (accept) socket on <code>addr</code>.
     * New connections will be assigned dispatch handlers by
     * calling the ConnectionHandlerFactory::get_instance method of the handler
     * factory pointed to by <code>chf</code>.  Since no default dispatch
     * handler is supplied for this listen (accept) socket,
     * Event::CONNECTION_ESTABLISHED events are logged, but not delivered to the
     * application.
     * @param addr IP address and port on which to listen for connections
     * @param chf Smart pointer to connection handler factory
     * @throws Exception Code set to error::COMM_SOCKET_ERROR,
     * error::COMM_BIND_ERROR, error::COMM_LISTEN_ERROR,
     * error::COMM_SEND_ERROR, or error::COMM_RECEIVE_ERROR
     */
    int listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf);

    /** Creates listen (accept) socket on <code>addr</code> and attaches a
     * default dispatch handler.
     * New connections will be assigned dispatch handlers by
     * calling the ConnectionHandlerFactory::get_instance method of the handler
     * factory pointed to by <code>chf</code>.  <code>default_handler</code>
     * is registered as the default dispatch handler for the newly created
     * listen (accept) socket and Event::CONNECTION_ESTABLISHED events will be
     * delivered to the application via this handler.
     * @param addr IP address and port on which to listen for connections
     * @param chf Smart pointer to connection handler factory
     * @param default_handler Smart pointer to default dispatch handler
     * @throws Exception Code set to error::COMM_SOCKET_ERROR,
     * error::COMM_BIND_ERROR, error::COMM_LISTEN_ERROR,
     * error::COMM_SEND_ERROR, or error::COMM_RECEIVE_ERROR
     */
    int listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf,
            const DispatchHandlerPtr &default_handler);

    /** Sends a request message over a connection, expecting a response.  The
     * connection is specified by <code>addr</code> which is the remote end of
     * the connection.  The request message to send is encapsulated in
     * <code>cbuf</code> (see CommBuf) and should start with a valid header.
     * The* <code>response_handler</code> argument will get called
     * back with a response MESSAGE event, a TIMEOUT event if no response is
     * received within the number of seconds specified by the timeout argument,
     * or an ERROR event (see below).  The following errors may be returned by
     * this method:
     *
     *   - error::COMM_NOT_CONNECTED
     *   - error::COMM_BROKEN_CONNECTION
     *
     * A return value of error::COMM_NOT_CONNECTED implies that
     * <code>response_handler</code> was not installed and an Event::ERROR event
     * will <b>not</b> be delivered.  A return value of
     * error::COMM_BROKEN_CONNECTION implies that <code>response_handler</code>
     * was installed and an Event::ERROR event will be delivered to the
     * application.  If the server at the other end of the connection uses an
     * ApplicationQueue to carry out requests, then the gid field in the header
     * can be used to serialize request execution.  For example, the following
     * code serializes requests to the same file descriptor:
     * <pre>
     * HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
     * hbuilder.set_group_id(fd);
     * CommBuf *cbuf = new CommBuf(hbuilder, 14);
     * cbuf->AppendShort(COMMAND_READ);
     * cbuf->AppendInt(fd);
     * cbuf->AppendLong(amount);
     * </pre>
     *
     * This method locates the I/O handler associated with <code>addr</code>
     * and then calls the private method
     * @ref private_send_request "send_request" to carry out the send request.
     * If an error is encountered while trying to send the request, the
     * associated handler will be decomissioned.
     *
     * @param addr Connection address (remote address)
     * @param timeout_ms Number of milliseconds to wait before delivering
     *        TIMEOUT event
     * @param cbuf Request message to send (see CommBuf)
     * @param response_handler Pointer to response handler associated with the
     *        request
     * @return error::OK on success or error code on failure (see above)
     */
    int send_request(const CommAddress &addr, uint32_t timeout_ms,
            CommBufPtr &cbuf, DispatchHandler *response_handler);

    /** Sends a response message back over a connection.  It is assumed that the
     * CommHeader#id field of the header matches the id field of the request for
     * which this is a response to.  The connection is specified by the
     * <code>addr</code> which is the remote end of the connection.  The
     * response message to send is encapsulated in the cbuf (see CommBuf) object
     * and should start with a valid header.  The following code snippet
     * illustrates how a simple response message gets created to send back to a
     * client in response to a request message:
     *
     * <pre>
     * CommHeader header;
     * header.initialize_from_request_header(request_event->header);
     * CommBufPtr cbp(new CommBuf(header, 4));
     * cbp->append_i32(error::OK);
     * </pre>
     *
     * If an error is encountered while trying to send the response, the
     * associated handler will be decomissioned.
     * @param addr Connection address (remote address)
     * @param cbuf Response message to send (must have valid header with
     *        matching request id)
     * @return error::OK on success or error code on failure
     */
    int send_response(const CommAddress &addr, CommBufPtr &cbuf);

    /** Creates a socket for receiving datagrams and attaches <code>handler</code>
     * as the default dispatch handler.  This socket can
     * also be used for sending datagrams.  The events delivered for this socket
     * consist of either MESSAGE events or ERROR events.  In setting up the
     * datagram (UDP) socket, the following setup is performed:
     *
     *   - <code>O_NONBLOCK</code> option is set on socket
     *   - Socket send and receive buffers are set to <code>4*32768</code> bytes
     *   - If <code>tos</code> is non-zero, <code>IP_TOS</code> and
     *     <code>SO_PRIORITY</code> options are set using <code>tos</code> as
     *     the argument (Linux only)
     *   - If <code>tos</code> is non-zero, <code>IP_TOS</code> option is set
     *     with <code>IPTOS_LOWDELAY</code> as the argument (Apple, Sun, and
     *     FreeBSD)
     *
     * @param addr pointer to address structure
     * @param tos TOS value to set on IP packet
     * @param handler Default dispatch handler for socket
     * @throws Exception Code set to error::COMM_SOCKET_ERROR or
     * error::COMM_BIND_ERROR.
     */
    int create_datagram_receive_socket(CommAddress &addr, int tos,
            const DispatchHandlerPtr &handler);

    /** Sends a datagram to a remote address.  The remote address is specified
     * by <code>addr</code> and the local socket address to send it from is
     * specified by <code>send_addr</code>.  The <code>send_addr</code> argument
     * must refer to a socket that was created with a call to
     * #create_datagram_receive_socket.  If an error is encountered while trying
     * to send the datagram, the associated handler will be decomissioned.
     * @param addr Remote address to send datagram to
     * @param send_addr Local socket address to send from
     * @param cbuf Datagram message with valid header
     * @return error::OK on success or error code on failure
     */
    int send_datagram(const CommAddress &addr, const CommAddress &send_addr,
            CommBufPtr &cbuf);

    /** Sets a timer for <code>duration_millis</code> milliseconds in the
     * future.
     * This method will cause a Event::TIMER event to be generated after
     * <code>duration_millis</code> milliseconds have elapsed.
     * <code>handler</code> is the dispatch handler registered with the timer
     * to receive the Event::TIMER event.  This timer registration is
     * <i>one shot</i>.  To set up a periodic timer event, the timer must
     * be re-registered each time it is handled.
     * @note This method sets a smart pointer to <code>handler</code>, so if
     * the reference count to <code>handler</code> is zero when this method
     * is called, it will be deleted after the event is delivered.  To prevent
     * this from happening, the caller should hold a smart pointer
     * (DispatchHandlerPtr) to <code>handler</code>.
     * @param duration_millis Number of milliseconds to wait
     * @param handler Dispatch handler to receive Event::TIMER event upon
     *        expiration
     * @return error::OK
     */
    int set_timer(uint32_t duration_millis, const DispatchHandlerPtr &handler);

    /** Sets a timer for absolute time <code>expire_time</code>.
     * This method will cause a Event::TIMER event to be generated at the
     * absolute time specified by <code>expire_time</code>.
     * <code>handler</code> is the dispatch handler registered with the
     * timer to receive Event::TIMER events.  This timer registration is
     * <i>one shot</i>.  To set up a periodic timer event, the timer must
     * be re-registered each time it is handled.
     * @note This method sets a smart pointer to <code>handler</code>, so if
     * the reference count to <code>handler</code> is zero when this method
     * is called, it will be deleted after the event is delivered.  To prevent
     * this from happening, the caller should hold a smart pointer
     * (DispatchHandlerPtr) to <code>handler</code>.
     * @param expire_time Absolute expiration time
     * @param handler Dispatch handler to receive Event::TIMER event upon
     *        expiration
     * @return error::OK
     */
    int set_timer_absolute(ClockT::time_point expire_time, const DispatchHandlerPtr &handler);

    /** Cancels all scheduled timers registered with the dispatch handler
     * <code>handler</code>.
     *
     * @param handler Dispatch handler for which all scheduled timer should
     *        be cancelled
     */
    void cancel_timer(const DispatchHandlerPtr &handler);

    /** Closes the socket specified by the addr argument.  This has
     * the effect of closing the connection and removing it from the event
     * demultiplexer (e.g epoll).  It also causes all outstanding requests on
     * the connection to get purged.
     * @param addr Connection or accept or datagram address
     */
    void close_socket(const CommAddress &addr);

    /** Finds an unused TCP port starting from <code>addr</code>.
     * This method iterates through 15 ports starting with
     * <code>addr.sin_port</code> until it is able to bind to
     * one.  If an available port is found, <code>addr.sin_port</code>
     * will be set to the available port, otherwise the method will assert.
     * @param addr Starting address template
     */
    void find_available_tcp_port(InetAddr &addr);

    /** Finds an unused UDP port starting from <code>addr</code>.
     * This method iterates through 15 ports starting with
     * <code>addr.sin_port</code> until it is able to bind to
     * one.  If an available port is found, <code>addr.sin_port</code>
     * will be set to the available port, otherwise the method will assert.
     * @param addr Starting address template
     */
    void find_available_udp_port(InetAddr &addr);

private:

    /** Private constructor (prevent non-singleton usage). */
    Comm();
    // used for test
    Comm(const char* host);

    /** Destructor */
    ~Comm();

    /** Sends a request message over a connection.
     * @anchor private_send_request
     * This method sets the CommHeader::FLAGS_BIT_REQUEST bit of the flags
     * field of <code>cbuf->header</code>.  If <code>response_handler</code> is
     * 0, then the CommHeader::FLAGS_BIT_IGNORE_RESPONSE is also set.  The
     * CommHeader#id field of <code>cbuf->header</code> is assigned by
     * incrementing #ms_next_request_id and the CommHeader#timeout_ms field of
     * <code>cbuf->header</code> is set to <code>timeout_ms</code>.  Finally,
     * <code>cbuf->write_header_and_reset()</code> is called and the message
     * is sent via <code>data_handler</code>.
     * @param data_handler I/O handler for connection.
     * @param timeout_ms Number of milliseconds to wait before delivering
     *        TIMEOUT event
     * @param cbuf Request message to send (see CommBuf)
     * @param response_handler Pointer to response handler associated with the
     *        request
     * @return error::OK on success or error code on failure
     *         (<code>response_handler</code> is only installed on error::OK
     *          or error::COMM_BROKEN_CONNECTION).
     */
    int send_request(IOHandlerData *data_handler, uint32_t timeout_ms,
            CommBufPtr &cbuf, DispatchHandler *response_handler);

    /** Creates a TCP socket connection.
     * This method is called by the #connect methods to setup a socket,
     * connect to a remote address, and attach a data handler.
     * If <code>addr</code> is of type CommAddress::PROXY then it is
     * translated.  Then the socket is setup as follows:
     *
     *   - <code>O_NONBLOCK</code> option is set
     *   - <code>TCP_NODELAY</code> option is set (Linux and Sun)
     *   - <code>SO_NOSIGPIPE</code> option is set (Apple and FreeBSD)
     *
     * Then a data (TCP) handler is created for the socket and added to
     * the handler map.  Finally <code>connect</code> is called and
     * polling is started on the socket.
     * @param sd Socket descriptor
     * @param addr Remote address to connect to
     * @param default_handler Default dispatch handler
     * @return error::OK on success, or one of error::COMM_INVALID_PROXY,
     * error::COMM_CONNECT_ERROR, error::COMM_POLL_ERROR,
     * error::COMM_SEND_ERROR, error::COMM_RECEIVE_ERROR on error.
     */
    int connect_socket(int sd, const CommAddress &addr,
            const DispatchHandlerPtr &default_handler);

    /// Pointer to singleton instance of this class
    static Comm *ms_instance;

    /// Atomic integer used for assinging request IDs
    static std::atomic<uint32_t> ms_next_request_id;

    /// %Mutex for serializing access to #ms_instance
    static std::mutex ms_mutex;

    /// Pointer to IOHandler map    
    HandlerMapPtr m_handler_map;

    /// Pointer to dedicated reactor for handling timer events
    ReactorPtr m_timer_reactor;

    /// Local address initialized to primary interface and empty port
    InetAddr m_local_addr;
};

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_COMM_H
