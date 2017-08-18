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

#ifndef BDG_PALO_BE_SRC_RPC_CONNECTION_MANAGER_H
#define BDG_PALO_BE_SRC_RPC_CONNECTION_MANAGER_H

#include "comm.h"
#include "comm_address.h"
#include "connection_initializer.h"
#include "dispatch_handler.h"
#include "sock_addr_map.h"

#include "timer.h"
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

namespace palo {

class Event;

/**
 * Establishes and maintains a set of TCP connections.  If any of the
 * connections gets broken, then this class will continuously attempt
 * to re-establish the connection, pausing for a while in between attempts.
 */
class ConnectionManager : public DispatchHandler {
    enum class State {
        DISCONNECTED = 0,
        CONNECTED,
        READY,
        DECOMMISSIONED
    };

    /** Per-connection state.
     */
    class ConnectionState {
        public:
            /// Connection address supplied to the #add methods
            CommAddress addr;
            /// Local address to bind to
            CommAddress local_addr;
            /// Address initialized from Event object
            InetAddr inet_addr;
            /// Retry connection attempt after this many milliseconds
            uint32_t timeout_ms;
            /// Registered connection handler
            DispatchHandlerPtr handler;
            /// Connection initializer
            ConnectionInitializerPtr initializer;
            /// Connection state
            State state {};
            /// Mutex to serialize concurrent access
            std::mutex mutex;
            /// Condition variable used to signal connection state change
            std::condition_variable cond;
            /// Absolute time of next connect attempt
            std::chrono::steady_clock::time_point next_retry;
            /// Service name of connection for log messages
            std::string service_name;
            /// retry number
            uint32_t retry_number;
    };

    /// Smart pointer to ConnectionState
    typedef std::shared_ptr<ConnectionState> ConnectionStatePtr;

    /** StringWeakOrdering for connection retry heap
    */
    struct LtConnectionState {
        bool operator()(const ConnectionStatePtr &cs1,
                const ConnectionStatePtr &cs2) const {
            return std::chrono::operator>(cs1->next_retry, cs2->next_retry);
        }
    };

    /** Connection manager state shared between Connection manager objects.
    */
    class SharedImpl {
    public:
        /** Destructor.
        */
        ~SharedImpl() {
            shutdown = true;
            retry_cond.notify_one();
            if (thread.joinable())
                thread.join();
        }
        /// Pointer to Comm layer
        Comm *comm;
        /// Mutex to serialize concurrent access
        std::mutex mutex;
        /// Condition variable to signal if anything is on the retry heap
        std::condition_variable retry_cond;
        /// Pointer to connection manager thread object
        std::thread thread;
        /// InetAddr-to-ConnectionState map
        SockAddrMap<ConnectionStatePtr> conn_map;
        /// Proxy-to-ConnectionState map
        std::unordered_map<std::string, ConnectionStatePtr> conn_map_proxy;
        /// Connect retry heap
        std::priority_queue<ConnectionStatePtr, std::vector<ConnectionStatePtr>,
            LtConnectionState> retry_queue;
        /// Set to <i>true</i> to prevent connect failure log message
        bool quiet_mode;
        /// Set to <i>true</i> to signal shutdown in progress
        bool shutdown;
    };

    /// Smart pointer to SharedImpl object
    typedef std::shared_ptr<SharedImpl> SharedImplPtr;
    public:

    /**
     * Constructor.  Creates a thread to do connection retry attempts.
     *
     * @param comm Pointer to the comm object
     */
    ConnectionManager(Comm *comm = 0) {
        m_impl = std::make_shared<SharedImpl>();
        m_impl->comm = comm ? comm : Comm::instance();
        m_impl->quiet_mode = false;
        m_impl->shutdown = false;
    }

    ConnectionManager(const ConnectionManager &cm) = delete;

    /** Destructor.
    */
    virtual ~ConnectionManager() { }

    /** Adds a connection.  The <code>addr</code> parameter holds the address
     * to which the connection manager should maintain a connection.
     * This method first checks to see if the address is
     * already registered with the connection manager and returns immediately
     * if it is.  Otherwise, it adds the address to an internal connection map,
     * attempts to establish a connection to the address, and then returns.
     * Once a connection has been added, the internal manager thread will
     * maintian the connection by continually re-establishing the connection if
     * it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms When connection dies, wait this many milliseconds
     *        before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
            const char *service_name);

    /** Adds a connection with a dispatch handler.  The <code>addr</code>
     * parameter holds the address to which connection manager should maintain a
     * connection.  This method first checks to see if the address is
     * already registered with the connection manager and returns immediately
     * if it is.  Otherwise, it adds the address to an internal connection map,
     * attempts to establish a connection to the address, and then returns.
     * Once a connection has been added, the internal manager thread will
     * maintian the connection by continually re-establishing the connection if
     * it ever gets broken.
     *
     * This version of add accepts a DispatchHandler argument,
     * <code>handler</code>, which is registered as the connection handler for
     * the connection and receives all events that occur on the connection
     * (except for the initialization handshake messages).
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
            const char *service_name, DispatchHandlerPtr &handler);

    /** Adds a connection with a dispatch handler and connection initializer.
     * The <code>addr</code> parameter holds the address to which connection
     * manager should maintain a connection.  This method first checks to see
     * if the address is already registered with the connection manager and
     * returns immediately if it is.  Otherwise, it adds the address to an
     * internal connection map, attempts to establish a connection to the
     * address, and then returns.  Once a connection has been added, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * This version of add accepts a connection initializer,
     * <code>initializer</code>, which is used to carry out an initialization
     * handshake.  Once the connection has been established, the connection
     * initializer is used to generate an initialization handshake message
     * which is sent to the other end of the connection.  
     * and process the response t
     *
     * @param addr Address to maintain a connection to
     * @param timeout_ms Timeout value (in milliseconds) that gets passed into
     * Comm#connect and also used as the waiting period between connection
     * attempts
     * @param service_name Name of the serivce at the other end of the
     * connection used for descriptive log messages
     * @param handler Default dispatch handler for connection.
     * @param initializer Connection initialization handshake driver
     */
    void add_with_initializer(const CommAddress &addr, uint32_t timeout_ms,
            const char *service_name,
            DispatchHandlerPtr &handler,
            ConnectionInitializerPtr &initializer);

    /** Adds a connection bound to a local address.
     * The <code>addr</code> holds the address to which the
     * connection manager should maintain a connection.  This method first
     * checks to see if the address is already registered with the connection
     * manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a
     * connection to the address, and then returns.  From here on out, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms When connection dies, wait this many
     *        milliseconds before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
            uint32_t timeout_ms, const char *service_name);

    /** Adds a connection with a dispatch handler bound to a local address.
     * The <code>addr</code> holds the address to which the
     * connection manager should maintain a connection.  This method first
     * checks to see if the address is already registered with the connection
     * manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a
     * connection to the address, and then returns.  From here on out, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
            uint32_t timeout_ms, const char *service_name,
            DispatchHandlerPtr &handler);

    /** Removes a connection from the connection manager.
     *
     * @param addr remote address of connection to remove
     * @return Error code (error::OK on success)
     */
    int remove(const CommAddress &addr);

    /** Removes a connection from the connection manager.
     *
     * @param addr address of connection to remove
     * @return Error code (error::OK on success)
     */
    int remove_unlocked(const CommAddress &addr);
    /** Blocks until the connection to the given address is

     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established within
     * max_wait_ms, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param max_wait_ms The maximum time to wait for the connection before
     *        returning
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, uint32_t max_wait_ms);

    /** Blocks until the connection to the given address is
     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established before the timer
     * expires, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param timer running timer object
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, Timer &timer);

    /** Returns the Comm object associated with this connection manager
     *
     * @return the assocated comm object
     */
    Comm *get_comm() { return m_impl->comm; }

    /** Sets the SharedImpl#quiet_mode flag which will disable the
     * generation of log messages upon failed connection attempts.  It is set
     * to <i>false</i> by default.
     *
     * @param mode The new value for the SharedImpl#quiet_mode flag
     */
    void set_quiet_mode(bool mode) { m_impl->quiet_mode = mode; }

    /** Primary dispatch handler method.  The ConnectionManager is a dispatch
     * handler and is registered as the handler for all of the connections that
     * it manages.  This method does the job of maintianing connections.
     * This method will forward all events (except initialization handshake
     * messages) to any dispatch handler registered for the connection on
     * which the event was generated.
     * @param event Comm layer event
     */
    virtual void handle(EventPtr &event);

    /** Connect retry loop.
     * This method is called as the retry thread function.
     */
    void connect_retry_loop();

private:
    /** Called by the #add methods to add a connection.  This method creates
     * and initializes a ConnectionState object for the connnection, adds
     * it to either SharedImpl#conn_map or SharedImpl#conn_map_proxy depending
     * on the type of address, and then issues a connect request.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     * @param initializer Connection initialization handshake driver
     */
    void add_internal(const CommAddress &addr, const CommAddress &local_addr,
            uint32_t timeout_ms, const char *service_name,
            DispatchHandlerPtr &handler,
            ConnectionInitializerPtr &initializer);

    /** This method blocks until the connection represented by
     * <code>conn_state</code> is established.  If the connection is not
     * established before <code>timer</code> expires, then the method
     * returns <i>false</i>.
     * @param conn_state Pointer to connection state object representing
     * connection
     * @param timer Maximum wait timer
     * @return <i>true</i> if connected, <i>false</i> if <code>timer</code>
     * expired before connection was established.
     */
    bool wait_for_connection(ConnectionStatePtr &conn_state, Timer &timer);

    /** Calls Comm::connect to establish a connection.  If the connection
     * attempt results in an error, an error message is logged indicating
     * that the connection attempt to the given service failed, and another
     * connection attempt is scheduled for ConnectionState#timeout_ms in the
     * future.
     * @param conn_state Pointer to connection state object representing
     * connection
     */
    void send_connect_request(ConnectionStatePtr &conn_state);

    /// Sends an initialization request.
    /// @param conn_state Pointer to connection state object representing
    /// connection
    void send_initialization_request(ConnectionStatePtr &conn_state);

    /** Schedules a connection retry attempt.  Sets <code>conn_state</code> to
     * <i>disconnected</i> and schedules another connection attempt in the
     * future.
     * @param conn_state Pointer to connection state object representing
     * connection
     * @param message Message indicating why retry is being attempted
     */
    void schedule_retry(ConnectionStatePtr &conn_state, const std::string &message);

    /// Smart pointer to connection manager state
    SharedImplPtr m_impl;
};

/// Smart pointer to ConnectionManager
typedef std::shared_ptr<ConnectionManager> ConnectionManagerPtr;

}
#endif //BDG_PALO_BE_SRC_RPC_CONNECTION_MANAGER_H
