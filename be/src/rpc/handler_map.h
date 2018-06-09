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

#ifndef BDG_PALO_BE_SRC_RPC_HANDLER_MAP_H
#define BDG_PALO_BE_SRC_RPC_HANDLER_MAP_H

#include "comm_address.h"
#include "comm_buf.h"
#include "io_handler_data.h"
#include "io_handler_datagram.h"
#include "io_handler_raw.h"
#include "proxy_map.h"
#include "error.h"
#include "common/logging.h"
#include "sock_addr_map.h"

#include "timer.h"
#include <condition_variable>
#include <cassert>
#include <memory>
#include <mutex>

namespace palo {
class IOHandlerAccept;

/** Data structure for mapping socket addresses to I/O handlers.
 * An I/O handler is associated with each socket connection and is used
 * to handle polling events on the socket descriptor.  Examples incude
 * writing a message to the socket, reading a message from the socket,
 * or completing a connection request.  This class maintains three maps,
 * one for TCP socket connections, UDP socket connections, and one for
 * accept sockets.  The Comm methods use this map to locate the I/O
 * handler for a given address.
 */
class HandlerMap {
public:

    /** Constructor. */
    HandlerMap() : m_proxies_loaded(false) { }

    /** Inserts an accept handler.
     * Uses IOHandler#m_local_addr as the key
     * @param handler Accept I/O handler to insert
     */
    void insert_handler(IOHandlerAccept *handler);

    /** Inserts a data (TCP) handler.
     * Uses IOHandler#m_addr as the key.  If program is the proxy master,
     * a proxy map update message with the new mapping is broadcast to
     * all connections.
     * @param handler Data (TCP) I/O handler to insert
     * @param checkout Atomically checkout handler
     */
    void insert_handler(IOHandlerData *handler, bool checkout=false);

    /** Inserts a datagram (UDP) handler.
     * Uses IOHandler#m_local_addr as the key.
     * @param handler Datagram (UDP) I/O handler to insert
     */
    void insert_handler(IOHandlerDatagram *handler);

    /** Inserts a raw handler.
     * Uses IOHandler#m_addr as the key.
     * @param handler Raw I/O handler to insert
     */
    void insert_handler(IOHandlerRaw *handler);

    /** Checks out accept I/O handler associated with <code>addr</code>.
     * Looks up <code>addr</code> in accept map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return error::OK on success, or error::COMM_NOT_CONNECTED if no mapping
     * found for <code>addr</code>.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerAccept **handler);

    /** Checks out data (TCP) I/O handler associated with <code>addr</code>.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in data map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return error::OK on success, error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerData **handler);

    /** Checks out datagram (UDP) I/O handler associated with <code>addr</code>.
     * Looks up <code>addr</code> in datagram map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return error::OK on success, or error::COMM_NOT_CONNECTED if no mapping
     * found for <code>addr</code>.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerDatagram **handler);

    /** Checks out raw I/O handler associated with <code>addr</code>.
     * Looks up <code>addr</code> in raw map.  If an entry is found,
     * then its reference count is incremented and it is returned
     * in <code>handler</code>.
     * @param addr Connection address
     * @param handler Address of handler pointer returned
     * @return error::OK on success, or error::COMM_NOT_CONNECTED if no mapping
     * found for <code>addr</code>.
     */
    int checkout_handler(const CommAddress &addr, IOHandlerRaw **handler);

    /** Checks to see if <code>addr</code> is contained in map.
     * First translates <code>addr</code> to socket address and then
     * looks up translated address in data map.
     * @param addr Connection address
     * @return error::OK if found, error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no translation
     * exists, or error::COMM_NOT_CONNECTED if no mapping found for
     * translated address.
     */
    int contains_data_handler(const CommAddress &addr);

    /** Decrements the reference count of <code>handler</code>.
     * The decrementing of a handler's reference count is done by this method
     * with #m_mutex locked which avoids a race condition between checking
     * out handlers and purging them.
     * @param handler Pointer to I/O handler for which to decrement reference
     * count
     */
    void decrement_reference_count(IOHandler *handler);

    /** Sets an alias address for an existing TCP address in map.
     * RangeServers listen on a well-known port defined by the
     * <code>palo.RangeServer.Port</code> configuration property
     * (default = 15865).  However, RangeServers connect to the master using
     * an ephemeral port due to a bind conflict with its listen socket.  So that
     * the Master can refer to the RangeServer using the well-known port, an
     * alias address can be registered and subsequently used to reference the
     * connection.  This method adds an entry to the data (TCP) map for
     * <code>alias</code> which references the IOHandlerData object previously
     * registered under <code>addr</code>.
     * @param addr Address of previously registered data handler
     * @param alias Alias address to add
     * @return error::OK on success, or error::COMM_CONFLICTING_ADDRESS if
     * <code>alias</code> is already in the data map, or
     * error::COMM_NOT_CONNECTED if <code>addr</code> is not found in the data
     * map.
     */
    int set_alias(const InetAddr &addr, const InetAddr &alias);

    /** Removes <code>handler</code> from map.  This method removes
     * <code>handler</code> from the data, datagram, or accept map, depending
     * on the type of handler.  If <code>handler</code> refers to a data
     * handler, then its alias address entry is also removed from the data map.
     * @param handler IOHandler to remove
     * @return error::OK on success, or error::COMM_NOT_CONNECTED if
     * <code>handler</code> is not found in any of the maps.
     */
    int remove_handler(IOHandler *handler);

    /** Decomissions <code>handler</code>.  Since handler pointers are passed
     * into the polling mechanism and asynchronously referenced by reactor
     * threads, care must be taken to not delete a handler until it has
     * be removed from the polling mechanism.  This is accomplished by
     * introducing a two-step removal process.  First a handler is decomissioned
     * (by this method) by removing it from the associated map, adding it to the
     * #m_decomissioned_handlers set and marking it decomissioned.  Once there
     * are no more references to the handler, it may be safely removed.  The
     * removal is accomplished via #purge_handler which is called by the reactor
     * thread after it has been removed from the polling interface.
     * @param handler Pointer to IOHandler to decomission
     */
    void decomission_handler_unlocked(IOHandler *handler);

    /** Decomissions <code>handler</code> with #m_mutex locked.
     * This method locks #m_mutex and calls #decomission_handler_unlocked
     * @param handler Pointer to IOHandler to decomission
     */
    void decomission_handler(IOHandler *handler) {
        std::lock_guard<std::mutex> lock(m_mutex);

        decomission_handler_unlocked(handler);

    }
    /** Decomissions all handlers.  This method is called by the ~Comm to
     * decomission all of the handlers in the map.
     * @note It doesn't look like the Comm object ever gets deleted and
     * therefore this method never gets called.  See issue 1031.
     */
    void decomission_all();

    /** Determines if <code>handler</code> can be destoryed.  
     * @return <i>true</i> if <code>handler</code> is decomissioned and
     * has a reference count of 0.
     */
    bool destroy_ok(IOHandler *handler);

    /** Translates <code>proxy_addr</code> to its corresponding IPV4 address.
     * This method fetches the mapping for <code>proxy_addr</code> from
     * #m_proxy_map and returns the associated IPV4 address in
     * <code>addr</code>.  If <code>addr</code> is NULL, then no translation
     * occurs but the return value can be checked to see if the proxy name
     * contains a mapping.
     * @param proxy_addr Reference to proxy address
     * @param addr Pointer to return IPV4 address
     * @return <i>true</i> if mapping found, <i>false</i> otherwise
     */
    bool translate_proxy_address(const CommAddress &proxy_addr, InetAddr *addr);

    /** Purges (removes) <code>handler</code>.  This method removes
     * <code>handler</code> from the #m_decomissioned_handlers set, signals
     * #m_cond if #m_decomissioned_handlers becomes empty, calls
     * <code>hander->disconnect()</code>, and then deletes the handler.
     * This method must only be called from a reactor thread after the handler
     * has been removed from the polling interface and #destroy_ok returns
     * true for the handler.
     * @param handler Handler to purge
     */
    void purge_handler(IOHandler *handler);

    /** Waits for map to become empty.  This method assumes that all of the
     * handlers in the map have been decomissioned.  It waits for the
     * #m_decomissioned_handlers set to become empty, waiting on #m_cond
     * until it does.
     */
    void wait_for_empty();

    /** Adds or updates proxy information.  This method adds or updates proxy
     * information in #m_proxy_map.  For the data handler to which
     * <code>addr</code> refers, it updates its proxy name via a call to
     * IOHandler::set_proxy and then calls #propagate_proxy_map with the newly
     * added/updated proxy information to update all active data connections
     * with the new proxy information.
     * @note This method should only be called by the proxy master.
     * @param proxy Proxy name of new/updated mapping
     * @param hostname Hostname of new/updated mapping
     * @param addr InetAddr of new/updated mapping
     * @return error::OK on success, or one of the errors returned by
     * #propagate_proxy_map
     */
    int add_proxy(const std::string &proxy, const std::string &hostname, const InetAddr &addr);

    /** Removes a proxy name from the proxy map.  This method removes
     * <code>proxy</code> from #m_proxy_map and then calls #propagate_proxy_map
     * to propagate the removed mapping information to all connections.
     * @note This method should only be called by the proxy master.
     * @param proxy Proxy name to remove
     * @return error::OK if <code>proxy</code> not found in proxy map or if
     * it was successfully removed, or one of the errors returned by
     * #propagate_proxy_map
     */
    int remove_proxy(const std::string &proxy);

    /** Returns the proxy map
     * @param proxy_map reference to returned proxy map
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    /** Updates the proxy map with a proxy map update message received from the
     * proxy master.  Calls ProxyMap::update_mappings with <code>message</code>
     * to update the proxy map.  If any of the proxy names have changed, the
     * corresponding data handlers are updated with a call to
     * IOHandler::set_proxy.  For each mapping in <code>message</code> that has
     * the hostname set to <code>--DELETED--</code>, the associated data handler
     * is decomissioned.  After the proxy map has been successfuly updated, the
     * #m_proxies_loaded flag is set to <i>true</i> and the #m_cond_proxy
     * condition variable is signalled.
     * @param message Pointer to proxy map update message
     * @param message_len Length of proxy map update message
     */
    void update_proxy_map(const char *message, size_t message_len);

    /** Sends the current proxy map over connection identified by
     * <code>handler</code>.  This method must only be called by the proxy
     * master, otherwise it will assert.
     * @param handler Connection over which to send proxy map
     * @return Same set of error codes returned by IOHandlerData#send_message
     */
    int32_t propagate_proxy_map(IOHandlerData *handler);

    /** Waits for proxy map to get updated from a proxy map update message
     * received from the master.  This method waits on #m_cond_proxy for
     * #m_proxies_loaded to become <i>true</i> or <code>timer</code> expires.
     * @param timer Deadline timer
     * @return <i>true</i> if proxy map was loaded, <i>false</i> if
     * <code>timer</code> expired before proxy map was loaded.
     */
    bool wait_for_proxy_map(Timer &timer);

private:
    /** Propagates proxy map information in <code>mappings</code> to
     * all active data (TCP) connections.  This method creates a proxy
     * map update message from the mappings in <code>mappings</code>.
     * The update message is just a list of mapping entries in the
     * following format:
     * @verbatim <proxy> '\t' <hostname> '\t' <addr> '\n' @endverbatim
     * Then the proxy map update message is sent via each of the handlers in
     * the data (TCP) handler map.  If an error is encountered on a
     * handler when trying to send the proxy map, it will be decomissioned.
     * @param mappings Proxy map information to propagate.
     * @return error::OK if proxy map update message was successfully
     * sent across all data handlers, otherwise one of the error
     * codes returned by IOHandlerData#send_message
     */
    int propagate_proxy_map(ProxyMapT &mappings);

    /** Translates <code>addr</code> to an InetAddr (IP address).
     * If <code>addr</code> is of type CommAddress::PROXY, then the
     * #m_proxy_map is consulted to translate the proxy name to an IP address,
     * otherwise if <code>addr</code> is of type CommAddress::INET, the IP
     * address held in <code>addr</code> is copied to <code>inet_addr</code>.
     * @param addr Address to translate
     * @param inet_addr Pointer to valid InetAddr object to hold translated
     * address
     * @return error::OK on success, error::COMM_INVALID_PROXY if
     * <code>addr</code> is of type CommAddress::PROXY and no mapping is found.
     */
    int translate_address(const CommAddress &addr, InetAddr *inet_addr);

    /** Removes <code>handler</code> from map without locking #m_mutex.  This
     * method removes <code>handler</code> from the data, datagram, or accept
     * map, depending on the type of handler.  If <code>handler</code> refers to
     * a data handler, then its alias address entry is also removed from the
     * data map.
     * @param handler IOHandler to remove
     * @return error::OK on success, or error::COMM_NOT_CONNECTED if
     * <code>handler</code> is not found in any of the maps.
     */
    int remove_handler_unlocked(IOHandler *handler);

    /** Finds <i>accept</i> I/O handler associated with <code>addr</code>.
     * This method looks up <code>addr</code> in #m_accept_handler_map and
     * returns the handler, if found.
     * @param addr Address of accept handler to locate
     * @return Pointer to IOHandlerAccept object associated with
     * <code>addr</code>, or 0 if not found.
     */
    IOHandlerAccept *lookup_accept_handler(const InetAddr &addr);

    /** Finds <i>data (TCP)</i> I/O handler associated with <code>addr</code>.
     * This method looks up <code>addr</code> in #m_data_handler_map and
     * returns the handler, if found.
     * @param addr Address of data handler to locate
     * @return Pointer to IOHandlerData object associated with
     * <code>addr</code>, or 0 if not found.
     */
    IOHandlerData *lookup_data_handler(const InetAddr &addr);

    /** Finds <i>datagram</i> I/O handler associated with <code>addr</code>.
     * This method looks up <code>addr</code> in #m_datagram_handler_map and
     * returns the handler, if found.
     * @param addr Address of datagram handler to locate
     * @return Pointer to IOHandlerDatagram object associated with
     * <code>addr</code>, or 0 if not found.
     */
    IOHandlerDatagram *lookup_datagram_handler(const InetAddr &addr);

    /** Finds <i>raw</i> I/O handler associated with <code>addr</code>.
     * This method looks up <code>addr</code> in #m_raw_handler_map and
     * returns the handler, if found.
     * @param addr Address of raw handler to locate
     * @return Pointer to IOHandlerRaw object associated with
     * <code>addr</code>, or nullptr if not found.
     */
    IOHandlerRaw *lookup_raw_handler(const InetAddr &addr);

    /// %Mutex for serializing concurrent access
    std::mutex m_mutex;

    /// Condition variable for signalling empty map
    std::condition_variable m_cond;

    /// Condition variable for signalling proxy map load
    std::condition_variable m_cond_proxy;

    /// Accept map (InetAddr-to-IOHandlerAccept)
    SockAddrMap<IOHandlerAccept *> m_accept_handler_map;

    /// Data (TCP) map (InetAddr-to-IOHandlerData)
    SockAddrMap<IOHandlerData *> m_data_handler_map;

    /// Datagram (UDP) map (InetAddr-to-IOHandlerDatagram)
    SockAddrMap<IOHandlerDatagram *> m_datagram_handler_map;

    /// Raw map (InetAddr-to-IOHandlerRaw)
    SockAddrMap<IOHandlerRaw *> m_raw_handler_map;

    /// Decomissioned handler set
    std::set<IOHandler *> m_decomissioned_handlers;

    /// Proxy map
    ProxyMap m_proxy_map;

    /// Flag indicating if proxy map has been loaded
    bool m_proxies_loaded;

};

/// Smart pointer to HandlerMap
typedef std::shared_ptr<HandlerMap> HandlerMapPtr;

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_HANDLER_MAP_H
