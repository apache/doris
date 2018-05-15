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

#ifndef BDG_PALO_BE_SRC_RPC_REQUEST_CACHE_H
#define BDG_PALO_BE_SRC_RPC_REQUEST_CACHE_H

#include "clock.h"

#include "dispatch_handler.h"
#include <unordered_map>

namespace palo {

class IOHandler;

/** Class used to hold pending request callback handlers.  One RequestCache object is
 * associated with each Reactor.  When a request is sent (see Comm#send_request)
 * an entry, which includes the response handler, is inserted into the
 * RequestCache.  When the corresponding response is receive, the response
 * handler is obtained by looking up the corresponding request ID in this cache.
 */
class RequestCache {

    /** Internal cache node structure.
    */
    class CacheNode {
    public:
        CacheNode(uint32_t id, IOHandler *handler, DispatchHandler *dh)
                : id(id), handler(handler) {
            if (dh != nullptr) {
                dhp = dh->shared_from_this();
            }
        }
        ~CacheNode() {}
        CacheNode* prev;            //!< Doubly-linked list prev pointers
        CacheNode* next;            //!< Doubly-linked list next pointers
        ClockT::time_point expire;  //!< Absolute expiration time
        uint32_t           id;      //!< Request ID
        IOHandler         *handler; //!< IOHandler associated with this request
        /// Callback handler to which MESSAGE, TIMEOUT, ERROR, and DISCONNECT
        /// events are delivered
        DispatchHandlerPtr dhp;
    };

    /// RequestID-to-CacheNode map
    typedef std::unordered_map<uint32_t, CacheNode *> IdHandlerMap;

public:

    /// Constructor.
    RequestCache() { }

    /** Inserts pending request callback handler into cache.
     * @param id Request ID
     * @param handler IOHandler associated with
     * @param dh Callback handler to which MESSAGE, TIMEOUT, DISCONNECT events
     * are delivered
     * @param expire Absolute expiration time of request
     */
    void insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                ClockT::time_point &expire);

    /** Removes a request from the cache.
     * @param id Request ID
     * @param handler Removed dispatch handler
     * @return <i>true</i> if removed, <i>false</i> if not found
     */
    bool remove(uint32_t id, DispatchHandlerPtr &handler);

    /** Removes next request that has timed out.  This method finds the first
     * request starting from the head of the list and removes it and returns
     * it's associated handler information if it has timed out.  During the
     * search, it physically removes any cache nodes corresponding to requests
     * that have been purged.
     * @param now Current time
     * @param handlerp Return parameter to hold pointer to associated IOHandler
     *                 of timed out request
     * @param dh Removed dispatch handler
     * @param next_timeout Pointer to variable to hold expiration time
     * of next request <b>after</b> timed out request, set to 0 if cache is empty
     * @return <i>true</i> if pointer to timed out dispatch handler was removed,
     * <i>false</i> otherwise
     */
    bool get_next_timeout(ClockT::time_point &now, IOHandler *&handlerp,
                          DispatchHandlerPtr& dhp,
                          ClockT::time_point *next_timeout, uint32_t* header_id);

    /** Purges all requests assocated with <code>handler</code>.  This
     * method walks the entire cache and purges all requests whose
     * handler is equal to <code>handler</code>.  For each purged
     * request, an ERROR event with error code <code>error</code> is
     * delivered via the request's dispatch handler.
     * @param handler IOHandler of requests to purge
     * @param error Error code to be delivered with ERROR event
     */
    void purge_requests(IOHandler *handler, int32_t error);

private:

    IdHandlerMap m_id_map; //!< RequestID-to-CacheNode map

    CacheNode *m_head {};  //!< Head of doubly-linked list

    CacheNode *m_tail {};  //!< Tail of doubly-linked list
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_REQUEST_CACHE_H
