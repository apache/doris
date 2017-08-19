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

#ifndef BDG_PALO_BE_SRC_RPC_REACTOR_RUNNER_H
#define BDG_PALO_BE_SRC_RPC_REACTOR_RUNNER_H

#include "handler_map.h"
#include "reactor.h"

namespace palo {

class IOHandler;

/** Thread functor class for reacting to I/O events.
 * The AsyncComm layer is initialized with some number of <i>reactor</i>
 * threads (see ReactorFactory#initialize).  The ReactorRunner class
 * acts as the thread function for these reactor threads.  The primary
 * function of a reactor thread is to wait for I/O events (e.g. read/write
 * readiness) to occur on a set of registered socket descriptors and then
 * call into the associated IOHandler objects to handle the I/O events.
 * It also handles cleanup and removal of sockets that have been disconnected.
 */
class ReactorRunner {
public:

    /** Primary thread entry point */
    void operator()();

    /** Assocates reactor state object with this ReactorRunner.
     * @param reactor Reference to smart pointer to reactor state object
     */
    void set_reactor(ReactorPtr &reactor) { m_reactor = reactor; }

    /// Flag indicating that reactor thread is being shut down
    static bool shutdown;

    /// If set to <i>true</i> arrival time is recorded and passed into
    /// IOHandler#handle
    static bool record_arrival_time;

    /// Smart pointer to HandlerMap
    static HandlerMapPtr handler_map;

private:

    /** Cleans up and removes a set of handlers.
     * For each handler in <code>handlers</code>, this method destroys
     * the handler in the HandlerMap, cancels any outstanding requests
     * on the handler, removes polling interest from polling mechanism,
     * an purges the handler from the HandlerMap.
     * @param handlers Set of IOHandlers to remove
     */
    void cleanup_and_remove_handlers(std::set<IOHandler *> &handlers);

    ReactorPtr m_reactor; //!< Smart pointer to reactor state object
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_REACTOR_RUNNER_H
