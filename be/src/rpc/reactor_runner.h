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
