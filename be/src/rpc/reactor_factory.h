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

#ifndef BDG_PALO_BE_SRC_RPC_REACTOR_FACTORY_H
#define BDG_PALO_BE_SRC_RPC_REACTOR_FACTORY_H

#include "reactor.h"

#include <boost/thread/thread.hpp>
#include <atomic>
#include <cassert>
#include <mutex>
#include <random>
#include <set>
#include <vector>

namespace palo {

/** Static class used to setup and manage I/O reactors.  Since the I/O reactor
 * threads are a process-wide resource, the methods of this class are static.
 */
class ReactorFactory {
public:

    /** Initializes I/O reactors.  This method creates and initializes
     * <code>reactor_count</code> reactors, plus an additional dedicated timer
     * reactor.  It also initializes the #use_poll member based on the
     * <code>Comm.UsePoll</code> property and sets the #ms_epollet
     * ("edge triggered") flag to <i>false</i> if running on Linux version older
     * than 2.6.17.  It also allocates a HandlerMap and initializes
     * ReactorRunner::handler_map to point to it.
     * @param reactor_count number of reactor threads to create
     */
    static void initialize(uint16_t reactor_count);

    /** This method shuts down the reactors
    */
    static void destroy();

    /// Joins with reactor threads
    static void join();

    /** This method returns the 'next' reactor.  It returns pointers to
     * reactors in round-robin fashion and is used by the Comm subsystem to
     * evenly distribute descriptors across all of the reactors.  The
     * atomic integer variable #ms_next_reactor is used to keep track
     * of the next reactor in the list.
     * @param reactor Smart pointer reference to returned Reactor
     */
    static void get_reactor(ReactorPtr &reactor,
            Reactor::Priority priority = Reactor::Priority::NORMAL) {
        assert(ms_reactors.size() > 0);
        if (priority == Reactor::Priority::HIGH)
            reactor = ms_reactors.front();
        else
            reactor = ms_reactors[(ms_next_reactor++ % (ms_reactors.size()-2))+1];
    }

    /** This method returns the timer reactor.
     * @param reactor Smart pointer reference to returned Reactor
     */
    static void get_timer_reactor(ReactorPtr &reactor) {
        assert(ms_reactors.size() > 0);
        reactor = ms_reactors.back();
    }

    /// Vector of reactors (last position is timer reactor)
    static std::vector<ReactorPtr> ms_reactors;

    /// Boost thread_group for managing reactor threads
    static boost::thread_group ms_threads;

    /// Pseudo random number generator
    static std::default_random_engine rng;

    /// Use "edge triggered" epoll
    static bool ms_epollet;

    /// Set to <i>true</i> if this process is acting as "Proxy Master"
    static bool proxy_master;

private:

    /// Mutex to serialize calls to #initialize
    static std::mutex ms_mutex;

    /// Atomic integer used for round-robin assignment of reactors
    static std::atomic<int> ms_next_reactor;
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_REACTOR_FACTORY_H
