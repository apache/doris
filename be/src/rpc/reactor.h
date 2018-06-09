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

#ifndef BDG_PALO_BE_SRC_RPC_REACTOR_H
#define BDG_PALO_BE_SRC_RPC_REACTOR_H

#include "clock.h"
#include "poll_timeout.h"
#include "request_cache.h"
#include "expire_timer.h"

#include <boost/thread/thread.hpp>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <vector>

namespace palo {

/** Manages reactor (polling thread) state including poll interest, request cache,
 * and timers.
 */
class Reactor {

friend class ReactorFactory;

public:

    /** Enumeration for reactor priority */
    enum class Priority {
        HIGH = 0, //< High
        NORMAL    //< Normal
    };

    /** Constructor.
     * Initializes polling interface and creates interrupt socket.
     * If ReactorFactory::use_poll is set to <i>true</i>, then the reactor will
     * use the POSIX <code>poll()</code> interface, otherwise <code>epoll</code>
     * is used on Linux, <code>kqueue</code> on OSX and FreeBSD, and
     * <code>port_associate</code> on Solaris.  For polling mechanisms that
     * do not provide an interface for breaking out of the poll wait, a UDP
     * socket #m_interrupt_sd is created (and connected to itself) and
     * added to the poll set.
     */
    Reactor();

    /** Destructor.
    */
    ~Reactor() {
        poll_loop_interrupt();
    }

    /** Adds a request to request cache and adjusts poll timeout if necessary.
     * @param id Request ID
     * @param handler I/O handler with which request is associated
     * @param dh Application dispatch handler for response MESSAGE events
     * @param expire Absolute expiration time
     */
    void add_request(uint32_t id, IOHandler *handler, DispatchHandler *dh,
            ClockT::time_point expire) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_request_cache.insert(id, handler, dh, expire);
        if (m_next_wakeup == ClockT::time_point() || expire < m_next_wakeup) {
            poll_loop_interrupt();
        }
    }

    /** Removes request associated with <code>id</code>
     * @param id Request ID
     * @param handler Removed dispatch handler
     * @return <i>true</i> if request removed, <i>false</i> otherwise
     */
    bool remove_request(uint32_t id, DispatchHandlerPtr& handler) {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_request_cache.remove(id, handler);
    }

    /** Cancels outstanding requests associated with <code>handler</code>
     * @param handler I/O handler for which outstanding requests are to be
     * cancelled
     * @param error Error code to deliver with ERROR events
     */
    void cancel_requests(IOHandler *handler, int32_t error=error::COMM_BROKEN_CONNECTION) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_request_cache.purge_requests(handler, error);
    }

    /** Adds a timer.
     * Pushes timer onto #m_timer_heap and interrupts the polling loop so that
     * the poll timeout can be adjusted if necessary.
     * @param timer Reference to ExpireTimer object
     */
    void add_timer(ExpireTimer &timer) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_timer_heap.push(timer);
        poll_loop_interrupt();
    }

    /** Cancels timers associated with <code>handler</code>.
     * @param handler Dispatch handler for which associated timers are to be
     * cancelled
     */
    void cancel_timer(const DispatchHandlerPtr &handler) {
        std::lock_guard<std::mutex> lock(m_mutex);
        typedef TimerHeap::container_type container_t;
        container_t container;
        container.reserve(m_timer_heap.size());
        ExpireTimer timer;
        while (!m_timer_heap.empty()) {
            timer = m_timer_heap.top();
            if (timer.handler.get() != handler.get()) {
                container.push_back(timer);
            }
            m_timer_heap.pop();
        }
        for (const auto &t : container) {
            m_timer_heap.push(t);
        }
    }

    /** Schedules <code>handler</code> for removal.
     * This method schedules an I/O handler for removal.  It should be called
     * only when the handler has been decomissioned in the HandlerMap and when
     * there are no outstanding references to the handler.  The handler is
     * added to the #m_removed_handlers set and a timer is set for 200
     * milliseconds in the future so that the ReactorRunner will wake up and
     * complete the removal by removing it completely from the HandlerMap and
     * delivering a DISCONNECT event if it is for a TCP socket.
     * @param handler I/O handler to schedule for removal
     */
    void schedule_removal(IOHandler *handler) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_removed_handlers.insert(handler);
        ExpireTimer timer;
        timer.expire_time = ClockT::now() + std::chrono::milliseconds(200);
        timer.handler = 0;
        m_timer_heap.push(timer);
        poll_loop_interrupt();
    }

    /** Returns set of I/O handlers scheduled for removal.
     * This is a one shot method adds the handlers that have been added to
     * #m_removed_handlers and then clears the #m_removed_handlers set.
     * @param dst reference to set filled in with removed handlers
     */
    void get_removed_handlers(std::set<IOHandler *> &dst) {
        std::lock_guard<std::mutex> lock(m_mutex);
        dst = m_removed_handlers;
        m_removed_handlers.clear();
    }

    /** Processes request timeouts and timers.
     * This method removes timed out requests from the request cache, delivering
     * ERROR events (with error == error::REQUEST_TIMEOUT) via each request's
     * dispatch handler.  It also processes expired timers by removing them from
     * #m_timer_heap and delivering a TIMEOUT event via the timer handler if
     * it exsists.
     * @param next_timeout Set to next earliest timeout of active requests and
     * timers
     */
    void handle_timeouts(PollTimeout &next_timeout);

    int epoll_fd;

    /** Forces polling interface wait call to return.
     * @return error::OK on success, or Error code on failure
     */
    int poll_loop_interrupt();

    /** Reset state after call to #poll_loop_interrupt.
     * After calling #poll_loop_interrupt and handling the interrupt, this
     * method should be called to reset back to normal polling state.
     * @return error::OK on success, or Error code on failure
     */
    int poll_loop_continue();

    /** Returns interrupt socket.
     * @return Interrupt socket
     */
    int interrupt_sd() { return m_interrupt_sd; }

protected:

    /** Priority queue for timers.
    */
    typedef std::priority_queue<ExpireTimer,
            std::vector<ExpireTimer>, LtTimerHeap> TimerHeap;

    std::mutex m_mutex;           //!< Mutex to protect members

    RequestCache m_request_cache; //!< Request cache

    TimerHeap m_timer_heap;       //!< ExpireTimer heap

    int m_interrupt_sd;           //!< Interrupt socket

    /// Set to <i>true</i> if poll loop interrupt in progress
    bool m_interrupt_in_progress {};

    /// Next polling interface wait timeout (absolute)
    ClockT::time_point m_next_wakeup;

    /// Set of IOHandler objects scheduled for removal
    std::set<IOHandler *> m_removed_handlers;

};

/// Shared smart pointer to Reactor
typedef std::shared_ptr<Reactor> ReactorPtr;

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_REACTOR_H
