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

#ifndef BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_H
#define BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_H

#include "application_queue_interface.h"
#include "application_handler.h"
#include "common/logging.h"
#include "string_ext.h"

#include <boost/thread/thread.hpp>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace palo {

/**
 * Application queue.  
 * Helper class for use by server applications that are driven by messages
 * received over the network.  This class can be used in conjunction with the
 * ApplicationHandler class to implement an incoming request queue.  Worker
 * threads pull handlers (requests) off the queue and carry them out.  The
 * following features are supported:
 *
 * <b>Groups</b>
 *
 * Because a set of worker threads pull requests from the queue and carry
 * them out independently, it is possible for requests to get executed out
 * of order relative to the order in which they arrived in the queue.  This
 * can cause problems for certain request sequences such as appending data
 * to a file in the FsBroker, or fetching scanner results from a scanner
 * using multiple readahead requests.  <i>Groups</i> are a way to give
 * applications the ability to serialize a set of requests.  Each request
 * has a <i>group ID</i> that is returned by 
 * the ApplicationHandler#get_group_id method.  Requests that have the
 * same group ID will get executed in series, in the order in which they
 * arrived in the application queue.  Requests with group ID 0 don't belong to
 * any group and will get executed independently with no serialization
 * order.
 *
 * <b>Prioritization</b>
 *
 * The ApplicationQueue supports two-level request prioritization.  Requests
 * can be designated as <i>urgent</i> which will cause them to be executed
 * before other non-urgent requests.  Urgent requests will also be executed
 * even when the ApplicationQueue has been paused.  In palo, METADATA
 * scans and updates are marked urgent which allows them procede and prevent
 * deadlocks when the application queue gets paused due to low memory
 * condition in the RangeServer.  The ApplicationHandler#is_urgent
 * method is used to signal if a request is urgent.
 */

class ApplicationQueue : public ApplicationQueueInterface {
/** Tracks group execution state.
 * A GroupState object is created for each unique group ID to track the
 * queue execution state of requests in the group.
 */
    class GroupState {
    public:
        GroupState() : group_id(0), running(false), outstanding(1) { return; }
        uint64_t group_id;    //!< Group ID
        /** <i>true</i> if a request from this group is being executed */
        bool     running;
        /** Number of outstanding (uncompleted) requests in queue for this group*/
        int      outstanding;
    };

    /** Hash map of thread group ID to GroupState
    */ 
    typedef std::unordered_map<uint64_t, GroupState *> GroupStateMap;

    /** Request record.
    */
    class RequestRec {
        public:
            RequestRec(ApplicationHandler *arh) : handler(arh), group_state(0) { return; }
            ~RequestRec() { delete handler; }
            ApplicationHandler *handler; //!< Pointer to ApplicationHandler
            GroupState *group_state;     //!< Pointer to GroupState to which request belongs
    };

    /** Individual request queue
    */
    typedef std::list<RequestRec *> RequestQueue;

    /** Application queue state shared among worker threads.
    */
    class ApplicationQueueState {
    public:
        ApplicationQueueState() : threads_available(0), shutdown(false),
        paused(false) { }
        /// Normal request queue
        RequestQueue queue;
        /// Urgent request queue
        RequestQueue urgent_queue;
        /// Group ID to group state map
        GroupStateMap group_state_map;
        /// %Mutex for serializing concurrent access
        std::mutex mutex;
        /// Condition variable to signal pending handlers
        std::condition_variable cond;
        /// Condition variable used to signal <i>quiesced</i> queue
        std::condition_variable quiesce_cond;
        /// Idle thread count
        size_t threads_available;
        /// Total initial threads
        size_t threads_total;
        /// Flag indicating if shutdown is in progress
        bool shutdown;
        /// Flag indicating if queue has been paused
        bool paused;
    };

    /** Application queue worker thread function (functor)
    */
    class Worker {
    public:
        Worker(ApplicationQueueState &qstate, bool one_shot = false) 
            : m_state(qstate), m_one_shot(one_shot) { return; }
        /** thread run method
        */
        void operator()() {
            RequestRec *rec = 0;
            RequestQueue::iterator iter;
            while (true) {
                {
                    std::unique_lock<std::mutex> lock(m_state.mutex);
                    m_state.threads_available++;
                    while ((m_state.paused || m_state.queue.empty()) &&
                            m_state.urgent_queue.empty()) {
                        if (m_state.shutdown) {
                            m_state.threads_available--;
                            return;
                        }
                        if (m_state.threads_available == m_state.threads_total) {
                            m_state.quiesce_cond.notify_all();
                        }
                        m_state.cond.wait(lock);
                    }
                    if (m_state.shutdown) {
                        m_state.threads_available--;
                        return;
                    }
                    rec = 0;
                    iter = m_state.urgent_queue.begin();
                    while (iter != m_state.urgent_queue.end()) {
                        rec = (*iter);
                        if (rec->group_state == 0 || !rec->group_state->running) {
                            if (rec->group_state) {
                                rec->group_state->running = true;
                            }
                            m_state.urgent_queue.erase(iter);
                            break;
                        }
                        if (!rec->handler || rec->handler->is_expired()) {
                            iter = m_state.urgent_queue.erase(iter);
                            remove_expired(rec);
                        }
                        rec = 0;
                        iter++;
                    }
                    if (rec == 0 && !m_state.paused) {
                        iter = m_state.queue.begin();
                        while (iter != m_state.queue.end()) {
                            rec = (*iter);
                            if (rec->group_state == 0 || !rec->group_state->running) {
                                if (rec->group_state) {
                                    rec->group_state->running = true;
                                }
                                m_state.queue.erase(iter);
                                break;
                            }
                            if (!rec->handler || rec->handler->is_expired()) {
                                iter = m_state.queue.erase(iter);
                                remove_expired(rec);
                            }
                            rec = 0;
                            iter++;
                        }
                    }
                    if (rec == 0 && !m_one_shot) {
                        if (m_state.shutdown) {
                            m_state.threads_available--;
                            return;
                        }
                        m_state.cond.wait(lock);
                        if (m_state.shutdown) {
                            m_state.threads_available--;
                            return;
                        }
                    }
                    m_state.threads_available--;
                }
                if (rec) {
                    if (rec->handler) {
                        rec->handler->run();
                    }
                    remove(rec);
                    if (m_one_shot) {
                        return;
                    }
                }
                else if (m_one_shot) {
                    return;
                }
            }
            LOG(INFO) << "thread exit";
        }

    private:
        /** Removes and deletes a request.  This method updates the group
         * state associated with <code>rec</code> by setting the running flag to
         * <i>false</i> and decrementing the outstanding count.  If the
         * outstanding count for the group drops to 0, the group state record
         * is removed from ApplicationQueueState::group_state_map and is deleted.
         * @param rec Request record to remove
         */
        void remove(RequestRec *rec) {
            if (rec->group_state) {
                std::lock_guard<std::mutex> ulock(m_state.mutex);
                rec->group_state->running = false;
                rec->group_state->outstanding--;
                if (rec->group_state->outstanding == 0) {
                    m_state.group_state_map.erase(rec->group_state->group_id);
                    delete rec->group_state;
                }
            }
            delete rec;
        }

        /** Removes and deletes an expired request.  This method updates the group
         * state associated with <code>rec</code> by decrementing the outstanding
         * count.  If the outstanding count for the group drops to 0, the group
         * state record is removed from ApplicationQueueState::group_state_map and
         * is deleted.
         * @param rec Request record to remove
         */
        void remove_expired(RequestRec *rec) {
            if (rec->group_state) {
                rec->group_state->outstanding--;
                if (rec->group_state->outstanding == 0) {
                    m_state.group_state_map.erase(rec->group_state->group_id);
                    delete rec->group_state;
                }
            }
            delete rec;
        }

        /// Shared application queue state object
        ApplicationQueueState &m_state;

        /// Set to <i>true</i> if thread should exit after executing request
        bool m_one_shot;
    };

    /// Application queue state object
    ApplicationQueueState m_state;

    /// Boost thread group for managing threads
    boost::thread_group m_threads;

    /// Vector of thread IDs
    std::vector<boost::thread::id> m_thread_ids;

    /// Flag indicating if threads have joined after a shutdown
    bool joined;

    /** Set to <i>true</i> if queue is configured to allow dynamic thread
     * creation
     */
    bool m_dynamic_threads;

public:
    /** Default constructor used by derived classes only
    */
    ApplicationQueue() : joined(true) {}

    /**
     * Constructor initialized with worker thread count.
     * This constructor sets up the application queue with a number of worker
     * threads specified by <code>worker_count</code>.
     * @param worker_count Number of worker threads to create
     * @param dynamic_threads Dynamically create temporary thread to carry out
     * requests if none available.
     */
    ApplicationQueue(int worker_count, bool dynamic_threads = true) 
        : joined(false), m_dynamic_threads(dynamic_threads) {
            m_state.threads_total = worker_count;
            Worker Worker(m_state);
            assert(worker_count > 0);
            for (int i = 0; i < worker_count; ++i) {
                m_thread_ids.push_back(m_threads.create_thread(Worker)->get_id());
            }
            //threads
        }

    /** Destructor.
    */
    virtual ~ApplicationQueue() {
        if (!joined) {
            shutdown();
            join();
        }
    }

    /**
     * Returns all the thread IDs for this threadgroup
     * @return vector of thread::id
     */
    std::vector<boost::thread::id> get_thread_ids() const {
        return m_thread_ids;
    }

    /**
     * Shuts down the application queue.  All outstanding requests are carried
     * out and then all threads exit.  #join can be called to wait for
     * completion of the shutdown.
     */
    void shutdown() {
        m_state.shutdown = true;
        m_state.cond.notify_all();
    }

    /** Wait for queue to become idle (with timeout).
     * @param deadline Return by this time if queue does not become idle
     * @param reserve_threads Number of threads that can be active when queue is
     * idle
     * @return <i>false</i> if <code>deadline</code> was reached before queue
     * became idle, <i>true</i> otherwise
     */
    bool wait_for_idle(std::chrono::time_point<std::chrono::steady_clock> deadline,
            int reserve_threads = 0) {
        std::unique_lock<std::mutex> lock(m_state.mutex);
        return m_state.quiesce_cond.wait_until(lock, deadline, 
            [this, reserve_threads](){ 
                return m_state.threads_available >= (m_state.threads_total-reserve_threads); 
            });
    }

    /**
     * Waits for a shutdown to complete.  This method returns when all
     * application queue threads exit.
     */
    void join() {
        if (!joined) {
            m_threads.join_all();
            joined = true;
        }
    }

    /** Starts application queue.
    */
    void start() {
        std::lock_guard<std::mutex> lock(m_state.mutex);
        m_state.paused = false;
        m_state.cond.notify_all();
    }

    /** Stops (pauses) application queue, preventing non-urgent requests from
     * being executed.  Any requests that are being executed at the time of the
     * call are allowed to complete.
     */
    void stop() {
        std::lock_guard<std::mutex> lock(m_state.mutex);
        m_state.paused = true;
    }

    /** Adds a request (application request handler) to the application queue.
     * The request queue is designed to support the serialization of related
     * requests.  Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the
     * Event object.
     * @param app_handler Pointer to request to add
     */
    virtual void add(ApplicationHandler *app_handler) {
        GroupStateMap::iterator uiter;
        uint64_t group_id = app_handler->get_group_id();
        RequestRec *rec = new RequestRec(app_handler);
        rec->group_state = 0;
        assert(app_handler);
        if (group_id != 0) {
            std::lock_guard<std::mutex> ulock(m_state.mutex);
            if ((uiter = m_state.group_state_map.find(group_id))
                    != m_state.group_state_map.end()) {
                rec->group_state = (*uiter).second;
                rec->group_state->outstanding++;
            }
            else {
                rec->group_state = new GroupState();
                rec->group_state->group_id = group_id;
                m_state.group_state_map[group_id] = rec->group_state;
            }
        }
        {
            std::lock_guard<std::mutex> lock(m_state.mutex);
            if (app_handler->is_urgent()) {
                m_state.urgent_queue.push_back(rec);
                if (m_dynamic_threads && m_state.threads_available == 0) {
                    Worker worker(m_state, true);
                    boost::thread t(worker);
                }
            }
            else
                m_state.queue.push_back(rec);
            m_state.cond.notify_one();
        }
    }

    /** Adds a request (application request handler) to the application queue.
     * The request queue is designed to support the serialization of related
     * requests.  Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the
     * Event object.
     * @note This method is defined for symmetry and just calls #add
     * @param app_handler Pointer to request to add
     */
    virtual void add_unlocked(ApplicationHandler *app_handler) {
        add(app_handler);
    }

    /// Returns the request backlog
    /// Returns the request backlog, which is the number of requests waiting on
    /// the request queues for a thread to become available
    /// @return Request backlog
    size_t backlog() {
        std::lock_guard<std::mutex> lock(m_state.mutex);
        return m_state.queue.size() + m_state.urgent_queue.size();
    }
};

/// Shared smart pointer to ApplicationQueue object
typedef std::shared_ptr<ApplicationQueue> ApplicationQueuePtr;
}

#endif //BDG_PALO_BE_SRC_RPC_APPLICATION_QUEUE_H
