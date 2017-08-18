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

#ifndef BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_SYNCHRONIZER_H
#define BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_SYNCHRONIZER_H

#include "dispatch_handler.h"
#include "event.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

namespace palo {

/** DispatchHandler class used to synchronize with response messages.
 * This class is a specialization of DispatchHandler that is used to
 * synchronize with responses resulting from previously sent request messages.
 * It contains a queue of events (response events) and a condition variable
 * that gets signalled when an event gets put on the queue.
 *
 * Example usage:
 *
 * <pre>
 * {
 *   DispatchHandlerSynchronizer sync_handler;
 *   EventPtr event;
 *   CommBufPtr cbp(... create protocol message here ...);
 *   if ((error = m_comm->send_request(m_addr, cbp, &sync_handler))
 *       != error::OK) {
 *      // log error message here ...
 *      return error;
 *   }
 *   if (!sync_handler.wait_for_reply(event))
 *       // log error message here ...
 *   error = (int)Protocol::response_code(event);
 *   return error;
 * } </pre>
 *
 */
class DispatchHandlerSynchronizer : public DispatchHandler {
public:

    /** Constructor.  Initializes state.
    */
    DispatchHandlerSynchronizer();

    virtual ~DispatchHandlerSynchronizer() {}

    /** Event Dispatch method.  This gets called by the AsyncComm layer when an
     * event occurs in response to a previously sent request that was supplied
     * with this dispatch handler.  It pushes the event onto the event queue and
     * signals (notify_one) the condition variable.
     *
     * @param event Smart pointer to event object
     */
    virtual void handle(EventPtr &event);

    /** This method is used by a client to synchronize.  The client
     * sends a request via the AsyncComm layer with this object
     * as the dispatch handler.  It then calls this method to
     * wait for the response (or timeout event).  This method
     * just blocks on the condition variable until the event
     * queue is non-empty and then removes and returns the head of the
     * queue.
     *
     * @param event Smart pointer to event object
     * @return true if next returned event is type MESSAGE and contains
     *         status error::OK, false otherwise
     */
    bool wait_for_reply(EventPtr &event);

    /// Waits for CONNECTION_ESTABLISHED event.
    /// This function waits for an event to arrive on #m_receive_queue and if it
    /// is an ERROR event, it throws an exception, if it is a DISCONNECT event
    /// it returns <i>false</i>, and if it is a CONNECTION_ESTABLISHED event,
    /// it returns <i>true</i>.
    /// @return <i>true</i> if CONNECTION_ESTABLISHED event received,
    /// <i>false</i> if DISCONNECT event received.
    /// @throws Exception with code set to ERROR event error code.
    bool wait_for_connection();

private:
    /// Mutex for serializing concurrent access
    std::mutex m_mutex;
    /// Condition variable for signalling change in queue state
    std::condition_variable m_cond;
    /// Event queue
    std::queue<EventPtr> m_receive_queue;
};

/// Shared smart pointer to DispatchHandlerSynchronizer
typedef std::shared_ptr<DispatchHandlerSynchronizer> DispatchHandlerSynchronizerPtr;

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_DISPATCH_HANDLER_SYNCHRONIZER_H
