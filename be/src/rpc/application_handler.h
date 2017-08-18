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

#ifndef BDG_PALO_BE_SRC_RPC_APPLICATION_HANDLER_H
#define BDG_PALO_BE_SRC_RPC_APPLICATION_HANDLER_H

#include "clock.h"
#include "event.h"
#include "reactor_runner.h"

namespace palo {
/** Base clase for application handlers.  Objects of this type are what get
 * added to an ApplicationQueue.  Provides a constructor for initialization
 * from request MESSAGE event received from the comm layer.
 * There are two attributes of a request handler that control how it is
 * treated in the Application queue:
 *
 * <b>Group ID</b>
 *
 * The ApplicationQueue supports serial execution of requests that operate on
 * a shared resource.  This is achieved through the application request
 * handler <i>group ID</i>.  Application request handlers that contain the
 * same group ID will get executed in series.  When initialized from a MESSAGE
 * event, the group ID is the same as the CommHeader#gid field of the message
 * header, otherwise it is 0.
 *
 * <b>Urgency</b>
 *
 * The ApplicationQueue supports two-level request prioritization.  Requests
 * can be designated as <i>urgent</i> which will cause them to be executed
 * before other non-urgent requests.  Urgent requests will also be executed
 * even when the ApplicationQueue has been paused.  When initialized from a
 * MESSAGE Event, the #m_urgent field will get set to <i>true</i> if the
 * CommHeader::FLAGS_BIT_URGENT is set in the CommHeader#flags field of the
 * message header.
 */
class ApplicationHandler {
public:

    /** Constructor initializing from an Event object.
     * Initializes #m_event to <code>event</code> and sets #m_urgent to
     * <i>true</i> if the CommHeader::FLAGS_BIT_URGENT is set in the
     * flags field of Event#header member of <code>event</code>.
     * @param event %Event that generated the request
     */
    ApplicationHandler(EventPtr &event) : m_event(event) {
        if (m_event) {
            m_urgent = (bool)(m_event->header.flags & CommHeader::FLAGS_BIT_URGENT);
        } else {
            m_urgent = false;
        }
    }

    /** Default constructor with #m_urgent flag initialization.
     * @param urgent Handler should be marked as urgent
     */
    ApplicationHandler(bool urgent = false) 
        : m_urgent(urgent) { }

    /** Destructor */
    virtual ~ApplicationHandler() { }

    /** Carries out the request.  Called by an ApplicationQueue worker thread.
    */
    virtual void run() = 0;

    /** Returns the <i>group ID</i> that this handler belongs to.  This
     * value is taken from the associated event object (see Event#group_id)
     * if it exists, otherwise the value is 0 indicating that the handler
     * does not belong to a group.
     * @return Group ID
     */
    uint64_t get_group_id() {
        return (m_event) ?  m_event->group_id : 0;
    }

    /** Returns <i>true</i> if request is urgent.
     * @return <i>true</i> if urgent
     */
    bool is_urgent() { return m_urgent; }

    /** Returns <i>true</i> if request has expired.
     * @return <i>true</i> if request has expired.
     */
    bool is_expired() {
        if (m_event && m_event->type == Event::MESSAGE &&
                ReactorRunner::record_arrival_time &&
                (m_event->header.flags & CommHeader::FLAGS_BIT_REQUEST)) {
            auto now = ClockT::now();
            uint32_t wait_ms = (uint32_t)std::chrono::duration_cast<std::chrono::milliseconds>(now - m_event->arrival_time).count();
            if (wait_ms >= m_event->header.timeout_ms) {
                if (m_event->header.flags & CommHeader::FLAGS_BIT_REQUEST)
                    LOG(WARNING) << "Request expired, wait time %u > timeout %u" <<
                        (unsigned)wait_ms << m_event->header.timeout_ms;
                else
                    LOG(WARNING) << "Response expired, wait time %u > timeout %u" << (unsigned)wait_ms <<
                        m_event->header.timeout_ms;
                if (m_event->header.timeout_ms == 0) {
                    LOG(INFO) << "Changing zero timeout request to 120000 ms";
                    m_event->header.timeout_ms = 120000;
                    return false;
                }
                return true;
            }
        }
        return false;
    }

protected:
    EventPtr m_event; //!< MESSAGE Event from which handler was initialized
    bool m_urgent;    //!< Flag indicating if handler is urgent
};

} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_APPLICATION_HANDLER_H
