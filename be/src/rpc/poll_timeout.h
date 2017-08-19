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

#ifndef BDG_PALO_BE_SRC_RPC_POLL_TIMEOUT_H
#define BDG_PALO_BE_SRC_RPC_POLL_TIMEOUT_H

#include "clock.h"
#include <cassert>

namespace palo {

/** Maintains next timeout for event polling loop.  This class is used to
 * maintain and provide access to the next timeout for the event polling
 * loops.  It contains accessor methods to return the timeout in different
 * formats required by the various polling interfaces.
 */
class PollTimeout {
public:

    /** Constructor. */
    PollTimeout() { }

    /** Sets the next timeout.
     * @param now Current time
     * @param expire Absolute time of next timeout
     */
    void set(ClockT::time_point now, ClockT::time_point expire) {
        assert(std::chrono::operator<=(now, expire));
        auto diff_usec = expire - now;
        duration_ts.tv_sec = diff_usec.count() / 1000000;
        duration_ts.tv_nsec = (diff_usec.count() % 1000000) * 1000;
        ts_ptr = &duration_ts;
        duration_millis = std::chrono::duration_cast<std::chrono::milliseconds>(diff_usec);
    }

    /** Sets the next timeout to be an indefinite time in the future.
    */
    void set_indefinite() {
        ts_ptr = nullptr;
        duration_millis = std::chrono::milliseconds(-1);
    }

    /** Gets duration until next timeout in the form of milliseconds.
     * @return Milliseconds until next timeout
     */
    int get_millis() { return duration_millis.count(); }

    /** Gets duration until next timeout in the form of a pointer to timespec.
     * @return Pointer to timespec representing duration until next timeout.
     */
    struct timespec *get_timespec() { return ts_ptr; }

private:

    /// Pointer to to #duration_ts or 0 if indefinite
    struct timespec *ts_ptr {};

    /// timespec structure holding duration until next timeout
    struct timespec duration_ts;

    /// Duration until next timeout in milliseconds
    std::chrono::milliseconds duration_millis {-1};
};

} //namespace palo
#endif //BDG_PALO_BE_RPC_POLL_TIMEOUT_H
