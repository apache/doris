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
