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

#ifndef BDG_PALO_BE_SRC_RPC_FAST_CLOCK_H
#define BDG_PALO_BE_SRC_RPC_FAST_CLOCK_H

#include <chrono>
#include <ctime>

namespace std { namespace chrono {

class FastClock {
public:
    typedef microseconds duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef chrono::time_point<FastClock> time_point;
    static constexpr bool is_steady = false;
    static time_point now() noexcept;
    static time_t     to_time_t  (const time_point& __t) noexcept;
    static time_point from_time_t(time_t __t) noexcept;
};

} //namespace chrono

} //namespace std
#endif // BDG_PALO_BE_SRC_RPC_FAST_CLOCK_H
