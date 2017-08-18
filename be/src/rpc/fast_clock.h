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
