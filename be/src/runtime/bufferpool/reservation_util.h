// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_RUNTIME_BUFFERPOOL_RESERVATION_UTIL_H_
#define DORIS_BE_RUNTIME_BUFFERPOOL_RESERVATION_UTIL_H_

#include <stdint.h>

namespace doris {

/// Utility code related to buffer reservations.
class ReservationUtil {
public:
    /// There are currently two classes of memory: reserved memory (i.e. memory that is
    /// reserved with reservation trackers/allocated by the buffer pool), and unreserved
    /// memory (i.e. everything else; code that hasn't yet been updated to use reserved
    /// memory). Eventually, all memory should be in the former category, but each operator
    /// must be converted to use reserved memory and that work is ongoing. See IMPALA-4834.
    /// In the meantime, the system memory must be shared between these two classes of
    /// memory. RESERVATION_MEM_FRACTION and RESERVATION_MEM_MIN_REMAINING are used to
    /// determine an upper bound on reserved memory for a query. Operators operate reliably
    /// when they are using bounded reserved memory (e.g. staying under a limit by
    /// spilling), but will generally fail if they hit a limit when trying to allocate
    /// unreserved memory. Thus we need to ensure there is always space left in the query
    /// memory limit for unreserved memory.

    /// The fraction of the query mem limit that is used as the maximum buffer reservation
    /// limit, i.e. the bound on reserved memory. It is expected that unreserved memory
    /// (i.e. not accounted by buffer reservation trackers) stays within
    /// (1 - RESERVATION_MEM_FRACTION).
    /// TODO: remove once all operators use buffer reservations.
    static const double RESERVATION_MEM_FRACTION;

    /// The minimum amount of memory that should be left after buffer reservations, i.e.
    /// this is the minimum amount of memory that should be left for unreserved memory.
    /// TODO: remove once all operators use buffer reservations.
    static const int64_t RESERVATION_MEM_MIN_REMAINING;

    /// Helper function to get the query buffer reservation limit (in bytes) given a query
    /// mem_limit. In other words, this determines the maximum portion of the mem_limit
    /// that should go to reserved memory. The limit on reservations is computed as:
    /// min(query_limit * RESERVATION_MEM_FRACTION,
    ///     query_limit - RESERVATION_MEM_MIN_REMAINING)
    /// TODO: remove once all operators use buffer reservations.
    static int64_t GetReservationLimitFromMemLimit(int64_t mem_limit);

    /// Helper function to get the minimum query mem_limit (in bytes) that will be large
    /// enough for a buffer reservation of size 'buffer_reservation' bytes. In other words,
    /// this determines the minimum mem_limit that will be large enough to accomidate
    /// 'buffer_reservation' reserved memory, as well as some amount of unreserved memory
    /// (determined by a heuristic).
    /// The returned mem_limit X satisfies:
    ///    buffer_reservation <= GetReservationLimitFromMemLimit(X)
    /// TODO: remove once all operators use buffer reservations.
    static int64_t GetMinMemLimitFromReservation(int64_t buffer_reservation);
};

} // namespace doris

#endif
