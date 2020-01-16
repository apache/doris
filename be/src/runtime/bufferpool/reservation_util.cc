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

#include "runtime/bufferpool/reservation_util.h"

#include <algorithm>

namespace doris {

// Most operators that accumulate memory use reservations, so the majority of memory
// should be allocated to buffer reservations, as a heuristic.
const double ReservationUtil::RESERVATION_MEM_FRACTION = 0.8;
const int64_t ReservationUtil::RESERVATION_MEM_MIN_REMAINING = 75 * 1024 * 1024;

int64_t ReservationUtil::GetReservationLimitFromMemLimit(int64_t mem_limit) {
    int64_t max_reservation = std::min<int64_t>(RESERVATION_MEM_FRACTION * mem_limit,
                                                mem_limit - RESERVATION_MEM_MIN_REMAINING);
    return std::max<int64_t>(0, max_reservation);
}

int64_t ReservationUtil::GetMinMemLimitFromReservation(int64_t buffer_reservation) {
    buffer_reservation = std::max<int64_t>(0, buffer_reservation);
    return std::max<int64_t>(buffer_reservation * (1.0 / ReservationUtil::RESERVATION_MEM_FRACTION),
                             buffer_reservation + ReservationUtil::RESERVATION_MEM_MIN_REMAINING);
}
} // namespace doris
