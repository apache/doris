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

#ifndef DORIS_BE_RUNTIME_RESERVATION_TRACKER_COUNTERS_H
#define DORIS_BE_RUNTIME_RESERVATION_TRACKER_COUNTERS_H

#include "util/runtime_profile.h"

namespace doris {

/// A set of counters for each ReservationTracker for reporting purposes.
///
/// If the ReservationTracker is linked to a profile these have the same lifetime as that
/// profile, otherwise they have the same lifetime as the ReservationTracker itself.
struct ReservationTrackerCounters {
    /// The tracker's peak reservation in bytes.
    RuntimeProfile::HighWaterMarkCounter* peak_reservation;

    /// The tracker's peak usage in bytes.
    RuntimeProfile::HighWaterMarkCounter* peak_used_reservation;

    /// The hard limit on the tracker's reservations
    RuntimeProfile::Counter* reservation_limit;
};
} // namespace doris

#endif
