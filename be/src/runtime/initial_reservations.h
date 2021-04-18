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

#ifndef DORIS_BE_RUNTIME_INITIAL_RESERVATIONS_H
#define DORIS_BE_RUNTIME_INITIAL_RESERVATIONS_H

#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "util/spinlock.h"

namespace doris {

class ObjectPool;

/**
 * Manages the pool of initial reservations for different nodes in the plan tree.
 * Each plan node and sink claims its initial reservation from here, then returns it when
 * it is done executing. The frontend is responsible for making sure that enough initial
 * reservation is in this pool for all of the concurrent claims.
 */
class InitialReservations {
public:
    /// 'query_reservation' and 'query_mem_tracker' are the top-level trackers for the
    /// query. This creates trackers for initial reservations under those.
    /// 'initial_reservation_total_claims' is the total of initial reservations that will be
    /// claimed over the lifetime of the query. The total bytes claimed via Claim()
    /// cannot exceed this. Allocated objects are stored in 'obj_pool'.
    InitialReservations(ObjectPool* obj_pool, ReservationTracker* query_reservation,
                        std::shared_ptr<MemTracker> query_mem_tracker,
                        int64_t initial_reservation_total_claims);

    /// Initialize the query's pool of initial reservations by acquiring the minimum
    /// reservation required for the query on this host. Fails if the reservation could
    /// not be acquired, e.g. because it would exceed a pool or process limit.
    Status Init(const TUniqueId& query_id, int64_t query_min_reservation) WARN_UNUSED_RESULT;

    /// Claim the initial reservation of 'bytes' for 'dst'. Assumes that the transfer will
    /// not violate any reservation limits on 'dst'.
    void Claim(BufferPool::ClientHandle* dst, int64_t bytes);

    /// Return the initial reservation of 'bytes' from 'src'. The reservation is returned
    /// to the pool of reservations if it may be needed to satisfy a subsequent claim or
    /// otherwise is released.
    void Return(BufferPool::ClientHandle* src, int64_t bytes);

    /// Release any reservations held onto by this object.
    void ReleaseResources();

private:
    // Protects all below members to ensure that the internal state is consistent.
    SpinLock lock_;

    // The pool of initial reservations that Claim() returns reservations from and
    // Return() returns reservations to.
    ReservationTracker initial_reservations_;

    std::shared_ptr<MemTracker> const initial_reservation_mem_tracker_;

    /// The total bytes of additional reservations that we expect to be claimed.
    /// initial_reservations_->GetReservation() <= remaining_initial_reservation_claims_.
    int64_t remaining_initial_reservation_claims_;
};
} // namespace doris

#endif
