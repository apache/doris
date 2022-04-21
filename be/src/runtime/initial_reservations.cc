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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.10.0/be/src/runtime/initial-reservations.cc
// and modified by Doris

#include "runtime/initial_reservations.h"

#include <limits>
#include <mutex>

#include "common/logging.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"

using std::numeric_limits;

namespace doris {

InitialReservations::InitialReservations(ObjectPool* obj_pool,
                                         ReservationTracker* query_reservation,
                                         std::shared_ptr<MemTracker> query_mem_tracker,
                                         int64_t initial_reservation_total_claims)
        : initial_reservation_mem_tracker_(
                  MemTracker::create_tracker(-1, "InitialReservations", query_mem_tracker)),
          remaining_initial_reservation_claims_(initial_reservation_total_claims) {
    initial_reservations_.InitChildTracker(nullptr, query_reservation,
                                           initial_reservation_mem_tracker_.get(),
                                           numeric_limits<int64_t>::max());
}

Status InitialReservations::Init(const TUniqueId& query_id, int64_t query_min_reservation) {
    DCHECK_EQ(0, initial_reservations_.GetReservation()) << "Already inited";
    if (!initial_reservations_.IncreaseReservation(query_min_reservation)) {
        std::stringstream ss;
        ss << "Minimum reservation unavailable: " << query_min_reservation
           << " query id:" << query_id;
        return Status::MinimumReservationUnavailable(ss.str());
    }
    VLOG_QUERY << "Successfully claimed initial reservations ("
               << PrettyPrinter::print(query_min_reservation, TUnit::BYTES) << ") for"
               << " query " << print_id(query_id);
    return Status::OK();
}

void InitialReservations::Claim(BufferPool::ClientHandle* dst, int64_t bytes) {
    DCHECK_GE(bytes, 0);
    std::lock_guard<SpinLock> l(lock_);
    DCHECK_LE(bytes, remaining_initial_reservation_claims_);
    bool success = dst->TransferReservationFrom(&initial_reservations_, bytes);
    DCHECK(success) << "Planner computation should ensure enough initial reservations";
    remaining_initial_reservation_claims_ -= bytes;
}

void InitialReservations::Return(BufferPool::ClientHandle* src, int64_t bytes) {
    std::lock_guard<SpinLock> l(lock_);
    bool success = src->TransferReservationTo(&initial_reservations_, bytes);
    // No limits on our tracker - no way this should fail.
    DCHECK(success);
    // Check to see if we can release any reservation.
    int64_t excess_reservation =
            initial_reservations_.GetReservation() - remaining_initial_reservation_claims_;
    if (excess_reservation > 0) {
        initial_reservations_.DecreaseReservation(excess_reservation);
    }
}

void InitialReservations::ReleaseResources() {
    initial_reservations_.Close();
}
} // namespace doris
