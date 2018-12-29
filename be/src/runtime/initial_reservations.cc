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

#include "runtime/initial_reservations.h"

#include <limits>

#include <boost/thread/mutex.hpp>
#include <gflags/gflags.h>

#include "common/logging.h"
#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "util/uid_util.h"
#include "util/pretty_printer.h"

#include "common/names.h"

using std::numeric_limits;


namespace doris {

InitialReservations::InitialReservations(ObjectPool* obj_pool,
    ReservationTracker* query_reservation, MemTracker* query_mem_tracker,
    int64_t initial_reservation_total_claims)
  : initial_reservation_mem_tracker_(obj_pool->add(
      new MemTracker(-1, "Unclaimed reservations", query_mem_tracker, false))),
      remaining_initial_reservation_claims_(initial_reservation_total_claims) {
  initial_reservations_.InitChildTracker(nullptr, query_reservation,
      initial_reservation_mem_tracker_, numeric_limits<int64_t>::max());
}

Status InitialReservations::Init(
    const TUniqueId& query_id, int64_t query_min_reservation) {
  DCHECK_EQ(0, initial_reservations_.GetReservation()) << "Already inited";
  if (!initial_reservations_.IncreaseReservation(query_min_reservation)) {
      Status status;
      std::stringstream ss;
      ss  << "Minimum reservation unavaliable: " << query_min_reservation
          << " query id:" << query_id; 
      status.add_error_msg(TStatusCode::MINIMUM_RESERVATION_UNAVAILABLE, ss.str());
      return status;
  }
  VLOG_QUERY << "Successfully claimed initial reservations ("
            << PrettyPrinter::print(query_min_reservation, TUnit::BYTES) << ") for"
            << " query " << print_id(query_id);
  return Status::OK;
}

void InitialReservations::Claim(BufferPool::ClientHandle* dst, int64_t bytes) {
  DCHECK_GE(bytes, 0);
  lock_guard<SpinLock> l(lock_);
  DCHECK_LE(bytes, remaining_initial_reservation_claims_);
  bool success = dst->TransferReservationFrom(&initial_reservations_, bytes);
  DCHECK(success) << "Planner computation should ensure enough initial reservations";
  remaining_initial_reservation_claims_ -= bytes;
}

void InitialReservations::Return(BufferPool::ClientHandle* src, int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
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
  initial_reservation_mem_tracker_->close();
}
}
