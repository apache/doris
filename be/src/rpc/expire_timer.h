// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_EXPIRE_TIMER_H
#define BDG_PALO_BE_SRC_RPC_EXPIRE_TIMER_H

#include "clock.h"
#include "dispatch_handler.h"

namespace palo {

/** State record for timer.
*/
struct ExpireTimer {
    ClockT::time_point expire_time;   //!< Absolute expiration time
    DispatchHandlerPtr handler; //!< Dispatch handler to receive TIMER event
};

/** Comparison function (functor) for timer heap.
*/
struct LtTimerHeap {
    /** Parenthesis operator with two ExpireTimer parameters.
     * Provides "largest first" comparison.
     * @param t1 Lefthand side of comparison
     * @param t2 Righthand side of comparison
     * @return true if <code>t1</code> is greater than <code>t2</code>
     */
    bool operator()(const ExpireTimer &t1, const ExpireTimer &t2) const {
        return std::chrono::operator>(t1.expire_time, t2.expire_time);
    }
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_EXPIRE_TIMER_H
