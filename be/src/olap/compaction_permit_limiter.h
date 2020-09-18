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

#ifndef DORIS_BE_SRC_OLAP_COMPACTION_PERMIT_LIMITER_H
#define DORIS_BE_SRC_OLAP_COMPACTION_PERMIT_LIMITER_H

#include <vector>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "util/countdown_latch.h"

namespace doris {

// This class is a base class for compaction management.
class CompactionPermitLimiter {
public:
    CompactionPermitLimiter();
    virtual ~CompactionPermitLimiter() { };

    static OLAPStatus init(uint32_t total_permits, bool _over_sold);

    static bool request(uint32_t permits);

    static OLAPStatus release(uint32_t permits);

    inline uint32_t total_permits() const;
    inline void set_total_permits(uint32_t total_permits);

    inline uint32_t used_permits() const;

    inline bool is_over_sold() const;
    inline void set_over_sold(bool over_sold);

private:
    static uint32_t _total_permits;
    static uint32_t _used_permits;
    static bool _over_sold;

    static CountDownLatch _threads_latch;
};

inline uint32_t CompactionPermitLimiter::total_permits() const {
    return _total_permits;
}

inline void CompactionPermitLimiter::set_total_permits(uint32_t total_permits) {
    _total_permits = total_permits;
}

inline uint32_t CompactionPermitLimiter::used_permits() const {
    return _used_permits;
}

inline bool CompactionPermitLimiter::is_over_sold() const {
    return _over_sold;
}

inline void CompactionPermitLimiter::set_over_sold(bool over_sold) {
    _over_sold = over_sold;
}
}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_COMPACTION_PERMIT_LIMITER_H
