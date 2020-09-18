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

#include "olap/compaction_permit_limiter.h"

namespace doris {

uint32_t CompactionPermitLimiter::_total_permits;
uint32_t CompactionPermitLimiter::_used_permits;
bool CompactionPermitLimiter::_over_sold;
CountDownLatch CompactionPermitLimiter::_threads_latch;

CompactionPermitLimiter::CompactionPermitLimiter() : _threads_latch(1) { };

OLAPStatus CompactionPermitLimiter::init(uint32_t total_permits, bool over_sold) {
    _total_permits = total_permits;
    _over_sold = over_sold;
    return OLAP_SUCCESS;
}

bool CompactionPermitLimiter::request(uint32_t permits) {
    if (permits > _total_permits) {
        if (_over_sold) {
            // wait_until used_permits==0
            while(!_threads_latch.wait_for(MonoDelta::FromSeconds(5)) && _used_permits != 0);
            _used_permits = _total_permits;
            return true;
        } else {
            return false;
        }
    } else {
        if ((_total_permits - _used_permits) < permits) {
            return false;
        }
        _used_permits += permits;
        return true;
    }
}

OLAPStatus CompactionPermitLimiter::release(uint32_t permits) {
    if (permits > _total_permits) {
        _used_permits = 0;
    } else {
        _used_permits = _used_permits - permits;
    }
    return OLAP_SUCCESS;
}

}  // namespace doris
