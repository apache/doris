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

#pragma once
#include "runtime/memory/thread_mem_tracker_mgr.h"

namespace doris {

class MockThreadMemTrackerMgr : public ThreadMemTrackerMgr {
public:
    MockThreadMemTrackerMgr() : ThreadMemTrackerMgr() {}
    Status try_reserve(
            int64_t size,
            TryReserveChecker checker =
                    TryReserveChecker::CHECK_TASK_AND_WORKLOAD_GROUP_AND_PROCESS) override {
        return _test_low_memory ? Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>("")
                                : Status::OK();
    }

private:
    bool _test_low_memory = false;
};

} // namespace doris
