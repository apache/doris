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

#include <gmock/gmock.h>

#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::vectorized {
class MockSimplifiedScanScheduler : ThreadPoolSimplifiedScanScheduler {
public:
    MockSimplifiedScanScheduler(std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : ThreadPoolSimplifiedScanScheduler("ForTest", cgroup_cpu_ctl) {}

    MOCK_METHOD0(get_active_threads, int());
    MOCK_METHOD0(get_queue_size, int());
    MOCK_METHOD3(schedule_scan_task, Status(std::shared_ptr<ScannerContext> scanner_ctx,
                                            std::shared_ptr<ScanTask> current_scan_task,
                                            std::unique_lock<std::mutex>& transfer_lock));
};
} // namespace doris::vectorized
