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

#include <mach/mach.h>

#include "common/check.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"

namespace doris {

std::atomic<int64_t> PerfCounters::_vm_rss = 0;
std::atomic<int64_t> PerfCounters::_vm_hwm = 0;
std::atomic<int64_t> PerfCounters::_vm_peak = 0;
std::atomic<int64_t> PerfCounters::_vm_size = 0;

// macOS has no /proc/self/status, so read the task's basic info from the kernel: resident_size
// is the counterpart of VmRSS, resident_size_max of VmHWM and virtual_size of VmSize. Mach does
// not report a peak virtual size, so vm_peak keeps its default.
void PerfCounters::get_proc_status(ProcStatus* out) {
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    const kern_return_t ret = task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                                        reinterpret_cast<task_info_t>(&info), &count);
    DORIS_CHECK(ret == KERN_SUCCESS) << "task_info(MACH_TASK_BASIC_INFO) failed: " << ret;
    out->vm_size = static_cast<int64_t>(info.virtual_size);
    out->vm_rss = static_cast<int64_t>(info.resident_size);
    out->vm_hwm = static_cast<int64_t>(info.resident_size_max);
}

void PerfCounters::refresh_proc_status() {
    ProcStatus status;
    get_proc_status(&status);
    _vm_size.store(status.vm_size, std::memory_order_relaxed);
    _vm_rss.store(status.vm_rss, std::memory_order_relaxed);
    _vm_hwm.store(status.vm_hwm, std::memory_order_relaxed);
}

std::string PerfCounters::get_vm_rss_str() {
#ifdef ADDRESS_SANITIZER
    return "[ASAN]" + PrettyPrinter::print(get_vm_rss(), TUnit::BYTES);
#else
    return PrettyPrinter::print(get_vm_rss(), TUnit::BYTES);
#endif
}

} // namespace doris
