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

#include "runtime/workload_management/resource_context.h"

#include <gen_cpp/data.pb.h>
#include <glog/logging.h>

#include "util/time.h"

namespace doris {
#include "common/compile_check_begin.h"

void ResourceContext::to_thrift_query_statistics(TQueryStatistics* statistics) const {
    DCHECK(statistics != nullptr);
    statistics->__set_scan_rows(io_context()->scan_rows());
    statistics->__set_scan_bytes(io_context()->scan_bytes());
    statistics->__set_cpu_ms(cpu_context()->cpu_cost_ms() / NANOS_PER_MILLIS);
    statistics->__set_returned_rows(io_context()->returned_rows());
    statistics->__set_max_peak_memory_bytes(memory_context()->max_peak_memory_bytes());
    statistics->__set_current_used_memory_bytes(memory_context()->current_memory_bytes());
    statistics->__set_shuffle_send_bytes(io_context()->shuffle_send_bytes());
    statistics->__set_shuffle_send_rows(io_context()->shuffle_send_rows());
    statistics->__set_scan_bytes_from_remote_storage(
            io_context()->scan_bytes_from_remote_storage());
    statistics->__set_scan_bytes_from_local_storage(io_context()->scan_bytes_from_local_storage());
    statistics->__set_bytes_write_into_cache(io_context()->bytes_write_into_cache());

    if (workload_group() != nullptr) {
        statistics->__set_workload_group_id(workload_group()->id());
    } else {
        statistics->__set_workload_group_id(-1);
    }

    statistics->__set_spill_write_bytes_to_local_storage(
            io_context_->spill_write_bytes_to_local_storage());
    statistics->__set_spill_read_bytes_from_local_storage(
            io_context_->spill_read_bytes_from_local_storage());
}

#include "common/compile_check_end.h"
} // namespace doris
