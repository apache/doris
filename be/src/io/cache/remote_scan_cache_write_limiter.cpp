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

#include "io/cache/remote_scan_cache_write_limiter.h"

#include <bvar/bvar.h>

#include "common/logging.h"
#include "util/uid_util.h"

namespace doris::io {

bvar::Adder<uint64_t> g_remote_scan_no_write_file_cache_query_total(
        "remote_scan_no_write_file_cache_query_total");

RemoteScanCacheWriteLimiter::RemoteScanCacheWriteLimiter(TUniqueId query_id,
                                                         int64_t threshold_bytes)
        : _query_id(query_id),
          _threshold_bytes(threshold_bytes),
          _remote_only_on_miss(threshold_bytes == 0) {}

bool RemoteScanCacheWriteLimiter::remote_only_on_miss() const {
    return _remote_only_on_miss.load(std::memory_order_acquire);
}

int64_t RemoteScanCacheWriteLimiter::admitted_data_bytes() const {
    std::lock_guard lock(_mutex);
    return _admitted_data_bytes;
}

bool RemoteScanCacheWriteLimiter::try_admit_cache_write(int64_t bytes) {
    if (!enabled()) {
        return true;
    }
    if (_remote_only_on_miss.load(std::memory_order_acquire)) {
        return false;
    }
    if (bytes <= 0) {
        return true;
    }

    std::lock_guard lock(_mutex);
    if (_remote_only_on_miss.load(std::memory_order_relaxed)) {
        return false;
    }

    const int64_t remaining_budget_bytes = _threshold_bytes - _admitted_data_bytes;
    if (bytes > remaining_budget_bytes) {
        _remote_only_on_miss.store(true, std::memory_order_release);
        g_remote_scan_no_write_file_cache_query_total << 1;
        VLOG_DEBUG << "Remote scan file cache write threshold reached"
                   << ", query_id=" << print_id(_query_id)
                   << ", admitted_data_bytes=" << _admitted_data_bytes
                   << ", request_bytes=" << bytes
                   << ", remaining_budget_bytes=" << remaining_budget_bytes
                   << ", threshold_bytes=" << _threshold_bytes;
        return false;
    }

    _admitted_data_bytes += bytes;
    return true;
}

} // namespace doris::io
