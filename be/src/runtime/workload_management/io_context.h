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

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

class IOContext : public std::enable_shared_from_this<IOContext> {
    ENABLE_FACTORY_CREATOR(IOContext);

public:
    // Used to collect io execution stats.
    class IOStats {
    public:
        IOStats() = default;
        virtual ~IOStats() = default;
        void merge(IOStats stats);
        void reset();
        std::string debug_string();
        int64_t scan_rows() { return scan_rows_; }
        int64_t scan_bytes() { return scan_bytes_; }
        int64_t scan_bytes_from_local_storage() { return scan_bytes_from_local_storage_; }
        int64_t scan_bytes_from_remote_storage() { return scan_bytes_from_remote_storage_; }
        int64_t returned_rows() { return returned_rows_; }
        int64_t shuffle_send_bytes() { return shuffle_send_bytes_; }
        int64_t shuffle_send_rows() { return shuffle_send_rows_; }

        int64_t incr_scan_rows(int64_t delta) { return scan_rows_ + delta; }
        int64_t incr_scan_bytes(int64_t delta) { return scan_bytes_ + delta; }
        int64_t incr_scan_bytes_from_local_storage(int64_t delta) {
            return scan_bytes_from_local_storage_ + delta;
        }
        int64_t incr_scan_bytes_from_remote_storage(int64_t delta) {
            return scan_bytes_from_remote_storage_ + delta;
        }
        int64_t incr_returned_rows(int64_t delta) { return returned_rows_ + delta; }
        int64_t incr_shuffle_send_bytes(int64_t delta) { return shuffle_send_bytes_ + delta; }
        int64_t incr_shuffle_send_rows(int64_t delta) { return shuffle_send_rows_ + delta; }
        std::string debug_string();

    private:
        int64_t scan_rows_ = 0;
        int64_t scan_bytes_ = 0;
        int64_t scan_bytes_from_local_storage_ = 0;
        int64_t scan_bytes_from_remote_storage_ = 0;
        // number rows returned by query.
        // only set once by result sink when closing.
        int64_t returned_rows_ = 0;
        int64_t shuffle_send_bytes_ = 0;
        int64_t shuffle_send_rows_ = 0;
    };

public:
    IOContext() {}
    virtual ~IOContext() = default;
    IOThrottle* io_throttle() {
        // get io throttle from workload group
    }
    IOStats* stats() { return &stats_; }

private:
    IOStats stats_;
};

} // namespace doris
