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

#include "olap/fs/block_manager_metrics.h"

#include "util/doris_metrics.h"

namespace doris {
namespace fs {
namespace internal {

BlockManagerMetrics::BlockManagerMetrics() {
    blocks_open_reading = &DorisMetrics::blocks_open_reading;
    blocks_open_writing = &DorisMetrics::blocks_open_writing;

    total_readable_blocks = &DorisMetrics::readable_blocks_total;
    total_writable_blocks = &DorisMetrics::writable_blocks_total;
    total_blocks_created = &DorisMetrics::blocks_created_total;
    total_blocks_deleted = &DorisMetrics::blocks_deleted_total;
    total_bytes_read = &DorisMetrics::bytes_read_total;
    total_bytes_written = &DorisMetrics::bytes_written_total;
    total_disk_sync = &DorisMetrics::disk_sync_total;
}

} // namespace internal
} // namespace fs
} // namespace doris
