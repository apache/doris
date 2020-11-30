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

#include "util/metrics.h"

namespace doris {
namespace fs {
namespace internal {

// TODO(lingbin): we should add a registry mechanism to Metrics, so that for
// different BlockManager we can register different metrics.
struct BlockManagerMetrics {
    explicit BlockManagerMetrics();

    // Number of data blocks currently open for reading
    IntGauge* blocks_open_reading;
    // Number of data blocks currently open for writing
    IntGauge* blocks_open_writing;

    // Number of data blocks opened for writing since service start
    IntCounter* total_readable_blocks;
    // Number of data blocks opened for reading since service start
    IntCounter* total_writable_blocks;
    // Number of data blocks that were created since service start
    IntCounter* total_blocks_created;
    // Number of data blocks that were deleted since service start
    IntCounter* total_blocks_deleted;
    // Number of bytes of block data written since service start
    IntCounter* total_bytes_read;
    // Number of bytes of block data read since service start
    IntCounter* total_bytes_written;
    // Number of disk synchronizations of block data since service start
    IntCounter* total_disk_sync;
};

} // namespace internal
} // namespace fs
} // namespace doris
