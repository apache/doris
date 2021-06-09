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

#include "olap/fs/block_manager.h"

#include <mutex>
#include <ostream>

#include "common/logging.h"

namespace doris {
namespace fs {

// Controls when to pre-flush a block. Valid values are 'finalize',
// 'close', or 'never'.
// + If 'finalize', blocks will be pre-flushed when writing is finished.
// + If 'close', blocks will be pre-flushed when their transaction is committed.
// + If 'never', blocks will never be pre-flushed but still be flushed when closed.");
//
// The default value is optimized for throughput in the case that
// there are multiple drives backing the tablet. By asynchronously
// flushing each block before issuing any fsyncs, the IO across
// disks is done in parallel.
//
// This increases throughput but can harm latency in the case that
// there are few disks and the WAL is on the same disk as the
// data blocks. The default is chosen based on the assumptions that:
// - latency is leveled across machines by Raft
// - latency-sensitive applications can devote a disk to the WAL
// - super-sensitive applications can devote an SSD to the WAL.
// - users could always change this to "never", which slows down
//   throughput but may improve write latency.
//
// TODO(lingbin): move it to conf later, to allow adjust dynamically.
const std::string BlockManager::block_manager_preflush_control = "finalize";

} // namespace fs
} // namespace doris
