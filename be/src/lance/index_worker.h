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

#include <cstdint>

namespace doris::lance {

// Fixed protocol magic of the worker handshake frame: the five ASCII bytes of
// "LANCE" (0x4C 41 4E 43 45) followed by 0x00 and the layout version byte 0x01,
// read as one big-endian int64. Never an RPC surface; the supervisor compares it
// against its own constant before decoding anything else on the pipe.
inline constexpr int64_t HANDSHAKE_PROTOCOL_MAGIC = 0x4C414E43450001LL;
inline constexpr int32_t HANDSHAKE_PROTOCOL_VERSION = 1;

// Pure library entry of the isolated one-shot index worker: reads one
// length-prefixed thrift-compact TLanceIndexJobDispatch frame from dispatch_fd,
// writes one handshake frame followed by at most one result frame
// (TLanceIndexJobReport) to result_fd, and emits bounded static diagnostic lines
// to diag_fd. Returns the process exit code: 0 when a complete result frame (a
// native outcome or a trusted pre-invocation rejection) was written, nonzero when
// no trusted result exists (no result frame was written; the supervisor converges
// via termination proof or the FE deadline).
//
// The function touches no BE global state (no config, logging, metrics, or
// ExecEnv): a bare main can exec it directly, and unit tests can drive it over
// pipe pairs with tuned bounds. Its first action, before reading the dispatch, is
// prctl(PR_SET_DUMPABLE, 0) so credentials in memory can never reach a core file.
struct IndexWorkerParams {
    int dispatch_fd = 0;
    int result_fd = 1;
    int diag_fd = 2;
    // Protocol constants are injectable so unit tests can exercise the
    // length-cap paths with small values.
    uint32_t max_dispatch_bytes = 512 * 1024;
    uint32_t max_result_bytes = 8 * 1024;
};

int run_index_worker(const IndexWorkerParams& params);

} // namespace doris::lance
