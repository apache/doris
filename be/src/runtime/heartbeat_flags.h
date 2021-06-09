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

#include <atomic>

#include "gen_cpp/HeartbeatService_constants.h"

namespace doris {

// This class is for parse control flags from heartbeat message
// between FE and BE.
class HeartbeatFlags {
public:
    HeartbeatFlags(uint64_t origin_flags) : _flags(origin_flags) {}

    HeartbeatFlags() : HeartbeatFlags(0) {}

    void update(uint64_t flags) { _flags = flags; }

    bool is_set_default_rowset_type_to_beta() {
        return _flags & g_HeartbeatService_constants.IS_SET_DEFAULT_ROWSET_TO_BETA_BIT;
    }

private:
    std::atomic<uint64_t> _flags;
};

} // namespace doris
