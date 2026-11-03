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

#include <cctz/time_zone.h>

#include <functional>

namespace orc {
class Type;
}

namespace doris {

// Reader and session zones have different roles even when a batch mixes both timestamp kinds.
struct OrcTimestampDecodeContext {
    explicit OrcTimestampDecodeContext(const cctz::time_zone& timezone)
            : reader_timezone(timezone), session_timezone(timezone) {}

    cctz::time_zone timezone_for(const orc::Type& file_type, int64_t seconds = 0) const;

    // Java carriers follow the SDK writer-zone rules rather than cctz historical LMT.
    std::function<cctz::time_zone(int64_t)> writer_timezone;

    cctz::time_zone reader_timezone;
    cctz::time_zone session_timezone;
};

} // namespace doris
