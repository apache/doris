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

#ifndef DORIS_BE_UTIL_OS_INFO_H
#define DORIS_BE_UTIL_OS_INFO_H

#include <time.h>

#include <string>

#include "common/logging.h"

namespace doris {

/// Provides information about the OS we're running on.
class OsInfo {
public:
    /// Initialize OsInfo.
    static void Init();

    static const std::string os_version() {
        DCHECK(initialized_);
        return os_version_;
    }

    /// Return CLOCK_MONOTONIC if it's fast. Otherwise CLOCK_MONOTONIC_COARSE, which will be
    /// fast but lower resolution.
    static clockid_t fast_clock() {
        DCHECK(initialized_);
        return fast_clock_;
    }

    static std::string DebugString();

private:
    static bool initialized_;
    static std::string os_version_;
    static clockid_t fast_clock_;
    static std::string clock_name_;
};

} // namespace doris
#endif
