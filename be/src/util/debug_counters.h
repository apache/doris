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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DEBUG_COUNTERS_H
#define DORIS_BE_SRC_COMMON_UTIL_DEBUG_COUNTERS_H

#include "util/runtime_profile.h"

#define ENABLE_DEBUG_COUNTERS 1

namespace doris {

// Runtime counters have a two-phase lifecycle - creation and update. This is not
// convenient for debugging where we would like to add and remove counters with a minimum
// of boilerplate. This header adds a global debug runtime profile, and macros to
// update-or-create counters in one line of code. Counters created this way are not
// intended to remain in the code; they are a tool for identifying hotspots without having
// to run a full profiler.
// The AddCounter call adds some more overhead to each macro, and therefore they
// should not be used where minimal impact on performance is needed.
class DebugRuntimeProfile {
public:
    static RuntimeProfile& profile() {
        static RuntimeProfile profile(new ObjectPool(), "DebugProfile");
        return profile;
    }
};

#if ENABLE_DEBUG_COUNTERS

#define DEBUG_SCOPED_TIMER(counter_name) \
    COUNTER_SCOPED_TIMER(DebugRuntimeProfile::profile().AddCounter(counter_name, TUnit::CPU_TICKS))

#define DEBUG_COUNTER_UPDATE(counter_name, v) \
    COUNTER_UPDATE(DebugRuntimeProfile::profile().AddCounter(counter_name, TUnit::UNIT), v)

#define DEBUG_COUNTER_SET(counter_name, v) \
    COUNTER_SET(DebugRuntimeProfile::profile().AddCounter(counter_name, TUnit::UNIT), v)

#define PRETTY_PRINT_DEBUG_COUNTERS(ostream_ptr) \
    DebugRuntimeProfile::profile().PrettyPrint(ostream_ptr)

#else

#define DEBUG_SCOPED_TIMER(counter_name)
#define DEBUG_COUNTER_UPDATE(counter_name, v)
#define DEBUG_COUNTER_SET(counter_name, v)
#define PRETTY_PRINT_DEBUG_COUNTERS(ostream_ptr)

#endif // ENABLE_DEBUG_COUNTERS

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_UTIL_DEBUG_COUNTERS_H
