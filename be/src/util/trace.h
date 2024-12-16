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

#include <butil/macros.h>

#include "gutil/ref_counted.h"
#include "gutil/strings/substitute.h"
#include "gutil/threading/thread_collision_warner.h"
#include "util/scoped_cleanup.h"
#include "util/spinlock.h"
#include "util/time.h"

// If this scope times out, make a simple trace.
// It will log the cost time only.
// Timeout is chrono duration struct, eg: 5ms, 100 * 1s.
#define SCOPED_SIMPLE_TRACE_IF_TIMEOUT(timeout) \
    SCOPED_SIMPLE_TRACE_TO_STREAM_IF_TIMEOUT(timeout, LOG(WARNING))

// If this scope times out, then put simple trace to the stream.
// Timeout is chrono duration struct, eg: 5ms, 100 * 1s.
// For example:
//
//    std::string tag = "[foo]";
//    SCOPED_SIMPLE_TRACE_TO_STREAM_IF_TIMEOUT(5s, LOG(INFO) << tag);
//
#define SCOPED_SIMPLE_TRACE_TO_STREAM_IF_TIMEOUT(timeout, stream)                       \
    using namespace std::chrono_literals;                                               \
    auto VARNAME_LINENUM(scoped_simple_trace) = doris::MonotonicMicros();               \
    SCOPED_CLEANUP({                                                                    \
        auto VARNAME_LINENUM(timeout_us) =                                              \
                std::chrono::duration_cast<std::chrono::microseconds>(timeout).count(); \
        auto VARNAME_LINENUM(cost_us) =                                                 \
                doris::MonotonicMicros() - VARNAME_LINENUM(scoped_simple_trace);        \
        if (VARNAME_LINENUM(cost_us) >= VARNAME_LINENUM(timeout_us)) {                  \
            stream << "Simple trace cost(us): " << VARNAME_LINENUM(cost_us);            \
        }                                                                               \
    })
