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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/time.cc
// and modified by Doris

#include "util/time.h"

// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstdlib>
#include <iomanip>
#include <ratio>
#include <sstream>
#include <thread>

#include "common/logging.h"

using namespace doris;
using namespace std::chrono;

void doris::SleepForMs(const int64_t duration_ms) {
    std::this_thread::sleep_for(milliseconds(duration_ms));
}

// Convert the given time_point, 't', into a date-time string in the
// UTC time zone if 'utc' is true, or the local time zone if it is false.
// The returned string is of the form yyy-MM-dd HH::mm::SS.
static std::string TimepointToString(const system_clock::time_point& t, bool utc) {
    char buf[256];
    struct tm tmp;
    auto input_time = system_clock::to_time_t(t);

    // gcc 4.9 does not support C++14 get_time and put_time functions, so we're
    // stuck with strftime() for now.
    if (utc) {
        strftime(buf, sizeof(buf), "%F %T", gmtime_r(&input_time, &tmp));
    } else {
        strftime(buf, sizeof(buf), "%F %T", localtime_r(&input_time, &tmp));
    }
    return std::string(buf);
}

// Format the sub-second part of the input time point object 't', at the
// precision specified by 'p'. The returned string is meant to be appended to
// the string returned by TimePointToString() above.
// Note the use of abs(). This is to make sure we correctly format negative times,
// i.e., times before the Unix epoch.
static std::string FormatSubSecond(const system_clock::time_point& t, TimePrecision p) {
    std::stringstream ss;
    auto frac = t.time_since_epoch();
    if (p == TimePrecision::Millisecond) {
        auto subsec = duration_cast<milliseconds>(frac) % MILLIS_PER_SEC;
        ss << "." << std::setfill('0') << std::setw(3) << abs(subsec.count());
    } else if (p == TimePrecision::Microsecond) {
        auto subsec = duration_cast<microseconds>(frac) % MICROS_PER_SEC;
        ss << "." << std::setfill('0') << std::setw(6) << abs(subsec.count());
    } else if (p == TimePrecision::Nanosecond) {
        auto subsec = duration_cast<nanoseconds>(frac) % NANOS_PER_SEC;
        ss << "." << std::setfill('0') << std::setw(9) << abs(subsec.count());
    } else {
        // 1-second precision or unknown unit. Return empty string.
        DCHECK_EQ(TimePrecision::Second, p);
        ss << "";
    }
    return ss.str();
}

// Convert time point 't' into date-time string at precision 'p'.
// Output string is in UTC time zone if 'utc' is true, else it is in the
// local time zone.
static std::string ToString(const system_clock::time_point& t, TimePrecision p, bool utc) {
    std::stringstream ss;
    ss << TimepointToString(t, utc);
    ss << FormatSubSecond(t, p);
    return ss.str();
}

// Convenience function to convert Unix time, specified as seconds since
// the Unix epoch, into a C++ time_point object.
static system_clock::time_point TimepointFromUnix(int64_t s) {
    return system_clock::time_point(seconds(s));
}

// Convenience function to convert Unix time, specified as milliseconds since
// the Unix epoch, into a C++ time_point object.
static system_clock::time_point TimepointFromUnixMillis(int64_t ms) {
    return system_clock::time_point(milliseconds(ms));
}

// Convenience function to convert Unix time, specified as microseconds since
// the Unix epoch, into a C++ time_point object.
static system_clock::time_point TimepointFromUnixMicros(int64_t us) {
    return system_clock::time_point(microseconds(us));
}

std::string doris::ToStringFromUnix(int64_t s, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnix(s);
    return ToString(t, p, false);
}

std::string doris::ToUtcStringFromUnix(int64_t s, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnix(s);
    return ToString(t, p, true);
}

std::string doris::ToStringFromUnixMillis(int64_t ms, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnixMillis(ms);
    return ToString(t, p, false);
}

std::string doris::ToUtcStringFromUnixMillis(int64_t ms, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnixMillis(ms);
    return ToString(t, p, true);
}

std::string doris::ToStringFromUnixMicros(int64_t us, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnixMicros(us);
    return ToString(t, p, false);
}

std::string doris::ToUtcStringFromUnixMicros(int64_t us, TimePrecision p) {
    system_clock::time_point t = TimepointFromUnixMicros(us);
    return ToString(t, p, true);
}
