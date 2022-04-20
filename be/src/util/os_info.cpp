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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/os-info.cc
// and modified by Doris

#include "util/os_info.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>

namespace doris {

bool OsInfo::initialized_ = false;
std::string OsInfo::os_version_ = "Unknown";
clockid_t OsInfo::fast_clock_ = CLOCK_MONOTONIC;
std::string OsInfo::clock_name_ = "Unknown clocksource, clockid_t defaulting to CLOCK_MONOTONIC";

// CLOCK_MONOTONIC_COARSE was added in Linux 2.6.32. For now we still want to support
// older kernels by falling back to CLOCK_MONOTONIC.
#ifdef CLOCK_MONOTONIC_COARSE
#define HAVE_CLOCK_MONOTONIC_COARSE true
#else
#define HAVE_CLOCK_MONOTONIC_COARSE false
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

void OsInfo::Init() {
    DCHECK(!initialized_);
    // Read from /proc/version
    std::ifstream version("/proc/version", std::ios::in);
    if (version.good()) getline(version, os_version_);
    if (version.is_open()) version.close();

    // Read the current clocksource to see if CLOCK_MONOTONIC is known to be fast. "tsc" is
    // fast, while "xen" is slow (40 times slower than "tsc" on EC2). If CLOCK_MONOTONIC is
    // known to be slow, we use CLOCK_MONOTONIC_COARSE, which uses jiffies, with a
    // resolution measured in milliseconds, rather than nanoseconds.
    std::ifstream clocksource_file(
            "/sys/devices/system/clocksource/clocksource0/current_clocksource");
    if (clocksource_file.good()) {
        std::string clocksource;
        clocksource_file >> clocksource;
        clock_name_ = "clocksource: '" + clocksource + "', clockid_t: ";
        if (HAVE_CLOCK_MONOTONIC_COARSE && clocksource != "tsc") {
            clock_name_ += "CLOCK_MONOTONIC_COARSE";
            fast_clock_ = CLOCK_MONOTONIC_COARSE;
        } else {
            clock_name_ += "CLOCK_MONOTONIC";
            fast_clock_ = CLOCK_MONOTONIC;
        }
    }

    initialized_ = true;
}

std::string OsInfo::DebugString() {
    DCHECK(initialized_);
    std::stringstream stream;
    stream << "OS version: " << os_version_ << std::endl << "Clock: " << clock_name_ << std::endl;
    return stream.str();
}

} // namespace doris
