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

#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>

#include "common/check.h"

namespace doris::benchmark {

inline int control_fd() {
    static const int fd = [] {
        const char* value = std::getenv("QUERY_ENGINE_BENCH_CONTROL_FD");
        return value == nullptr ? -1 : std::stoi(value);
    }();
    return fd;
}

// The optional pipe lets two benchmark binaries alternate individual samples.
inline void wait_for_turn(std::string_view label, uint32_t sample) {
    const int fd = control_fd();
    if (fd < 0) {
        return;
    }
    std::cout << "QUERY_ENGINE_READY," << label << ',' << sample << std::endl;
    char command = 0;
    ssize_t count = 0;
    do {
        count = ::read(fd, &command, 1);
    } while (count < 0 && errno == EINTR);
    DORIS_CHECK_EQ(count, 1);
    DORIS_CHECK_EQ(command, 'r');
}

template <typename Checksum>
inline void report_sample(std::string_view label, uint32_t sample, uint32_t iterations,
                          uint64_t elapsed_ns, Checksum checksum,
                          bool report_uncontrolled = false) {
    if (control_fd() < 0 && !report_uncontrolled) {
        return;
    }
    const auto flags = std::cout.flags();
    const auto precision = std::cout.precision();
    std::cout << std::defaultfloat << std::setprecision(std::numeric_limits<double>::max_digits10)
              << "QUERY_ENGINE_BENCH," << label << ',' << sample << ',' << iterations << ','
              << elapsed_ns << ',' << checksum << std::endl;
    std::cout.flags(flags);
    std::cout.precision(precision);
}

} // namespace doris::benchmark
