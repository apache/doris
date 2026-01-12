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

#include "histogram.h"

#include <iomanip>
#include <sstream>

namespace doris {
namespace vectorized {

std::string HistogramUtils::format_nanos(int64_t nanos) {
    constexpr int64_t NS_PER_SEC = 1'000'000'000;
    constexpr int64_t NS_PER_MS = 1'000'000;
    constexpr int64_t NS_PER_MICRO = 1'000;
    constexpr int64_t SEC_PER_MIN = 60;
    constexpr int64_t MIN_PER_HOUR = 60;
    constexpr int64_t HOUR_PER_DAY = 24;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    if (nanos >= NS_PER_SEC * SEC_PER_MIN * MIN_PER_HOUR * HOUR_PER_DAY) {
        double days = static_cast<double>(nanos) /
                      (NS_PER_SEC * SEC_PER_MIN * MIN_PER_HOUR * HOUR_PER_DAY);
        oss << std::setw(6) << days << "d";
    } else if (nanos >= NS_PER_SEC * SEC_PER_MIN * MIN_PER_HOUR) {
        double hours = static_cast<double>(nanos) / (NS_PER_SEC * SEC_PER_MIN * MIN_PER_HOUR);
        oss << std::setw(6) << hours << "h";
    } else if (nanos >= NS_PER_SEC * SEC_PER_MIN) {
        double minutes = static_cast<double>(nanos) / (NS_PER_SEC * SEC_PER_MIN);
        oss << std::setw(6) << minutes << "m";
    } else if (nanos >= NS_PER_SEC) {
        double seconds = static_cast<double>(nanos) / NS_PER_SEC;
        oss << std::setw(6) << seconds << "s";
    } else if (nanos >= NS_PER_MS) {
        double milliseconds = static_cast<double>(nanos) / NS_PER_MS;
        oss << std::setw(6) << milliseconds << "ms";
    } else if (nanos >= NS_PER_MICRO) {
        double microseconds = static_cast<double>(nanos) / NS_PER_MICRO;
        oss << std::setw(6) << microseconds << "μs";
    } else {
        oss << std::setw(6) << nanos << "ns";
    }

    return oss.str();
}

} // namespace vectorized
} // namespace doris
