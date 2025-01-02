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

#include <boost/circular_buffer.hpp>
#include <shared_mutex>

namespace doris {

// A thread-safe interval histogram stat class.
// IntervalHistogramStat will keep a FIXED-SIZE window of values and provide
// statistics like mean, median, max, min.

template <typename T>
class IntervalHistogramStat {
public:
    explicit IntervalHistogramStat(size_t N);

    void add(T value);

    T mean();
    T median();
    T max();
    T min();

private:
    boost::circular_buffer<T> window;
    mutable std::shared_mutex mutex;
};

} // namespace doris
