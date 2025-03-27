//// Licensed to the Apache Software Foundation (ASF) under one
//// or more contributor license agreements.  See the NOTICE file
//// distributed with this work for additional information
//// regarding copyright ownership.  The ASF licenses this file
//// to you under the Apache License, Version 2.0 (the
//// "License"); you may not use this file except in compliance
//// with the License.  You may obtain a copy of the License at
////
////   http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing,
//// software distributed under the License is distributed on an
//// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//// KIND, either express or implied.  See the License for the
//// specific language governing permissions and limitations
//// under the License.
//
//#pragma once
//#include <array>
//#include <atomic>
//#include <chrono>
//#include <cmath>
//#include <vector>
//
//namespace doris {
//namespace vectorized {
//
//class TimeStats {
//public:
//    void add(std::chrono::nanoseconds duration) {
//        totalTime.fetch_add(duration.count());
//        count.fetch_add(1);
//    }
//
//    double getAverage() const {
//        auto total = totalTime.load();
//        auto cnt = count.load();
//        return cnt > 0 ? static_cast<double>(total) / cnt : 0.0;
//    }
//
//private:
//    std::atomic<int64_t> totalTime {0};
//    std::atomic<int64_t> count {0};
//};
//
//class TimeDistribution {
//public:
//    void add(std::chrono::microseconds duration) {
//        auto bucket = getBucket(duration.count());
//        buckets[bucket].fetch_add(1);
//        count.fetch_add(1);
//    }
//
//private:
//    static constexpr size_t BUCKET_COUNT = 32;
//    std::array<std::atomic<int64_t>, BUCKET_COUNT> buckets {};
//    std::atomic<int64_t> count {0};
//
//    static size_t getBucket(int64_t value) {
//        return std::min(static_cast<size_t>(std::log2(static_cast<double>(value) + 1)),
//                        BUCKET_COUNT - 1);
//    }
//};
//
//class CounterStats {
//public:
//    void add(int64_t value) { total.fetch_add(value); }
//
//    int64_t getTotal() const { return total.load(); }
//
//private:
//    std::atomic<int64_t> total {0};
//};
//
//class DistributionStats {
//public:
//    void add(double value) {
//        count.fetch_add(1);
//        sum.fetch_add(value);
//        updateMin(value);
//        updateMax(value);
//    }
//
//private:
//    std::atomic<int64_t> count {0};
//    std::atomic<double> sum {0.0};
//    std::atomic<double> min {std::numeric_limits<double>::max()};
//    std::atomic<double> max {std::numeric_limits<double>::lowest()};
//
//    void updateMin(double value) {
//        double current = min.load();
//        while (value < current && !min.compare_exchange_weak(current, value)) {
//        }
//    }
//
//    void updateMax(double value) {
//        double current = max.load();
//        while (value > current && !max.compare_exchange_weak(current, value)) {
//        }
//    }
//};
//
//} // namespace vectorized
//} // namespace doris
