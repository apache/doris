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

#include "util/bvar_metrics.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include <thread>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/stopwatch.hpp"

namespace doris {

class BvarMetricsTest : public testing::Test {
public:
    BvarMetricsTest() {}
    virtual ~BvarMetricsTest() {}
};

TEST_F(BvarMetricsTest, Counter) {
    {
        BvarAdderMetric<int64_t> counter(BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "counter");
        EXPECT_EQ(0, counter.get_value());
        counter.increment(100);
        EXPECT_EQ(100, counter.get_value());
        EXPECT_STREQ("100", counter.value_string().c_str());
    }
    {
        BvarAdderMetric<uint64_t> counter(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                          "counter");
        EXPECT_EQ(0, counter.get_value());
        counter.increment(100);
        EXPECT_EQ(100, counter.get_value());
        EXPECT_STREQ("100", counter.value_string().c_str());
    }
    {
        BvarAdderMetric<double> counter(BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "counter");
        EXPECT_EQ(0, counter.get_value());
        counter.increment(1.23);
        EXPECT_EQ(1.23, counter.get_value());
        EXPECT_STREQ("1.230000", counter.value_string().c_str());
    }
}

template <typename T>
void mt_updater(int32_t loop, T* counter, std::atomic<uint64_t>* used_time) {
    sleep(1);
    MonotonicStopWatch watch;
    watch.start();
    for (int i = 0; i < loop; ++i) {
        counter->increment(1);
    }
    uint64_t elapsed = watch.elapsed_time();
    used_time->fetch_add(elapsed);
}

TEST_F(BvarMetricsTest, CounterPerf) {
    static const int kLoopCount = LOOP_LESS_OR_MORE(10, 100000000);
    static const int kThreadLoopCount = LOOP_LESS_OR_MORE(1000, 1000000);
    {
        int64_t sum = 0;
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            sum += 1;
        }
        uint64_t elapsed = watch.elapsed_time();
        EXPECT_EQ(kLoopCount, sum);
        LOG(INFO) << "int64_t: elapsed: " << elapsed << "ns, ns/iter:" << elapsed / kLoopCount;
    }
    // IntCounter
    {
        BvarAdderMetric<int64_t> counter(BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "counter");
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        EXPECT_EQ(kLoopCount, counter.get_value());
        LOG(INFO) << "IntCounter: elapsed: " << elapsed << "ns, ns/iter:" << elapsed / kLoopCount;
    }

    // multi-thread for IntCounter
    {
        BvarAdderMetric<int64_t> mt_counter(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                            "counter");
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<BvarAdderMetric<int64_t>>, kThreadLoopCount,
                                  &mt_counter, &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * kThreadLoopCount);
        EXPECT_EQ(8 * kThreadLoopCount, mt_counter.get_value());
    }
}

TEST_F(BvarMetricsTest, Gauge) {
    // IntGauge
    {
        BvarAdderMetric<int64_t> gauge(BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "gauge");
        EXPECT_EQ(0, gauge.get_value());
        gauge.set_value(100);
        EXPECT_EQ(100, gauge.get_value());

        EXPECT_STREQ("100", gauge.value_string().c_str());
    }
    // UIntGauge
    {
        BvarAdderMetric<uint64_t> gauge(BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "gauge");
        EXPECT_EQ(0, gauge.get_value());
        gauge.set_value(100);
        EXPECT_EQ(100, gauge.get_value());

        EXPECT_STREQ("100", gauge.value_string().c_str());
    }
    // DoubleGauge
    {
        BvarAdderMetric<double> gauge(BvarMetricType::GAUGE, BvarMetricUnit::BYTES, "gauge");
        EXPECT_EQ(0.0, gauge.get_value());
        gauge.set_value(1.23);
        EXPECT_EQ(1.23, gauge.get_value());

        EXPECT_STREQ("1.230000", gauge.value_string().c_str());
    }
}

} // namespace doris