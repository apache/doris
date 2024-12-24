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

#include "util/metrics.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include <thread>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/stopwatch.hpp"

namespace doris {

class MetricsTest : public testing::Test {
public:
    MetricsTest() {}
    virtual ~MetricsTest() {}
};

TEST_F(MetricsTest, Counter) {
    {
        IntCounter counter;
        EXPECT_EQ(0, counter.value());
        counter.increment(100);
        EXPECT_EQ(100, counter.value());

        EXPECT_STREQ("100", counter.to_string().c_str());
    }
    {
        IntCounter counter;
        EXPECT_EQ(0, counter.value());
        counter.increment(100);
        EXPECT_EQ(100, counter.value());

        EXPECT_STREQ("100", counter.to_string().c_str());
    }
    {
        UIntCounter counter;
        EXPECT_EQ(0, counter.value());
        counter.increment(100);
        EXPECT_EQ(100, counter.value());

        EXPECT_STREQ("100", counter.to_string().c_str());
    }
    {
        DoubleCounter counter;
        EXPECT_EQ(0, counter.value());
        counter.increment(1.23);
        EXPECT_EQ(1.23, counter.value());

        EXPECT_STREQ("1.230000", counter.to_string().c_str());
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

TEST_F(MetricsTest, CounterPerf) {
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
        IntCounter counter;
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        EXPECT_EQ(kLoopCount, counter.value());
        LOG(INFO) << "IntCounter: elapsed: " << elapsed << "ns, ns/iter:" << elapsed / kLoopCount;
    }
    // IntCounter
    {
        IntCounter counter;
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        EXPECT_EQ(kLoopCount, counter.value());
        LOG(INFO) << "IntCounter: elapsed: " << elapsed << "ns, ns/iter:" << elapsed / kLoopCount;
    }

    // multi-thread for IntCounter
    {
        IntCounter mt_counter;
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<IntCounter>, kThreadLoopCount, &mt_counter,
                                  &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * kThreadLoopCount);
        EXPECT_EQ(8 * kThreadLoopCount, mt_counter.value());
    }
    // multi-thread for IntCounter
    {
        IntCounter mt_counter;
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<IntCounter>, kThreadLoopCount, &mt_counter,
                                  &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * kThreadLoopCount);
        EXPECT_EQ(8 * kThreadLoopCount, mt_counter.value());
    }
}

TEST_F(MetricsTest, Gauge) {
    // IntGauge
    {
        IntGauge gauge;
        EXPECT_EQ(0, gauge.value());
        gauge.set_value(100);
        EXPECT_EQ(100, gauge.value());

        EXPECT_STREQ("100", gauge.to_string().c_str());
    }
    // UIntGauge
    {
        UIntGauge gauge;
        EXPECT_EQ(0, gauge.value());
        gauge.set_value(100);
        EXPECT_EQ(100, gauge.value());

        EXPECT_STREQ("100", gauge.to_string().c_str());
    }
    // DoubleGauge
    {
        DoubleGauge gauge;
        EXPECT_EQ(0.0, gauge.value());
        gauge.set_value(1.23);
        EXPECT_EQ(1.23, gauge.value());

        EXPECT_STREQ("1.230000", gauge.to_string().c_str());
    }
}

TEST_F(MetricsTest, MetricPrototype) {
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT,
                                      "fragment_requests_total",
                                      "Total fragment requests received.");

        EXPECT_EQ("fragment_requests_total", cpu_idle_type.simple_name());
        EXPECT_EQ("fragment_requests_total", cpu_idle_type.combine_name(""));
        EXPECT_EQ("doris_be_fragment_requests_total", cpu_idle_type.combine_name("doris_be"));
    }
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle",
                                      "CPU's idle time percent", "cpu");

        EXPECT_EQ("cpu", cpu_idle_type.simple_name());
        EXPECT_EQ("cpu", cpu_idle_type.combine_name(""));
        EXPECT_EQ("doris_be_cpu", cpu_idle_type.combine_name("doris_be"));
    }
}

TEST_F(MetricsTest, MetricEntityWithMetric) {
    MetricEntity entity(MetricEntityType::kServer, "test_entity", {});

    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Before register
    Metric* metric = entity.get_metric("cpu_idle");
    EXPECT_EQ(nullptr, metric);

    // Register
    IntCounter* cpu_idle = (IntCounter*)entity.register_metric<IntCounter>(&cpu_idle_type);
    cpu_idle->increment(12);

    metric = entity.get_metric("cpu_idle");
    EXPECT_NE(nullptr, metric);
    EXPECT_EQ("12", metric->to_string());

    cpu_idle->increment(8);
    EXPECT_EQ("20", metric->to_string());

    // Deregister
    entity.deregister_metric(&cpu_idle_type);

    // After deregister
    metric = entity.get_metric("cpu_idle");
    EXPECT_EQ(nullptr, metric);
}

TEST_F(MetricsTest, MetricEntityWithHook) {
    MetricEntity entity(MetricEntityType::kServer, "test_entity", {});

    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Register
    IntCounter* cpu_idle = (IntCounter*)entity.register_metric<IntCounter>(&cpu_idle_type);
    entity.register_hook("test_hook", [cpu_idle]() { cpu_idle->increment(6); });

    // Before hook
    Metric* metric = entity.get_metric("cpu_idle");
    EXPECT_NE(nullptr, metric);
    EXPECT_EQ("0", metric->to_string());

    // Hook
    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("6", metric->to_string());

    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("12", metric->to_string());

    // Deregister hook
    entity.deregister_hook("test_hook");
    // Hook but no effect
    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("12", metric->to_string());
}

TEST_F(MetricsTest, MetricRegistryRegister) {
    MetricRegistry registry("test_registry");

    // No entity
    EXPECT_EQ("", registry.to_prometheus());
    EXPECT_EQ("[]", registry.to_json());
    EXPECT_EQ("", registry.to_core_string());

    // Register
    auto entity1 = registry.register_entity("test_entity");
    EXPECT_NE(nullptr, entity1);

    // Register again
    auto entity2 = registry.register_entity("test_entity");
    EXPECT_NE(nullptr, entity2);
    EXPECT_EQ(entity1.get(), entity2.get());

    // Deregister entity once
    registry.deregister_entity(entity1);

    // Still exist and equal to entity1
    entity2 = registry.get_entity("test_entity");
    EXPECT_NE(nullptr, entity2);
    EXPECT_EQ(entity1.get(), entity2.get());

    // Deregister entity twice
    registry.deregister_entity(entity2);

    // Not exist and registry is empty
    entity2 = registry.get_entity("test_entity");
    EXPECT_EQ(nullptr, entity2);
    EXPECT_EQ("", registry.to_prometheus());
}

TEST_F(MetricsTest, MetricRegistryOutput) {
    MetricRegistry registry("test_registry");

    {
        // No entity
        EXPECT_EQ("", registry.to_prometheus());
        EXPECT_EQ("[]", registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
    }

    {
        // Register one common metric to the entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "",
                                      {}, true);
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(8);

        EXPECT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle 8
)",
                  registry.to_prometheus());
        EXPECT_EQ(R"([{"tags":{"metric":"cpu_idle"},"unit":"percent","value":8}])",
                  registry.to_json());
        EXPECT_EQ("test_registry_cpu_idle LONG 8\n", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one metric with group name to the entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu",
                                      {{"mode", "idle"}}, false);
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(18);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{mode="idle"} 18
)",
                  registry.to_prometheus());
        EXPECT_EQ(R"([{"tags":{"metric":"cpu","mode":"idle"},"unit":"percent","value":18}])",
                  registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one common metric to an entity with label
        auto entity = registry.register_entity("test_entity", {{"name", "label_test"}});

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle");
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(28);

        EXPECT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle{name="label_test"} 28
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"([{"tags":{"metric":"cpu_idle","name":"label_test"},"unit":"percent","value":28}])",
                registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one common metric with group name to an entity with label
        auto entity = registry.register_entity("test_entity", {{"name", "label_test"}});

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu",
                                      {{"mode", "idle"}});
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(38);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{name="label_test",mode="idle"} 38
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"([{"tags":{"metric":"cpu","mode":"idle","name":"label_test"},"unit":"percent","value":38}])",
                registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register two common metrics to one entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu",
                                      {{"mode", "idle"}});
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(48);

        MetricPrototype cpu_guest_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_guest", "",
                                       "cpu", {{"mode", "guest"}});
        IntGauge* cpu_guest = (IntGauge*)entity->register_metric<IntGauge>(&cpu_guest_type);
        cpu_guest->increment(58);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{mode="idle"} 48
test_registry_cpu{mode="guest"} 58
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"([{"tags":{"metric":"cpu","mode":"guest"},"unit":"percent","value":58},{"tags":{"metric":"cpu","mode":"idle"},"unit":"percent","value":48}])",
                registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }
}

TEST_F(MetricsTest, HistogramRegistryOutput) {
    MetricRegistry registry("test_registry");

    {
        // Register one histogram metric to the entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype task_duration_type(MetricType::HISTOGRAM, MetricUnit::MILLISECONDS,
                                           "task_duration");
        HistogramMetric* task_duration =
                (HistogramMetric*)entity->register_metric<HistogramMetric>(&task_duration_type);
        for (int j = 1; j <= 100; j++) {
            task_duration->add(j);
        }
        EXPECT_EQ(R"(# TYPE test_registry_task_duration histogram
test_registry_task_duration{quantile="0.50"} 50
test_registry_task_duration{quantile="0.75"} 75
test_registry_task_duration{quantile="0.90"} 95.8333
test_registry_task_duration{quantile="0.95"} 100
test_registry_task_duration{quantile="0.99"} 100
test_registry_task_duration_sum 5050
test_registry_task_duration_count 100
test_registry_task_duration_max 100
test_registry_task_duration_min 1
test_registry_task_duration_average 50.5
test_registry_task_duration_median 50
test_registry_task_duration_standard_deviation 28.8661
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"*([{"tags":{"metric":"task_duration"},"unit":"milliseconds",)*"
                R"*("value":{"total_count":100,"min":1,"average":50.5,"median":50.0,)*"
                R"*("percentile_50":50.0,"percentile_75":75.0,"percentile_90":95.83333333333334,"percentile_95":100.0,"percentile_99":100.0,)*"
                R"*("standard_deviation":28.86607004772212,"max":100,"total_sum":5050}}])*",
                registry.to_json());
        registry.deregister_entity(entity);
    }

    {
        // Register one histogram metric with labels to the entity
        auto entity = registry.register_entity("test_entity", {{"instance", "test"}});

        MetricPrototype task_duration_type(MetricType::HISTOGRAM, MetricUnit::MILLISECONDS,
                                           "task_duration", "", "", {{"type", "create_tablet"}});
        HistogramMetric* task_duration =
                (HistogramMetric*)entity->register_metric<HistogramMetric>(&task_duration_type);
        for (int j = 1; j <= 100; j++) {
            task_duration->add(j);
        }

        EXPECT_EQ(R"(# TYPE test_registry_task_duration histogram
test_registry_task_duration{instance="test",type="create_tablet",quantile="0.50"} 50
test_registry_task_duration{instance="test",type="create_tablet",quantile="0.75"} 75
test_registry_task_duration{instance="test",type="create_tablet",quantile="0.90"} 95.8333
test_registry_task_duration{instance="test",type="create_tablet",quantile="0.95"} 100
test_registry_task_duration{instance="test",type="create_tablet",quantile="0.99"} 100
test_registry_task_duration_sum{instance="test",type="create_tablet"} 5050
test_registry_task_duration_count{instance="test",type="create_tablet"} 100
test_registry_task_duration_max{instance="test",type="create_tablet"} 100
test_registry_task_duration_min{instance="test",type="create_tablet"} 1
test_registry_task_duration_average{instance="test",type="create_tablet"} 50.5
test_registry_task_duration_median{instance="test",type="create_tablet"} 50
test_registry_task_duration_standard_deviation{instance="test",type="create_tablet"} 28.8661
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"*([{"tags":{"metric":"task_duration","type":"create_tablet","instance":"test"},"unit":"milliseconds",)*"
                R"*("value":{"total_count":100,"min":1,"average":50.5,"median":50.0,)*"
                R"*("percentile_50":50.0,"percentile_75":75.0,"percentile_90":95.83333333333334,"percentile_95":100.0,"percentile_99":100.0,)*"
                R"*("standard_deviation":28.86607004772212,"max":100,"total_sum":5050}}])*",
                registry.to_json());
        registry.deregister_entity(entity);
    }
}
} // namespace doris
