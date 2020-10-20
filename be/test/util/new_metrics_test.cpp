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

#include <gtest/gtest.h>

#include <iostream>
#include <thread>

#include "common/config.h"
#include "util/logging.h"
#include "util/metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

class MetricsTest : public testing::Test {
public:
    MetricsTest() { }
    virtual ~MetricsTest() {
    }
};

TEST_F(MetricsTest, Counter) {
    {
        IntCounter counter;
        ASSERT_EQ(0, counter.value());
        counter.increment(100);
        ASSERT_EQ(100, counter.value());

        ASSERT_STREQ("100", counter.to_string().c_str());
    }
    {
        IntAtomicCounter counter;
        ASSERT_EQ(0, counter.value());
        counter.increment(100);
        ASSERT_EQ(100, counter.value());

        ASSERT_STREQ("100", counter.to_string().c_str());
    }
    {
        UIntCounter counter;
        ASSERT_EQ(0, counter.value());
        counter.increment(100);
        ASSERT_EQ(100, counter.value());

        ASSERT_STREQ("100", counter.to_string().c_str());
    }
    {
        DoubleCounter counter;
        ASSERT_EQ(0, counter.value());
        counter.increment(1.23);
        ASSERT_EQ(1.23, counter.value());

        ASSERT_STREQ("1.230000", counter.to_string().c_str());
    }
}

template<typename T>
void mt_updater(T* counter, std::atomic<uint64_t>* used_time) {
    sleep(1);
    MonotonicStopWatch watch;
    watch.start();
    for (int i = 0; i < 1000000L; ++i) {
        counter->increment(1);
    }
    uint64_t elapsed = watch.elapsed_time();
    used_time->fetch_add(elapsed);
}

TEST_F(MetricsTest, CounterPerf) {
    static const int kLoopCount = 100000000;
    // volatile int64_t
    {
        volatile int64_t sum = 0;
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            sum += 1;
        }
        uint64_t elapsed = watch.elapsed_time();
        ASSERT_EQ(kLoopCount, sum);
        LOG(INFO) << "int64_t: elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / kLoopCount;
    }
    // IntAtomicCounter
    {
        IntAtomicCounter counter;
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        ASSERT_EQ(kLoopCount, counter.value());
        LOG(INFO) << "IntAtomicCounter: elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / kLoopCount;
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
        ASSERT_EQ(kLoopCount, counter.value());
        LOG(INFO) << "IntCounter: elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / kLoopCount;
    }

    // multi-thread for IntCounter
    {
        IntCounter mt_counter;
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<IntCounter>, &mt_counter, &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * 1000000L);
        ASSERT_EQ(8 * 1000000L, mt_counter.value());
    }
    // multi-thread for IntAtomicCounter
    {
        IntAtomicCounter mt_counter;
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<IntAtomicCounter>, &mt_counter, &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntAtomicCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * 1000000L);
        ASSERT_EQ(8 * 1000000L, mt_counter.value());
    }
}

TEST_F(MetricsTest, Gauge) {
    // IntGauge
    {
        IntGauge gauge;
        ASSERT_EQ(0, gauge.value());
        gauge.set_value(100);
        ASSERT_EQ(100, gauge.value());

        ASSERT_STREQ("100", gauge.to_string().c_str());
    }
    // UIntGauge
    {
        UIntGauge gauge;
        ASSERT_EQ(0, gauge.value());
        gauge.set_value(100);
        ASSERT_EQ(100, gauge.value());

        ASSERT_STREQ("100", gauge.to_string().c_str());
    }
    // DoubleGauge
    {
        DoubleGauge gauge;
        ASSERT_EQ(0.0, gauge.value());
        gauge.set_value(1.23);
        ASSERT_EQ(1.23, gauge.value());

        ASSERT_STREQ("1.230000", gauge.to_string().c_str());
    }
}

TEST_F(MetricsTest, MetricPrototype) {
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "fragment_requests_total",
                                      "Total fragment requests received.");

        ASSERT_EQ("fragment_requests_total", cpu_idle_type.simple_name());
        ASSERT_EQ("fragment_requests_total", cpu_idle_type.combine_name(""));
        ASSERT_EQ("doris_be_fragment_requests_total", cpu_idle_type.combine_name("doris_be"));
    }
    {
        MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle",
                                      "CPU's idle time percent", "cpu");

        ASSERT_EQ("cpu", cpu_idle_type.simple_name());
        ASSERT_EQ("cpu", cpu_idle_type.combine_name(""));
        ASSERT_EQ("doris_be_cpu", cpu_idle_type.combine_name("doris_be"));
    }
}

TEST_F(MetricsTest, MetricEntityWithMetric) {
    MetricEntity entity(MetricEntityType::kServer, "test_entity", {});

    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Before register
    Metric* metric = entity.get_metric("cpu_idle");
    ASSERT_EQ(nullptr, metric);

    // Register
    IntCounter* cpu_idle = (IntCounter*)entity.register_metric<IntCounter>(&cpu_idle_type);
    cpu_idle->increment(12);

    metric = entity.get_metric("cpu_idle");
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ("12", metric->to_string());

    cpu_idle->increment(8);
    ASSERT_EQ("20", metric->to_string());

    // Deregister
    entity.deregister_metric(&cpu_idle_type);

    // After deregister
    metric = entity.get_metric("cpu_idle");
    ASSERT_EQ(nullptr, metric);
}

TEST_F(MetricsTest, MetricEntityWithHook) {
    MetricEntity entity(MetricEntityType::kServer, "test_entity", {});

    MetricPrototype cpu_idle_type(MetricType::COUNTER, MetricUnit::PERCENT, "cpu_idle");

    // Register
    IntCounter* cpu_idle = (IntCounter*)entity.register_metric<IntCounter>(&cpu_idle_type);
    entity.register_hook("test_hook", [cpu_idle]() {
        cpu_idle->increment(6);
    });

    // Before hook
    Metric* metric = entity.get_metric("cpu_idle");
    ASSERT_NE(nullptr, metric);
    ASSERT_EQ("0", metric->to_string());

    // Hook
    entity.trigger_hook_unlocked(true);
    ASSERT_EQ("6", metric->to_string());

    entity.trigger_hook_unlocked(true);
    ASSERT_EQ("12", metric->to_string());

    // Deregister hook
    entity.deregister_hook("test_hook");
    // Hook but no effect
    entity.trigger_hook_unlocked(true);
    ASSERT_EQ("12", metric->to_string());
}

TEST_F(MetricsTest, MetricRegistryRegister) {
    MetricRegistry registry("test_registry");

    // No entity
    ASSERT_EQ("", registry.to_prometheus());
    ASSERT_EQ("[]", registry.to_json());
    ASSERT_EQ("", registry.to_core_string());

    // Register
    auto entity1 = registry.register_entity("test_entity");
    ASSERT_NE(nullptr, entity1);

    // Register again
    auto entity2 = registry.register_entity("test_entity");
    ASSERT_NE(nullptr, entity2);
    ASSERT_EQ(entity1.get(), entity2.get());

    // Deregister entity once
    registry.deregister_entity(entity1);

    // Still exist and equal to entity1
    entity2 = registry.get_entity("test_entity");
    ASSERT_NE(nullptr, entity2);
    ASSERT_EQ(entity1.get(), entity2.get());

    // Deregister entity twice
    registry.deregister_entity(entity2);

    // Not exist and registry is empty
    entity2 = registry.get_entity("test_entity");
    ASSERT_EQ(nullptr, entity2);
    ASSERT_EQ("", registry.to_prometheus());
}

TEST_F(MetricsTest, MetricRegistryOutput) {
    MetricRegistry registry("test_registry");

    {
        // No entity
        ASSERT_EQ("", registry.to_prometheus());
        ASSERT_EQ("[]", registry.to_json());
        ASSERT_EQ("", registry.to_core_string());
    }

    {
        // Register one common metric to the entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "", {}, true);
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(8);

        ASSERT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle 8
)", registry.to_prometheus());
        ASSERT_EQ(R"([{"tags":{"metric":"cpu_idle"},"unit":"percent","value":8}])", registry.to_json());
        ASSERT_EQ("test_registry_cpu_idle LONG 8\n", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one metric with group name to the entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu", {{"mode", "idle"}}, false);
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(18);

        ASSERT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{mode="idle"} 18
)", registry.to_prometheus());
        ASSERT_EQ(R"([{"tags":{"metric":"cpu","mode":"idle"},"unit":"percent","value":18}])", registry.to_json());
        ASSERT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one common metric to an entity with label
        auto entity = registry.register_entity("test_entity", {{"name", "label_test"}});

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle");
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(28);

        ASSERT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle{name="label_test"} 28
)", registry.to_prometheus());
        ASSERT_EQ(R"([{"tags":{"metric":"cpu_idle","name":"label_test"},"unit":"percent","value":28}])", registry.to_json());
        ASSERT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register one common metric with group name to an entity with label
        auto entity = registry.register_entity("test_entity", {{"name", "label_test"}});

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu", {{"mode", "idle"}});
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(38);

        ASSERT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{name="label_test",mode="idle"} 38
)", registry.to_prometheus());
        ASSERT_EQ(R"([{"tags":{"metric":"cpu","mode":"idle","name":"label_test"},"unit":"percent","value":38}])", registry.to_json());
        ASSERT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }

    {
        // Register two common metrics to one entity
        auto entity = registry.register_entity("test_entity");

        MetricPrototype cpu_idle_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_idle", "", "cpu", {{"mode", "idle"}});
        IntCounter* cpu_idle = (IntCounter*)entity->register_metric<IntCounter>(&cpu_idle_type);
        cpu_idle->increment(48);

        MetricPrototype cpu_guest_type(MetricType::GAUGE, MetricUnit::PERCENT, "cpu_guest", "", "cpu", {{"mode", "guest"}});
        IntGauge* cpu_guest = (IntGauge*)entity->register_metric<IntGauge>(&cpu_guest_type);
        cpu_guest->increment(58);

        ASSERT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu{mode="idle"} 48
test_registry_cpu{mode="guest"} 58
)", registry.to_prometheus());
        ASSERT_EQ(R"([{"tags":{"metric":"cpu","mode":"guest"},"unit":"percent","value":58},{"tags":{"metric":"cpu","mode":"idle"},"unit":"percent","value":48}])", registry.to_json());
        ASSERT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }
}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
