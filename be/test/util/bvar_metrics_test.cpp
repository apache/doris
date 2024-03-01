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
    BvarMetricsTest() = default;
    virtual ~BvarMetricsTest() = default;
};

TEST_F(BvarMetricsTest, BvarAdderMetricsValue) {
    {
        // int64_6
        BvarAdderMetric<int64_t> test_metric(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                             "test_metric");
        EXPECT_EQ(0, test_metric.get_value());
        test_metric.increment(100);
        EXPECT_EQ(100, test_metric.get_value());
        EXPECT_STREQ("100", test_metric.value_string().c_str());
        test_metric.set_value(1000);
        EXPECT_EQ(1000, test_metric.get_value());
        test_metric.reset();
        EXPECT_EQ(0, test_metric.get_value());
    }
    {
        // uint64_6
        BvarAdderMetric<uint64_t> test_metric(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                              "test_metric");
        EXPECT_EQ(0, test_metric.get_value());
        test_metric.increment(100);
        EXPECT_EQ(100, test_metric.get_value());
        EXPECT_STREQ("100", test_metric.value_string().c_str());
        test_metric.set_value(1000);
        EXPECT_EQ(1000, test_metric.get_value());
        test_metric.reset();
        EXPECT_EQ(0, test_metric.get_value());
    }
    {
        // double
        BvarAdderMetric<double> test_metric(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                            "test_metric");
        EXPECT_EQ(0, test_metric.get_value());
        test_metric.increment(1.23);
        EXPECT_EQ(1.23, test_metric.get_value());
        EXPECT_STREQ("1.230000", test_metric.value_string().c_str());
        test_metric.set_value(12.345);
        EXPECT_EQ(12.345, test_metric.get_value());
        test_metric.reset();
        EXPECT_EQ(0, test_metric.get_value());
    }
}

template <typename T>
void mt_updater(int32_t loop, T* metric, std::atomic<uint64_t>* used_time) {
    sleep(1);
    MonotonicStopWatch watch;
    watch.start();
    for (int i = 0; i < loop; ++i) {
        metric->increment(1);
    }
    uint64_t elapsed = watch.elapsed_time();
    used_time->fetch_add(elapsed);
}

TEST_F(BvarMetricsTest, BvarAdderMetricsValuePerf) {
    static const int kLoopCount = LOOP_LESS_OR_MORE(10, 100000000);
    static const int kThreadLoopCount = LOOP_LESS_OR_MORE(1000, 1000000);
    {
        // check mt_updater func()
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
    {
        //int64_t
        BvarAdderMetric<int64_t> metric(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                        "test_metric");
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < kLoopCount; ++i) {
            metric.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        EXPECT_EQ(kLoopCount, metric.get_value());
        LOG(INFO) << "IntAtomicCounter: elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / kLoopCount;
    }
    {
        // multi-thread for BvarAdderMetric<int64_t>
        BvarAdderMetric<int64_t> metric(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                        "test_metric");
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater<BvarAdderMetric<int64_t>>, kThreadLoopCount, &metric,
                                  &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "IntCounter multi-thread elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * kThreadLoopCount);
        EXPECT_EQ(8 * kThreadLoopCount, metric.get_value());
    }
}

TEST_F(BvarMetricsTest, BvarMetricsName) {
    {
        // name
        BvarAdderMetric<int64_t> cpu_idle_type(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                               "fragment_requests_total",
                                               "Total fragment requests received.");

        EXPECT_EQ("fragment_requests_total", cpu_idle_type.simple_name());
        EXPECT_EQ("fragment_requests_total", cpu_idle_type.combine_name(""));
        EXPECT_EQ("doris_be_fragment_requests_total", cpu_idle_type.combine_name("doris_be"));
    }
    {
        // group_name
        BvarAdderMetric<int64_t> cpu_idle_type(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                               "cpu_idle", "CPU's idle time percent", "cpu");

        EXPECT_EQ("cpu", cpu_idle_type.simple_name());
        EXPECT_EQ("cpu", cpu_idle_type.combine_name(""));
        EXPECT_EQ("doris_be_cpu", cpu_idle_type.combine_name("doris_be"));
    }
}

TEST_F(BvarMetricsTest, MetricEntityWithMetric) {
    BvarMetricEntity entity("test_entity", BvarMetricEntityType::kServer, {});
    BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_idle");

    // Before register
    std::shared_ptr<BvarMetric> metric = entity.get_metric("cpu_idle");
    EXPECT_NE(nullptr, metric);

    // Register
    entity.register_metric("cpu_idle", cpu_idle);
    cpu_idle.increment(12);

    metric = entity.get_metric("cpu_idle");
    EXPECT_NE(nullptr, metric);
    EXPECT_EQ("12", metric->value_string());

    cpu_idle.increment(8);
    EXPECT_EQ("20", metric->value_string());

    // Deregister
    entity.deregister_metric("cpu_idle");

    // After deregister
    metric = entity.get_metric("cpu_idle");
    EXPECT_EQ(nullptr, metric);
}

TEST_F(BvarMetricsTest, MetricEntityWithHook) {
    BvarMetricEntity entity("test_entity", BvarMetricEntityType::kServer, {});
    BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_idle");

    // Register
    entity.register_metric("cpu_idle", cpu_idle);
    entity.register_hook("test_hook", [&]() { cpu_idle.increment(6); });

    // Before hook
    std::shared_ptr<BvarMetric> metric = entity.get_metric("cpu_idle");
    EXPECT_NE(nullptr, metric);
    EXPECT_EQ("0", metric->value_string());

    // Hook
    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("6", metric->value_string());

    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("12", metric->value_string());

    // Deregister hook
    entity.deregister_hook("test_hook");
    // Hook but no effect
    entity.trigger_hook_unlocked(true);
    EXPECT_EQ("12", metric->value_string());
}

TEST_F(BvarMetricsTest, BvarMetricRegistryRegister) {
    BvarMetricRegistry registry("test_registry");

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

TEST_F(BvarMetricsTest, BvarMetricRegistryOutput) {
    BvarMetricRegistry registry("test_registry");

    {
        // No entity
        EXPECT_EQ("", registry.to_prometheus());
        EXPECT_EQ("[]", registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
    }

    {
        // Register one common metric to the entity
        auto entity = registry.register_entity("test_entity");

        BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                          "cpu_idle", "", "", {}, true);
        entity->register_metric("cpu_idle", cpu_idle);
        cpu_idle.increment(8);

        EXPECT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle  8
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

        BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                          "cpu_idle", "", "cpu", {{"mode", "idle"}}, false);
        entity->register_metric("cpu_idle", cpu_idle);
        cpu_idle.increment(18);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu {mode="idle"} 18
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

        BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                          "cpu_idle");
        entity->register_metric("cpu_idle", cpu_idle);
        cpu_idle.increment(28);

        EXPECT_EQ(R"(# TYPE test_registry_cpu_idle gauge
test_registry_cpu_idle {name="label_test"} 28
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

        BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                          "cpu_idle", "", "cpu", {{"mode", "idle"}}, false);
        entity->register_metric("cpu_idle", cpu_idle);
        cpu_idle.increment(38);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu {name="label_test",mode="idle"} 38
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

        BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                          "cpu_idle", "", "cpu", {{"mode", "idle"}});
        entity->register_metric("cpu_idle", cpu_idle);
        cpu_idle.increment(48);

        BvarAdderMetric<int64_t> cpu_guest(BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                                           "cpu_guest", "", "cpu", {{"mode", "guest"}});
        entity->register_metric("cpu_guest", cpu_guest);
        cpu_guest.increment(58);

        EXPECT_EQ(R"(# TYPE test_registry_cpu gauge
test_registry_cpu {mode="idle"} 48
test_registry_cpu {mode="guest"} 58
)",
                  registry.to_prometheus());
        EXPECT_EQ(
                R"([{"tags":{"metric":"cpu","mode":"guest"},"unit":"percent","value":58},{"tags":{"metric":"cpu","mode":"idle"},"unit":"percent","value":48}])",
                registry.to_json());
        EXPECT_EQ("", registry.to_core_string());
        registry.deregister_entity(entity);
    }
}

} // namespace doris