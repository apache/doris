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
        IntCounter counter(MetricUnit::NUMBER);
        ASSERT_EQ(0, counter.value());
        counter.increment(100);
        ASSERT_EQ(100, counter.value());

        ASSERT_STREQ("100", counter.to_string().c_str());
    }
    {
        DoubleCounter counter(MetricUnit::NUMBER);
        ASSERT_EQ(0.0, counter.value());
        counter.increment(1.23);
        ASSERT_EQ(1.23, counter.value());

        ASSERT_STREQ("1.23", counter.to_string().c_str());
    }
}

void mt_updater(IntCounter* counter, std::atomic<uint64_t>* used_time) {
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
    IntCounter counter(MetricUnit::NUMBER);
    volatile int64_t sum = 0;

    {
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < 100000000; ++i) {
            counter.increment(1);
        }
        uint64_t elapsed = watch.elapsed_time();
        LOG(INFO) << "counter elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / 100000000;
    }
    {
        MonotonicStopWatch watch;
        watch.start();
        for (int i = 0; i < 100000000; ++i) {
            sum += 1;
        }
        uint64_t elapsed = watch.elapsed_time();
        LOG(INFO) << "value elapsed: " << elapsed
                  << "ns, ns/iter:" << elapsed / 100000000;
    }
    ASSERT_EQ(100000000, counter.value());
    ASSERT_EQ(100000000, sum);
    {
        IntCounter mt_counter(MetricUnit::NUMBER);
        std::vector<std::thread> updaters;
        std::atomic<uint64_t> used_time(0);
        for (int i = 0; i < 8; ++i) {
            updaters.emplace_back(&mt_updater, &mt_counter, &used_time);
        }
        for (int i = 0; i < 8; ++i) {
            updaters[i].join();
        }
        LOG(INFO) << "mt_counter elapsed: " << used_time.load()
                  << "ns, ns/iter:" << used_time.load() / (8 * 1000000L);
        ASSERT_EQ(8 * 1000000L, mt_counter.value());
    }
}

TEST_F(MetricsTest, Gauge) {
    {
        IntGauge gauge(MetricUnit::NUMBER);
        ASSERT_EQ(0, gauge.value());
        gauge.set_value(100);
        ASSERT_EQ(100, gauge.value());

        ASSERT_STREQ("100", gauge.to_string().c_str());
    }
    {
        DoubleGauge gauge(MetricUnit::NUMBER);
        ASSERT_EQ(0.0, gauge.value());
        gauge.set_value(1.23);
        ASSERT_EQ(1.23, gauge.value());

        ASSERT_STREQ("1.23", gauge.to_string().c_str());
    }
}

TEST_F(MetricsTest, MetricLabel) {
    std::string put("put");
    MetricLabel label("type", put);

    ASSERT_TRUE(label == MetricLabel("type", "put"));
    ASSERT_TRUE(label != MetricLabel("type", "get"));
    ASSERT_TRUE(label < MetricLabel("type", "quit"));
    ASSERT_TRUE(label < MetricLabel("typee", "put"));
    ASSERT_TRUE(label.compare(MetricLabel("type", "put")) == 0);
    ASSERT_TRUE(label.compare(MetricLabel("typee", "put")) < 0);

    ASSERT_STREQ("type=put", label.to_string().c_str());
}

TEST_F(MetricsTest, MetricLabels) {
    MetricLabels empty_labels;

    ASSERT_TRUE(empty_labels == MetricLabels());
    ASSERT_TRUE(empty_labels < MetricLabels().add("type", "put"));
    ASSERT_TRUE(empty_labels.empty());

    ASSERT_STREQ("", empty_labels.to_string().c_str());

    MetricLabels labels;
    labels.add("path", "/home").add("type", "put");

    ASSERT_TRUE(labels == MetricLabels().add("path", "/home").add("type", "put"));
    ASSERT_FALSE(labels == MetricLabels().add("path", "/home").add("type", "get"));
    ASSERT_FALSE(labels == MetricLabels().add("path", "/home"));
    ASSERT_TRUE(labels < MetricLabels().add("path", "/sports"));
    ASSERT_TRUE(labels < MetricLabels().add("path", "/home").add("type", "put").add("xstatus", "404"));
    ASSERT_FALSE(labels < MetricLabels().add("path", "/abc"));
    ASSERT_FALSE(labels < MetricLabels().add("path", "/home").add("type", "put"));

    ASSERT_STREQ("path=/home,type=put", labels.to_string().c_str());
}

class TestMetricsVisitor : public MetricsVisitor {
public:
    virtual ~TestMetricsVisitor() { }
    void visit(const std::string& prefix, const std::string& name,
               MetricCollector* collector) {
        for (auto& it : collector->metrics()) {
            Metric* metric = it.second;
            auto& labels = it.first;
            switch (metric->type()) {
            case MetricType::COUNTER: {
                bool has_prev = false;
                if (!prefix.empty()) {
                    _ss << prefix;
                    has_prev = true;
                }
                if (!name.empty()) {
                    if (has_prev) {
                        _ss << "_";
                    }
                    _ss << name;
                }
                if (!labels.empty()) {
                    if (has_prev) {
                        _ss << "_";
                    }
                    _ss << labels.to_string();
                }
                _ss << " " << metric->to_string() << std::endl;
                break;
            }
            default:
                break;
            }
        }
    }
    std::string to_string() {
        return _ss.str();
    }
private:
    std::stringstream _ss;
};

TEST_F(MetricsTest, MetricCollector) {
    IntCounter puts(MetricUnit::NUMBER);
    puts.increment(101);
    IntCounter gets(MetricUnit::NUMBER);
    gets.increment(201);
    MetricCollector collector;
    ASSERT_TRUE(collector.add_metic(MetricLabels().add("type", "put"), &puts));
    ASSERT_TRUE(collector.add_metic(MetricLabels().add("type", "get"), &gets));
    ASSERT_FALSE(collector.add_metic(MetricLabels().add("type", "get"), &gets));

    {
        // Can't add different type to one collector
        IntGauge post(MetricUnit::NUMBER);
        ASSERT_FALSE(collector.add_metic(MetricLabels().add("type", "post"), &post));
    }

    {
        TestMetricsVisitor visitor;
        collector.collect("", "", &visitor);
        ASSERT_STREQ("type=get 201\ntype=put 101\n", visitor.to_string().c_str());
    }
    collector.remove_metric(&puts);
    {
        TestMetricsVisitor visitor;
        collector.collect("", "", &visitor);
        ASSERT_STREQ("type=get 201\n", visitor.to_string().c_str());
    }
    // test get_metric
    ASSERT_TRUE(collector.get_metric(MetricLabels()) == nullptr);
    ASSERT_TRUE(collector.get_metric(MetricLabels().add("type" ,"get")) != nullptr);
    std::vector<Metric*> metrics;
    collector.get_metrics(&metrics);
    ASSERT_EQ(1, metrics.size());
}

TEST_F(MetricsTest, MetricRegistry) {
    MetricRegistry registry("test");
    IntCounter cpu_idle(MetricUnit::PERCENT);
    cpu_idle.increment(12);
    ASSERT_TRUE(registry.register_metric("cpu_idle", &cpu_idle));
    // registry failed
    IntCounter dummy(MetricUnit::PERCENT);
    ASSERT_FALSE(registry.register_metric("cpu_idle", &dummy));
    IntCounter memory_usage(MetricUnit::BYTES);
    memory_usage.increment(24);
    ASSERT_TRUE(registry.register_metric("memory_usage", &memory_usage));
    {
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\ntest_memory_usage 24\n", visitor.to_string().c_str());
    }
    registry.deregister_metric(&memory_usage);
    {
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\n", visitor.to_string().c_str());
    }
    // test get_metric
    ASSERT_TRUE(registry.get_metric("cpu_idle") != nullptr);
    ASSERT_TRUE(registry.get_metric("memory_usage") == nullptr);
}

TEST_F(MetricsTest, MetricRegistry2) {
    MetricRegistry registry("test");
    IntCounter cpu_idle(MetricUnit::PERCENT);
    cpu_idle.increment(12);
    ASSERT_TRUE(registry.register_metric("cpu_idle", &cpu_idle));

    {
        // memory_usage will deregister after this block
        IntCounter memory_usage(MetricUnit::BYTES);
        memory_usage.increment(24);
        ASSERT_TRUE(registry.register_metric("memory_usage", &memory_usage));
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\ntest_memory_usage 24\n", visitor.to_string().c_str());
    }

    {
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_STREQ("test_cpu_idle 12\n", visitor.to_string().c_str());
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
