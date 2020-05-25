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

#include "util/system_metrics.h"

#include <gtest/gtest.h>
#include <libgen.h>

#include "common/config.h"
#include "util/logging.h"
#include "util/metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

class SystemMetricsTest : public testing::Test {
public:
    SystemMetricsTest() { }
    virtual ~SystemMetricsTest() {
    }
};

class TestMetricsVisitor : public MetricsVisitor {
public:
    virtual ~TestMetricsVisitor() { }
    void visit(const std::string& prefix, const std::string& name,
               MetricCollector* collector) {
        for (auto& it : collector->metrics()) {
            Metric* metric = it.second;
            auto& labels = it.first;
            switch (metric->type()) {
            case MetricType::GAUGE:
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
                        _ss << "{";
                    }
                    _ss << labels.to_string();
                    if (has_prev) {
                        _ss << "}";
                    }
                }
                _ss << " " << ((SimpleMetric*)metric)->to_string() << std::endl;
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

extern const char* k_ut_stat_path;
extern const char* k_ut_diskstats_path;
extern const char* k_ut_net_dev_path;
extern const char* k_ut_fd_path;

TEST_F(SystemMetricsTest, normal) {
    MetricRegistry registry("test");
    {
        char buf[1024];
        readlink("/proc/self/exe", buf, 1023);
        char* dir_path = dirname(buf);
        std::string stat_path(dir_path);
        stat_path += "/test_data/stat_normal";
        LOG(INFO) << stat_path;
        k_ut_stat_path = stat_path.c_str();
        std::string diskstats_path(dir_path);
        diskstats_path += "/test_data/diskstats_normal";
        k_ut_diskstats_path = diskstats_path.c_str();
        std::string net_dev_path(dir_path);
        net_dev_path += "/test_data/net_dev_normal";
        k_ut_net_dev_path = net_dev_path.c_str();
        std::string fd_path(dir_path);
        fd_path += "/test_data/fd_file_nr";
        k_ut_fd_path = fd_path.c_str();

        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics;
        metrics.install(&registry, disk_devices, network_interfaces);
        metrics.update();

        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        LOG(INFO) << "\n" << visitor.to_string();

        // cpu
        SimpleMetric* cpu_user = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "user"));
        ASSERT_TRUE(cpu_user != nullptr);
        // ASSERT_STREQ("57199151", cpu_user->to_string().c_str());
        SimpleMetric* cpu_nice = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "nice"));
        ASSERT_TRUE(cpu_nice != nullptr);
        ASSERT_STREQ("2616310", cpu_nice->to_string().c_str());
        SimpleMetric* cpu_system = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "system"));
        ASSERT_TRUE(cpu_system != nullptr);
        ASSERT_STREQ("10600935", cpu_system->to_string().c_str());
        SimpleMetric* cpu_idle = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "idle"));
        ASSERT_TRUE(cpu_idle != nullptr);
        ASSERT_STREQ("1517505423", cpu_idle->to_string().c_str());
        SimpleMetric* cpu_iowait = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "iowait"));
        ASSERT_TRUE(cpu_iowait != nullptr);
        ASSERT_STREQ("2137148", cpu_iowait->to_string().c_str());
        SimpleMetric* cpu_irq = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "irq"));
        ASSERT_TRUE(cpu_irq != nullptr);
        ASSERT_STREQ("0", cpu_irq->to_string().c_str());
        SimpleMetric* cpu_softirq = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "soft_irq"));
        ASSERT_TRUE(cpu_softirq != nullptr);
        ASSERT_STREQ("108277", cpu_softirq->to_string().c_str());
        SimpleMetric* cpu_steal = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "steal"));
        ASSERT_TRUE(cpu_steal != nullptr);
        ASSERT_STREQ("0", cpu_steal->to_string().c_str());
        SimpleMetric* cpu_guest = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "guest"));
        ASSERT_TRUE(cpu_guest != nullptr);
        ASSERT_STREQ("0", cpu_guest->to_string().c_str());
        // memroy
        SimpleMetric* memory_allocated_bytes = (SimpleMetric*)registry.get_metric(
            "memory_allocated_bytes");
        ASSERT_TRUE(memory_allocated_bytes != nullptr);
        // network
        SimpleMetric* receive_bytes = (SimpleMetric*)registry.get_metric(
            "network_receive_bytes", MetricLabels().add("device", "xgbe0"));
        ASSERT_TRUE(receive_bytes != nullptr);
        ASSERT_STREQ("52567436039", receive_bytes->to_string().c_str());
        SimpleMetric* receive_packets = (SimpleMetric*)registry.get_metric(
            "network_receive_packets", MetricLabels().add("device", "xgbe0"));
        ASSERT_TRUE(receive_packets != nullptr);
        ASSERT_STREQ("65066152", receive_packets->to_string().c_str());
        SimpleMetric* send_bytes = (SimpleMetric*)registry.get_metric(
            "network_send_bytes", MetricLabels().add("device", "xgbe0"));
        ASSERT_TRUE(send_bytes != nullptr);
        ASSERT_STREQ("45480856156", send_bytes->to_string().c_str());
        SimpleMetric* send_packets = (SimpleMetric*)registry.get_metric(
            "network_send_packets", MetricLabels().add("device", "xgbe0"));
        ASSERT_TRUE(send_packets != nullptr);
        ASSERT_STREQ("88277614", send_packets->to_string().c_str());
        // disk
        SimpleMetric* bytes_read = (SimpleMetric*)registry.get_metric(
            "disk_bytes_read", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(bytes_read != nullptr);
        ASSERT_STREQ("20142745600", bytes_read->to_string().c_str());
        SimpleMetric* reads_completed = (SimpleMetric*)registry.get_metric(
            "disk_reads_completed", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(reads_completed != nullptr);
        ASSERT_STREQ("759548", reads_completed->to_string().c_str());
        SimpleMetric* read_time_ms = (SimpleMetric*)registry.get_metric(
            "disk_read_time_ms", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(read_time_ms != nullptr);
        ASSERT_STREQ("4308146", read_time_ms->to_string().c_str());

        SimpleMetric* bytes_written = (SimpleMetric*)registry.get_metric(
            "disk_bytes_written", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(bytes_written != nullptr);
        ASSERT_STREQ("1624753500160", bytes_written->to_string().c_str());
        SimpleMetric* writes_completed = (SimpleMetric*)registry.get_metric(
            "disk_writes_completed", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(writes_completed != nullptr);
        ASSERT_STREQ("18282936", writes_completed->to_string().c_str());
        SimpleMetric* write_time_ms = (SimpleMetric*)registry.get_metric(
            "disk_write_time_ms", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(write_time_ms != nullptr);
        ASSERT_STREQ("1907755230", write_time_ms->to_string().c_str());
        SimpleMetric* io_time_ms = (SimpleMetric*)registry.get_metric(
            "disk_io_time_ms", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(io_time_ms != nullptr);
        ASSERT_STREQ("19003350", io_time_ms->to_string().c_str());
        SimpleMetric* io_time_weigthed = (SimpleMetric*)registry.get_metric(
            "disk_io_time_weigthed", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(write_time_ms != nullptr);
        ASSERT_STREQ("1912122964", io_time_weigthed->to_string().c_str());

        // fd
        SimpleMetric* fd_metric = (SimpleMetric*)registry.get_metric(
            "fd_num_limit");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("13052138", fd_metric->to_string().c_str());
        fd_metric = (SimpleMetric*)registry.get_metric(
            "fd_num_used");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("19520", fd_metric->to_string().c_str());
    }
    {
        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        ASSERT_TRUE(visitor.to_string().empty());

        Metric* cpu_idle = registry.get_metric("cpu", MetricLabels().add("mode", "idle"));
        ASSERT_TRUE(cpu_idle == nullptr);
        Metric* cpu_user = registry.get_metric("cpu", MetricLabels().add("mode", "user"));
        ASSERT_TRUE(cpu_user == nullptr);
        Metric* memory_allocated_bytes = registry.get_metric("memory_allocated_bytes");
        ASSERT_TRUE(memory_allocated_bytes == nullptr);
    }
}

TEST_F(SystemMetricsTest, no_proc_file) {
    MetricRegistry registry("test");
    {
        char buf[1024];
        readlink("/proc/self/exe", buf, 1023);
        char* dir_path = dirname(buf);
        std::string stat_path(dir_path);
        stat_path += "/test_data/no_stat_normal";
        LOG(INFO) << stat_path;
        k_ut_stat_path = stat_path.c_str();
        std::string diskstats_path(dir_path);
        diskstats_path += "/test_data/no_diskstats_normal";
        k_ut_diskstats_path = diskstats_path.c_str();
        std::string net_dev_path(dir_path);
        net_dev_path += "/test_data/no_net_dev_normal";
        k_ut_net_dev_path = net_dev_path.c_str();

        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics;
        metrics.install(&registry, disk_devices, network_interfaces);

        TestMetricsVisitor visitor;
        registry.collect(&visitor);
        LOG(INFO) << "\n" << visitor.to_string();

        // cpu
        SimpleMetric* cpu_user = (SimpleMetric*)registry.get_metric(
            "cpu", MetricLabels().add("mode", "user"));
        ASSERT_TRUE(cpu_user != nullptr);
        ASSERT_STREQ("0", cpu_user->to_string().c_str());
        // memroy
        SimpleMetric* memory_allocated_bytes = (SimpleMetric*)registry.get_metric(
            "memory_allocated_bytes");
        ASSERT_TRUE(memory_allocated_bytes != nullptr);
        // network
        SimpleMetric* receive_bytes = (SimpleMetric*)registry.get_metric(
            "network_receive_bytes", MetricLabels().add("device", "xgbe0"));
        ASSERT_TRUE(receive_bytes != nullptr);
        ASSERT_STREQ("0", receive_bytes->to_string().c_str());
        // disk
        SimpleMetric* bytes_read = (SimpleMetric*)registry.get_metric(
            "disk_bytes_read", MetricLabels().add("device", "sda"));
        ASSERT_TRUE(bytes_read != nullptr);
        ASSERT_STREQ("0", bytes_read->to_string().c_str());
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
