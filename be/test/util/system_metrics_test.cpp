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

#include "common/config.h"
#include "test_util/test_util.h"
#include "util/logging.h"
#include "util/metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

class SystemMetricsTest : public testing::Test {
public:
    SystemMetricsTest() {}
    virtual ~SystemMetricsTest() {}
};

extern const char* k_ut_stat_path;
extern const char* k_ut_diskstats_path;
extern const char* k_ut_net_dev_path;
extern const char* k_ut_fd_path;
extern const char* k_ut_net_snmp_path;
extern const char* k_ut_load_avg_path;

TEST_F(SystemMetricsTest, normal) {
    std::string dir_path = GetCurrentRunningDir();
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
    std::string net_snmp_path(dir_path);
    net_snmp_path += "/test_data/net_snmp_normal";
    k_ut_net_snmp_path = net_snmp_path.c_str();
    std::string load_avg_path(dir_path);
    load_avg_path += "/test_data/load_avg_normal";
    k_ut_load_avg_path = load_avg_path.c_str();

    MetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics(&registry, disk_devices, network_interfaces);
        auto entity = registry.get_entity("server");
        ASSERT_TRUE(entity != nullptr);

        metrics.update();

        // cpu
        Metric* cpu_user = entity->get_metric("cpu_user", "cpu");
        ASSERT_TRUE(cpu_user != nullptr);
        // ASSERT_STREQ("57199151", cpu_user->to_string().c_str());
        Metric* cpu_nice = entity->get_metric("cpu_nice", "cpu");
        ASSERT_TRUE(cpu_nice != nullptr);
        ASSERT_STREQ("2616310", cpu_nice->to_string().c_str());
        Metric* cpu_system = entity->get_metric("cpu_system", "cpu");
        ASSERT_TRUE(cpu_system != nullptr);
        ASSERT_STREQ("10600935", cpu_system->to_string().c_str());
        Metric* cpu_idle = entity->get_metric("cpu_idle", "cpu");
        ASSERT_TRUE(cpu_idle != nullptr);
        ASSERT_STREQ("1517505423", cpu_idle->to_string().c_str());
        Metric* cpu_iowait = entity->get_metric("cpu_iowait", "cpu");
        ASSERT_TRUE(cpu_iowait != nullptr);
        ASSERT_STREQ("2137148", cpu_iowait->to_string().c_str());
        Metric* cpu_irq = entity->get_metric("cpu_irq", "cpu");
        ASSERT_TRUE(cpu_irq != nullptr);
        ASSERT_STREQ("0", cpu_irq->to_string().c_str());
        Metric* cpu_softirq = entity->get_metric("cpu_soft_irq", "cpu");
        ASSERT_TRUE(cpu_softirq != nullptr);
        ASSERT_STREQ("108277", cpu_softirq->to_string().c_str());
        Metric* cpu_steal = entity->get_metric("cpu_steal", "cpu");
        ASSERT_TRUE(cpu_steal != nullptr);
        ASSERT_STREQ("0", cpu_steal->to_string().c_str());
        Metric* cpu_guest = entity->get_metric("cpu_guest", "cpu");
        ASSERT_TRUE(cpu_guest != nullptr);
        ASSERT_STREQ("0", cpu_guest->to_string().c_str());
        Metric* cpu_guest_nice = entity->get_metric("cpu_guest_nice", "cpu");
        ASSERT_TRUE(cpu_guest_nice != nullptr);
        ASSERT_STREQ("0", cpu_guest_nice->to_string().c_str());
        // memroy
        Metric* memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        ASSERT_TRUE(memory_allocated_bytes != nullptr);

        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        ASSERT_TRUE(net_entity != nullptr);

        Metric* receive_bytes = net_entity->get_metric("network_receive_bytes");
        ASSERT_TRUE(receive_bytes != nullptr);
        ASSERT_STREQ("52567436039", receive_bytes->to_string().c_str());
        Metric* receive_packets = net_entity->get_metric("network_receive_packets");
        ASSERT_TRUE(receive_packets != nullptr);
        ASSERT_STREQ("65066152", receive_packets->to_string().c_str());
        Metric* send_bytes = net_entity->get_metric("network_send_bytes");
        ASSERT_TRUE(send_bytes != nullptr);
        ASSERT_STREQ("45480856156", send_bytes->to_string().c_str());
        Metric* send_packets = net_entity->get_metric("network_send_packets");
        ASSERT_TRUE(send_packets != nullptr);
        ASSERT_STREQ("88277614", send_packets->to_string().c_str());

        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        ASSERT_TRUE(disk_entity != nullptr);
        Metric* bytes_read = disk_entity->get_metric("disk_bytes_read");
        ASSERT_TRUE(bytes_read != nullptr);
        ASSERT_STREQ("20142745600", bytes_read->to_string().c_str());
        Metric* reads_completed = disk_entity->get_metric("disk_reads_completed");
        ASSERT_TRUE(reads_completed != nullptr);
        ASSERT_STREQ("759548", reads_completed->to_string().c_str());
        Metric* read_time_ms = disk_entity->get_metric("disk_read_time_ms");
        ASSERT_TRUE(read_time_ms != nullptr);
        ASSERT_STREQ("4308146", read_time_ms->to_string().c_str());

        Metric* bytes_written = disk_entity->get_metric("disk_bytes_written");
        ASSERT_TRUE(bytes_written != nullptr);
        ASSERT_STREQ("1624753500160", bytes_written->to_string().c_str());
        Metric* writes_completed = disk_entity->get_metric("disk_writes_completed");
        ASSERT_TRUE(writes_completed != nullptr);
        ASSERT_STREQ("18282936", writes_completed->to_string().c_str());
        Metric* write_time_ms = disk_entity->get_metric("disk_write_time_ms");
        ASSERT_TRUE(write_time_ms != nullptr);
        ASSERT_STREQ("1907755230", write_time_ms->to_string().c_str());
        Metric* io_time_ms = disk_entity->get_metric("disk_io_time_ms");
        ASSERT_TRUE(io_time_ms != nullptr);
        ASSERT_STREQ("19003350", io_time_ms->to_string().c_str());
        Metric* io_time_weigthed = disk_entity->get_metric("disk_io_time_weigthed");
        ASSERT_TRUE(write_time_ms != nullptr);
        ASSERT_STREQ("1912122964", io_time_weigthed->to_string().c_str());

        // fd
        Metric* fd_metric = entity->get_metric("fd_num_limit");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("13052138", fd_metric->to_string().c_str());
        fd_metric = entity->get_metric("fd_num_used");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("19520", fd_metric->to_string().c_str());

        // net snmp
        Metric* tcp_retrans_segs = entity->get_metric("snmp_tcp_retrans_segs");
        ASSERT_TRUE(tcp_retrans_segs != nullptr);
        Metric* tcp_in_errs = entity->get_metric("snmp_tcp_in_errs");
        ASSERT_TRUE(tcp_in_errs != nullptr);
        ASSERT_STREQ("826271", tcp_retrans_segs->to_string().c_str());
        ASSERT_STREQ("12712", tcp_in_errs->to_string().c_str());

        // load average
        Metric* load_average_1_minutes =
                entity->get_metric("load_average_1_minutes", "load_average");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("1.090000", load_average_1_minutes->to_string().c_str());
        Metric* load_average_5_minutes =
                entity->get_metric("load_average_5_minutes", "load_average");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("1.400000", load_average_5_minutes->to_string().c_str());
        Metric* load_average_15_minutes =
                entity->get_metric("load_average_15_minutes", "load_average");
        ASSERT_TRUE(fd_metric != nullptr);
        ASSERT_STREQ("2.020000", load_average_15_minutes->to_string().c_str());
    }
}

TEST_F(SystemMetricsTest, no_proc_file) {
    std::string dir_path = GetCurrentRunningDir();
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
    k_ut_fd_path = "";
    k_ut_net_snmp_path = "";

    MetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics(&registry, disk_devices, network_interfaces);

        auto entity = registry.get_entity("server");
        ASSERT_TRUE(entity != nullptr);

        // cpu
        Metric* cpu_user = entity->get_metric("cpu_user", "cpu");
        ASSERT_TRUE(cpu_user != nullptr);
        ASSERT_STREQ("0", cpu_user->to_string().c_str());
        // memroy
        Metric* memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        ASSERT_TRUE(memory_allocated_bytes != nullptr);
        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        ASSERT_TRUE(net_entity != nullptr);
        Metric* receive_bytes = net_entity->get_metric("network_receive_bytes");
        ASSERT_TRUE(receive_bytes != nullptr);
        ASSERT_STREQ("0", receive_bytes->to_string().c_str());
        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        ASSERT_TRUE(disk_entity != nullptr);
        Metric* bytes_read = disk_entity->get_metric("disk_bytes_read");
        ASSERT_TRUE(bytes_read != nullptr);
        ASSERT_STREQ("0", bytes_read->to_string().c_str());
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
