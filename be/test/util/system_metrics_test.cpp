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
#include "testutil/test_util.h"
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
extern const char* k_ut_vmstat_path;

TEST_F(SystemMetricsTest, normal) {
    std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/stat_normal";
    LOG(INFO) << stat_path;
    k_ut_stat_path = stat_path.c_str();
    std::string diskstats_path(dir_path);
    diskstats_path += "/util/test_data/diskstats_normal";
    k_ut_diskstats_path = diskstats_path.c_str();
    std::string net_dev_path(dir_path);
    net_dev_path += "/util/test_data/net_dev_normal";
    k_ut_net_dev_path = net_dev_path.c_str();
    std::string fd_path(dir_path);
    fd_path += "/util/test_data/fd_file_nr";
    k_ut_fd_path = fd_path.c_str();
    std::string net_snmp_path(dir_path);
    net_snmp_path += "/util/test_data/net_snmp_normal";
    k_ut_net_snmp_path = net_snmp_path.c_str();
    std::string load_avg_path(dir_path);
    load_avg_path += "/util/test_data/load_avg_normal";
    k_ut_load_avg_path = load_avg_path.c_str();
    std::string vmstat_path(dir_path);
    vmstat_path += "/util/test_data/vmstat_normal";
    k_ut_vmstat_path = vmstat_path.c_str();

    MetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics(&registry, disk_devices, network_interfaces);
        auto entity = registry.get_entity("server");
        EXPECT_TRUE(entity != nullptr);

        metrics.update();

        // cpu
        auto cpu_entity = registry.get_entity("cpu", {{"device", "cpu"}});
        EXPECT_TRUE(cpu_entity != nullptr);
        EXPECT_TRUE("cpu" == cpu_entity->name());
        Metric* cpu_user = cpu_entity->get_metric("cpu_user", "cpu");
        EXPECT_TRUE(cpu_user != nullptr);
        EXPECT_STREQ("57199151", cpu_user->to_string().c_str());
        Metric* cpu_nice = cpu_entity->get_metric("cpu_nice", "cpu");
        EXPECT_TRUE(cpu_nice != nullptr);
        EXPECT_STREQ("2616310", cpu_nice->to_string().c_str());
        Metric* cpu_system = cpu_entity->get_metric("cpu_system", "cpu");
        EXPECT_TRUE(cpu_system != nullptr);
        EXPECT_STREQ("10600935", cpu_system->to_string().c_str());
        Metric* cpu_idle = cpu_entity->get_metric("cpu_idle", "cpu");
        EXPECT_TRUE(cpu_idle != nullptr);
        EXPECT_STREQ("1517505423", cpu_idle->to_string().c_str());
        Metric* cpu_iowait = cpu_entity->get_metric("cpu_iowait", "cpu");
        EXPECT_TRUE(cpu_iowait != nullptr);
        EXPECT_STREQ("2137148", cpu_iowait->to_string().c_str());
        Metric* cpu_irq = cpu_entity->get_metric("cpu_irq", "cpu");
        EXPECT_TRUE(cpu_irq != nullptr);
        EXPECT_STREQ("0", cpu_irq->to_string().c_str());
        Metric* cpu_softirq = cpu_entity->get_metric("cpu_soft_irq", "cpu");
        EXPECT_TRUE(cpu_softirq != nullptr);
        EXPECT_STREQ("108277", cpu_softirq->to_string().c_str());
        Metric* cpu_steal = cpu_entity->get_metric("cpu_steal", "cpu");
        EXPECT_TRUE(cpu_steal != nullptr);
        EXPECT_STREQ("0", cpu_steal->to_string().c_str());
        Metric* cpu_guest = cpu_entity->get_metric("cpu_guest", "cpu");
        EXPECT_TRUE(cpu_guest != nullptr);
        EXPECT_STREQ("0", cpu_guest->to_string().c_str());
        Metric* cpu_guest_nice = cpu_entity->get_metric("cpu_guest_nice", "cpu");
        EXPECT_TRUE(cpu_guest_nice != nullptr);
        EXPECT_STREQ("0", cpu_guest_nice->to_string().c_str());

        // memroy
        Metric* memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        EXPECT_TRUE(memory_allocated_bytes != nullptr);
        Metric* memory_pgpgin = entity->get_metric("memory_pgpgin");
        EXPECT_TRUE(memory_pgpgin != nullptr);
        EXPECT_STREQ("21458611100", memory_pgpgin->to_string().c_str());
        Metric* memory_pgpgout = entity->get_metric("memory_pgpgout");
        EXPECT_TRUE(memory_pgpgout != nullptr);
        EXPECT_STREQ("149080494692", memory_pgpgout->to_string().c_str());
        Metric* memory_pswpin = entity->get_metric("memory_pswpin");
        EXPECT_TRUE(memory_pswpin != nullptr);
        EXPECT_STREQ("167785", memory_pswpin->to_string().c_str());
        Metric* memory_pswpout = entity->get_metric("memory_pswpout");
        EXPECT_TRUE(memory_pswpout != nullptr);
        EXPECT_STREQ("203724", memory_pswpout->to_string().c_str());

        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        EXPECT_TRUE(net_entity != nullptr);

        Metric* receive_bytes = net_entity->get_metric("network_receive_bytes");
        EXPECT_TRUE(receive_bytes != nullptr);
        EXPECT_STREQ("52567436039", receive_bytes->to_string().c_str());
        Metric* receive_packets = net_entity->get_metric("network_receive_packets");
        EXPECT_TRUE(receive_packets != nullptr);
        EXPECT_STREQ("65066152", receive_packets->to_string().c_str());
        Metric* send_bytes = net_entity->get_metric("network_send_bytes");
        EXPECT_TRUE(send_bytes != nullptr);
        EXPECT_STREQ("45480856156", send_bytes->to_string().c_str());
        Metric* send_packets = net_entity->get_metric("network_send_packets");
        EXPECT_TRUE(send_packets != nullptr);
        EXPECT_STREQ("88277614", send_packets->to_string().c_str());

        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        EXPECT_TRUE(disk_entity != nullptr);
        Metric* bytes_read = disk_entity->get_metric("disk_bytes_read");
        EXPECT_TRUE(bytes_read != nullptr);
        EXPECT_STREQ("20142745600", bytes_read->to_string().c_str());
        Metric* reads_completed = disk_entity->get_metric("disk_reads_completed");
        EXPECT_TRUE(reads_completed != nullptr);
        EXPECT_STREQ("759548", reads_completed->to_string().c_str());
        Metric* read_time_ms = disk_entity->get_metric("disk_read_time_ms");
        EXPECT_TRUE(read_time_ms != nullptr);
        EXPECT_STREQ("4308146", read_time_ms->to_string().c_str());

        Metric* bytes_written = disk_entity->get_metric("disk_bytes_written");
        EXPECT_TRUE(bytes_written != nullptr);
        EXPECT_STREQ("1624753500160", bytes_written->to_string().c_str());
        Metric* writes_completed = disk_entity->get_metric("disk_writes_completed");
        EXPECT_TRUE(writes_completed != nullptr);
        EXPECT_STREQ("18282936", writes_completed->to_string().c_str());
        Metric* write_time_ms = disk_entity->get_metric("disk_write_time_ms");
        EXPECT_TRUE(write_time_ms != nullptr);
        EXPECT_STREQ("1907755230", write_time_ms->to_string().c_str());
        Metric* io_time_ms = disk_entity->get_metric("disk_io_time_ms");
        EXPECT_TRUE(io_time_ms != nullptr);
        EXPECT_STREQ("19003350", io_time_ms->to_string().c_str());
        Metric* io_time_weigthed = disk_entity->get_metric("disk_io_time_weigthed");
        EXPECT_TRUE(write_time_ms != nullptr);
        EXPECT_STREQ("1912122964", io_time_weigthed->to_string().c_str());

        // fd
        Metric* fd_metric = entity->get_metric("fd_num_limit");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("13052138", fd_metric->to_string().c_str());
        fd_metric = entity->get_metric("fd_num_used");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("19520", fd_metric->to_string().c_str());

        // net snmp
        Metric* tcp_retrans_segs = entity->get_metric("snmp_tcp_retrans_segs");
        EXPECT_TRUE(tcp_retrans_segs != nullptr);
        Metric* tcp_in_errs = entity->get_metric("snmp_tcp_in_errs");
        EXPECT_TRUE(tcp_in_errs != nullptr);
        EXPECT_STREQ("826271", tcp_retrans_segs->to_string().c_str());
        EXPECT_STREQ("12712", tcp_in_errs->to_string().c_str());

        // load average
        Metric* load_average_1_minutes =
                entity->get_metric("load_average_1_minutes", "load_average");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("1.090000", load_average_1_minutes->to_string().c_str());
        Metric* load_average_5_minutes =
                entity->get_metric("load_average_5_minutes", "load_average");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("1.400000", load_average_5_minutes->to_string().c_str());
        Metric* load_average_15_minutes =
                entity->get_metric("load_average_15_minutes", "load_average");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("2.020000", load_average_15_minutes->to_string().c_str());

        // proc
        Metric* proc_interrupt = entity->get_metric("proc_interrupt", "proc");
        EXPECT_TRUE(proc_interrupt != nullptr);
        EXPECT_STREQ("20935913098", proc_interrupt->to_string().c_str());
        Metric* proc_ctxt_switch = entity->get_metric("proc_ctxt_switch", "proc");
        EXPECT_TRUE(proc_ctxt_switch != nullptr);
        EXPECT_STREQ("11043516832", proc_ctxt_switch->to_string().c_str());
        Metric* proc_procs_running = entity->get_metric("proc_procs_running", "proc");
        EXPECT_TRUE(proc_procs_running != nullptr);
        EXPECT_STREQ("1", proc_procs_running->to_string().c_str());
        Metric* proc_procs_blocked = entity->get_metric("proc_procs_blocked", "proc");
        EXPECT_TRUE(proc_procs_blocked != nullptr);
        EXPECT_STREQ("0", proc_procs_blocked->to_string().c_str());
    }
}

TEST_F(SystemMetricsTest, no_proc_file) {
    std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/no_stat_normal";
    LOG(INFO) << stat_path;
    k_ut_stat_path = stat_path.c_str();
    std::string diskstats_path(dir_path);
    diskstats_path += "/util/test_data/no_diskstats_normal";
    k_ut_diskstats_path = diskstats_path.c_str();
    std::string net_dev_path(dir_path);
    net_dev_path += "/util/test_data/no_net_dev_normal";
    k_ut_net_dev_path = net_dev_path.c_str();
    k_ut_fd_path = "";
    k_ut_net_snmp_path = "";
    std::string vmstat_path(dir_path);
    vmstat_path += "/util/test_data/no_vmstat_normal";
    k_ut_vmstat_path = vmstat_path.c_str();

    MetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemMetrics metrics(&registry, disk_devices, network_interfaces);

        auto entity = registry.get_entity("server");
        EXPECT_TRUE(entity != nullptr);

        // cpu
        auto cpu_entity = registry.get_entity("cpu", {{"device", "cpu"}});
        EXPECT_TRUE(cpu_entity == nullptr);

        // memroy
        Metric* memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        EXPECT_TRUE(memory_allocated_bytes != nullptr);
        Metric* memory_pgpgin = entity->get_metric("memory_pgpgin");
        EXPECT_TRUE(memory_pgpgin != nullptr);
        EXPECT_STREQ("0", memory_pgpgin->to_string().c_str());
        Metric* memory_pgpgout = entity->get_metric("memory_pgpgout");
        EXPECT_TRUE(memory_pgpgout != nullptr);
        EXPECT_STREQ("0", memory_pgpgout->to_string().c_str());
        Metric* memory_pswpin = entity->get_metric("memory_pswpin");
        EXPECT_TRUE(memory_pswpin != nullptr);
        EXPECT_STREQ("0", memory_pswpin->to_string().c_str());
        Metric* memory_pswpout = entity->get_metric("memory_pswpout");
        EXPECT_TRUE(memory_pswpout != nullptr);
        EXPECT_STREQ("0", memory_pswpout->to_string().c_str());

        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        EXPECT_TRUE(net_entity != nullptr);
        Metric* receive_bytes = net_entity->get_metric("network_receive_bytes");
        EXPECT_TRUE(receive_bytes != nullptr);
        EXPECT_STREQ("0", receive_bytes->to_string().c_str());

        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        EXPECT_TRUE(disk_entity != nullptr);
        Metric* bytes_read = disk_entity->get_metric("disk_bytes_read");
        EXPECT_TRUE(bytes_read != nullptr);
        EXPECT_STREQ("0", bytes_read->to_string().c_str());

        // proc
        Metric* proc_interrupt = entity->get_metric("proc_interrupt", "proc");
        EXPECT_TRUE(proc_interrupt != nullptr);
        EXPECT_STREQ("0", proc_interrupt->to_string().c_str());
        Metric* proc_ctxt_switch = entity->get_metric("proc_ctxt_switch", "proc");
        EXPECT_TRUE(proc_ctxt_switch != nullptr);
        EXPECT_STREQ("0", proc_ctxt_switch->to_string().c_str());
        Metric* proc_procs_running = entity->get_metric("proc_procs_running", "proc");
        EXPECT_TRUE(proc_procs_running != nullptr);
        EXPECT_STREQ("0", proc_procs_running->to_string().c_str());
        Metric* proc_procs_blocked = entity->get_metric("proc_procs_blocked", "proc");
        EXPECT_TRUE(proc_procs_blocked != nullptr);
        EXPECT_STREQ("0", proc_procs_blocked->to_string().c_str());
    }
}

} // namespace doris
