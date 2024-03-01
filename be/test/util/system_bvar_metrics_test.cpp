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

#include "util/system_bvar_metrics.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/bvar_metrics.h"

namespace doris {

class SystemBvarMetricsTest : public testing::Test {
public:
    SystemBvarMetricsTest() = default;
    virtual ~SystemBvarMetricsTest() = default;
};

extern const char* bvar_k_utstat_path;
extern const char* bvar_k_utdiskstats_path;
extern const char* bvar_k_utnet_dev_path;
extern const char* bvar_k_utfd_path;
extern const char* bvar_k_utnet_snmp_path;
extern const char* bvar_k_utload_avg_path;
extern const char* bvar_k_utvmstat_path;

TEST_F(SystemBvarMetricsTest, normal) {
        std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/stat_normal";
    LOG(INFO) << stat_path;
    bvar_k_utstat_path = stat_path.c_str();
    std::string diskstats_path(dir_path);
    diskstats_path += "/util/test_data/diskstats_normal";
    bvar_k_utdiskstats_path = diskstats_path.c_str();
    std::string net_dev_path(dir_path);
    net_dev_path += "/util/test_data/net_dev_normal";
    bvar_k_utnet_dev_path = net_dev_path.c_str();
    std::string fd_path(dir_path);
    fd_path += "/util/test_data/fd_file_nr";
    bvar_k_utfd_path = fd_path.c_str();
    std::string net_snmp_path(dir_path);
    net_snmp_path += "/util/test_data/net_snmp_normal";
    bvar_k_utnet_snmp_path = net_snmp_path.c_str();
    std::string load_avg_path(dir_path);
    load_avg_path += "/util/test_data/load_avg_normal";
    bvar_k_utload_avg_path = load_avg_path.c_str();
    std::string vmstat_path(dir_path);
    vmstat_path += "/util/test_data/vmstat_normal";
    bvar_k_utvmstat_path = vmstat_path.c_str();

    BvarMetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemBvarMetrics metrics(&registry, disk_devices, network_interfaces);
        auto entity = registry.get_entity("server");
        EXPECT_TRUE(entity != nullptr);

        metrics.update();

        // cpu
        auto cpu_entity = registry.get_entity("cpu", {{"device", "cpu"}});
        EXPECT_TRUE(cpu_entity != nullptr);
        EXPECT_TRUE("cpu" == cpu_entity->name());
        std::shared_ptr<BvarMetric> cpu_user = cpu_entity->get_metric("cpu_user");
        EXPECT_TRUE(cpu_user != nullptr);
        EXPECT_STREQ("57199151", cpu_user->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_nice = cpu_entity->get_metric("cpu_nice");
        EXPECT_TRUE(cpu_nice != nullptr);
        EXPECT_STREQ("2616310", cpu_nice->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_system = cpu_entity->get_metric("cpu_system");
        EXPECT_TRUE(cpu_system != nullptr);
        EXPECT_STREQ("10600935", cpu_system->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_idle = cpu_entity->get_metric("cpu_idle");
        EXPECT_TRUE(cpu_idle != nullptr);
        EXPECT_STREQ("1517505423", cpu_idle->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_iowait = cpu_entity->get_metric("cpu_iowait");
        EXPECT_TRUE(cpu_iowait != nullptr);
        EXPECT_STREQ("2137148", cpu_iowait->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_irq = cpu_entity->get_metric("cpu_irq");
        EXPECT_TRUE(cpu_irq != nullptr);
        EXPECT_STREQ("0", cpu_irq->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_softirq = cpu_entity->get_metric("cpu_soft_irq");
        EXPECT_TRUE(cpu_softirq != nullptr);
        EXPECT_STREQ("108277", cpu_softirq->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_steal = cpu_entity->get_metric("cpu_steal");
        EXPECT_TRUE(cpu_steal != nullptr);
        EXPECT_STREQ("0", cpu_steal->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_guest = cpu_entity->get_metric("cpu_guest");
        EXPECT_TRUE(cpu_guest != nullptr);
        EXPECT_STREQ("0", cpu_guest->value_string().c_str());
        std::shared_ptr<BvarMetric> cpu_guest_nice = cpu_entity->get_metric("cpu_guest_nice");
        EXPECT_TRUE(cpu_guest_nice != nullptr);
        EXPECT_STREQ("0", cpu_guest_nice->value_string().c_str());

        // memory
        std::shared_ptr<BvarMetric> memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        EXPECT_TRUE(memory_allocated_bytes != nullptr);
        std::shared_ptr<BvarMetric> memory_pgpgin = entity->get_metric("memory_pgpgin");
        EXPECT_TRUE(memory_pgpgin != nullptr);
        EXPECT_STREQ("21458611100", memory_pgpgin->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pgpgout = entity->get_metric("memory_pgpgout");
        EXPECT_TRUE(memory_pgpgout != nullptr);
        EXPECT_STREQ("149080494692", memory_pgpgout->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pswpin = entity->get_metric("memory_pswpin");
        EXPECT_TRUE(memory_pswpin != nullptr);
        EXPECT_STREQ("167785", memory_pswpin->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pswpout = entity->get_metric("memory_pswpout");
        EXPECT_TRUE(memory_pswpout != nullptr);
        EXPECT_STREQ("203724", memory_pswpout->value_string().c_str());

        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        EXPECT_TRUE(net_entity != nullptr);

        std::shared_ptr<BvarMetric> receive_bytes = net_entity->get_metric("network_receive_bytes");
        EXPECT_TRUE(receive_bytes != nullptr);
        EXPECT_STREQ("52567436039", receive_bytes->value_string().c_str());
        std::shared_ptr<BvarMetric> receive_packets = net_entity->get_metric("network_receive_packets");
        EXPECT_TRUE(receive_packets != nullptr);
        EXPECT_STREQ("65066152", receive_packets->value_string().c_str());
        std::shared_ptr<BvarMetric> send_bytes = net_entity->get_metric("network_send_bytes");
        EXPECT_TRUE(send_bytes != nullptr);
        EXPECT_STREQ("45480856156", send_bytes->value_string().c_str());
        std::shared_ptr<BvarMetric> send_packets = net_entity->get_metric("network_send_packets");
        EXPECT_TRUE(send_packets != nullptr);
        EXPECT_STREQ("88277614", send_packets->value_string().c_str());

        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        EXPECT_TRUE(disk_entity != nullptr);
        std::shared_ptr<BvarMetric> bytes_read = disk_entity->get_metric("disk_bytes_read");
        EXPECT_TRUE(bytes_read != nullptr);
        EXPECT_STREQ("20142745600", bytes_read->value_string().c_str());
        std::shared_ptr<BvarMetric> reads_completed = disk_entity->get_metric("disk_reads_completed");
        EXPECT_TRUE(reads_completed != nullptr);
        EXPECT_STREQ("759548", reads_completed->value_string().c_str());
        std::shared_ptr<BvarMetric> read_time_ms = disk_entity->get_metric("disk_read_time_ms");
        EXPECT_TRUE(read_time_ms != nullptr);
        EXPECT_STREQ("4308146", read_time_ms->value_string().c_str());

        std::shared_ptr<BvarMetric> bytes_written = disk_entity->get_metric("disk_bytes_written");
        EXPECT_TRUE(bytes_written != nullptr);
        EXPECT_STREQ("1624753500160", bytes_written->value_string().c_str());
        std::shared_ptr<BvarMetric> writes_completed = disk_entity->get_metric("disk_writes_completed");
        EXPECT_TRUE(writes_completed != nullptr);
        EXPECT_STREQ("18282936", writes_completed->value_string().c_str());
        std::shared_ptr<BvarMetric> write_time_ms = disk_entity->get_metric("disk_write_time_ms");
        EXPECT_TRUE(write_time_ms != nullptr);
        EXPECT_STREQ("1907755230", write_time_ms->value_string().c_str());
        std::shared_ptr<BvarMetric> io_time_ms = disk_entity->get_metric("disk_io_time_ms");
        EXPECT_TRUE(io_time_ms != nullptr);
        EXPECT_STREQ("19003350", io_time_ms->value_string().c_str());
        std::shared_ptr<BvarMetric> io_time_weigthed = disk_entity->get_metric("disk_io_time_weigthed");
        EXPECT_TRUE(write_time_ms != nullptr);
        EXPECT_STREQ("1912122964", io_time_weigthed->value_string().c_str());

        // fd
        std::shared_ptr<BvarMetric> fd_metric = entity->get_metric("fd_num_limit");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("13052138", fd_metric->value_string().c_str());
        fd_metric = entity->get_metric("fd_num_used");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("19520", fd_metric->value_string().c_str());

        // net snmp
        std::shared_ptr<BvarMetric> tcp_retrans_segs = entity->get_metric("snmp_tcp_retrans_segs");
        EXPECT_TRUE(tcp_retrans_segs != nullptr);
        std::shared_ptr<BvarMetric> tcp_in_errs = entity->get_metric("snmp_tcp_in_errs");
        EXPECT_TRUE(tcp_in_errs != nullptr);
        EXPECT_STREQ("826271", tcp_retrans_segs->value_string().c_str());
        EXPECT_STREQ("12712", tcp_in_errs->value_string().c_str());

        // load average
        std::shared_ptr<BvarMetric> load_average_1_minutes =
                entity->get_metric("load_average_1_minutes");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("1.090000", load_average_1_minutes->value_string().c_str());
        std::shared_ptr<BvarMetric> load_average_5_minutes =
                entity->get_metric("load_average_5_minutes");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("1.400000", load_average_5_minutes->value_string().c_str());
        std::shared_ptr<BvarMetric> load_average_15_minutes =
                entity->get_metric("load_average_15_minutes");
        EXPECT_TRUE(fd_metric != nullptr);
        EXPECT_STREQ("2.020000", load_average_15_minutes->value_string().c_str());

        // proc
        std::shared_ptr<BvarMetric> proc_interrupt = entity->get_metric("proc_interrupt");
        EXPECT_TRUE(proc_interrupt != nullptr);
        EXPECT_STREQ("20935913098", proc_interrupt->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_ctxt_switch = entity->get_metric("proc_ctxt_switch");
        EXPECT_TRUE(proc_ctxt_switch != nullptr);
        EXPECT_STREQ("11043516832", proc_ctxt_switch->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_procs_running = entity->get_metric("proc_procs_running");
        EXPECT_TRUE(proc_procs_running != nullptr);
        EXPECT_STREQ("1", proc_procs_running->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_procs_blocked = entity->get_metric("proc_procs_blocked");
        EXPECT_TRUE(proc_procs_blocked != nullptr);
        EXPECT_STREQ("0", proc_procs_blocked->value_string().c_str());
    }
}

TEST_F(SystemBvarMetricsTest, no_proc_file) {
    std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/no_stat_normal";
    LOG(INFO) << stat_path;
    bvar_k_utstat_path = stat_path.c_str();
    std::string diskstats_path(dir_path);
    diskstats_path += "/util/test_data/no_diskstats_normal";
    bvar_k_utdiskstats_path = diskstats_path.c_str();
    std::string net_dev_path(dir_path);
    net_dev_path += "/util/test_data/no_net_dev_normal";
    bvar_k_utnet_dev_path = net_dev_path.c_str();
    bvar_k_utfd_path = "";
    bvar_k_utnet_snmp_path = "";
    std::string vmstat_path(dir_path);
    vmstat_path += "/util/test_data/no_vmstat_normal";
    bvar_k_utvmstat_path = vmstat_path.c_str();

    BvarMetricRegistry registry("test");
    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemBvarMetrics metrics(&registry, disk_devices, network_interfaces);

        auto entity = registry.get_entity("server");
        EXPECT_TRUE(entity != nullptr);

        // cpu
        auto cpu_entity = registry.get_entity("cpu", {{"device", "cpu"}});
        EXPECT_TRUE(cpu_entity == nullptr);

        // memory
        std::shared_ptr<BvarMetric> memory_allocated_bytes = entity->get_metric("memory_allocated_bytes");
        EXPECT_TRUE(memory_allocated_bytes != nullptr);
        std::shared_ptr<BvarMetric> memory_pgpgin = entity->get_metric("memory_pgpgin");
        EXPECT_TRUE(memory_pgpgin != nullptr);
        EXPECT_STREQ("0", memory_pgpgin->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pgpgout = entity->get_metric("memory_pgpgout");
        EXPECT_TRUE(memory_pgpgout != nullptr);
        EXPECT_STREQ("0", memory_pgpgout->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pswpin = entity->get_metric("memory_pswpin");
        EXPECT_TRUE(memory_pswpin != nullptr);
        EXPECT_STREQ("0", memory_pswpin->value_string().c_str());
        std::shared_ptr<BvarMetric> memory_pswpout = entity->get_metric("memory_pswpout");
        EXPECT_TRUE(memory_pswpout != nullptr);
        EXPECT_STREQ("0", memory_pswpout->value_string().c_str());

        // network
        auto net_entity = registry.get_entity("network_metrics.xgbe0", {{"device", "xgbe0"}});
        EXPECT_TRUE(net_entity != nullptr);
        std::shared_ptr<BvarMetric> receive_bytes = net_entity->get_metric("network_receive_bytes");
        EXPECT_TRUE(receive_bytes != nullptr);
        EXPECT_STREQ("0", receive_bytes->value_string().c_str());

        // disk
        auto disk_entity = registry.get_entity("disk_metrics.sda", {{"device", "sda"}});
        EXPECT_TRUE(disk_entity != nullptr);
        std::shared_ptr<BvarMetric> bytes_read = disk_entity->get_metric("disk_bytes_read");
        EXPECT_TRUE(bytes_read != nullptr);
        EXPECT_STREQ("0", bytes_read->value_string().c_str());

        // proc
        std::shared_ptr<BvarMetric> proc_interrupt = entity->get_metric("proc_interrupt");
        EXPECT_TRUE(proc_interrupt != nullptr);
        EXPECT_STREQ("0", proc_interrupt->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_ctxt_switch = entity->get_metric("proc_ctxt_switch");
        EXPECT_TRUE(proc_ctxt_switch != nullptr);
        EXPECT_STREQ("0", proc_ctxt_switch->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_procs_running = entity->get_metric("proc_procs_running");
        EXPECT_TRUE(proc_procs_running != nullptr);
        EXPECT_STREQ("0", proc_procs_running->value_string().c_str());
        std::shared_ptr<BvarMetric> proc_procs_blocked = entity->get_metric("proc_procs_blocked");
        EXPECT_TRUE(proc_procs_blocked != nullptr);
        EXPECT_STREQ("0", proc_procs_blocked->value_string().c_str());
    }
}

}