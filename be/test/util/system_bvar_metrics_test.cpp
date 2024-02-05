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

#include <set>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"
#include "util/bvar_metrics.h"

namespace doris {

class SyetenBvarMetricsTest : public testing::Test {
public:
    SyetenBvarMetricsTest() {}
    virtual ~SyetenBvarMetricsTest() {}
};

extern const char* k_ut_stat_path;
extern const char* k_ut_diskstats_path;
extern const char* k_ut_net_dev_path;
extern const char* k_ut_fd_path;
extern const char* k_ut_net_snmp_path;
extern const char* k_ut_load_avg_path;
extern const char* k_ut_vmstat_path;

TEST_F(SyetenBvarMetricsTest, normal) {
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

    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemBvarMetrics system_metrics(disk_devices, network_interfaces);

        system_metrics.update();

        // cpu
        CpuBvarMetrics* cpu_metrics = system_metrics.cpu_metrics("cpu");
        // cpu_user
        EXPECT_STREQ("57199151", cpu_metrics->metrics[0]->value_string().c_str());
        // cpu_nice
        EXPECT_STREQ("2616310", cpu_metrics->metrics[1]->value_string().c_str());
        // cpu_system
        EXPECT_STREQ("10600935", cpu_metrics->metrics[2]->value_string().c_str());
        // cpu_idle
        EXPECT_STREQ("1517505423", cpu_metrics->metrics[3]->value_string().c_str());
        // cpu_iowait
        EXPECT_STREQ("2137148", cpu_metrics->metrics[4]->value_string().c_str());
        // cpu_irq
        EXPECT_STREQ("0", cpu_metrics->metrics[5]->value_string().c_str());
        // cpu_softirq
        EXPECT_STREQ("108277", cpu_metrics->metrics[6]->value_string().c_str());
        // cpu_steal
        EXPECT_STREQ("0", cpu_metrics->metrics[7]->value_string().c_str());
        // cpu_guest
        EXPECT_STREQ("0", cpu_metrics->metrics[8]->value_string().c_str());
        // cpu_guest_nice
        EXPECT_STREQ("0", cpu_metrics->metrics[9]->value_string().c_str());

        // memory
        MemoryBvarMetrics* memory_metrics = system_metrics.memory_metrics();
        EXPECT_STREQ("21458611100", memory_metrics->memory_pgpgin->value_string().c_str());
        EXPECT_STREQ("149080494692", memory_metrics->memory_pgpgout->value_string().c_str());
        EXPECT_STREQ("167785", memory_metrics->memory_pswpin->value_string().c_str());
        EXPECT_STREQ("203724", memory_metrics->memory_pswpout->value_string().c_str());

        // network
        NetworkBvarMetrics* network_metrics = system_metrics.network_metrics("xgbe0");
        EXPECT_STREQ("52567436039", network_metrics->receive_bytes->value_string().c_str());
        EXPECT_STREQ("65066152", network_metrics->receive_packets->value_string().c_str());
        EXPECT_STREQ("45480856156", network_metrics->send_bytes->value_string().c_str());
        EXPECT_STREQ("88277614", network_metrics->send_packets->value_string().c_str());

        // disk
        DiskBvarMetrics* disk_metrics = system_metrics.disk_metrics("sda");
        EXPECT_STREQ("20142745600", disk_metrics->bytes_read->value_string().c_str());
        EXPECT_STREQ("759548", disk_metrics->reads_completed->value_string().c_str());
        EXPECT_STREQ("4308146", disk_metrics->read_time_ms->value_string().c_str());
        EXPECT_STREQ("1624753500160", disk_metrics->bytes_written->value_string().c_str());
        EXPECT_STREQ("18282936", disk_metrics->writes_completed->value_string().c_str());
        EXPECT_STREQ("1907755230", disk_metrics->write_time_ms->value_string().c_str());
        EXPECT_STREQ("19003350", disk_metrics->io_time_ms->value_string().c_str());
        EXPECT_STREQ("1912122964", disk_metrics->io_time_weigthed->value_string().c_str());

        // fd
        FileDescriptorBvarMetrics* fd_metrics = system_metrics.fd_metrics();
        EXPECT_STREQ("13052138", fd_metrics->fd_num_limit->value_string().c_str());
        EXPECT_STREQ("19520", fd_metric->fd_num_used->value_string().c_str());

        // net snmp
        SnmpBvarMetrics* snmp_metrics = system_metrics.snmp_metrics();
        EXPECT_STREQ("826271", snmp_metrics->snmp_tcp_retrans_segs->value_string().c_str());
        EXPECT_STREQ("12712", snmp_metrics->snmp_tcp_in_errs->value_string().c_str());

        // load average
        LoadAverageBvarMetrics* load_average_metrics = system_metrics.load_average_metrics();
        EXPECT_STREQ("1.090000",
                     load_average_metrics->load_average_1_minutes->value_string().c_str());
        EXPECT_STREQ("1.400000",
                     load_average_metrics->load_average_5_minutes->value_string().c_str());
        EXPECT_STREQ("2.020000",
                     load_average_metrics->load_average_15_minutes->value_string().c_str());

        // proc
        ProcBvarMetrics* proc_metrics = system_metrics.proc_metrics();
        EXPECT_STREQ("20935913098", proc_metrics->proc_interrupt->to_string().c_str());
        EXPECT_STREQ("11043516832", proc_metrics->proc_ctxt_switch->to_string().c_str());
        EXPECT_STREQ("1", proc_metrics->proc_procs_running->to_string().c_str());
        EXPECT_STREQ("0", proc_metrics->proc_procs_blocked->to_string().c_str());
    }
}

TEST_F(SyetenBvarMetricsTest, no_proc_file) {
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

    {
        std::set<std::string> disk_devices;
        disk_devices.emplace("sda");
        std::vector<std::string> network_interfaces;
        network_interfaces.emplace_back("xgbe0");
        SystemBvarMetrics system_metrics(disk_devices, network_interfaces);

        system_metrics.update();

        // cpu
        // none

        // memory
        MemoryBvarMetrics* memory_metrics = system_metrics.memory_metrics();
        EXPECT_STREQ("0", memory_metrics->memory_pgpgin->value_string().c_str());
        EXPECT_STREQ("0", memory_metrics->memory_pgpgout->value_string().c_str());
        EXPECT_STREQ("0", memory_metrics->memory_pswpin->value_string().c_str());
        EXPECT_STREQ("0", memory_metrics->memory_pswpout->value_string().c_str());

        // network
        NetworkBvarMetrics* network_metrics = system_metrics.network_metrics("xgbe0");
        EXPECT_STREQ("0", network_metrics->receive_bytes->value_string().c_str());

        // disk
        DiskBvarMetrics* disk_metrics = system_metrics.disk_metrics("sda");
        EXPECT_STREQ("0", disk_metrics->bytes_read->value_string().c_str());

        // fd
        // none

        // net snmp
        // none

        // load average
        // none

        // proc
        ProcBvarMetrics* proc_metrics = system_metrics.proc_metrics();
        EXPECT_STREQ("0", proc_metrics->proc_interrupt->to_string().c_str());
        EXPECT_STREQ("0", proc_metrics->proc_ctxt_switch->to_string().c_str());
        EXPECT_STREQ("0", proc_metrics->proc_procs_running->to_string().c_str());
        EXPECT_STREQ("0", proc_metrics->proc_procs_blocked->to_string().c_str());
    }
}

} // namespace doris
