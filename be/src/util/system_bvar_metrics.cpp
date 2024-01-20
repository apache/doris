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

#include <ctype.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <functional>
#include <ostream>
#include <unordered_map>
#include <utility>

#include "gutil/strings/split.h" // for string split
#include "gutil/strtoint.h"      //  for atoi64
#include "util/mem_info.h"
#include "util/perf_counters.h"

namespace doris {

#define INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, metric)                                  \
    BvarAdderMetric<int64_t> metric(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, #metric, \
                            "", "cpu", Labels({{"mode", #metric}}), false);                          \
    entity->register_metric(#metric, metric)                                                         \

// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
struct CpuMetrics {
    CpuMetrics(BvarMetricEntity* ent) : entity(ent) {
        
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_user); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_nice); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_system); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_idle);  
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_iowait); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_irq); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_soft_irq); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_steal);     
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_guest); 
        INT64_CPU_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, cpu_guest_nice);     
        metrics[0] = &cpu_user;
        metrics[1] = &cpu_nice;
        metrics[2] = &cpu_system;
        metrics[3] = &cpu_idle;
        metrics[4] = &cpu_iowait;
        metrics[5] = &cpu_irq;
        metrics[6] = &cpu_soft_irq;
        metrics[7] = &cpu_steal;
        metrics[8] = &cpu_guest;
        metrics[9] = &cpu_guest_nice;
    }

    static constexpr int cpu_num_metrics = 10;

    BvarMetricEntity* entity = nullptr;
    BvarAdderMetric<int64_t>* cpu_user = nullptr;
    BvarAdderMetric<int64_t>* cpu_nice = nullptr;
    BvarAdderMetric<int64_t>* cpu_system = nullptr;
    BvarAdderMetric<int64_t>* cpu_idle = nullptr;
    BvarAdderMetric<int64_t>* cpu_iowait = nullptr;
    BvarAdderMetric<int64_t>* cpu_irq = nullptr;
    BvarAdderMetric<int64_t>* cpu_soft_irq = nullptr;
    BvarAdderMetric<int64_t>* cpu_steal = nullptr;
    BvarAdderMetric<int64_t>* cpu_guest = nullptr;
    BvarAdderMetric<int64_t>* cpu_guest_nice = nullptr; 

    BvarAdderMetric<int64_t>* metrics[cpu_num_metrics];
};

// -------------------------------------------------------------
// MemoryMetrics， Gauge is not suppoted
// -------------------------------------------------------------

#define INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, metric, unit)        \
    metric = new BvarAdderMetric<int64_t>(BvarMetricType::COUNTER, unit, #metric, \
                            "", "", Labels(), false);                             \
    entity->register_metric(#metric, metric)                                      \

struct DiskMetrics {
    DiskMetrics(BvarMetricEntity* ent) : entity(ent) {
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_reads_completed, BvarMetricUnit::OPERATIONS);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_bytes_read, BvarMetricUnit::BYTES);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_read_time_ms, BvarMetricUnit::MILLISECONDS);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_writes_completed, BvarMetricUnit::OPERATIONS);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_bytes_written, BvarMetricUnit::BYTES);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_write_time_ms, BvarMetricUnit::MILLISECONDS);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_io_time_ms, BvarMetricUnit::MILLISECONDS);
        INT64_DISK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, disk_io_time_weigthed, BvarMetricUnit::MILLISECONDS);
    }

    BvarMetricEntity* entity = nullptr;
    BvarAdderMetric<int64_t>* disk_reads_completed;
    BvarAdderMetric<int64_t>* disk_bytes_read;
    BvarAdderMetric<int64_t>* disk_read_time_ms;
    BvarAdderMetric<int64_t>* disk_writes_completed;
    BvarAdderMetric<int64_t>* disk_bytes_written;
    BvarAdderMetric<int64_t>* disk_write_time_ms;
    BvarAdderMetric<int64_t>* disk_io_time_ms;
    BvarAdderMetric<int64_t>* disk_io_time_weigthed;
};


#define INT64_NETWORK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, metric, unit)     \
    metric = new BvarAdderMetric<int64_t>(BvarMetricType::COUNTER, unit, #metric, \
                            "", "", Labels(), false);                             \
    entity->register_metric(#metric, metric)                                      \

struct NetworkMetrics {
    NetworkMetrics(BvarMetricEntity* ent) : entity(ent) {
        INT64_NETWORK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, network_receive_bytes, BvarMetricUnit::BYTES);
        INT64_NETWORK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, network_receive_packets, BvarMetricUnit::PACKETS);
        INT64_NETWORK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, network_send_bytes, BvarMetricUnit::BYTES);
        INT64_NETWORK_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, network_send_packets, BvarMetricUnit::PACKETS);
    }

    BvarMetricEntity* entity = nullptr;
    BvarAdderMetric<int64_t>* network_receive_bytes;
    BvarAdderMetric<int64_t>* network_receive_packets;
    BvarAdderMetric<int64_t>* network_send_bytes;
    BvarAdderMetric<int64_t>* network_send_packets;
};

// ------------------------------------------------------------
// struct FileDescriptorMetrics;  Gauge Type is not supported
// ------------------------------------------------------------

#define INT64_SNMP_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, metric, unit, description) \
    metric = new BvarAdderMetric<int64_t>(BvarMetricType::COUNTER, unit, #metric,       \
                            description, "", Labels(), false);                          \
    entity->register_metric(#metric, metric)                                            \

// metrics read from /proc/net/snmp
struct SnmpMetrics {
    SnmpMetrics(BvarMetricEntity* ent) : entity(ent) {
        INT64_SNMP_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, snmp_tcp_in_errs, BvarMetricUnit::NOUNIT,
                            "The number of all problematic TCP packets received");
        INT64_SNMP_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, snmp_tcp_retrans_segs, BvarMetricUnit::NOUNIT,
                            "All TCP packets retransmitted");
        INT64_SNMP_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, snmp_tcp_in_segs, BvarMetricUnit::NOUNIT,
                            "All received TCP packets");
        INT64_SNMP_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, snmp_tcp_out_segs, BvarMetricUnit::NOUNIT,
                            "All send TCP packets with RST mark");
    }

    BvarMetricEntity* entity = nullptr;
    BvarAdderMetric<int64_t>* snmp_tcp_in_errs;
    BvarAdderMetric<int64_t>* snmp_tcp_retrans_segs;
    BvarAdderMetric<int64_t>* snmp_tcp_in_segs;
    BvarAdderMetric<int64_t>* snmp_tcp_out_segs;
};

// -------------------------------------------------------
// struct LoadAverageMetrics; Gauge Type is not supported
// --------------------------------------------------------



#define INT64_PROC_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, metric)                            \
    metric = new BvarAdderMetric<int64_t>(BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT, #metric, \
                            "", "proc", Labels({{"mode", #metric}}), false);                    \
    entity->register_metric(#metric, metric)                                                    \



struct ProcMetrics {
    ProcMetrics(BvarMetricEntity* ent) : entity(ent) {
        INT64_PROC_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, proc_interrupt);
        INT64_PROC_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, proc_ctxt_switch);
        INT64_PROC_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, proc_procs_running);
        INT64_PROC_COUNTER_BVAR_ADDR_METRIC_REGISTER(entity, proc_procs_blocked);
    }

    BvarMetricEntity* entity = nullptr;

    BvarAdderMetric<int64_t>* proc_interrupt;
    BvarAdderMetric<int64_t>* proc_ctxt_switch;
    BvarAdderMetric<int64_t>* proc_procs_running;
    BvarAdderMetric<int64_t>* proc_procs_blocked;
};

const char* SystemBvarMetrics::s_hook_name_ = "system_bvar_metric";

SystemBvarMetrics::SystemBvarMetrics(BvarMetricRegistry* registry, const std::set<std::string>& disk_devices,
                    const std::vector<std::string>& network_interfaces) {
    DCHECK(registry != nullptr);
    registry_ = registry;
    server_entity_ = registry_->register_entity("server");

    DCHECK(server_entity_ != nullptr);
    server_entity_->register_hook(s_hook_name_, std::bind(&SystemBvarMetrics::update, this));

    install_cpu_metrics();
    // install_memory_metrics(server_entity_.get());
    // install_disk_metrics(disk_devices);
    // install_net_metrics(network_interfaces);
    // install_fd_metrics(server_entity_.get());
    // install_snmp_metrics(server_entity_.get());
    // install_load_avg_metrics(server_entity_.get());
    // install_proc_metrics(server_entity_.get());
    
    // INT_GAUGE_METRIC_REGISTER(_server_entity.get(), max_disk_io_util_percent);
    // INT_GAUGE_METRIC_REGISTER(_server_entity.get(), max_network_send_bytes_rate);
    // INT_GAUGE_METRIC_REGISTER(_server_entity.get(), max_network_receive_bytes_rate);
}

SystemBvarMetrics::~SystemBvarMetrics() {
    DCHECK(server_entity_ != nullptr);
    server_entity_->deregister_hook(s_hook_name_);

    for (auto& it : cpu_metrics_) {
        delete it.second;
    }
    // for (auto& it : disk_metrics_) {
    //     delete it.second;
    // }
    // for (auto& it : network_metrics_) {
    //     delete it.second;
    // }
    if (line_ptr_ != nullptr) {
        free(line_ptr_);
    }
}

void SystemBvarMetrics::update() {
    update_cpu_metrics();
    // update_memory_metrics();
    // update_disk_metrics();
    // update_net_metrics();
    // update_fd_metrics();
    // update_snmp_metrics();
    // update_load_avg_metrics();
    // update_proc_metrics();
}

void SystemBvarMetrics::install_cpu_metrics() {
    get_cpu_name();
    for (auto cpu_name : cpu_names_) {
        auto cpu_entity = registry_->register_entity(cpu_name);
        CpuMetrics* metrics = new CpuMetrics(cpu_entity.get());
        cpu_metrics_.emplace(cpu_name, metrics);
    }
}

#ifdef BE_TEST
const char* k_ut_stat_path;
const char* k_ut_diskstats_path;
const char* k_ut_net_dev_path;
const char* k_ut_fd_path;
const char* k_ut_net_snmp_path;
const char* k_ut_load_avg_path;
const char* k_ut_vmstat_path;
#endif

void SystemBvarMetrics::update_cpu_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_stat_path, "r");
#else
    FILE* fp = fopen("/proc/stat", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/stat failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    while (getline(&line_ptr_, &line_buf_size_, fp) > 0) {
        char cpu[16];
        int64_t values[CpuMetrics::cpu_num_metrics];
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr_,
                         "%15s"
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64,
                         cpu, &values[0], &values[1], &values[2], &values[3], &values[4],
                         &values[5], &values[6], &values[7], &values[8], &values[9]);
        if (num < 4) {
            continue;
        }

        std::string cpu_name(cpu);
        auto it = cpu_metrics_.find(cpu_name);
        if (it == cpu_metrics_.end()) {
            continue;
        }

        for (int i = 0; i < CpuMetrics::cpu_num_metrics; ++i) {
            it->second->metrics[i]->set_value(values[i]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

void SystemBvarMetrics::get_cpu_name() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_stat_path, "r");
#else
    FILE* fp = fopen("/proc/stat", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/stat failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    while (getline(&line_ptr_, &line_buf_size_, fp) > 0) {
        char cpu[16];
        char* start_pos = nullptr;
        start_pos = strstr(line_ptr_, "cpu");
        if (start_pos) {
            sscanf(line_ptr_, "%15s", cpu);
            std::string cpu_name(cpu);
            cpu_names_.push_back(cpu_name);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

} // namespace doris