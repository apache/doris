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
#include <vector>

#include "gutil/strings/split.h" // for string split
#include "gutil/strtoint.h"      //  for atoi64
#include "util/mem_info.h"
#include "util/perf_counters.h"

namespace doris {

#define DECLARE_INT64_BVAR_METRIC(name, type, unit, description, group_name, labels, core) \
    auto name = std::make_shared<BvarAdderMetric<int64_t>>(type, unit, #name, description, \
                                                           group_name, labels, core);
#define INIT_INT64_BVAR_METRIC(name, type, unit, description, group_name, labels, core)           \
    name = std::make_shared<BvarAdderMetric<int64_t>>(type, unit, #name, description, group_name, \
                                                      labels, core);

#define INIT_DOUBLE_BVAR_METRIC(name, type, unit, description, group_name, labels, core)         \
    name = std::make_shared<BvarAdderMetric<double>>(type, unit, #name, description, group_name, \
                                                     labels, core);
// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
struct CpuBvarMetrics {
    CpuBvarMetrics(BvarMetricEntity* entity) {
        DECLARE_INT64_BVAR_METRIC(cpu_user, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "user"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_nice, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "nice"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_system, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "system"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_idle, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "idle"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_iowait, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "iowait"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_irq, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "irq"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_soft_irq, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                  "", "cpu", BvarMetric::Labels({{"mode", "soft_irq"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_steal, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "steal"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_guest, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", BvarMetric::Labels({{"mode", "guest"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_guest_nice, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                  "", "cpu", BvarMetric::Labels({{"mode", "guest_nice"}}), false)
        entity->register_metric("cpu_user", *cpu_user);
        entity->register_metric("cpu_nice", *cpu_nice);
        entity->register_metric("cpu_system", *cpu_system);
        entity->register_metric("cpu_idle", *cpu_idle);
        entity->register_metric("cpu_iowait", *cpu_iowait);
        entity->register_metric("cpu_irq", *cpu_irq);
        entity->register_metric("cpu_soft_irq", *cpu_soft_irq);
        entity->register_metric("cpu_steal", *cpu_steal);
        entity->register_metric("cpu_guest", *cpu_guest);
        entity->register_metric("cpu_guest_nice", *cpu_guest_nice);

        metrics.emplace_back(cpu_user);
        metrics.emplace_back(cpu_nice);
        metrics.emplace_back(cpu_system);
        metrics.emplace_back(cpu_idle);
        metrics.emplace_back(cpu_iowait);
        metrics.emplace_back(cpu_irq);
        metrics.emplace_back(cpu_soft_irq);
        metrics.emplace_back(cpu_steal);
        metrics.emplace_back(cpu_guest);
        metrics.emplace_back(cpu_guest_nice);
    }

    static constexpr int cpu_num_metrics = 10;
    std::vector<std::shared_ptr<BvarAdderMetric<int64_t>>> metrics;
};

struct MemoryBvarMetrics {
    MemoryBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(memory_allocated_bytes, BvarMetricType::GAUGE, BvarMetricUnit::BYTES,
                               "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pgpgin, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pgpgout, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pswpin, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pswpout, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", BvarMetric::Labels(), false)
        entity->register_metric("memory_allocated_bytes", *memory_allocated_bytes);
        entity->register_metric("memory_pgpgin", *memory_pgpgin);
        entity->register_metric("memory_pgpgout", *memory_pgpgout);
        entity->register_metric("memory_pswpin", *memory_pswpin);
        entity->register_metric("memory_pswpout", *memory_pswpout);
#ifndef USE_JEMALLOC
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_allocated_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_total_thread_cache_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_central_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_transfer_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_thread_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_pageheap_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_pageheap_unmapped_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        entity->register_metric("memory_tcmalloc_allocated_bytes",
                                *memory_tcmalloc_allocated_bytes);
        entity->register_metric("memory_tcmalloc_total_thread_cache_bytes",
                                *memory_tcmalloc_total_thread_cache_bytes);
        entity->register_metric("memory_tcmalloc_central_cache_free_bytes",
                                *memory_tcmalloc_central_cache_free_bytes);
        entity->register_metric("memory_tcmalloc_transfer_cache_free_bytes",
                                *memory_tcmalloc_transfer_cache_free_bytes);
        entity->register_metric("memory_tcmalloc_thread_cache_free_bytes",
                                *memory_tcmalloc_thread_cache_free_bytes);
        entity->register_metric("memory_tcmalloc_pageheap_free_bytes",
                                *memory_tcmalloc_pageheap_free_bytes);
        entity->register_metric("memory_tcmalloc_pageheap_unmapped_bytes",
                                *memory_tcmalloc_pageheap_unmapped_bytes);
#else
        INIT_INT64_BVAR_METRIC(memory_jemalloc_allocated_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_active_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_metadata_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_resident_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_mapped_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_retained_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_tcache_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pactive_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pdirty_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pmuzzy_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_dirty_purged_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_muzzy_purged_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);

        entity->register_metric("memory_jemalloc_allocated_bytes",
                                *memory_jemalloc_allocated_bytes);
        entity->register_metric("memory_jemalloc_active_bytes", *memory_jemalloc_active_bytes);
        entity->register_metric("memory_jemalloc_metadata_bytes", *memory_jemalloc_metadata_bytes);
        entity->register_metric("memory_jemalloc_resident_bytes", *memory_jemalloc_resident_bytes);
        entity->register_metric("memory_jemalloc_mapped_bytes", *memory_jemalloc_mapped_bytes);
        entity->register_metric("memory_jemalloc_retained_bytes", *memory_jemalloc_retained_bytes);
        entity->register_metric("memory_jemalloc_tcache_bytes", *memory_jemalloc_tcache_bytes);
        entity->register_metric("memory_jemalloc_pactive_num", *memory_jemalloc_pactive_num);
        entity->register_metric("memory_jemalloc_pdirty_num", *memory_jemalloc_pdirty_num);
        entity->register_metric("memory_jemalloc_pmuzzy_num", *memory_jemalloc_pmuzzy_num);
        entity->register_metric("memory_jemalloc_dirty_purged_num",
                                *memory_jemalloc_dirty_purged_num);
        entity->register_metric("memory_jemalloc_muzzy_purged_num",
                                *memory_jemalloc_muzzy_purged_num);

#endif
    }

    // MetricEntity* entity = nullptr;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_allocated_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pgpgin;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pgpgout;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pswpin;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_pswpout;

#ifndef USE_JEMALLOC
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_allocated_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_total_thread_cache_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_central_cache_free_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_transfer_cache_free_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_thread_cache_free_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_pageheap_free_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_tcmalloc_pageheap_unmapped_bytes;
#else
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_allocated_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_active_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_metadata_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_resident_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_mapped_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_retained_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_tcache_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_pactive_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_pdirty_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_pmuzzy_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_dirty_purged_num;
    std::shared_ptr<BvarAdderMetric<int64_t>> memory_jemalloc_muzzy_purged_num;
#endif
};

struct DiskBvarMetrics {
    DiskBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(disk_reads_completed, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_bytes_read, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "",
                               "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_read_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_writes_completed, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_bytes_written, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                               "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(disk_write_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_io_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(disk_io_time_weigthed, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "", BvarMetric::Labels(), false)
        entity->register_metric("disk_reads_completed", *disk_reads_completed);
        entity->register_metric("disk_bytes_read", *disk_bytes_read);
        entity->register_metric("disk_read_time_ms", *disk_read_time_ms);
        entity->register_metric("disk_writes_completed", *disk_writes_completed);
        entity->register_metric("disk_bytes_written", *disk_bytes_written);
        entity->register_metric("disk_write_time_ms", *disk_write_time_ms);
        entity->register_metric("disk_io_time_ms", *disk_io_time_ms);
        entity->register_metric("disk_io_time_weigthed", *disk_io_time_weigthed);
    }

    std::shared_ptr<BvarAdderMetric<int64_t>> disk_reads_completed;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_bytes_read;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_read_time_ms;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_writes_completed;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_bytes_written;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_write_time_ms;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_io_time_ms;
    std::shared_ptr<BvarAdderMetric<int64_t>> disk_io_time_weigthed;
};

struct NetworkBvarMetrics {
    NetworkBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(network_receive_bytes, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(network_receive_packets, BvarMetricType::COUNTER,
                               BvarMetricUnit::PACKETS, "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(network_send_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                               "", "", BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(network_send_packets, BvarMetricType::COUNTER,
                               BvarMetricUnit::PACKETS, "", "", BvarMetric::Labels(), false);
        entity->register_metric("network_receive_bytes", *network_receive_bytes);
        entity->register_metric("network_receive_packets", *network_receive_packets);
        entity->register_metric("network_send_bytes", *network_send_bytes);
        entity->register_metric("network_send_packets", *network_send_packets);
    }

    std::shared_ptr<BvarAdderMetric<int64_t>> network_receive_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> network_receive_packets;
    std::shared_ptr<BvarAdderMetric<int64_t>> network_send_bytes;
    std::shared_ptr<BvarAdderMetric<int64_t>> network_send_packets;
};

struct FileDescriptorBvarMetrics {
    FileDescriptorBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(fd_num_limit, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false);
        INIT_INT64_BVAR_METRIC(fd_num_used, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               BvarMetric::Labels(), false);
        entity->register_metric("fd_num_limit", *fd_num_limit);
        entity->register_metric("fd_num_used", *fd_num_used);
    }

    std::shared_ptr<BvarAdderMetric<int64_t>> fd_num_limit;
    std::shared_ptr<BvarAdderMetric<int64_t>> fd_num_used;
};

// metrics read from /proc/net/snmp
struct SnmpBvarMetrics {
    SnmpBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(snmp_tcp_in_errs, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "The number of all problematic TCP packets received", "",
                               BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(snmp_tcp_retrans_segs, BvarMetricType::COUNTER,
                               BvarMetricUnit::NOUNIT, "All TCP packets retransmitted", "",
                               BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(snmp_tcp_in_segs, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "All received TCP packets", "", BvarMetric::Labels(), false)
        INIT_INT64_BVAR_METRIC(snmp_tcp_out_segs, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "All send TCP packets with RST mark", "", BvarMetric::Labels(),
                               false)
        entity->register_metric("snmp_tcp_in_errs", *snmp_tcp_in_errs);
        entity->register_metric("snmp_tcp_retrans_segs", *snmp_tcp_retrans_segs);
        entity->register_metric("snmp_tcp_in_segs", *snmp_tcp_in_segs);
        entity->register_metric("snmp_tcp_out_segs", *snmp_tcp_out_segs);
    }

    std::shared_ptr<BvarAdderMetric<int64_t>> snmp_tcp_in_errs;
    std::shared_ptr<BvarAdderMetric<int64_t>> snmp_tcp_retrans_segs;
    std::shared_ptr<BvarAdderMetric<int64_t>> snmp_tcp_in_segs;
    std::shared_ptr<BvarAdderMetric<int64_t>> snmp_tcp_out_segs;
};

struct LoadAverageBvarMetrics {
    LoadAverageBvarMetrics(BvarMetricEntity* entity) {
        INIT_DOUBLE_BVAR_METRIC(load_average_1_minutes, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "load_average",
                                BvarMetric::Labels({{"mode", "1_minutes"}}), false);
        INIT_DOUBLE_BVAR_METRIC(load_average_5_minutes, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "load_average",
                                BvarMetric::Labels({{"mode", "5_minutes"}}), false);
        INIT_DOUBLE_BVAR_METRIC(load_average_15_minutes, BvarMetricType::GAUGE,
                                BvarMetricUnit::NOUNIT, "", "load_average",
                                BvarMetric::Labels({{"mode", "15_minutes"}}), false);
        entity->register_metric("load_average_1_minutes", *load_average_1_minutes);
        entity->register_metric("load_average_5_minutes", *load_average_5_minutes);
        entity->register_metric("load_average_15_minutes", *load_average_15_minutes);
    }

    std::shared_ptr<BvarAdderMetric<double>> load_average_1_minutes;
    std::shared_ptr<BvarAdderMetric<double>> load_average_5_minutes;
    std::shared_ptr<BvarAdderMetric<double>> load_average_15_minutes;
};

struct ProcBvarMetrics {
    ProcBvarMetrics(BvarMetricEntity* entity) {
        INIT_INT64_BVAR_METRIC(proc_interrupt, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT, "",
                               "proc", BvarMetric::Labels({{"mode", "interrupt"}}), false);
        INIT_INT64_BVAR_METRIC(proc_ctxt_switch, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "", "proc", BvarMetric::Labels({{"mode", "ctxt_switch"}}), false);
        INIT_INT64_BVAR_METRIC(proc_procs_running, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "", "proc", BvarMetric::Labels({{"mode", "procs_running"}}), false);
        INIT_INT64_BVAR_METRIC(proc_procs_blocked, BvarMetricType::COUNTER, BvarMetricUnit::NOUNIT,
                               "", "proc", BvarMetric::Labels({{"mode", "procs_blocked"}}), false);
        entity->register_metric("proc_interrupt", *proc_interrupt);
        entity->register_metric("proc_ctxt_switch", *proc_ctxt_switch);
        entity->register_metric("proc_procs_running", *proc_procs_running);
        entity->register_metric("proc_procs_blocked", *proc_procs_blocked);
    }

    std::shared_ptr<BvarAdderMetric<int64_t>> proc_interrupt;
    std::shared_ptr<BvarAdderMetric<int64_t>> proc_ctxt_switch;
    std::shared_ptr<BvarAdderMetric<int64_t>> proc_procs_running;
    std::shared_ptr<BvarAdderMetric<int64_t>> proc_procs_blocked;
};

const char* SystemBvarMetrics::s_hook_name_ = "system_metrics";

SystemBvarMetrics::SystemBvarMetrics(BvarMetricRegistry* registry,
                                     const std::set<std::string>& disk_devices,
                                     const std::vector<std::string>& network_interfaces) {
    DCHECK(registry != nullptr);
    registry_ = registry;
    server_entity_ = registry->register_entity("server");
    DCHECK(server_entity_ != nullptr);
    server_entity_->register_hook(s_hook_name_, std::bind(&SystemBvarMetrics::update, this));
    install_cpu_metrics();
    install_memory_metrics(server_entity_.get());
    install_disk_metrics(disk_devices);
    install_net_metrics(network_interfaces);
    install_fd_metrics(server_entity_.get());
    install_snmp_metrics(server_entity_.get());
    install_load_avg_metrics(server_entity_.get());
    install_proc_metrics(server_entity_.get());
    install_max_metrics(server_entity_.get());
}

SystemBvarMetrics::~SystemBvarMetrics() {
    DCHECK(server_entity_ != nullptr);
    server_entity_->deregister_hook(s_hook_name_);
    for (auto& it : cpu_metrics_) {
        delete it.second;
    }
    for (auto& it : disk_metrics_) {
        delete it.second;
    }
    for (auto& it : network_metrics_) {
        delete it.second;
    }
}

void SystemBvarMetrics::update() {
    update_cpu_metrics();
    update_memory_metrics();
    update_disk_metrics();
    update_net_metrics();
    update_fd_metrics();
    update_snmp_metrics();
    update_load_avg_metrics();
    update_proc_metrics();
}

void SystemBvarMetrics::install_max_metrics(BvarMetricEntity* entity) {
    INIT_INT64_BVAR_METRIC(max_disk_io_util_percent, BvarMetricType::GAUGE, BvarMetricUnit::PERCENT,
                           "", "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(max_network_send_bytes_rate, BvarMetricType::GAUGE,
                           BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    INIT_INT64_BVAR_METRIC(max_network_receive_bytes_rate, BvarMetricType::GAUGE,
                           BvarMetricUnit::BYTES, "", "", BvarMetric::Labels(), true)
    entity->register_metric("max_disk_io_util_percent", *max_disk_io_util_percent);
    entity->register_metric("max_network_send_bytes_rate", *max_network_send_bytes_rate);
    entity->register_metric("max_network_receive_bytes_rate", *max_network_receive_bytes_rate);
}

void SystemBvarMetrics::install_cpu_metrics() {
    get_cpu_name();
    for (auto cpu_name : cpu_names_) {
        auto cpu_entity = registry_->register_entity(cpu_name, {{"device", cpu_name}});
        CpuBvarMetrics* metrics = new CpuBvarMetrics(cpu_entity.get());
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
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        char cpu[16];
        int64_t values[CpuBvarMetrics::cpu_num_metrics];
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr,
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

        for (int i = 0; i < CpuBvarMetrics::cpu_num_metrics; ++i) {
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

void SystemBvarMetrics::install_memory_metrics(BvarMetricEntity* entity) {
    memory_metrics_.reset(new MemoryBvarMetrics(entity));
}

void SystemBvarMetrics::update_memory_metrics() {
    memory_metrics_->memory_allocated_bytes->set_value(PerfCounters::get_vm_rss());
    get_metrics_from_proc_vmstat();
}

void SystemBvarMetrics::install_disk_metrics(const std::set<std::string>& disk_devices) {
    for (auto& disk_device : disk_devices) {
        auto disk_entity = registry_->register_entity(std::string("disk_metrics.") + disk_device,
                                                      {{"device", disk_device}});
        DiskBvarMetrics* metrics = new DiskBvarMetrics(disk_entity.get());
        disk_metrics_.emplace(disk_device, metrics);
    }
}

void SystemBvarMetrics::update_disk_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_diskstats_path, "r");
#else
    FILE* fp = fopen("/proc/diskstats", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/diskstats failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // /proc/diskstats: https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
    // 1 - major number
    // 2 - minor mumber
    // 3 - device name
    // 4 - reads completed successfully
    // 5 - reads merged
    // 6 - sectors read
    // 7 - time spent reading (ms)
    // 8 - writes completed
    // 9 - writes merged
    // 10 - sectors written
    // 11 - time spent writing (ms)
    // 12 - I/Os currently in progress
    // 13 - time spent doing I/Os (ms)
    // 14 - weighted time spent doing I/Os (ms)
    // I think 1024 is enough for device name
    int major = 0;
    int minor = 0;
    char device[1024];
    int64_t values[11];
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr,
                         "%d %d %1023s"
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64,
                         &major, &minor, device, &values[0], &values[1], &values[2], &values[3],
                         &values[4], &values[5], &values[6], &values[7], &values[8], &values[9],
                         &values[10]);
        if (num < 4) {
            continue;
        }
        auto it = disk_metrics_.find(device);
        if (it == disk_metrics_.end()) {
            continue;
        }
        // update disk metrics
        // reads_completed: 4 reads completed successfully
        it->second->disk_reads_completed->set_value(values[0]);
        // bytes_read: 6 sectors read * 512; 5 reads merged is ignored
        it->second->disk_bytes_read->set_value(values[2] * 512);
        // read_time_ms: 7 time spent reading (ms)
        it->second->disk_read_time_ms->set_value(values[3]);
        // writes_completed: 8 writes completed
        it->second->disk_writes_completed->set_value(values[4]);
        // bytes_written: 10 sectors write * 512; 9 writes merged is ignored
        it->second->disk_bytes_written->set_value(values[6] * 512);
        // write_time_ms: 11 time spent writing (ms)
        it->second->disk_write_time_ms->set_value(values[7]);
        // io_time_ms: 13 time spent doing I/Os (ms)
        it->second->disk_io_time_ms->set_value(values[9]);
        // io_time_weigthed: 14 - weighted time spent doing I/Os (ms)
        it->second->disk_io_time_weigthed->set_value(values[10]);
    }
    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemBvarMetrics::install_net_metrics(const std::vector<std::string>& interfaces) {
    for (auto& interface : interfaces) {
        auto interface_entity = registry_->register_entity(
                std::string("network_metrics.") + interface, {{"device", interface}});
        NetworkBvarMetrics* metrics = new NetworkBvarMetrics(interface_entity.get());
        network_metrics_.emplace(interface, metrics);
    }
}

void SystemBvarMetrics::update_net_metrics() {
#ifdef BE_TEST
    // to mock proc
    FILE* fp = fopen(k_ut_net_dev_path, "r");
#else
    FILE* fp = fopen("/proc/net/dev", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/net/dev failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    // Ignore header
    if (getline(&line_ptr, &line_buf_size, fp) < 0 || getline(&line_ptr, &line_buf_size, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "read /proc/net/dev first two line failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }
    if (proc_net_dev_version_ == 0) {
        if (strstr(line_ptr, "compressed") != nullptr) {
            proc_net_dev_version_ = 3;
        } else if (strstr(line_ptr, "bytes") != nullptr) {
            proc_net_dev_version_ = 2;
        } else {
            proc_net_dev_version_ = 1;
        }
    }

    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        char* ptr = strrchr(line_ptr, ':');
        if (ptr == nullptr) {
            continue;
        }
        char* start = line_ptr;
        while (isspace(*start)) {
            start++;
        }
        std::string interface(start, ptr - start);
        auto it = network_metrics_.find(interface);
        if (it == network_metrics_.end()) {
            continue;
        }
        ptr++;
        int64_t receive_bytes = 0;
        int64_t receive_packets = 0;
        int64_t send_bytes = 0;
        int64_t send_packets = 0;
        switch (proc_net_dev_version_) {
        case 3:
            // receive: bytes packets errs drop fifo frame compressed multicast
            // send:    bytes packets errs drop fifo colls carrier compressed
            sscanf(ptr,
                   " %" PRId64 " %" PRId64
                   " %*d %*d %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets, &send_bytes, &send_packets);
            break;
        case 2:
            // receive: bytes packets errs drop fifo frame
            // send:    bytes packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64 " %" PRId64
                   " %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets, &send_bytes, &send_packets);
            break;
        case 1:
            // receive: packets errs drop fifo frame
            // send: packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64
                   " %*d %*d %*d %*d"
                   " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_packets, &send_packets);
            break;
        default:
            break;
        }
        it->second->network_receive_bytes->set_value(receive_bytes);
        it->second->network_receive_packets->set_value(receive_packets);
        it->second->network_send_bytes->set_value(send_bytes);
        it->second->network_send_packets->set_value(send_packets);
    }
    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemBvarMetrics::install_fd_metrics(BvarMetricEntity* entity) {
    fd_metrics_.reset(new FileDescriptorBvarMetrics(entity));
}

void SystemBvarMetrics::update_fd_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_fd_path, "r");
#else
    FILE* fp = fopen("/proc/sys/fs/file-nr", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/sys/fs/file-nr failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // /proc/sys/fs/file-nr: https://www.kernel.org/doc/Documentation/sysctl/fs.txt
    // 1 - the number of allocated file handles
    // 2 - the number of allocated but unused file handles
    // 3 - the maximum number of file handles
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    int64_t values[3];
    if (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "%" PRId64 " %" PRId64 " %" PRId64, &values[0], &values[1],
                         &values[2]);
        if (num == 3) {
            fd_metrics_->fd_num_limit->set_value(values[2]);
            fd_metrics_->fd_num_used->set_value(values[0] - values[1]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemBvarMetrics::install_snmp_metrics(BvarMetricEntity* entity) {
    snmp_metrics_.reset(new SnmpBvarMetrics(entity));
}

void SystemBvarMetrics::update_snmp_metrics() {
#ifdef BE_TEST
    // to mock proc
    FILE* fp = fopen(k_ut_net_snmp_path, "r");
#else
    FILE* fp = fopen("/proc/net/snmp", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/net/snmp failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }

    // We only care about Tcp lines, so skip other lines in front of Tcp line
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    int res = 0;
    while ((res = getline(&line_ptr, &line_buf_size, fp)) > 0) {
        if (strstr(line_ptr, "Tcp") != nullptr) {
            break;
        }
    }
    if (res <= 0) {
        char buf[64];
        LOG(WARNING) << "failed to skip lines of /proc/net/snmp, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }

    // parse the Tcp header
    // Tcp: RtoAlgorithm RtoMin RtoMax MaxConn ActiveOpens PassiveOpens AttemptFails EstabResets CurrEstab InSegs OutSegs RetransSegs InErrs OutRsts InCsumErrors
    std::vector<std::string> headers = strings::Split(line_ptr, " ");
    std::unordered_map<std::string, int32_t> header_map;
    int32_t pos = 0;
    for (auto& h : headers) {
        header_map.emplace(h, pos++);
    }

    // read the metrics of TCP
    if (getline(&line_ptr, &line_buf_size, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "failed to skip Tcp header line of /proc/net/snmp, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }

    // metric line looks like:
    // Tcp: 1 200 120000 -1 47849374 38601877 3353843 2320314 276 1033354613 1166025166 825439 12694 23238924 0
    std::vector<std::string> metrics = strings::Split(line_ptr, " ");
    if (metrics.size() != headers.size()) {
        LOG(WARNING) << "invalid tcp metrics line: " << line_ptr;
        fclose(fp);
        return;
    }
    int64_t retrans_segs = atoi64(metrics[header_map["RetransSegs"]]);
    int64_t in_errs = atoi64(metrics[header_map["InErrs"]]);
    int64_t in_segs = atoi64(metrics[header_map["InSegs"]]);
    int64_t out_segs = atoi64(metrics[header_map["OutSegs"]]);
    snmp_metrics_->snmp_tcp_retrans_segs->set_value(retrans_segs);
    snmp_metrics_->snmp_tcp_in_errs->set_value(in_errs);
    snmp_metrics_->snmp_tcp_in_segs->set_value(in_segs);
    snmp_metrics_->snmp_tcp_out_segs->set_value(out_segs);

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemBvarMetrics::install_load_avg_metrics(BvarMetricEntity* entity) {
    load_average_metrics_.reset(new LoadAverageBvarMetrics(entity));
}

void SystemBvarMetrics::update_load_avg_metrics() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_load_avg_path, "r");
#else
    FILE* fp = fopen("/proc/loadavg", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/loadavg failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    double values[3];
    if (getline(&line_ptr, &line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr, "%lf %lf %lf", &values[0], &values[1], &values[2]);
        if (num == 3) {
            load_average_metrics_->load_average_1_minutes->set_value(values[0]);
            load_average_metrics_->load_average_5_minutes->set_value(values[1]);
            load_average_metrics_->load_average_15_minutes->set_value(values[2]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemBvarMetrics::install_proc_metrics(BvarMetricEntity* entity) {
    proc_metrics_.reset(new ProcBvarMetrics(entity));
}

static void SystemBvarMetrics::update_proc_metrics() {
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
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    uint64_t inter = 0, ctxt = 0, procs_r = 0, procs_b = 0;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        char* start_pos = nullptr;
        start_pos = strstr(line_ptr, "intr ");
        if (start_pos) {
            sscanf(start_pos, "intr %" PRIu64, &inter);
            proc_metrics_->proc_interrupt->set_value(inter);
        }

        start_pos = strstr(line_ptr, "ctxt ");
        if (start_pos) {
            sscanf(start_pos, "ctxt %" PRIu64, &ctxt);
            proc_metrics_->proc_ctxt_switch->set_value(ctxt);
        }

        start_pos = strstr(line_ptr, "procs_running ");
        if (start_pos) {
            sscanf(start_pos, "procs_running %" PRIu64, &procs_r);
            proc_metrics_->proc_procs_running->set_value(procs_r);
        }

        start_pos = strstr(line_ptr, "procs_blocked ");
        if (start_pos) {
            sscanf(start_pos, "procs_blocked %" PRIu64, &procs_b);
            proc_metrics_->proc_procs_blocked->set_value(procs_b);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

static void SystemBvarMetrics::get_metrics_from_proc_vmstat() {
#ifdef BE_TEST
    FILE* fp = fopen(k_ut_vmstat_path, "r");
#else
    FILE* fp = fopen("/proc/vmstat", "r");
#endif
    if (fp == nullptr) {
        char buf[64];
        LOG(WARNING) << "open /proc/vmstat failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        return;
    }
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        uint64_t value;
        char name[64];
        int num = sscanf(line_ptr, "%s %" PRIu64, name, &value);
        if (num < 2) {
            continue;
        }

        if (strcmp(name, "pgpgin") == 0) {
            memory_metrics_->memory_pgpgin->set_value(value);
        } else if (strcmp(name, "pgpgout") == 0) {
            memory_metrics_->memory_pgpgout->set_value(value);
        } else if (strcmp(name, "pswpin") == 0) {
            memory_metrics_->memory_pswpin->set_value(value);
        } else if (strcmp(name, "pswpout") == 0) {
            memory_metrics_->memory_pswpout->set_value(value);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

static void SystemBvarMetrics::get_cpu_name() {
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
    char* line_ptr = nullptr;
    size_t line_buf_size = 0;
    while (getline(&line_ptr, &line_buf_size, fp) > 0) {
        char cpu[16];
        char* start_pos = nullptr;
        start_pos = strstr(line_ptr, "cpu");
        if (start_pos) {
            sscanf(line_ptr, "%15s", cpu);
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

static void SystemBvarMetrics::get_disks_io_time(std::map<std::string, int64_t>* map) {
    map->clear();
    for (auto& it : disk_metrics_) {
        map->emplace(it.first, it.second->disk_io_time_ms->get_value());
    }
}

static int64_t SystemBvarMetrics::get_max_io_util(const std::map<std::string, int64_t>& lst_value,
                                           int64_t interval_sec) {
    int64_t max = 0;
    for (auto& it : disk_metrics_) {
        int64_t cur = it.second->disk_io_time_ms->get_value();
        const auto find = lst_value.find(it.first);
        if (find == lst_value.end()) {
            continue;
        }
        int64_t incr = cur - find->second;
        if (incr > max) max = incr;
    }
    return max / interval_sec / 10;
}

static void SystemBvarMetrics::get_network_traffic(std::map<std::string, int64_t>* send_map,
                                            std::map<std::string, int64_t>* rcv_map) {
    send_map->clear();
    rcv_map->clear();
    for (auto& it : network_metrics_) {
        if (it.first == "lo") {
            continue;
        }
        send_map->emplace(it.first, it.second->network_send_bytes->get_value());
        rcv_map->emplace(it.first, it.second->network_receive_bytes->get_value());
    }
}

static void SystemBvarMetrics::get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                                            const std::map<std::string, int64_t>& lst_rcv_map,
                                            int64_t interval_sec, int64_t* send_rate,
                                            int64_t* rcv_rate) {
    int64_t max_send = 0;
    int64_t max_rcv = 0;
    for (auto& it : network_metrics_) {
        int64_t cur_send = it.second->network_send_bytes->get_value();
        int64_t cur_rcv = it.second->network_receive_bytes->get_value();

        const auto find_send = lst_send_map.find(it.first);
        if (find_send != lst_send_map.end()) {
            int64_t incr = cur_send - find_send->second;
            if (incr > max_send) max_send = incr;
        }
        const auto find_rcv = lst_rcv_map.find(it.first);
        if (find_rcv != lst_rcv_map.end()) {
            int64_t incr = cur_rcv - find_rcv->second;
            if (incr > max_rcv) max_rcv = incr;
        }
    }

    *send_rate = max_send / interval_sec;
    *rcv_rate = max_rcv / interval_sec;
}

void SystemBvarMetrics::update_max_disk_io_util_percent(
        const std::map<std::string, int64_t>& lst_value, int64_t interval_sec) {
    max_disk_io_util_percent->set_value(get_max_io_util(lst_value, interval_sec));
}

void SystemBvarMetrics::update_max_network_send_bytes_rate(int64_t max_send_bytes_rate) {
    max_network_send_bytes_rate->set_value(max_send_bytes_rate);
}

void SystemBvarMetrics::update_max_network_receive_bytes_rate(int64_t max_receive_bytes_rate) {
    max_network_receive_bytes_rate->set_value(max_receive_bytes_rate);
}

void SystemBvarMetrics::update_allocator_metrics() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    LOG(INFO) << "Memory tracking is not available with address sanitizer builds.";
#elif defined(USE_JEMALLOC)
    memory_metrics_->memory_jemalloc_allocated_bytes->set_value(
            MemInfo::get_je_metrics("stats.allocated"));
    memory_metrics_->memory_jemalloc_active_bytes->set_value(
            MemInfo::get_je_metrics("stats.active"));
    memory_metrics_->memory_jemalloc_metadata_bytes->set_value(
            MemInfo::get_je_metrics("stats.metadata"));
    memory_metrics_->memory_jemalloc_resident_bytes->set_value(
            MemInfo::get_je_metrics("stats.resident"));
    memory_metrics_->memory_jemalloc_mapped_bytes->set_value(
            MemInfo::get_je_metrics("stats.mapped"));
    memory_metrics_->memory_jemalloc_retained_bytes->set_value(
            MemInfo::get_je_metrics("stats.retained"));
    memory_metrics_->memory_jemalloc_tcache_bytes->set_value(
            MemInfo::get_je_all_arena_metrics("tcache_bytes"));
    memory_metrics_->memory_jemalloc_pactive_num->set_value(
            MemInfo::get_je_all_arena_metrics("pactive"));
    memory_metrics_->memory_jemalloc_pdirty_num->set_value(
            MemInfo::get_je_all_arena_metrics("pdirty"));
    memory_metrics_->memory_jemalloc_pmuzzy_num->set_value(
            MemInfo::get_je_all_arena_metrics("pmuzzy"));
    memory_metrics_->memory_jemalloc_dirty_purged_num->set_value(
            MemInfo::get_je_all_arena_metrics("dirty_purged"));
    memory_metrics_->memory_jemalloc_muzzy_purged_num->set_value(
            MemInfo::get_je_all_arena_metrics("muzzy_purged"));
#else
    memory_metrics_->memory_tcmalloc_allocated_bytes->set_value(
            MemInfo::get_tc_metrics("generic.total_physical_bytes"));
    memory_metrics_->memory_tcmalloc_total_thread_cache_bytes->set_value(
            MemInfo::allocator_cache_mem());
    memory_metrics_->memory_tcmalloc_central_cache_free_bytes->set_value(
            MemInfo::get_tc_metrics("tcmalloc.central_cache_free_bytes"));
    memory_metrics_->memory_tcmalloc_transfer_cache_free_bytes->set_value(
            MemInfo::get_tc_metrics("tcmalloc.transfer_cache_free_bytes"));
    memory_metrics_->memory_tcmalloc_thread_cache_free_bytes->set_value(
            MemInfo::get_tc_metrics("tcmalloc.thread_cache_free_bytes"));
    memory_metrics_->memory_tcmalloc_pageheap_free_bytes->set_value(
            MemInfo::get_tc_metrics("tcmalloc.pageheap_free_bytes"));
    memory_metrics_->memory_tcmalloc_pageheap_unmapped_bytes->set_value(
            MemInfo::get_tc_metrics("tcmalloc.pageheap_unmapped_bytes"));
#endif
}

} // namespace doris