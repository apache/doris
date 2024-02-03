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

// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
struct CpuBvarMetrics {
    CpuBvarMetrics(std::shared_ptr<BvarMetricEntity> entity, std::string cpu_name) {
        DECLARE_INT64_BVAR_METRIC(cpu_user, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "user"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_nice, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "nice"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_system, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "system"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_idle, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "idle"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_iowait, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "iowait"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_irq, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "irq"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_soft_irq, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                  "", "cpu", Labels({{"device", cpu_name}, {"mode", "soft_irq"}}),
                                  false)
        DECLARE_INT64_BVAR_METRIC(cpu_steal, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "steal"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_guest, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "",
                                  "cpu", Labels({{"device", cpu_name}, {"mode", "guest"}}), false)
        DECLARE_INT64_BVAR_METRIC(cpu_guest_nice, BvarMetricType::COUNTER, BvarMetricUnit::PERCENT,
                                  "", "cpu", Labels({{"device", cpu_name}, {"mode", "guest_nice"}}),
                                  false)
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
    MemoryBvarMetrics(std::shared_ptr<BvarMetricEntity> entity) {
        INIT_INT64_BVAR_METRIC(memory_allocated_bytes, BvarMetricType::GAUGE, BvarMetricUnit::BYTES,
                               "", "", Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pgpgin, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pgpgout, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pswpin, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "", "",
                               Labels(), false)
        INIT_INT64_BVAR_METRIC(memory_pswpout, BvarMetricType::GAUGE, BvarMetricUnit::NOUNIT, "",
                               "", Labels(), false)
        entity->register_metric("memory_allocated_bytes", *memory_allocated_bytes);
        entity->register_metric("memory_pgpgin", *memory_pgpgin);
        entity->register_metric("memory_pgpgout", *memory_pgpgout);
        entity->register_metric("memory_pswpin", *memory_pswpin);
        entity->register_metric("memory_pswpout", *memory_pswpout);
#ifndef USE_JEMALLOC
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_allocated_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_total_thread_cache_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_central_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_transfer_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_thread_cache_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_pageheap_free_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_tcmalloc_pageheap_unmapped_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
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
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_active_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_metadata_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_resident_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_mapped_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_retained_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_tcache_bytes, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pactive_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pdirty_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_pmuzzy_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_dirty_purged_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);
        INIT_INT64_BVAR_METRIC(memory_jemalloc_muzzy_purged_num, BvarMetricType::GAUGE,
                               BvarMetricUnit::BYTES, "", "", Labels(), false);

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
    DiskBvarMetrics(std::shared_ptr<BvarMetricEntity> entity, const std::string& disk_device) {
        INIT_INT64_BVAR_METRIC(disk_reads_completed, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "",
                               Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_bytes_read, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, "",
                               "", Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_read_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "",
                               Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_writes_completed, BvarMetricType::COUNTER,
                               BvarMetricUnit::OPERATIONS, "", "",
                               Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_bytes_written, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                               "", "", Labels({{"device", disk_device}}), false);
        INIT_INT64_BVAR_METRIC(disk_write_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "",
                               Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_io_time_ms, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "",
                               Labels({{"device", disk_device}}), false)
        INIT_INT64_BVAR_METRIC(disk_io_time_weigthed, BvarMetricType::COUNTER,
                               BvarMetricUnit::MILLISECONDS, "", "",
                               Labels({{"device", disk_device}}), false)
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
    NetworkBvarMetrics(std::shared_ptr<BvarMetricEntity> entity, const std::string& interface) {
        INIT_INT64_BVAR_METRIC(network_receive_bytes, BvarMetricType::COUNTER,
                               BvarMetricUnit::BYTES, "", "", Labels({{"device", interface}}),
                               false);
        INIT_INT64_BVAR_METRIC(network_receive_packets, BvarMetricType::COUNTER,
                               BvarMetricUnit::PACKETS, "", "", Labels({{"device", interface}}),
                               false);
        INIT_INT64_BVAR_METRIC(network_send_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                               "", "", Labels({{"device", interface}}), false);
        INIT_INT64_BVAR_METRIC(network_send_packets, BvarMetricType::COUNTER,
                               BvarMetricUnit::PACKETS, "", "", Labels({{"device", interface}}),
                               false);
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

std::string SystemBvarMetrics::to_prometheus(const std::string& registry_name) const {
    std::stringstream ss;
    for (auto& entities : entities_map_) {
        if (entities.second.empty()) {
            continue;
        }
        int count = 0;
        for (auto& entity : entities.second) {
            if (!count) {
                ss << "# TYPE " << registry_name << "_" << entity->get_name() << " "
                   << entity->get_type() << "\n";
                count++;
            }
            ss << entity->to_prometheus(registry_name);
        }
    }
    return ss.str();
}

SystemBvarMetrics::SystemBvarMetrics(const std::set<std::string>& disk_devices,
                                     const std::vector<std::string>& network_interfaces) {
    install_cpu_metrics();
    install_memory_metrics();
    install_disk_metrics(disk_devices);
    install_net_metrics(network_interfaces);
    update();
}

SystemBvarMetrics::~SystemBvarMetrics() {
    for (auto& it : cpu_metrics_) {
        delete it.second;
    }
    for (auto& it : disk_metrics_) {
        delete it.second;
    }
    for (auto& it : network_metrics_) {
        delete it.second;
    }

    if (line_ptr_ != nullptr) {
        free(line_ptr_);
    }
}

void SystemBvarMetrics::update() {
    update_cpu_metrics();
    update_memory_metrics();
    update_disk_metrics();
    update_net_metrics();
}

void SystemBvarMetrics::install_cpu_metrics() {
    get_cpu_name();
    for (auto cpu_name : cpu_names_) {
        auto cpu_entity = std::make_shared<BvarMetricEntity>("cpu", BvarMetricType::COUNTER);
        CpuBvarMetrics* metrics = new CpuBvarMetrics(cpu_entity, cpu_name);
        cpu_metrics_.emplace(cpu_name, metrics);
        entities_map_["cpu"].push_back(cpu_entity);
    }
}

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
        int64_t values[CpuBvarMetrics::cpu_num_metrics];
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

void SystemBvarMetrics::install_memory_metrics() {
    auto memory_entity = std::make_shared<BvarMetricEntity>("memory", BvarMetricType::GAUGE);
    memory_metrics_ = std::make_shared<MemoryBvarMetrics>(memory_entity);
    entities_map_["memory"].push_back(memory_entity);
}

void SystemBvarMetrics::update_memory_metrics() {
    memory_metrics_->memory_allocated_bytes->set_value(PerfCounters::get_vm_rss());
    get_metrics_from_proc_vmstat();
}

void SystemBvarMetrics::install_disk_metrics(const std::set<std::string>& disk_devices) {
    for (auto& disk_device : disk_devices) {
        auto disk_entity = std::make_shared<BvarMetricEntity>("disk", BvarMetricType::COUNTER);
        DiskBvarMetrics* metrics = new DiskBvarMetrics(disk_entity, disk_device);
        entities_map_["disk"].push_back(disk_entity);
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
    while (getline(&line_ptr_, &line_buf_size_, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(line_ptr_,
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
        auto interface_entity =
                std::make_shared<BvarMetricEntity>("network", BvarMetricType::COUNTER);
        NetworkBvarMetrics* metrics = new NetworkBvarMetrics(interface_entity, interface);
        entities_map_["network"].push_back(interface_entity);
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

    // Ignore header
    if (getline(&line_ptr_, &line_buf_size_, fp) < 0 ||
        getline(&line_ptr_, &line_buf_size_, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "read /proc/net/dev first two line failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }
    if (proc_net_dev_version_ == 0) {
        if (strstr(line_ptr_, "compressed") != nullptr) {
            proc_net_dev_version_ = 3;
        } else if (strstr(line_ptr_, "bytes") != nullptr) {
            proc_net_dev_version_ = 2;
        } else {
            proc_net_dev_version_ = 1;
        }
    }

    while (getline(&line_ptr_, &line_buf_size_, fp) > 0) {
        char* ptr = strrchr(line_ptr_, ':');
        if (ptr == nullptr) {
            continue;
        }
        char* start = line_ptr_;
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

void SystemBvarMetrics::get_metrics_from_proc_vmstat() {
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

    while (getline(&line_ptr_, &line_buf_size_, fp) > 0) {
        uint64_t value;
        char name[64];
        int num = sscanf(line_ptr_, "%s %" PRIu64, name, &value);
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