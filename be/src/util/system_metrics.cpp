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

#include <stdio.h>

#include <functional>

#include "gutil/strings/split.h" // for string split
#include "gutil/strtoint.h"      //  for atoi64
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/perf_counters.h"

namespace doris {

#define DEFINE_CPU_COUNTER_METRIC(metric)                                            \
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(cpu_##metric, MetricUnit::PERCENT, "", cpu, \
                                         Labels({{"mode", #metric}}));
DEFINE_CPU_COUNTER_METRIC(user);
DEFINE_CPU_COUNTER_METRIC(nice);
DEFINE_CPU_COUNTER_METRIC(system);
DEFINE_CPU_COUNTER_METRIC(idle);
DEFINE_CPU_COUNTER_METRIC(iowait);
DEFINE_CPU_COUNTER_METRIC(irq);
DEFINE_CPU_COUNTER_METRIC(soft_irq);
DEFINE_CPU_COUNTER_METRIC(steal);
DEFINE_CPU_COUNTER_METRIC(guest);
DEFINE_CPU_COUNTER_METRIC(guest_nice);

// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
struct CpuMetrics {
    CpuMetrics(MetricEntity* ent) : entity(ent) {
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_user);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_nice);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_system);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_idle);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_iowait);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_irq);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_soft_irq);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_steal);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_guest);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, cpu_guest_nice);

        metrics[0] = cpu_user;
        metrics[1] = cpu_nice;
        metrics[2] = cpu_system;
        metrics[3] = cpu_idle;
        metrics[4] = cpu_iowait;
        metrics[5] = cpu_irq;
        metrics[6] = cpu_soft_irq;
        metrics[7] = cpu_steal;
        metrics[8] = cpu_guest;
        metrics[9] = cpu_guest_nice;
    }

    static constexpr int cpu_num_metrics = 10;

    MetricEntity* entity = nullptr;
    IntAtomicCounter* cpu_user;
    IntAtomicCounter* cpu_nice;
    IntAtomicCounter* cpu_system;
    IntAtomicCounter* cpu_idle;
    IntAtomicCounter* cpu_iowait;
    IntAtomicCounter* cpu_irq;
    IntAtomicCounter* cpu_soft_irq;
    IntAtomicCounter* cpu_steal;
    IntAtomicCounter* cpu_guest;
    IntAtomicCounter* cpu_guest_nice;

    IntAtomicCounter* metrics[cpu_num_metrics];
};

#define DEFINE_MEMORY_GAUGE_METRIC(metric, unit) \
    DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(memory_##metric, unit);
DEFINE_MEMORY_GAUGE_METRIC(allocated_bytes, MetricUnit::BYTES);
DEFINE_MEMORY_GAUGE_METRIC(pgpgin, MetricUnit::NOUNIT);
DEFINE_MEMORY_GAUGE_METRIC(pgpgout, MetricUnit::NOUNIT);
DEFINE_MEMORY_GAUGE_METRIC(pswpin, MetricUnit::NOUNIT);
DEFINE_MEMORY_GAUGE_METRIC(pswpout, MetricUnit::NOUNIT);

struct MemoryMetrics {
    MemoryMetrics(MetricEntity* ent) : entity(ent) {
        INT_GAUGE_METRIC_REGISTER(entity, memory_allocated_bytes);
        INT_GAUGE_METRIC_REGISTER(entity, memory_pgpgin);
        INT_GAUGE_METRIC_REGISTER(entity, memory_pgpgout);
        INT_GAUGE_METRIC_REGISTER(entity, memory_pswpin);
        INT_GAUGE_METRIC_REGISTER(entity, memory_pswpout);
    }

    MetricEntity* entity = nullptr;
    IntGauge* memory_allocated_bytes;
    IntGauge* memory_pgpgin;
    IntGauge* memory_pgpgout;
    IntGauge* memory_pswpin;
    IntGauge* memory_pswpout;
};

#define DEFINE_DISK_COUNTER_METRIC(metric, unit) \
    DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(disk_##metric, unit);
DEFINE_DISK_COUNTER_METRIC(reads_completed, MetricUnit::OPERATIONS);
DEFINE_DISK_COUNTER_METRIC(bytes_read, MetricUnit::BYTES);
DEFINE_DISK_COUNTER_METRIC(read_time_ms, MetricUnit::MILLISECONDS);
DEFINE_DISK_COUNTER_METRIC(writes_completed, MetricUnit::OPERATIONS);
DEFINE_DISK_COUNTER_METRIC(bytes_written, MetricUnit::BYTES);
DEFINE_DISK_COUNTER_METRIC(write_time_ms, MetricUnit::MILLISECONDS);
DEFINE_DISK_COUNTER_METRIC(io_time_ms, MetricUnit::MILLISECONDS);
DEFINE_DISK_COUNTER_METRIC(io_time_weigthed, MetricUnit::MILLISECONDS);

struct DiskMetrics {
    DiskMetrics(MetricEntity* ent) : entity(ent) {
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_reads_completed);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_bytes_read);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_read_time_ms);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_writes_completed);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_bytes_written);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_write_time_ms);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_io_time_ms);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, disk_io_time_weigthed);
    }

    MetricEntity* entity = nullptr;
    IntAtomicCounter* disk_reads_completed;
    IntAtomicCounter* disk_bytes_read;
    IntAtomicCounter* disk_read_time_ms;
    IntAtomicCounter* disk_writes_completed;
    IntAtomicCounter* disk_bytes_written;
    IntAtomicCounter* disk_write_time_ms;
    IntAtomicCounter* disk_io_time_ms;
    IntAtomicCounter* disk_io_time_weigthed;
};

#define DEFINE_NETWORK_COUNTER_METRIC(metric, unit) \
    DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(network_##metric, unit);
DEFINE_NETWORK_COUNTER_METRIC(receive_bytes, MetricUnit::BYTES);
DEFINE_NETWORK_COUNTER_METRIC(receive_packets, MetricUnit::PACKETS);
DEFINE_NETWORK_COUNTER_METRIC(send_bytes, MetricUnit::BYTES);
DEFINE_NETWORK_COUNTER_METRIC(send_packets, MetricUnit::PACKETS);

struct NetworkMetrics {
    NetworkMetrics(MetricEntity* ent) : entity(ent) {
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, network_receive_bytes);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, network_receive_packets);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, network_send_bytes);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, network_send_packets);
    }

    MetricEntity* entity = nullptr;
    IntAtomicCounter* network_receive_bytes;
    IntAtomicCounter* network_receive_packets;
    IntAtomicCounter* network_send_bytes;
    IntAtomicCounter* network_send_packets;
};

#define DEFINE_SNMP_COUNTER_METRIC(metric, unit, desc) \
    DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(snmp_##metric, unit, desc);
DEFINE_SNMP_COUNTER_METRIC(tcp_in_errs, MetricUnit::NOUNIT,
                           "The number of all problematic TCP packets received");
DEFINE_SNMP_COUNTER_METRIC(tcp_retrans_segs, MetricUnit::NOUNIT, "All TCP packets retransmitted");
DEFINE_SNMP_COUNTER_METRIC(tcp_in_segs, MetricUnit::NOUNIT, "All received TCP packets");
DEFINE_SNMP_COUNTER_METRIC(tcp_out_segs, MetricUnit::NOUNIT, "All send TCP packets with RST mark");

// metrics read from /proc/net/snmp
struct SnmpMetrics {
    SnmpMetrics(MetricEntity* ent) : entity(ent) {
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, snmp_tcp_in_errs);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, snmp_tcp_retrans_segs);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, snmp_tcp_in_segs);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, snmp_tcp_out_segs);
    }

    MetricEntity* entity = nullptr;
    IntAtomicCounter* snmp_tcp_in_errs;
    IntAtomicCounter* snmp_tcp_retrans_segs;
    IntAtomicCounter* snmp_tcp_in_segs;
    IntAtomicCounter* snmp_tcp_out_segs;
};

#define DEFINE_FD_COUNTER_METRIC(metric, unit) \
    DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(fd_##metric, unit);
DEFINE_FD_COUNTER_METRIC(num_limit, MetricUnit::NOUNIT);
DEFINE_FD_COUNTER_METRIC(num_used, MetricUnit::NOUNIT);

struct FileDescriptorMetrics {
    FileDescriptorMetrics(MetricEntity* ent) : entity(ent) {
        INT_GAUGE_METRIC_REGISTER(entity, fd_num_limit);
        INT_GAUGE_METRIC_REGISTER(entity, fd_num_used);
    }

    MetricEntity* entity = nullptr;
    IntGauge* fd_num_limit;
    IntGauge* fd_num_used;
};

#define DEFINE_LOAD_AVERAGE_DOUBLE_METRIC(metric)                                     \
    DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(load_average_##metric, MetricUnit::NOUNIT, "", \
                                       load_average, Labels({{"mode", #metric}}));
DEFINE_LOAD_AVERAGE_DOUBLE_METRIC(1_minutes);
DEFINE_LOAD_AVERAGE_DOUBLE_METRIC(5_minutes);
DEFINE_LOAD_AVERAGE_DOUBLE_METRIC(15_minutes);

struct LoadAverageMetrics {
    LoadAverageMetrics(MetricEntity* ent) : entity(ent) {
        INT_DOUBLE_METRIC_REGISTER(entity, load_average_1_minutes);
        INT_DOUBLE_METRIC_REGISTER(entity, load_average_5_minutes);
        INT_DOUBLE_METRIC_REGISTER(entity, load_average_15_minutes);
    }

    MetricEntity* entity = nullptr;
    DoubleGauge* load_average_1_minutes;
    DoubleGauge* load_average_5_minutes;
    DoubleGauge* load_average_15_minutes;
};

#define DEFINE_PROC_STAT_COUNTER_METRIC(metric)                                       \
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(proc_##metric, MetricUnit::NOUNIT, "", proc, \
                                         Labels({{"mode", #metric}}));
DEFINE_PROC_STAT_COUNTER_METRIC(interrupt);
DEFINE_PROC_STAT_COUNTER_METRIC(ctxt_switch);
DEFINE_PROC_STAT_COUNTER_METRIC(procs_running);
DEFINE_PROC_STAT_COUNTER_METRIC(procs_blocked);

struct ProcMetrics {
    ProcMetrics(MetricEntity* ent) : entity(ent) {
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, proc_interrupt);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, proc_ctxt_switch);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, proc_procs_running);
        INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, proc_procs_blocked);
    }

    MetricEntity* entity = nullptr;

    IntAtomicCounter* proc_interrupt;
    IntAtomicCounter* proc_ctxt_switch;
    IntAtomicCounter* proc_procs_running;
    IntAtomicCounter* proc_procs_blocked;
};

const char* SystemMetrics::_s_hook_name = "system_metrics";

SystemMetrics::SystemMetrics(MetricRegistry* registry, const std::set<std::string>& disk_devices,
                             const std::vector<std::string>& network_interfaces) {
    DCHECK(registry != nullptr);
    _registry = registry;
    _server_entity = _registry->register_entity("server");
    DCHECK(_server_entity != nullptr);
    _server_entity->register_hook(_s_hook_name, std::bind(&SystemMetrics::update, this));
    _install_cpu_metrics();
    _install_memory_metrics(_server_entity.get());
    _install_disk_metrics(disk_devices);
    _install_net_metrics(network_interfaces);
    _install_fd_metrics(_server_entity.get());
    _install_snmp_metrics(_server_entity.get());
    _install_load_avg_metrics(_server_entity.get());
    _install_proc_metrics(_server_entity.get());
}

SystemMetrics::~SystemMetrics() {
    DCHECK(_server_entity != nullptr);
    _server_entity->deregister_hook(_s_hook_name);

    for (auto& it : _cpu_metrics) {
        delete it.second;
    }
    for (auto& it : _disk_metrics) {
        delete it.second;
    }
    for (auto& it : _network_metrics) {
        delete it.second;
    }
    if (_line_ptr != nullptr) {
        free(_line_ptr);
    }
}

void SystemMetrics::update() {
    _update_cpu_metrics();
    _update_memory_metrics();
    _update_disk_metrics();
    _update_net_metrics();
    _update_fd_metrics();
    _update_snmp_metrics();
    _update_load_avg_metrics();
    _update_proc_metrics();
}

void SystemMetrics::_install_cpu_metrics() {
    get_cpu_name();
    for (auto cpu_name : _cpu_names) {
        auto cpu_entity = _registry->register_entity(cpu_name, {{"device", cpu_name}});
        CpuMetrics* metrics = new CpuMetrics(cpu_entity.get());
        _cpu_metrics.emplace(cpu_name, metrics);
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

void SystemMetrics::_update_cpu_metrics() {
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

    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        char cpu[16];
        int64_t values[CpuMetrics::cpu_num_metrics];
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr,
                         "%15s"
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64,
                         cpu, &values[0], &values[1], &values[2], &values[3], &values[4],
                         &values[5], &values[6], &values[7], &values[8], &values[9]);
        if (num < 4) {
            continue;
        }

        std::string cpu_name(cpu);
        auto it = _cpu_metrics.find(cpu_name);
        if (it == _cpu_metrics.end()) {
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

void SystemMetrics::_install_memory_metrics(MetricEntity* entity) {
    _memory_metrics.reset(new MemoryMetrics(entity));
}

void SystemMetrics::_update_memory_metrics() {
    _memory_metrics->memory_allocated_bytes->set_value(PerfCounters::get_vm_rss());
    get_metrics_from_proc_vmstat();
}

void SystemMetrics::_install_disk_metrics(const std::set<std::string>& disk_devices) {
    for (auto& disk_device : disk_devices) {
        auto disk_entity = _registry->register_entity(std::string("disk_metrics.") + disk_device,
                                                      {{"device", disk_device}});
        DiskMetrics* metrics = new DiskMetrics(disk_entity.get());
        _disk_metrics.emplace(disk_device, metrics);
    }
}

void SystemMetrics::_update_disk_metrics() {
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
    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr,
                         "%d %d %1023s"
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64,
                         &major, &minor, device, &values[0], &values[1], &values[2], &values[3],
                         &values[4], &values[5], &values[6], &values[7], &values[8], &values[9],
                         &values[10]);
        if (num < 4) {
            continue;
        }
        auto it = _disk_metrics.find(device);
        if (it == _disk_metrics.end()) {
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

void SystemMetrics::_install_net_metrics(const std::vector<std::string>& interfaces) {
    for (auto& interface : interfaces) {
        auto interface_entity = _registry->register_entity(
                std::string("network_metrics.") + interface, {{"device", interface}});
        NetworkMetrics* metrics = new NetworkMetrics(interface_entity.get());
        _network_metrics.emplace(interface, metrics);
    }
}

void SystemMetrics::_install_snmp_metrics(MetricEntity* entity) {
    _snmp_metrics.reset(new SnmpMetrics(entity));
}

void SystemMetrics::_update_net_metrics() {
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
    if (getline(&_line_ptr, &_line_buf_size, fp) < 0 ||
        getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "read /proc/net/dev first two line failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }
    if (_proc_net_dev_version == 0) {
        if (strstr(_line_ptr, "compressed") != nullptr) {
            _proc_net_dev_version = 3;
        } else if (strstr(_line_ptr, "bytes") != nullptr) {
            _proc_net_dev_version = 2;
        } else {
            _proc_net_dev_version = 1;
        }
    }

    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        char* ptr = strrchr(_line_ptr, ':');
        if (ptr == nullptr) {
            continue;
        }
        char* start = _line_ptr;
        while (isspace(*start)) {
            start++;
        }
        std::string interface(start, ptr - start);
        auto it = _network_metrics.find(interface);
        if (it == _network_metrics.end()) {
            continue;
        }
        ptr++;
        int64_t receive_bytes = 0;
        int64_t receive_packets = 0;
        int64_t send_bytes = 0;
        int64_t send_packets = 0;
        switch (_proc_net_dev_version) {
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

void SystemMetrics::_update_snmp_metrics() {
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
    int res = 0;
    while ((res = getline(&_line_ptr, &_line_buf_size, fp)) > 0) {
        if (strstr(_line_ptr, "Tcp") != nullptr) {
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
    std::vector<std::string> headers = strings::Split(_line_ptr, " ");
    std::unordered_map<std::string, int32_t> header_map;
    int32_t pos = 0;
    for (auto& h : headers) {
        header_map.emplace(h, pos++);
    }

    // read the metrics of TCP
    if (getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "failed to skip Tcp header line of /proc/net/snmp, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }

    // metric line looks like:
    // Tcp: 1 200 120000 -1 47849374 38601877 3353843 2320314 276 1033354613 1166025166 825439 12694 23238924 0
    std::vector<std::string> metrics = strings::Split(_line_ptr, " ");
    if (metrics.size() != headers.size()) {
        LOG(WARNING) << "invalid tcp metrics line: " << _line_ptr;
        fclose(fp);
        return;
    }
    int64_t retrans_segs = atoi64(metrics[header_map["RetransSegs"]]);
    int64_t in_errs = atoi64(metrics[header_map["InErrs"]]);
    int64_t in_segs = atoi64(metrics[header_map["InSegs"]]);
    int64_t out_segs = atoi64(metrics[header_map["OutSegs"]]);
    _snmp_metrics->snmp_tcp_retrans_segs->set_value(retrans_segs);
    _snmp_metrics->snmp_tcp_in_errs->set_value(in_errs);
    _snmp_metrics->snmp_tcp_in_segs->set_value(in_segs);
    _snmp_metrics->snmp_tcp_out_segs->set_value(out_segs);

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemMetrics::_install_fd_metrics(MetricEntity* entity) {
    _fd_metrics.reset(new FileDescriptorMetrics(entity));
}

void SystemMetrics::_update_fd_metrics() {
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

    int64_t values[3];
    if (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr, "%" PRId64 " %" PRId64 " %" PRId64, &values[0], &values[1],
                         &values[2]);
        if (num == 3) {
            _fd_metrics->fd_num_limit->set_value(values[2]);
            _fd_metrics->fd_num_used->set_value(values[0] - values[1]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemMetrics::_install_load_avg_metrics(MetricEntity* entity) {
    _load_average_metrics.reset(new LoadAverageMetrics(entity));
}

void SystemMetrics::_update_load_avg_metrics() {
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

    double values[3];
    if (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr, "%lf %lf %lf", &values[0], &values[1], &values[2]);
        if (num == 3) {
            _load_average_metrics->load_average_1_minutes->set_value(values[0]);
            _load_average_metrics->load_average_5_minutes->set_value(values[1]);
            _load_average_metrics->load_average_15_minutes->set_value(values[2]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

int64_t SystemMetrics::get_max_io_util(const std::map<std::string, int64_t>& lst_value,
                                       int64_t interval_sec) {
    int64_t max = 0;
    for (auto& it : _disk_metrics) {
        int64_t cur = it.second->disk_io_time_ms->value();
        const auto find = lst_value.find(it.first);
        if (find == lst_value.end()) {
            continue;
        }
        int64_t incr = cur - find->second;
        if (incr > max) max = incr;
    }
    return max / interval_sec / 10;
}

void SystemMetrics::get_disks_io_time(std::map<std::string, int64_t>* map) {
    map->clear();
    for (auto& it : _disk_metrics) {
        map->emplace(it.first, it.second->disk_io_time_ms->value());
    }
}

void SystemMetrics::get_network_traffic(std::map<std::string, int64_t>* send_map,
                                        std::map<std::string, int64_t>* rcv_map) {
    send_map->clear();
    rcv_map->clear();
    for (auto& it : _network_metrics) {
        if (it.first == "lo") {
            continue;
        }
        send_map->emplace(it.first, it.second->network_send_bytes->value());
        rcv_map->emplace(it.first, it.second->network_receive_bytes->value());
    }
}

void SystemMetrics::get_max_net_traffic(const std::map<std::string, int64_t>& lst_send_map,
                                        const std::map<std::string, int64_t>& lst_rcv_map,
                                        int64_t interval_sec, int64_t* send_rate,
                                        int64_t* rcv_rate) {
    int64_t max_send = 0;
    int64_t max_rcv = 0;
    for (auto& it : _network_metrics) {
        int64_t cur_send = it.second->network_send_bytes->value();
        int64_t cur_rcv = it.second->network_receive_bytes->value();

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

void SystemMetrics::_install_proc_metrics(MetricEntity* entity) {
    _proc_metrics.reset(new ProcMetrics(entity));
}

void SystemMetrics::_update_proc_metrics() {
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

    uint64_t inter = 0, ctxt = 0, procs_r = 0, procs_b = 0;
    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        char* start_pos = nullptr;
        start_pos = strstr(_line_ptr, "intr ");
        if (start_pos) {
            sscanf(start_pos, "intr %" PRIu64, &inter);
            _proc_metrics->proc_interrupt->set_value(inter);
        }

        start_pos = strstr(_line_ptr, "ctxt ");
        if (start_pos) {
            sscanf(start_pos, "ctxt %" PRIu64, &ctxt);
            _proc_metrics->proc_ctxt_switch->set_value(ctxt);
        }

        start_pos = strstr(_line_ptr, "procs_running ");
        if (start_pos) {
            sscanf(start_pos, "procs_running %" PRIu64, &procs_r);
            _proc_metrics->proc_procs_running->set_value(procs_r);
        }

        start_pos = strstr(_line_ptr, "procs_blocked ");
        if (start_pos) {
            sscanf(start_pos, "procs_blocked %" PRIu64, &procs_b);
            _proc_metrics->proc_procs_blocked->set_value(procs_b);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

void SystemMetrics::get_metrics_from_proc_vmstat() {
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

    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        uint64_t value;
        char name[64];
        int num = sscanf(_line_ptr, "%s %" PRIu64, name, &value);
        if (num < 2) {
            continue;
        }

        if (strcmp(name, "pgpgin") == 0) {
            _memory_metrics->memory_pgpgin->set_value(value);
        } else if (strcmp(name, "pgpgout") == 0) {
            _memory_metrics->memory_pgpgout->set_value(value);
        } else if (strcmp(name, "pswpin") == 0) {
            _memory_metrics->memory_pswpin->set_value(value);
        } else if (strcmp(name, "pswpout") == 0) {
            _memory_metrics->memory_pswpout->set_value(value);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
                     << ", message=" << strerror_r(errno, buf, 64);
    }

    fclose(fp);
}

void SystemMetrics::get_cpu_name() {
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

    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        char cpu[16];
        char* start_pos = nullptr;
        start_pos = strstr(_line_ptr, "cpu");
        if (start_pos) {
            sscanf(_line_ptr, "%15s", cpu);
            std::string cpu_name(cpu);
            _cpu_names.push_back(cpu_name);
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
