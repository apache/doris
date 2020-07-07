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
#include "gutil/strings/split.h" // for string split
#include "gutil/strtoint.h" //  for atoi64

#include <stdio.h>
#include <gperftools/malloc_extension.h>

#include <functional>

namespace doris {

const char* SystemMetrics::_s_hook_name = "system_metrics";

// /proc/stat: http://www.linuxhowtos.org/System/procstat.htm
struct CpuMetrics {
    static constexpr int cpu_num_metrics = 10;
    IntLockCounter metrics[cpu_num_metrics] = {
        {MetricUnit::PERCENT}, {MetricUnit::PERCENT},
        {MetricUnit::PERCENT}, {MetricUnit::PERCENT},
        {MetricUnit::PERCENT}, {MetricUnit::PERCENT},
        {MetricUnit::PERCENT}, {MetricUnit::PERCENT},
        {MetricUnit::PERCENT}, {MetricUnit::PERCENT}
    };
    static const char* cpu_metrics[cpu_num_metrics];
};

const char* CpuMetrics::cpu_metrics[] = {
    "user", "nice", "system", "idle", "iowait",
    "irq", "soft_irq", "steal", "guest", "guest_nice"
};

struct MemoryMetrics {
    METRIC_DEFINE_INT_GAUGE(allocated_bytes, MetricUnit::BYTES);
};

struct DiskMetrics {
    METRIC_DEFINE_INT_LOCK_COUNTER(reads_completed, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_LOCK_COUNTER(bytes_read, MetricUnit::BYTES);
    METRIC_DEFINE_INT_LOCK_COUNTER(read_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_LOCK_COUNTER(writes_completed, MetricUnit::OPERATIONS);
    METRIC_DEFINE_INT_LOCK_COUNTER(bytes_written, MetricUnit::BYTES);
    METRIC_DEFINE_INT_LOCK_COUNTER(write_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_LOCK_COUNTER(io_time_ms, MetricUnit::MILLISECONDS);
    METRIC_DEFINE_INT_LOCK_COUNTER(io_time_weigthed, MetricUnit::MILLISECONDS);
};

struct NetMetrics {
    METRIC_DEFINE_INT_LOCK_COUNTER(receive_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_LOCK_COUNTER(receive_packets, MetricUnit::PACKETS);
    METRIC_DEFINE_INT_LOCK_COUNTER(send_bytes, MetricUnit::BYTES);
    METRIC_DEFINE_INT_LOCK_COUNTER(send_packets, MetricUnit::PACKETS);
};

// metrics read from /proc/net/snmp
struct SnmpMetrics {
    // The number of all problematic TCP packets received
    METRIC_DEFINE_INT_LOCK_COUNTER(tcp_in_errs, MetricUnit::NOUNIT);
    // All TCP packets retransmitted
    METRIC_DEFINE_INT_LOCK_COUNTER(tcp_retrans_segs, MetricUnit::NOUNIT);
    // All received TCP packets
    METRIC_DEFINE_INT_LOCK_COUNTER(tcp_in_segs, MetricUnit::NOUNIT);
    // All send TCP packets with RST mark
    METRIC_DEFINE_INT_LOCK_COUNTER(tcp_out_segs, MetricUnit::NOUNIT);
};

struct FileDescriptorMetrics {
    METRIC_DEFINE_INT_GAUGE(fd_num_limit, MetricUnit::NOUNIT);
    METRIC_DEFINE_INT_GAUGE(fd_num_used, MetricUnit::NOUNIT);
};

SystemMetrics::SystemMetrics() {
}

SystemMetrics::~SystemMetrics() {
    // we must deregister us from registry
    if (_registry != nullptr) {
        _registry->deregister_hook(_s_hook_name);
        _registry = nullptr;
    }
    for (auto& it : _disk_metrics) {
        delete it.second;
    }
    for (auto& it : _net_metrics) {
        delete it.second;
    }
    if (_line_ptr != nullptr) {
        free(_line_ptr);
    }
}

void SystemMetrics::install(MetricRegistry* registry,
                            const std::set<std::string>& disk_devices,
                            const std::vector<std::string>& network_interfaces) {
    DCHECK(_registry == nullptr);
    if (!registry->register_hook(_s_hook_name, std::bind(&SystemMetrics::update, this))) {
        return;
    }
    _install_cpu_metrics(registry);
    _install_memory_metrics(registry);
    _install_disk_metrics(registry, disk_devices);
    _install_net_metrics(registry, network_interfaces);
    _install_fd_metrics(registry);
    _install_snmp_metrics(registry);
    _registry = registry;
}

void SystemMetrics::update() {
    _update_cpu_metrics();
    _update_memory_metrics();
    _update_disk_metrics();
    _update_net_metrics();
    _update_fd_metrics();
    _update_snmp_metrics();
}

void SystemMetrics::_install_cpu_metrics(MetricRegistry* registry) {
    _cpu_total.reset(new CpuMetrics());

    for (int i = 0; i < CpuMetrics::cpu_num_metrics; ++i) {
        registry->register_metric("cpu",
                                  MetricLabels().add("mode", CpuMetrics::cpu_metrics[i]),
                                  &_cpu_total->metrics[i]);
    }
}

#ifdef BE_TEST
const char* k_ut_stat_path;
const char* k_ut_diskstats_path;
const char* k_ut_net_dev_path;
const char* k_ut_fd_path;
const char* k_ut_net_snmp_path;
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

    if (getline(&_line_ptr, &_line_buf_size, fp) < 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
        fclose(fp);
        return;
    }

    char cpu[16];
    int64_t values[CpuMetrics::cpu_num_metrics];
    memset(values, 0, sizeof(values));
    sscanf(_line_ptr, "%15s"
           " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64 " %" PRId64 " %" PRId64
           " %" PRId64,
           cpu,
           &values[0], &values[1], &values[2],
           &values[3], &values[4], &values[5],
           &values[6], &values[7], &values[8],
           &values[9]);

    for (int i = 0; i < CpuMetrics::cpu_num_metrics; ++i) {
        _cpu_total->metrics[i].set_value(values[i]);
    }

    fclose(fp);
}

void SystemMetrics::_install_memory_metrics(MetricRegistry* registry) {
    _memory_metrics.reset(new MemoryMetrics());

    registry->register_metric("memory_allocated_bytes", &_memory_metrics->allocated_bytes);
}

void SystemMetrics::_update_memory_metrics() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    LOG(INFO) << "Memory tracking is not available with address sanitizer builds.";
#else
    size_t allocated_bytes = 0;
    MallocExtension::instance()->GetNumericProperty(
        "generic.current_allocated_bytes", &allocated_bytes);
    _memory_metrics->allocated_bytes.set_value(allocated_bytes);
#endif
}

void SystemMetrics::_install_disk_metrics(MetricRegistry* registry,
                                          const std::set<std::string>& devices) {
    for (auto& disk : devices) {
        DiskMetrics* metrics = new DiskMetrics();
#define REGISTER_DISK_METRIC(name) \
        registry->register_metric("disk_"#name, \
                                  MetricLabels().add("device", disk), \
                                  &metrics->name)
        REGISTER_DISK_METRIC(reads_completed);
        REGISTER_DISK_METRIC(bytes_read);
        REGISTER_DISK_METRIC(read_time_ms);
        REGISTER_DISK_METRIC(writes_completed);
        REGISTER_DISK_METRIC(bytes_written);
        REGISTER_DISK_METRIC(write_time_ms);
        REGISTER_DISK_METRIC(io_time_ms);
        REGISTER_DISK_METRIC(io_time_weigthed);
        _disk_metrics.emplace(disk, metrics);
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
    // I think 1024 is enougth for device name
    int major = 0;
    int minor = 0;
    char device[1024];
    int64_t values[11];
    while (getline(&_line_ptr, &_line_buf_size, fp) > 0) {
        memset(values, 0, sizeof(values));
        int num = sscanf(_line_ptr, "%d %d %1023s"
                         " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64 " %" PRId64
                         " %" PRId64 " %" PRId64,
                         &major, &minor, device,
                         &values[0], &values[1], &values[2],
                         &values[3], &values[4], &values[5],
                         &values[6], &values[7], &values[8],
                         &values[9], &values[10]);
        if (num < 4) {
            continue;
        }
        auto it = _disk_metrics.find(device);
        if (it == std::end(_disk_metrics)) {
            continue;
        }
        // update disk metrics
        // reads_completed: 4 reads completed successfully
        it->second->reads_completed.set_value(values[0]);
        // bytes_read: 6 sectors read * 512; 5 reads merged is ignored
        it->second->bytes_read.set_value(values[2] * 512);
        // read_time_ms: 7 time spent reading (ms)
        it->second->read_time_ms.set_value(values[3]);
        // writes_completed: 8 writes completed
        it->second->writes_completed.set_value(values[4]);
        // bytes_written: 10 sectors write * 512; 9 writes merged is ignored
        it->second->bytes_written.set_value(values[6] * 512);
        // write_time_ms: 11 time spent writing (ms)
        it->second->write_time_ms.set_value(values[7]);
        // io_time_ms: 13 time spent doing I/Os (ms)
        it->second->io_time_ms.set_value(values[9]);
        // io_time_weigthed: 14 - weighted time spent doing I/Os (ms)
        it->second->io_time_weigthed.set_value(values[10]);
    }
    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemMetrics::_install_net_metrics(MetricRegistry* registry,
                                         const std::vector<std::string>& interfaces) {
    for (auto& net : interfaces) {
        NetMetrics* metrics = new NetMetrics();
#define REGISTER_NETWORK_METRIC(name) \
        registry->register_metric("network_"#name, \
                                  MetricLabels().add("device", net), \
                                  &metrics->name)
        REGISTER_NETWORK_METRIC(receive_bytes);
        REGISTER_NETWORK_METRIC(receive_packets);
        REGISTER_NETWORK_METRIC(send_bytes);
        REGISTER_NETWORK_METRIC(send_packets);
        _net_metrics.emplace(net, metrics);
    }
}

void SystemMetrics::_install_snmp_metrics(MetricRegistry* registry) {
    _snmp_metrics.reset(new SnmpMetrics());
#define REGISTER_SNMP_METRIC(name) \
        registry->register_metric("snmp", \
                                  MetricLabels().add("name", #name), \
                                  &_snmp_metrics->name)
    REGISTER_SNMP_METRIC(tcp_in_errs);
    REGISTER_SNMP_METRIC(tcp_retrans_segs);
    REGISTER_SNMP_METRIC(tcp_in_segs);
    REGISTER_SNMP_METRIC(tcp_out_segs);
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
        char *start = _line_ptr;
        while (isspace(*start)) {
            start++;
        }
        std::string interface(start, ptr - start);
        auto it = _net_metrics.find(interface);
        if (it == std::end(_net_metrics)) {
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
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets,
                   &send_bytes, &send_packets);
            break;
        case 2:
            // receive: bytes packets errs drop fifo frame
            // send:    bytes packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d"
                   " %" PRId64 " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_bytes, &receive_packets,
                   &send_bytes, &send_packets);
            break;
        case 1:
            // receive: packets errs drop fifo frame
            // send: packets errs drop fifo colls carrier
            sscanf(ptr,
                   " %" PRId64 " %*d %*d %*d %*d"
                   " %" PRId64 " %*d %*d %*d %*d %*d",
                   &receive_packets, &send_packets);
            break;
        default:
            break;
        }
        it->second->receive_bytes.set_value(receive_bytes);
        it->second->receive_packets.set_value(receive_packets);
        it->second->send_bytes.set_value(send_bytes);
        it->second->send_packets.set_value(send_packets);
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
    _snmp_metrics->tcp_retrans_segs.set_value(retrans_segs);
    _snmp_metrics->tcp_in_errs.set_value(in_errs);
    _snmp_metrics->tcp_in_segs.set_value(in_segs);
    _snmp_metrics->tcp_out_segs.set_value(out_segs);

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

void SystemMetrics::_install_fd_metrics(MetricRegistry* registry) {
    _fd_metrics.reset(new FileDescriptorMetrics());
    registry->register_metric("fd_num_limit", &_fd_metrics->fd_num_limit);
    registry->register_metric("fd_num_used", &_fd_metrics->fd_num_used);
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
        int num = sscanf(_line_ptr, "%" PRId64 " %" PRId64 " %" PRId64,
                         &values[0], &values[1], &values[2]);
        if (num == 3) {
            _fd_metrics->fd_num_limit.set_value(values[2]);
            _fd_metrics->fd_num_used.set_value(values[0] - values[1]);
        }
    }

    if (ferror(fp) != 0) {
        char buf[64];
        LOG(WARNING) << "getline failed, errno=" << errno
            << ", message=" << strerror_r(errno, buf, 64);
    }
    fclose(fp);
}

int64_t SystemMetrics::get_max_io_util(
        const std::map<std::string, int64_t>& lst_value, int64_t interval_sec) {
    int64_t max = 0;
    for (auto& it : _disk_metrics) {
        int64_t cur = it.second->io_time_ms.value();
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
        map->emplace(it.first, it.second->io_time_ms.value());
    }
}

void SystemMetrics::get_network_traffic(
            std::map<std::string, int64_t>* send_map,
            std::map<std::string, int64_t>* rcv_map) {
    send_map->clear();
    rcv_map->clear();
    for (auto& it : _net_metrics) {
        if (it.first == "lo") { continue; }
        send_map->emplace(it.first, it.second->send_bytes.value());
        rcv_map->emplace(it.first, it.second->receive_bytes.value());
    }
}

void SystemMetrics::get_max_net_traffic(
            const std::map<std::string, int64_t>& lst_send_map,
            const std::map<std::string, int64_t>& lst_rcv_map,
            int64_t interval_sec,
            int64_t* send_rate, int64_t* rcv_rate) {
    int64_t max_send = 0;
    int64_t max_rcv = 0;
    for (auto& it : _net_metrics) {
        int64_t cur_send = it.second->send_bytes.value();
        int64_t cur_rcv = it.second->receive_bytes.value();

        const auto find_send = lst_send_map.find(it.first);
        if (find_send != lst_send_map.end()) {
            int64_t incr = cur_send - find_send->second;
            if (incr > max_send) max_send = incr;
        }
        const auto find_rcv= lst_rcv_map.find(it.first);
        if (find_rcv != lst_rcv_map.end()) {
            int64_t incr = cur_rcv - find_rcv->second;
            if (incr > max_rcv) max_rcv = incr;
        }
    }

    *send_rate = max_send / interval_sec;
    *rcv_rate = max_rcv / interval_sec;
}
} // end namespace
