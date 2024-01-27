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

std::string SystemBvarMetrics::to_prometheus(const std::string& registry_name) const {
    std::stringstream ss;
    for (auto& entities : entities_map_) {
        if (entities.second.empty()) {
            continue;
        }
        int count = 0;
        for (auto& entity : entities.second) {
            if (!count) {
                ss << "# TYPE " << registry_name << "_" << entity->get_name() << " " << entity->get_type() << "\n";
                count ++;
            }
            ss << entity->to_prometheus(registry_name);
        }
    }
    return ss.str();
}

SystemBvarMetrics::SystemBvarMetrics() {
    install_cpu_metrics();
}

void SystemBvarMetrics::install_cpu_metrics() {
    get_cpu_name();
    for (auto cpu_name : cpu_names_) {
        auto cpu_entity = std::make_shared<BvarMetricEntity>("cpu", BvarMetricType::COUNTER);
        register_cpu_metrics(cpu_entity.get(), cpu_name);
        entities_map_["cpu"].push_back(cpu_entity);
    }
}

void SystemBvarMetrics::register_cpu_metrics(BvarMetricEntity* entity, const std::string& cpu_name) {
    BvarAdderMetric<int64_t> cpu_user(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_user", "", 
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "user"}}), false);
    BvarAdderMetric<int64_t> cpu_nice(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_nice", "", 
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "nice"}}), false);
    BvarAdderMetric<int64_t> cpu_system(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_system", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "system"}}), false);
    BvarAdderMetric<int64_t> cpu_idle(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_idle", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "idle"}}), false);
    BvarAdderMetric<int64_t> cpu_iowait(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_iowait", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "iowait"}}), false);
    BvarAdderMetric<int64_t> cpu_irq(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_irq", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "irq"}}), false);
    BvarAdderMetric<int64_t> cpu_soft_irq(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_soft_irq", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "soft_irq"}}), false);
    BvarAdderMetric<int64_t> cpu_steal(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_steal", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "steal"}}), false);
    BvarAdderMetric<int64_t> cpu_guest(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_guest", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "guest"}}), false);
    BvarAdderMetric<int64_t> cpu_guest_nice(BvarMetricType::COUNTER, BvarMetricUnit::PERCENT, "cpu_guest_nice", "",
                                     "cpu", Labels({{"device", cpu_name}, {"mode", "guest_nice"}}), false);
    entity->register_metric("cpu_user", cpu_user);
    entity->register_metric("cpu_nice", cpu_nice);
    entity->register_metric("cpu_system", cpu_system);
    entity->register_metric("cpu_idle", cpu_idle);
    entity->register_metric("cpu_iowait", cpu_iowait);
    entity->register_metric("cpu_irq", cpu_irq);
    entity->register_metric("cpu_soft_irq", cpu_soft_irq);
    entity->register_metric("cpu_steal", cpu_steal);
    entity->register_metric("cpu_guest", cpu_guest);
    entity->register_metric("cpu_guest_nice", cpu_guest_nice);
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