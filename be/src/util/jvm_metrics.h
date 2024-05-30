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

#pragma once

#include "jni.h"
#include "util/jni-util.h"
#include "util/metrics.h"

namespace doris {

class JvmMetrics;

class JvmStats {
private:
    JNIEnv* env = nullptr;
    jclass _managementFactoryClass = nullptr;
    jmethodID _getMemoryMXBeanMethod = nullptr;
    jclass _memoryUsageClass = nullptr;
    jclass _memoryMXBeanClass = nullptr;
    jmethodID _getHeapMemoryUsageMethod = nullptr;
    jmethodID _getNonHeapMemoryUsageMethod = nullptr;
    jmethodID _getMemoryUsageUsedMethod = nullptr;
    jmethodID _getMemoryUsageCommittedMethod = nullptr;
    jmethodID _getMemoryUsageMaxMethod = nullptr;

    jmethodID _getMemoryPoolMXBeansMethod = nullptr;

    jclass _listClass = nullptr;
    jmethodID _getListSizeMethod = nullptr;
    jmethodID _getListUseIndexMethod = nullptr;

    jclass _memoryPoolMXBeanClass = nullptr;
    jmethodID _getMemoryPoolMXBeanUsageMethod = nullptr;

    jmethodID _getMemoryPollMXBeanPeakMethod = nullptr;
    jmethodID _getMemoryPollMXBeanNameMethod = nullptr;

    enum memoryPoolNameEnum { YOUNG, SURVIVOR, OLD };
    const std::map<std::string, memoryPoolNameEnum> _memoryPoolName = {
            {"Eden Space", YOUNG},
            {"PS Eden Space", YOUNG},
            {"Par Eden Space", YOUNG},
            {"G1 Eden Space", YOUNG},

            {"Survivor Space", SURVIVOR},
            {"PS Survivor Space", SURVIVOR},
            {"Par Survivor Space", SURVIVOR},
            {"G1 Survivor Space", SURVIVOR},

            {"Tenured Gen", OLD},
            {"PS Old Gen", OLD},
            {"CMS Old Gen", OLD},
            {"G1 Old Gen", OLD},

    };

    jmethodID _getThreadMXBeanMethod = nullptr;
    jclass _threadMXBeanClass = nullptr;
    jmethodID _getAllThreadIdsMethod = nullptr;
    jmethodID _getThreadInfoMethod = nullptr;
    jclass _threadInfoClass = nullptr;

    jmethodID _getPeakThreadCountMethod = nullptr;

    jmethodID _getThreadStateMethod = nullptr;
    jclass _threadStateClass = nullptr;

    jobject _newThreadStateObj = nullptr;
    jobject _runnableThreadStateObj = nullptr;
    jobject _blockedThreadStateObj = nullptr;
    jobject _waitingThreadStateObj = nullptr;
    jobject _timedWaitingThreadStateObj = nullptr;
    jobject _terminatedThreadStateObj = nullptr;

    jclass _garbageCollectorMXBeanClass = nullptr;
    jmethodID _getGCNameMethod = nullptr;
    jmethodID _getGarbageCollectorMXBeansMethod = nullptr;
    jmethodID _getGCCollectionCountMethod = nullptr;
    jmethodID _getGCCollectionTimeMethod = nullptr;

    bool _init_complete = false;

public:
    //    JvmStats(JNIEnv* ENV);
    void init(JNIEnv* ENV);
    bool init_complete() const { return _init_complete; }
    void set_complete(bool val) { _init_complete = val; }
    void refresh(JvmMetrics* jvm_metrics);
    ~JvmStats();
};

class JvmMetrics {
public:
    JvmMetrics(MetricRegistry* registry, JNIEnv* env);
    ~JvmMetrics() = default;
    void update();

    IntGauge* jvm_heap_size_bytes_max = nullptr;
    IntGauge* jvm_heap_size_bytes_committed = nullptr;
    IntGauge* jvm_heap_size_bytes_used = nullptr;

    IntGauge* jvm_non_heap_size_bytes_used = nullptr;
    IntGauge* jvm_non_heap_size_bytes_committed = nullptr;

    IntGauge* jvm_young_size_bytes_used = nullptr;
    IntGauge* jvm_young_size_bytes_peak_used = nullptr;
    IntGauge* jvm_young_size_bytes_max = nullptr;

    IntGauge* jvm_old_size_bytes_used = nullptr;
    IntGauge* jvm_old_size_bytes_peak_used = nullptr;
    IntGauge* jvm_old_size_bytes_max = nullptr;

    IntGauge* jvm_thread_count = nullptr;
    IntGauge* jvm_thread_peak_count = nullptr;
    IntGauge* jvm_thread_new_count = nullptr;
    IntGauge* jvm_thread_runnable_count = nullptr;
    IntGauge* jvm_thread_blocked_count = nullptr;
    IntGauge* jvm_thread_waiting_count = nullptr;
    IntGauge* jvm_thread_timed_waiting_count = nullptr;
    IntGauge* jvm_thread_terminated_count = nullptr;

    IntGauge* jvm_gc_g1_young_generation_count = nullptr;
    IntGauge* jvm_gc_g1_young_generation_time_ms = nullptr;
    IntGauge* jvm_gc_g1_old_generation_count = nullptr;
    IntGauge* jvm_gc_g1_old_generation_time_ms = nullptr;

private:
    JvmStats _jvm_stats;
    std::shared_ptr<MetricEntity> _server_entity;
    static const char* _s_hook_name;
    MetricRegistry* _registry = nullptr;
};

} // namespace doris
