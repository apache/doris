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

#include "jvm_metrics.h"

#include <util/jni-util.h>

#include <functional>

#include "common/config.h"
#include "util/defer_op.h"
#include "util/metrics.h"

namespace doris {

#define DEFINE_JVM_SIZE_BYTES_METRIC(name, type)                                     \
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(name##_##type, MetricUnit::BYTES, "", name, \
                                         Labels({{"type", #type}}));

DEFINE_JVM_SIZE_BYTES_METRIC(jvm_heap_size_bytes, max);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_heap_size_bytes, committed);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_heap_size_bytes, used);

DEFINE_JVM_SIZE_BYTES_METRIC(jvm_non_heap_size_bytes, used);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_non_heap_size_bytes, committed);

DEFINE_JVM_SIZE_BYTES_METRIC(jvm_young_size_bytes, used);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_young_size_bytes, peak_used);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_young_size_bytes, max);

DEFINE_JVM_SIZE_BYTES_METRIC(jvm_old_size_bytes, used);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_old_size_bytes, peak_used);
DEFINE_JVM_SIZE_BYTES_METRIC(jvm_old_size_bytes, max);

#define DEFINE_JVM_THREAD_METRIC(type)                                                          \
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(jvm_thread_##type, MetricUnit::NOUNIT, "", jvm_thread, \
                                         Labels({{"type", #type}}));

DEFINE_JVM_THREAD_METRIC(count);
DEFINE_JVM_THREAD_METRIC(peak_count);
DEFINE_JVM_THREAD_METRIC(new_count);
DEFINE_JVM_THREAD_METRIC(runnable_count);
DEFINE_JVM_THREAD_METRIC(blocked_count);
DEFINE_JVM_THREAD_METRIC(waiting_count);
DEFINE_JVM_THREAD_METRIC(timed_waiting_count);
DEFINE_JVM_THREAD_METRIC(terminated_count);

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(jvm_gc_g1_young_generation_count, MetricUnit::NOUNIT, "",
                                     jvm_gc,
                                     Labels({{"name", "G1 Young generation Count"},
                                             {"type", "count"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(jvm_gc_g1_young_generation_time_ms, MetricUnit::MILLISECONDS,
                                     "", jvm_gc,
                                     Labels({{"name", "G1 Young generation Time"},
                                             {"type", "time"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(jvm_gc_g1_old_generation_count, MetricUnit::NOUNIT, "", jvm_gc,
                                     Labels({{"name", "G1 Old generation Count"},
                                             {"type", "count"}}));

DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(jvm_gc_g1_old_generation_time_ms, MetricUnit::MILLISECONDS, "",
                                     jvm_gc,
                                     Labels({{"name", "G1 Old generation Time"},
                                             {"type", "time"}}));

const char* JvmMetrics::_s_hook_name = "jvm_metrics";

JvmMetrics::JvmMetrics(MetricRegistry* registry) {
    DCHECK(registry != nullptr);
    _registry = registry;

    _server_entity = _registry->register_entity("server");
    DCHECK(_server_entity != nullptr);

    do {
        if (!doris::config::enable_jvm_monitor) {
            break;
        }
        try {
            Status st = _jvm_stats.init();
            if (!st) {
                LOG(WARNING) << "jvm Stats Init Fail. " << st.to_string();
                break;
            }
        } catch (...) {
            LOG(WARNING) << "jvm Stats Throw Exception Init Fail.";
            break;
        }
        if (!_jvm_stats.init_complete()) {
            break;
        }
        _server_entity->register_hook(_s_hook_name, std::bind(&JvmMetrics::update, this));
    } while (false);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_heap_size_bytes_max);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_heap_size_bytes_committed);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_heap_size_bytes_used);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_non_heap_size_bytes_used);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_non_heap_size_bytes_committed);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_young_size_bytes_used);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_young_size_bytes_peak_used);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_young_size_bytes_max);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_old_size_bytes_used);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_old_size_bytes_peak_used);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_old_size_bytes_max);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_peak_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_new_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_runnable_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_blocked_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_waiting_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_timed_waiting_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_thread_terminated_count);

    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_gc_g1_young_generation_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_gc_g1_young_generation_time_ms);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_gc_g1_old_generation_count);
    INT_GAUGE_METRIC_REGISTER(_server_entity, jvm_gc_g1_old_generation_time_ms);
}

JvmMetrics::~JvmMetrics() {
    if (_jvm_stats.init_complete()) {
        _server_entity->deregister_hook(_s_hook_name);
    }
}

void JvmMetrics::update() {
    // If enable_jvm_monitor is false, the jvm stats object is not initialized. call jvm_stats.refresh() may core.
    if (!doris::config::enable_jvm_monitor) {
        return;
    }
    static long fail_count = 0;
    if (fail_count >= 30) {
        return;
    }

    try {
        Status st = _jvm_stats.refresh(this);
        if (!st) {
            fail_count++;
            LOG(WARNING) << "Jvm Stats update Fail! " << st.to_string();
        } else {
            fail_count = 0;
        }
    } catch (...) {
        LOG(WARNING) << "Jvm Stats update throw Exception!";
        fail_count++;
    }

    //When 30 consecutive exceptions occur, turn off jvm information collection.
    if (fail_count >= 30) {
        LOG(WARNING) << "Jvm Stats CLOSE!";
        jvm_heap_size_bytes_max->set_value(0);
        jvm_heap_size_bytes_committed->set_value(0);
        jvm_heap_size_bytes_used->set_value(0);

        jvm_non_heap_size_bytes_used->set_value(0);
        jvm_non_heap_size_bytes_committed->set_value(0);

        jvm_young_size_bytes_used->set_value(0);
        jvm_young_size_bytes_peak_used->set_value(0);
        jvm_young_size_bytes_max->set_value(0);

        jvm_old_size_bytes_used->set_value(0);
        jvm_old_size_bytes_peak_used->set_value(0);
        jvm_old_size_bytes_max->set_value(0);

        jvm_thread_count->set_value(0);
        jvm_thread_peak_count->set_value(0);
        jvm_thread_new_count->set_value(0);
        jvm_thread_runnable_count->set_value(0);
        jvm_thread_blocked_count->set_value(0);
        jvm_thread_waiting_count->set_value(0);
        jvm_thread_timed_waiting_count->set_value(0);
        jvm_thread_terminated_count->set_value(0);

        jvm_gc_g1_young_generation_count->set_value(0);
        jvm_gc_g1_young_generation_time_ms->set_value(0);
        jvm_gc_g1_old_generation_count->set_value(0);
        jvm_gc_g1_old_generation_time_ms->set_value(0);
    }
}

Status JvmStats::init() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/management/ManagementFactory",
                                          &_managementFactoryClass));
    RETURN_IF_ERROR(_managementFactoryClass.get_static_method(
            env, "getMemoryMXBean", "()Ljava/lang/management/MemoryMXBean;",
            &_getMemoryMXBeanMethod));

    RETURN_IF_ERROR(
            Jni::Util::find_class(env, "java/lang/management/MemoryUsage", &_memoryUsageClass));
    RETURN_IF_ERROR(
            _memoryUsageClass.get_method(env, "getUsed", "()J", &_getMemoryUsageUsedMethod));
    RETURN_IF_ERROR(_memoryUsageClass.get_method(env, "getCommitted", "()J",
                                                 &_getMemoryUsageCommittedMethod));
    RETURN_IF_ERROR(_memoryUsageClass.get_method(env, "getMax", "()J", &_getMemoryUsageMaxMethod));

    RETURN_IF_ERROR(
            Jni::Util::find_class(env, "java/lang/management/MemoryMXBean", &_memoryMXBeanClass));
    RETURN_IF_ERROR(_memoryMXBeanClass.get_method(env, "getHeapMemoryUsage",
                                                  "()Ljava/lang/management/MemoryUsage;",
                                                  &_getHeapMemoryUsageMethod));
    RETURN_IF_ERROR(_memoryMXBeanClass.get_method(env, "getNonHeapMemoryUsage",
                                                  "()Ljava/lang/management/MemoryUsage;",
                                                  &_getNonHeapMemoryUsageMethod));

    RETURN_IF_ERROR(_managementFactoryClass.get_static_method(
            env, "getMemoryPoolMXBeans", "()Ljava/util/List;", &_getMemoryPoolMXBeansMethod));

    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/util/List", &_listClass));
    RETURN_IF_ERROR(_listClass.get_method(env, "size", "()I", &_getListSizeMethod));
    RETURN_IF_ERROR(
            _listClass.get_method(env, "get", "(I)Ljava/lang/Object;", &_getListUseIndexMethod));

    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/management/MemoryPoolMXBean",
                                          &_memoryPoolMXBeanClass));
    RETURN_IF_ERROR(_memoryPoolMXBeanClass.get_method(env, "getUsage",
                                                      "()Ljava/lang/management/MemoryUsage;",
                                                      &_getMemoryPoolMXBeanUsageMethod));
    RETURN_IF_ERROR(_memoryPoolMXBeanClass.get_method(env, "getPeakUsage",
                                                      "()Ljava/lang/management/MemoryUsage;",
                                                      &_getMemoryPoolMXBeanPeakMethod));
    RETURN_IF_ERROR(_memoryPoolMXBeanClass.get_method(env, "getName", "()Ljava/lang/String;",
                                                      &_getMemoryPoolMXBeanNameMethod));

    RETURN_IF_ERROR(_managementFactoryClass.get_static_method(
            env, "getThreadMXBean", "()Ljava/lang/management/ThreadMXBean;",
            &_getThreadMXBeanMethod));
    RETURN_IF_ERROR(_managementFactoryClass.get_static_method(env, "getGarbageCollectorMXBeans",
                                                              "()Ljava/util/List;",
                                                              &_getGarbageCollectorMXBeansMethod));

    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/management/GarbageCollectorMXBean",
                                          &_garbageCollectorMXBeanClass));
    RETURN_IF_ERROR(_garbageCollectorMXBeanClass.get_method(env, "getName", "()Ljava/lang/String;",
                                                            &_getGCNameMethod));
    RETURN_IF_ERROR(_garbageCollectorMXBeanClass.get_method(env, "getCollectionCount", "()J",
                                                            &_getGCCollectionCountMethod));
    RETURN_IF_ERROR(_garbageCollectorMXBeanClass.get_method(env, "getCollectionTime", "()J",
                                                            &_getGCCollectionTimeMethod));

    RETURN_IF_ERROR(
            Jni::Util::find_class(env, "java/lang/management/ThreadMXBean", &_threadMXBeanClass));
    RETURN_IF_ERROR(
            _threadMXBeanClass.get_method(env, "getAllThreadIds", "()[J", &_getAllThreadIdsMethod));
    RETURN_IF_ERROR(_threadMXBeanClass.get_method(env, "getThreadInfo",
                                                  "([JI)[Ljava/lang/management/ThreadInfo;",
                                                  &_getThreadInfoMethod));
    RETURN_IF_ERROR(_threadMXBeanClass.get_method(env, "getPeakThreadCount", "()I",
                                                  &_getPeakThreadCountMethod));

    RETURN_IF_ERROR(
            Jni::Util::find_class(env, "java/lang/management/ThreadInfo", &_threadInfoClass));
    RETURN_IF_ERROR(_threadInfoClass.get_method(env, "getThreadState", "()Ljava/lang/Thread$State;",
                                                &_getThreadStateMethod));

    RETURN_IF_ERROR(Jni::Util::find_class(env, "java/lang/Thread$State", &_threadStateClass));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "NEW", "Ljava/lang/Thread$State;", &_newThreadStateObj));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "RUNNABLE", "Ljava/lang/Thread$State;", &_runnableThreadStateObj));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "BLOCKED", "Ljava/lang/Thread$State;", &_blockedThreadStateObj));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "WAITING", "Ljava/lang/Thread$State;", &_waitingThreadStateObj));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "TIMED_WAITING", "Ljava/lang/Thread$State;", &_timedWaitingThreadStateObj));
    RETURN_IF_ERROR(_threadStateClass.get_static_object_field(
            env, "TERMINATED", "Ljava/lang/Thread$State;", &_terminatedThreadStateObj));

    _init_complete = true;
    LOG(INFO) << "Start JVM monitoring.";
    return Status::OK();
}

Status JvmStats::refresh(JvmMetrics* jvm_metrics) const {
    if (!_init_complete) {
        return Status::InternalError("Jvm Stats not init complete.");
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    Jni::LocalObject memoryMXBeanObj;
    RETURN_IF_ERROR(_managementFactoryClass.call_static_object_method(env, _getMemoryMXBeanMethod)
                            .call(&memoryMXBeanObj));

    Jni::LocalObject heapMemoryUsageObj;
    RETURN_IF_ERROR(memoryMXBeanObj.call_object_method(env, _getHeapMemoryUsageMethod)
                            .call(&heapMemoryUsageObj));

    jlong heapMemoryUsed = 0;
    RETURN_IF_ERROR(heapMemoryUsageObj.call_long_method(env, _getMemoryUsageUsedMethod)
                            .call(&heapMemoryUsed));

    jlong heapMemoryCommitted = 0;
    RETURN_IF_ERROR(heapMemoryUsageObj.call_long_method(env, _getMemoryUsageCommittedMethod)
                            .call(&heapMemoryCommitted));

    jlong heapMemoryMax = 0;
    RETURN_IF_ERROR(heapMemoryUsageObj.call_long_method(env, _getMemoryUsageMaxMethod)
                            .call(&heapMemoryMax));

    jvm_metrics->jvm_heap_size_bytes_used->set_value(heapMemoryUsed < 0 ? 0 : heapMemoryUsed);
    jvm_metrics->jvm_heap_size_bytes_committed->set_value(
            heapMemoryCommitted < 0 ? 0 : heapMemoryCommitted);
    jvm_metrics->jvm_heap_size_bytes_max->set_value(heapMemoryMax < 0 ? 0 : heapMemoryMax);

    Jni::LocalObject nonHeapMemoryUsageObj;
    RETURN_IF_ERROR(memoryMXBeanObj.call_object_method(env, _getNonHeapMemoryUsageMethod)
                            .call(&nonHeapMemoryUsageObj));

    jlong nonHeapMemoryCommitted = 0;
    RETURN_IF_ERROR(nonHeapMemoryUsageObj.call_long_method(env, _getMemoryUsageCommittedMethod)
                            .call(&nonHeapMemoryCommitted));

    jlong nonHeapMemoryUsed = 0;
    RETURN_IF_ERROR(nonHeapMemoryUsageObj.call_long_method(env, _getMemoryUsageUsedMethod)
                            .call(&nonHeapMemoryUsed));

    jvm_metrics->jvm_non_heap_size_bytes_committed->set_value(
            nonHeapMemoryCommitted < 0 ? 0 : nonHeapMemoryCommitted);
    jvm_metrics->jvm_non_heap_size_bytes_used->set_value(nonHeapMemoryUsed < 0 ? 0
                                                                               : nonHeapMemoryUsed);

    Jni::LocalObject memoryPoolMXBeansList;
    RETURN_IF_ERROR(
            _managementFactoryClass.call_static_object_method(env, _getMemoryPoolMXBeansMethod)
                    .call(&memoryPoolMXBeansList));

    jint beanSize = 0;
    RETURN_IF_ERROR(memoryPoolMXBeansList.call_int_method(env, _getListSizeMethod).call(&beanSize));

    for (int i = 0; i < beanSize; ++i) {
        Jni::LocalObject memoryPoolMXBean;
        RETURN_IF_ERROR(memoryPoolMXBeansList.call_object_method(env, _getListUseIndexMethod)
                                .with_arg(i)
                                .call(&memoryPoolMXBean));

        Jni::LocalObject usageObject;
        RETURN_IF_ERROR(memoryPoolMXBean.call_object_method(env, _getMemoryPoolMXBeanUsageMethod)
                                .call(&usageObject));

        jlong used = 0;
        RETURN_IF_ERROR(usageObject.call_long_method(env, _getMemoryUsageUsedMethod).call(&used));

        jlong max = 0;
        RETURN_IF_ERROR(usageObject.call_long_method(env, _getMemoryUsageMaxMethod).call(&max));

        Jni::LocalObject peakUsageObject;
        RETURN_IF_ERROR(memoryPoolMXBean.call_object_method(env, _getMemoryPoolMXBeanPeakMethod)
                                .call(&peakUsageObject));

        jlong peakUsed = 0;
        RETURN_IF_ERROR(
                peakUsageObject.call_long_method(env, _getMemoryUsageUsedMethod).call(&peakUsed));

        Jni::LocalString name;
        RETURN_IF_ERROR(memoryPoolMXBean.call_object_method(env, _getMemoryPoolMXBeanNameMethod)
                                .call(&name));

        Jni::LocalStringBufferGuard nameStr;
        RETURN_IF_ERROR(name.get_string_chars(env, &nameStr));
        if (nameStr.get() != nullptr) {
            auto it = _memoryPoolName.find(nameStr.get());
            if (it == _memoryPoolName.end()) {
                continue;
            }
            if (it->second == memoryPoolNameEnum::YOUNG) {
                jvm_metrics->jvm_young_size_bytes_used->set_value(used < 0 ? 0 : used);
                jvm_metrics->jvm_young_size_bytes_peak_used->set_value(peakUsed < 0 ? 0 : peakUsed);
                jvm_metrics->jvm_young_size_bytes_max->set_value(max < 0 ? 0 : max);

            } else if (it->second == memoryPoolNameEnum::OLD) {
                jvm_metrics->jvm_old_size_bytes_used->set_value(used < 0 ? 0 : used);
                jvm_metrics->jvm_old_size_bytes_peak_used->set_value(peakUsed < 0 ? 0 : peakUsed);
                jvm_metrics->jvm_old_size_bytes_max->set_value(max < 0 ? 0 : max);
            }
        }
    }

    Jni::LocalObject threadMXBean;
    RETURN_IF_ERROR(_managementFactoryClass.call_static_object_method(env, _getThreadMXBeanMethod)
                            .call(&threadMXBean));

    Jni::LocalArray threadIds;
    RETURN_IF_ERROR(threadMXBean.call_object_method(env, _getAllThreadIdsMethod).call(&threadIds));

    jsize threadCount = 0;
    RETURN_IF_ERROR(threadIds.get_length(env, &threadCount));

    Jni::LocalArray threadInfos;
    RETURN_IF_ERROR(threadMXBean.call_object_method(env, _getThreadInfoMethod)
                            .with_arg(threadIds)
                            .with_arg(0)
                            .call(&threadInfos));

    int threadsNew = 0, threadsRunnable = 0, threadsBlocked = 0, threadsWaiting = 0,
        threadsTimedWaiting = 0, threadsTerminated = 0;

    jint peakThreadCount = 0;
    RETURN_IF_ERROR(
            threadMXBean.call_int_method(env, _getPeakThreadCountMethod).call(&peakThreadCount));

    jvm_metrics->jvm_thread_peak_count->set_value(peakThreadCount < 0 ? 0 : peakThreadCount);
    jvm_metrics->jvm_thread_count->set_value(threadCount < 0 ? 0 : threadCount);

    for (int i = 0; i < threadCount; i++) {
        Jni::LocalObject threadInfo;
        RETURN_IF_ERROR(threadInfos.get_object_array_element(env, i, &threadInfo));
        if (threadInfo.uninitialized()) {
            continue;
        }
        Jni::LocalObject threadState;
        RETURN_IF_ERROR(
                threadInfo.call_object_method(env, _getThreadStateMethod).call(&threadState));

        if (threadState.equal(env, _newThreadStateObj)) {
            threadsNew++;
        } else if (threadState.equal(env, _runnableThreadStateObj)) {
            threadsRunnable++;
        } else if (threadState.equal(env, _blockedThreadStateObj)) {
            threadsBlocked++;
        } else if (threadState.equal(env, _waitingThreadStateObj)) {
            threadsWaiting++;
        } else if (threadState.equal(env, _timedWaitingThreadStateObj)) {
            threadsTimedWaiting++;
        } else if (threadState.equal(env, _terminatedThreadStateObj)) {
            threadsTerminated++;
        }
    }

    jvm_metrics->jvm_thread_new_count->set_value(threadsNew < 0 ? 0 : threadsNew);
    jvm_metrics->jvm_thread_runnable_count->set_value(threadsRunnable < 0 ? 0 : threadsRunnable);
    jvm_metrics->jvm_thread_blocked_count->set_value(threadsBlocked < 0 ? 0 : threadsBlocked);
    jvm_metrics->jvm_thread_waiting_count->set_value(threadsWaiting < 0 ? 0 : threadsWaiting);
    jvm_metrics->jvm_thread_timed_waiting_count->set_value(
            threadsTimedWaiting < 0 ? 0 : threadsTimedWaiting);
    jvm_metrics->jvm_thread_terminated_count->set_value(threadsTerminated < 0 ? 0
                                                                              : threadsTerminated);

    Jni::LocalObject gcMXBeansList;
    RETURN_IF_ERROR(_managementFactoryClass
                            .call_static_object_method(env, _getGarbageCollectorMXBeansMethod)
                            .call(&gcMXBeansList));
    jint numCollectors = 0;
    RETURN_IF_ERROR(gcMXBeansList.call_int_method(env, _getListSizeMethod).call(&numCollectors));

    for (int i = 0; i < numCollectors; i++) {
        Jni::LocalObject gcMXBean;
        RETURN_IF_ERROR(gcMXBeansList.call_object_method(env, _getListUseIndexMethod)
                                .with_arg(i)
                                .call(&gcMXBean));

        Jni::LocalString gcName;
        RETURN_IF_ERROR(gcMXBean.call_object_method(env, _getGCNameMethod).call(&gcName));

        jlong gcCollectionCount = 0;
        RETURN_IF_ERROR(gcMXBean.call_long_method(env, _getGCCollectionCountMethod)
                                .call(&gcCollectionCount));

        jlong gcCollectionTime = 0;
        RETURN_IF_ERROR(
                gcMXBean.call_long_method(env, _getGCCollectionTimeMethod).call(&gcCollectionTime));

        Jni::LocalStringBufferGuard gcNameStr;
        RETURN_IF_ERROR(gcName.get_string_chars(env, &gcNameStr));

        if (gcNameStr.get() != nullptr) {
            if (strcmp(gcNameStr.get(), "G1 Young Generation") == 0) {
                jvm_metrics->jvm_gc_g1_young_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_young_generation_time_ms->set_value(gcCollectionTime);

            } else {
                jvm_metrics->jvm_gc_g1_old_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_old_generation_time_ms->set_value(gcCollectionTime);
            }
        }
    }

    return Status::OK();
}
JvmStats::~JvmStats() {}

} // namespace doris
