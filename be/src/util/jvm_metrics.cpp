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

JvmMetrics::JvmMetrics(MetricRegistry* registry, JNIEnv* env) {
    DCHECK(registry != nullptr);
    _registry = registry;

    _server_entity = _registry->register_entity("server");
    DCHECK(_server_entity != nullptr);

    do {
        if (!doris::config::enable_jvm_monitor) {
            break;
        }
        try {
            _jvm_stats.init(env);
        } catch (...) {
            LOG(WARNING) << "JVM STATS INIT FAIL";
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

void JvmMetrics::update() {
    static long fail_count = 0;
    bool have_exception = false;
    try {
        _jvm_stats.refresh(this);
    } catch (...) {
        have_exception = true;
        LOG(WARNING) << "JVM MONITOR UPDATE FAIL!";
        fail_count++;
    }

    //When 30 consecutive exceptions occur, turn off jvm information collection.
    if (!have_exception) {
        fail_count = 0;
    }
    if (fail_count >= 30) {
        LOG(WARNING) << "JVM MONITOR CLOSE!";
        _jvm_stats.set_complete(false);
        _server_entity->deregister_hook(_s_hook_name);

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

void JvmStats::init(JNIEnv* ENV) {
    env = ENV;
    _managementFactoryClass = env->FindClass("java/lang/management/ManagementFactory");
    if (_managementFactoryClass == nullptr) {
        LOG(WARNING)
                << "Class java/lang/management/ManagementFactory Not Find.JVM monitoring fails.";
        return;
    }

    _getMemoryMXBeanMethod = env->GetStaticMethodID(_managementFactoryClass, "getMemoryMXBean",
                                                    "()Ljava/lang/management/MemoryMXBean;");

    _memoryUsageClass = env->FindClass("java/lang/management/MemoryUsage");
    if (_memoryUsageClass == nullptr) {
        LOG(WARNING) << "Class java/lang/management/MemoryUsage Not Find.JVM monitoring fails.";
        return;
    }
    _getMemoryUsageUsedMethod = env->GetMethodID(_memoryUsageClass, "getUsed", "()J");
    _getMemoryUsageCommittedMethod = env->GetMethodID(_memoryUsageClass, "getCommitted", "()J");
    _getMemoryUsageMaxMethod = env->GetMethodID(_memoryUsageClass, "getMax", "()J");

    _memoryMXBeanClass = env->FindClass("java/lang/management/MemoryMXBean");
    if (_memoryMXBeanClass == nullptr) {
        LOG(WARNING) << "Class java/lang/management/MemoryMXBean Not Find.JVM monitoring fails.";
        return;
    }
    _getHeapMemoryUsageMethod = env->GetMethodID(_memoryMXBeanClass, "getHeapMemoryUsage",
                                                 "()Ljava/lang/management/MemoryUsage;");
    _getNonHeapMemoryUsageMethod = env->GetMethodID(_memoryMXBeanClass, "getNonHeapMemoryUsage",
                                                    "()Ljava/lang/management/MemoryUsage;");

    _getMemoryPoolMXBeansMethod = env->GetStaticMethodID(
            _managementFactoryClass, "getMemoryPoolMXBeans", "()Ljava/util/List;");

    _listClass = env->FindClass("java/util/List");
    if (_listClass == nullptr) {
        LOG(WARNING) << "Class java/util/List Not Find.JVM monitoring fails.";
        return;
    }
    _getListSizeMethod = env->GetMethodID(_listClass, "size", "()I");
    _getListUseIndexMethod = env->GetMethodID(_listClass, "get", "(I)Ljava/lang/Object;");

    _memoryPoolMXBeanClass = env->FindClass("java/lang/management/MemoryPoolMXBean");
    if (_memoryPoolMXBeanClass == nullptr) {
        LOG(WARNING)
                << "Class java/lang/management/MemoryPoolMXBean Not Find.JVM monitoring fails.";
        return;
    }
    _getMemoryPoolMXBeanUsageMethod = env->GetMethodID(_memoryPoolMXBeanClass, "getUsage",
                                                       "()Ljava/lang/management/MemoryUsage;");
    _getMemoryPollMXBeanPeakMethod = env->GetMethodID(_memoryPoolMXBeanClass, "getPeakUsage",
                                                      "()Ljava/lang/management/MemoryUsage;");
    _getMemoryPollMXBeanNameMethod =
            env->GetMethodID(_memoryPoolMXBeanClass, "getName", "()Ljava/lang/String;");

    _getThreadMXBeanMethod = env->GetStaticMethodID(_managementFactoryClass, "getThreadMXBean",
                                                    "()Ljava/lang/management/ThreadMXBean;");

    _getGarbageCollectorMXBeansMethod = env->GetStaticMethodID(
            _managementFactoryClass, "getGarbageCollectorMXBeans", "()Ljava/util/List;");

    _garbageCollectorMXBeanClass = env->FindClass("java/lang/management/GarbageCollectorMXBean");
    if (_garbageCollectorMXBeanClass == nullptr) {
        LOG(WARNING) << "Class java/lang/management/GarbageCollectorMXBean Not Find.JVM monitoring "
                        "fails.";
        return;
    }
    _getGCNameMethod =
            env->GetMethodID(_garbageCollectorMXBeanClass, "getName", "()Ljava/lang/String;");
    _getGCCollectionCountMethod =
            env->GetMethodID(_garbageCollectorMXBeanClass, "getCollectionCount", "()J");
    _getGCCollectionTimeMethod =
            env->GetMethodID(_garbageCollectorMXBeanClass, "getCollectionTime", "()J");

    _threadMXBeanClass = env->FindClass("java/lang/management/ThreadMXBean");
    if (_threadMXBeanClass == nullptr) {
        LOG(WARNING) << "Class java/lang/management/ThreadMXBean Not Find.JVM monitoring fails.";
        return;
    }
    _getAllThreadIdsMethod = env->GetMethodID(_threadMXBeanClass, "getAllThreadIds", "()[J");
    _getThreadInfoMethod = env->GetMethodID(_threadMXBeanClass, "getThreadInfo",
                                            "([JI)[Ljava/lang/management/ThreadInfo;");
    _getPeakThreadCountMethod = env->GetMethodID(_threadMXBeanClass, "getPeakThreadCount", "()I");

    _threadInfoClass = env->FindClass("java/lang/management/ThreadInfo");
    if (_threadInfoClass == nullptr) {
        LOG(WARNING) << "Class java/lang/management/ThreadInfo Not Find.JVM monitoring fails.";
        return;
    }

    _getThreadStateMethod =
            env->GetMethodID(_threadInfoClass, "getThreadState", "()Ljava/lang/Thread$State;");

    _threadStateClass = env->FindClass("java/lang/Thread$State");
    if (_threadStateClass == nullptr) {
        LOG(WARNING) << "Class java/lang/Thread$State Not Find.JVM monitoring fails.";
        return;
    }

    jfieldID newThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "NEW", "Ljava/lang/Thread$State;");
    jfieldID runnableThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "RUNNABLE", "Ljava/lang/Thread$State;");
    jfieldID blockedThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "BLOCKED", "Ljava/lang/Thread$State;");
    jfieldID waitingThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "WAITING", "Ljava/lang/Thread$State;");
    jfieldID timedWaitingThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "TIMED_WAITING", "Ljava/lang/Thread$State;");
    jfieldID terminatedThreadFieldID =
            env->GetStaticFieldID(_threadStateClass, "TERMINATED", "Ljava/lang/Thread$State;");

    _newThreadStateObj = env->GetStaticObjectField(_threadStateClass, newThreadFieldID);
    _runnableThreadStateObj = env->GetStaticObjectField(_threadStateClass, runnableThreadFieldID);
    _blockedThreadStateObj = env->GetStaticObjectField(_threadStateClass, blockedThreadFieldID);
    _waitingThreadStateObj = env->GetStaticObjectField(_threadStateClass, waitingThreadFieldID);
    _timedWaitingThreadStateObj =
            env->GetStaticObjectField(_threadStateClass, timedWaitingThreadFieldID);
    _terminatedThreadStateObj =
            env->GetStaticObjectField(_threadStateClass, terminatedThreadFieldID);

    LOG(INFO) << "Start JVM monitoring.";

    _init_complete = true;
    return;
}

void JvmStats::refresh(JvmMetrics* jvm_metrics) {
    if (!_init_complete) {
        return;
    }

    Status st = JniUtil::GetJNIEnv(&env);
    if (!st.ok()) {
        LOG(WARNING) << "JVM STATS GET JNI ENV FAIL";
        return;
    }

    jobject memoryMXBeanObj =
            env->CallStaticObjectMethod(_managementFactoryClass, _getMemoryMXBeanMethod);

    jobject heapMemoryUsageObj = env->CallObjectMethod(memoryMXBeanObj, _getHeapMemoryUsageMethod);

    jlong heapMemoryUsed = env->CallLongMethod(heapMemoryUsageObj, _getMemoryUsageUsedMethod);
    jlong heapMemoryCommitted =
            env->CallLongMethod(heapMemoryUsageObj, _getMemoryUsageCommittedMethod);
    jlong heapMemoryMax = env->CallLongMethod(heapMemoryUsageObj, _getMemoryUsageMaxMethod);

    jvm_metrics->jvm_heap_size_bytes_used->set_value(heapMemoryUsed < 0 ? 0 : heapMemoryUsed);
    jvm_metrics->jvm_heap_size_bytes_committed->set_value(
            heapMemoryCommitted < 0 ? 0 : heapMemoryCommitted);
    jvm_metrics->jvm_heap_size_bytes_max->set_value(heapMemoryMax < 0 ? 0 : heapMemoryMax);

    jobject nonHeapMemoryUsageObj =
            env->CallObjectMethod(memoryMXBeanObj, _getNonHeapMemoryUsageMethod);

    jlong nonHeapMemoryCommitted =
            env->CallLongMethod(nonHeapMemoryUsageObj, _getMemoryUsageCommittedMethod);
    jlong nonHeapMemoryUsed = env->CallLongMethod(nonHeapMemoryUsageObj, _getMemoryUsageUsedMethod);

    jvm_metrics->jvm_non_heap_size_bytes_committed->set_value(
            nonHeapMemoryCommitted < 0 ? 0 : nonHeapMemoryCommitted);
    jvm_metrics->jvm_non_heap_size_bytes_used->set_value(nonHeapMemoryUsed < 0 ? 0
                                                                               : nonHeapMemoryUsed);

    jobject memoryPoolMXBeansList =
            env->CallStaticObjectMethod(_managementFactoryClass, _getMemoryPoolMXBeansMethod);

    jint size = env->CallIntMethod(memoryPoolMXBeansList, _getListSizeMethod);

    for (int i = 0; i < size; ++i) {
        jobject memoryPoolMXBean =
                env->CallObjectMethod(memoryPoolMXBeansList, _getListUseIndexMethod, i);
        jobject usageObject =
                env->CallObjectMethod(memoryPoolMXBean, _getMemoryPoolMXBeanUsageMethod);

        jlong used = env->CallLongMethod(usageObject, _getMemoryUsageUsedMethod);
        jlong max = env->CallLongMethod(usageObject, _getMemoryUsageMaxMethod);

        jobject peakUsageObject =
                env->CallObjectMethod(memoryPoolMXBean, _getMemoryPollMXBeanPeakMethod);

        jlong peakUsed = env->CallLongMethod(peakUsageObject, _getMemoryUsageUsedMethod);

        jstring name =
                (jstring)env->CallObjectMethod(memoryPoolMXBean, _getMemoryPollMXBeanNameMethod);
        const char* nameStr = env->GetStringUTFChars(name, nullptr);
        if (nameStr != nullptr) {
            auto it = _memoryPoolName.find(nameStr);
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

            env->ReleaseStringUTFChars(name, nameStr);
        }
        env->DeleteLocalRef(memoryPoolMXBean);
        env->DeleteLocalRef(usageObject);
        env->DeleteLocalRef(peakUsageObject);
    }

    jobject threadMXBean =
            env->CallStaticObjectMethod(_managementFactoryClass, _getThreadMXBeanMethod);

    jlongArray threadIds = (jlongArray)env->CallObjectMethod(threadMXBean, _getAllThreadIdsMethod);
    jint threadCount = env->GetArrayLength(threadIds);

    jobjectArray threadInfos =
            (jobjectArray)env->CallObjectMethod(threadMXBean, _getThreadInfoMethod, threadIds, 0);

    int threadsNew = 0, threadsRunnable = 0, threadsBlocked = 0, threadsWaiting = 0,
        threadsTimedWaiting = 0, threadsTerminated = 0;
    jint peakThreadCount = env->CallIntMethod(threadMXBean, _getPeakThreadCountMethod);

    jvm_metrics->jvm_thread_peak_count->set_value(peakThreadCount < 0 ? 0 : peakThreadCount);
    jvm_metrics->jvm_thread_count->set_value(threadCount < 0 ? 0 : threadCount);

    for (int i = 0; i < threadCount; i++) {
        jobject threadInfo = env->GetObjectArrayElement(threadInfos, i);
        if (threadInfo == nullptr) {
            continue;
        }
        jobject threadState = env->CallObjectMethod(threadInfo, _getThreadStateMethod);

        if (env->IsSameObject(threadState, _newThreadStateObj)) {
            threadsNew++;
        } else if (env->IsSameObject(threadState, _runnableThreadStateObj)) {
            threadsRunnable++;
        } else if (env->IsSameObject(threadState, _blockedThreadStateObj)) {
            threadsBlocked++;
        } else if (env->IsSameObject(threadState, _waitingThreadStateObj)) {
            threadsWaiting++;
        } else if (env->IsSameObject(threadState, _timedWaitingThreadStateObj)) {
            threadsTimedWaiting++;
        } else if (env->IsSameObject(threadState, _terminatedThreadStateObj)) {
            threadsTerminated++;
        }
        env->DeleteLocalRef(threadInfo);
        env->DeleteLocalRef(threadState);
    }

    jvm_metrics->jvm_thread_new_count->set_value(threadsNew < 0 ? 0 : threadsNew);
    jvm_metrics->jvm_thread_runnable_count->set_value(threadsRunnable < 0 ? 0 : threadsRunnable);
    jvm_metrics->jvm_thread_blocked_count->set_value(threadsBlocked < 0 ? 0 : threadsBlocked);
    jvm_metrics->jvm_thread_waiting_count->set_value(threadsWaiting < 0 ? 0 : threadsWaiting);
    jvm_metrics->jvm_thread_timed_waiting_count->set_value(
            threadsTimedWaiting < 0 ? 0 : threadsTimedWaiting);
    jvm_metrics->jvm_thread_terminated_count->set_value(threadsTerminated < 0 ? 0
                                                                              : threadsTerminated);

    jobject gcMXBeansList =
            env->CallStaticObjectMethod(_managementFactoryClass, _getGarbageCollectorMXBeansMethod);

    jint numCollectors = env->CallIntMethod(gcMXBeansList, _getListSizeMethod);

    for (int i = 0; i < numCollectors; i++) {
        jobject gcMXBean = env->CallObjectMethod(gcMXBeansList, _getListUseIndexMethod, i);

        jstring gcName = (jstring)env->CallObjectMethod(gcMXBean, _getGCNameMethod);
        jlong gcCollectionCount = env->CallLongMethod(gcMXBean, _getGCCollectionCountMethod);
        jlong gcCollectionTime = env->CallLongMethod(gcMXBean, _getGCCollectionTimeMethod);
        const char* gcNameStr = env->GetStringUTFChars(gcName, NULL);
        if (gcNameStr != nullptr) {
            if (strcmp(gcNameStr, "G1 Young Generation") == 0) {
                jvm_metrics->jvm_gc_g1_young_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_young_generation_time_ms->set_value(gcCollectionTime);

            } else {
                jvm_metrics->jvm_gc_g1_old_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_old_generation_time_ms->set_value(gcCollectionTime);
            }

            env->ReleaseStringUTFChars(gcName, gcNameStr);
        }
        env->DeleteLocalRef(gcMXBean);
    }
    env->DeleteLocalRef(memoryMXBeanObj);
    env->DeleteLocalRef(heapMemoryUsageObj);
    env->DeleteLocalRef(nonHeapMemoryUsageObj);
    env->DeleteLocalRef(memoryPoolMXBeansList);
    env->DeleteLocalRef(threadMXBean);
    env->DeleteLocalRef(gcMXBeansList);
}
JvmStats::~JvmStats() {
    if (!_init_complete) {
        return;
    }
    try {
        env->DeleteLocalRef(_newThreadStateObj);
        env->DeleteLocalRef(_runnableThreadStateObj);
        env->DeleteLocalRef(_blockedThreadStateObj);
        env->DeleteLocalRef(_waitingThreadStateObj);
        env->DeleteLocalRef(_timedWaitingThreadStateObj);
        env->DeleteLocalRef(_terminatedThreadStateObj);

    } catch (...) {
        // When be is killed, DeleteLocalRef may fail.
        // In order to exit more gracefully, we catch the exception here.
    }
}

} // namespace doris
