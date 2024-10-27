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
            Status st = _jvm_stats.init(env);
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

void JvmMetrics::update() {
    static long fail_count = 0;
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

Status JvmStats::init(JNIEnv* env) {
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/ManagementFactory",
                                               &_managementFactoryClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryMXBeanMethod, env,
                                    GetStaticMethodID(_managementFactoryClass, "getMemoryMXBean",
                                                      "()Ljava/lang/management/MemoryMXBean;"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/MemoryUsage",
                                               &_memoryUsageClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryUsageUsedMethod, env,
                                    GetMethodID(_memoryUsageClass, "getUsed", "()J"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryUsageCommittedMethod, env,
                                    GetMethodID(_memoryUsageClass, "getCommitted", "()J"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryUsageMaxMethod, env,
                                    GetMethodID(_memoryUsageClass, "getMax", "()J"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/MemoryMXBean",
                                               &_memoryMXBeanClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getHeapMemoryUsageMethod, env,
                                    GetMethodID(_memoryMXBeanClass, "getHeapMemoryUsage",
                                                "()Ljava/lang/management/MemoryUsage;"));
    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getNonHeapMemoryUsageMethod, env,
                                    GetMethodID(_memoryMXBeanClass, "getNonHeapMemoryUsage",
                                                "()Ljava/lang/management/MemoryUsage;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getMemoryPoolMXBeansMethod, env,
            GetStaticMethodID(_managementFactoryClass, "getMemoryPoolMXBeans",
                              "()Ljava/util/List;"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/util/List", &_listClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getListSizeMethod, env,
                                    GetMethodID(_listClass, "size", "()I"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getListUseIndexMethod, env,
                                    GetMethodID(_listClass, "get", "(I)Ljava/lang/Object;"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/MemoryPoolMXBean",
                                               &_memoryPoolMXBeanClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryPoolMXBeanUsageMethod, env,
                                    GetMethodID(_memoryPoolMXBeanClass, "getUsage",
                                                "()Ljava/lang/management/MemoryUsage;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getMemoryPollMXBeanPeakMethod, env,
                                    GetMethodID(_memoryPoolMXBeanClass, "getPeakUsage",
                                                "()Ljava/lang/management/MemoryUsage;"));
    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getMemoryPollMXBeanNameMethod, env,
            GetMethodID(_memoryPoolMXBeanClass, "getName", "()Ljava/lang/String;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(, _getThreadMXBeanMethod, env,
                                    GetStaticMethodID(_managementFactoryClass, "getThreadMXBean",
                                                      "()Ljava/lang/management/ThreadMXBean;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getGarbageCollectorMXBeansMethod, env,
            GetStaticMethodID(_managementFactoryClass, "getGarbageCollectorMXBeans",
                              "()Ljava/util/List;"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/GarbageCollectorMXBean",
                                               &_garbageCollectorMXBeanClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getGCNameMethod, env,
            GetMethodID(_garbageCollectorMXBeanClass, "getName", "()Ljava/lang/String;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getGCCollectionCountMethod, env,
            GetMethodID(_garbageCollectorMXBeanClass, "getCollectionCount", "()J"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _getGCCollectionTimeMethod, env,
            GetMethodID(_garbageCollectorMXBeanClass, "getCollectionTime", "()J"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/management/ThreadMXBean",
                                               &_threadMXBeanClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(,

                                    _getAllThreadIdsMethod, env,
                                    GetMethodID(_threadMXBeanClass, "getAllThreadIds", "()[J"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(,

                                    _getThreadInfoMethod, env,
                                    GetMethodID(_threadMXBeanClass, "getThreadInfo",
                                                "([JI)[Ljava/lang/management/ThreadInfo;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(,

                                    _getPeakThreadCountMethod, env,
                                    GetMethodID(_threadMXBeanClass, "getPeakThreadCount", "()I"));

    RETURN_IF_ERROR(
            JniUtil::GetGlobalClassRef(env, "java/lang/management/ThreadInfo", &_threadInfoClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            ,

            _getThreadStateMethod, env,
            GetMethodID(_threadInfoClass, "getThreadState", "()Ljava/lang/Thread$State;"));

    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, "java/lang/Thread$State", &_threadStateClass));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, newThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "NEW", "Ljava/lang/Thread$State;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, runnableThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "RUNNABLE", "Ljava/lang/Thread$State;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, blockedThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "BLOCKED", "Ljava/lang/Thread$State;"));
    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, waitingThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "WAITING", "Ljava/lang/Thread$State;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, timedWaitingThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "TIMED_WAITING", "Ljava/lang/Thread$State;"));
    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jfieldID, terminatedThreadFieldID, env,
            GetStaticFieldID(_threadStateClass, "TERMINATED", "Ljava/lang/Thread$State;"));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jobject, newThreadStateObj, env,
                                    GetStaticObjectField(_threadStateClass, newThreadFieldID));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, newThreadStateObj, &_newThreadStateObj));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jobject, runnableThreadStateObj, env,
                                    GetStaticObjectField(_threadStateClass, runnableThreadFieldID));
    RETURN_IF_ERROR(
            JniUtil::LocalToGlobalRef(env, runnableThreadStateObj, &_runnableThreadStateObj));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jobject, blockedThreadStateObj, env,
                                    GetStaticObjectField(_threadStateClass, blockedThreadFieldID));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, blockedThreadStateObj, &_blockedThreadStateObj));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jobject, waitingThreadStateObj, env,
                                    GetStaticObjectField(_threadStateClass, waitingThreadFieldID));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, waitingThreadStateObj, &_waitingThreadStateObj));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jobject, timedWaitingThreadStateObj, env,
            GetStaticObjectField(_threadStateClass, timedWaitingThreadFieldID));
    RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, timedWaitingThreadStateObj,
                                              &_timedWaitingThreadStateObj));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jobject, terminatedThreadStateObj, env,
            GetStaticObjectField(_threadStateClass, terminatedThreadFieldID));
    RETURN_IF_ERROR(
            JniUtil::LocalToGlobalRef(env, terminatedThreadStateObj, &_terminatedThreadStateObj));

    _init_complete = true;

    LOG(INFO) << "Start JVM monitoring.";
    return Status::OK();
}

Status JvmStats::refresh(JvmMetrics* jvm_metrics) const {
    if (!_init_complete) {
        return Status::InternalError("Jvm Stats not init complete.");
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, memoryMXBeanObj, env,
            CallStaticObjectMethod(_managementFactoryClass, _getMemoryMXBeanMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, heapMemoryUsageObj, env,
            CallObjectMethod(memoryMXBeanObj, _getHeapMemoryUsageMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, heapMemoryUsed, env,
                                    CallLongMethod(heapMemoryUsageObj, _getMemoryUsageUsedMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jlong, heapMemoryCommitted, env,
            CallLongMethod(heapMemoryUsageObj, _getMemoryUsageCommittedMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, heapMemoryMax, env,
                                    CallLongMethod(heapMemoryUsageObj, _getMemoryUsageMaxMethod));

    jvm_metrics->jvm_heap_size_bytes_used->set_value(heapMemoryUsed < 0 ? 0 : heapMemoryUsed);
    jvm_metrics->jvm_heap_size_bytes_committed->set_value(
            heapMemoryCommitted < 0 ? 0 : heapMemoryCommitted);
    jvm_metrics->jvm_heap_size_bytes_max->set_value(heapMemoryMax < 0 ? 0 : heapMemoryMax);

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, nonHeapMemoryUsageObj, env,
            CallObjectMethod(memoryMXBeanObj, _getNonHeapMemoryUsageMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jlong, nonHeapMemoryCommitted, env,
            CallLongMethod(nonHeapMemoryUsageObj, _getMemoryUsageCommittedMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(
            jlong, nonHeapMemoryUsed, env,
            CallLongMethod(nonHeapMemoryUsageObj, _getMemoryUsageUsedMethod));

    jvm_metrics->jvm_non_heap_size_bytes_committed->set_value(
            nonHeapMemoryCommitted < 0 ? 0 : nonHeapMemoryCommitted);
    jvm_metrics->jvm_non_heap_size_bytes_used->set_value(nonHeapMemoryUsed < 0 ? 0
                                                                               : nonHeapMemoryUsed);

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, memoryPoolMXBeansList, env,
            CallStaticObjectMethod(_managementFactoryClass, _getMemoryPoolMXBeansMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jint, size, env,
                                    CallIntMethod(memoryPoolMXBeansList, _getListSizeMethod));

    for (int i = 0; i < size; ++i) {
        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, memoryPoolMXBean, env,
                CallObjectMethod(memoryPoolMXBeansList, _getListUseIndexMethod, i));

        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, usageObject, env,
                CallObjectMethod(memoryPoolMXBean, _getMemoryPoolMXBeanUsageMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, used, env,
                                        CallLongMethod(usageObject, _getMemoryUsageUsedMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, max, env,
                                        CallLongMethod(usageObject, _getMemoryUsageMaxMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, peakUsageObject, env,
                CallObjectMethod(memoryPoolMXBean, _getMemoryPollMXBeanPeakMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, peakUsed, env,
                                        CallLongMethod(peakUsageObject, _getMemoryUsageUsedMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, name, env,
                CallObjectMethod(memoryPoolMXBean, _getMemoryPollMXBeanNameMethod));

        const char* nameStr = env->GetStringUTFChars(
                (jstring)name, nullptr); // GetStringUTFChars not throw exception
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

            env->ReleaseStringUTFChars((jstring)name,
                                       nameStr); // ReleaseStringUTFChars not throw exception
        }
    }
    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, threadMXBean, env,
            CallStaticObjectMethod(_managementFactoryClass, _getThreadMXBeanMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, threadIdsObject, env, CallObjectMethod(threadMXBean, _getAllThreadIdsMethod));

    auto threadIds = (jlongArray)threadIdsObject;

    JNI_CALL_METHOD_CHECK_EXCEPTION(jint, threadCount, env, GetArrayLength(threadIds));

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, threadInfos, env,
            CallObjectMethod(threadMXBean, _getThreadInfoMethod, (jlongArray)threadIds, 0));

    int threadsNew = 0, threadsRunnable = 0, threadsBlocked = 0, threadsWaiting = 0,
        threadsTimedWaiting = 0, threadsTerminated = 0;

    JNI_CALL_METHOD_CHECK_EXCEPTION(jint, peakThreadCount, env,
                                    CallIntMethod(threadMXBean, _getPeakThreadCountMethod));

    jvm_metrics->jvm_thread_peak_count->set_value(peakThreadCount < 0 ? 0 : peakThreadCount);
    jvm_metrics->jvm_thread_count->set_value(threadCount < 0 ? 0 : threadCount);

    for (int i = 0; i < threadCount; i++) {
        JNI_CALL_METHOD_CHECK_EXCEPTION(jobject, threadInfo, env,
                                        GetObjectArrayElement((jobjectArray)threadInfos, i));

        if (threadInfo == nullptr) {
            continue;
        }

        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, threadState, env, CallObjectMethod(threadInfo, _getThreadStateMethod));

        //IsSameObject not throw exception
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
    }

    jvm_metrics->jvm_thread_new_count->set_value(threadsNew < 0 ? 0 : threadsNew);
    jvm_metrics->jvm_thread_runnable_count->set_value(threadsRunnable < 0 ? 0 : threadsRunnable);
    jvm_metrics->jvm_thread_blocked_count->set_value(threadsBlocked < 0 ? 0 : threadsBlocked);
    jvm_metrics->jvm_thread_waiting_count->set_value(threadsWaiting < 0 ? 0 : threadsWaiting);
    jvm_metrics->jvm_thread_timed_waiting_count->set_value(
            threadsTimedWaiting < 0 ? 0 : threadsTimedWaiting);
    jvm_metrics->jvm_thread_terminated_count->set_value(threadsTerminated < 0 ? 0
                                                                              : threadsTerminated);

    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, gcMXBeansList, env,
            CallStaticObjectMethod(_managementFactoryClass, _getGarbageCollectorMXBeansMethod));

    JNI_CALL_METHOD_CHECK_EXCEPTION(jint, numCollectors, env,
                                    CallIntMethod(gcMXBeansList, _getListSizeMethod));

    for (int i = 0; i < numCollectors; i++) {
        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
                jobject, gcMXBean, env, CallObjectMethod(gcMXBeansList, _getListUseIndexMethod, i));

        JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(jobject, gcName, env,
                                                   CallObjectMethod(gcMXBean, _getGCNameMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, gcCollectionCount, env,
                                        CallLongMethod(gcMXBean, _getGCCollectionCountMethod));

        JNI_CALL_METHOD_CHECK_EXCEPTION(jlong, gcCollectionTime, env,
                                        CallLongMethod(gcMXBean, _getGCCollectionTimeMethod));

        const char* gcNameStr = env->GetStringUTFChars((jstring)gcName, NULL);
        if (gcNameStr != nullptr) {
            if (strcmp(gcNameStr, "G1 Young Generation") == 0) {
                jvm_metrics->jvm_gc_g1_young_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_young_generation_time_ms->set_value(gcCollectionTime);

            } else {
                jvm_metrics->jvm_gc_g1_old_generation_count->set_value(gcCollectionCount);
                jvm_metrics->jvm_gc_g1_old_generation_time_ms->set_value(gcCollectionTime);
            }

            env->ReleaseStringUTFChars((jstring)gcName, gcNameStr);
        }
    }

    return Status::OK();
}
JvmStats::~JvmStats() {
    if (!_init_complete) {
        return;
    }
    try {
        JNIEnv* env = nullptr;
        Status st = JniUtil::GetJNIEnv(&env);
        if (!st.ok()) {
            return;
        }
        env->DeleteGlobalRef(_managementFactoryClass);
        env->DeleteGlobalRef(_memoryUsageClass);
        env->DeleteGlobalRef(_memoryMXBeanClass);
        env->DeleteGlobalRef(_listClass);
        env->DeleteGlobalRef(_memoryPoolMXBeanClass);
        env->DeleteGlobalRef(_threadMXBeanClass);
        env->DeleteGlobalRef(_threadInfoClass);
        env->DeleteGlobalRef(_threadStateClass);
        env->DeleteGlobalRef(_garbageCollectorMXBeanClass);

        env->DeleteGlobalRef(_newThreadStateObj);
        env->DeleteGlobalRef(_runnableThreadStateObj);
        env->DeleteGlobalRef(_blockedThreadStateObj);
        env->DeleteGlobalRef(_waitingThreadStateObj);
        env->DeleteGlobalRef(_timedWaitingThreadStateObj);
        env->DeleteGlobalRef(_terminatedThreadStateObj);

    } catch (...) {
        // In order to exit more gracefully, we catch the exception here.
    }
}

} // namespace doris
