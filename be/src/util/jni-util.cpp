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

#include "util/jni-util.h"

#include <glog/logging.h>
#include <jni.h>
#include <jni_md.h>

#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>

#include "absl/strings/substitute.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "util/jni_native_method.h"
#include "util/jvm_launcher.h"

using std::string;

namespace doris {
namespace Jni {
__thread JNIEnv* Env::tls_env_ = nullptr;
jclass Env::jni_util_cl_ = nullptr;
jmethodID Env::throwable_to_string_id_ = nullptr;
jmethodID Env::throwable_to_stack_trace_id_ = nullptr;

Status Env::GetJNIEnvSlowPath(JNIEnv** env) {
    DCHECK(!tls_env_) << "Call GetJNIEnv() fast path";

    RETURN_IF_ERROR(JvmLauncher::attach_current_thread(&tls_env_));
    // Only now, with tls_env_ in place: resolving the base classes goes through Env::Get()
    // itself, which must find this thread's env on the fast path instead of coming back
    // here. On failure the thread looks unattached again, so the next call retries and
    // reports the same error instead of running without the natives registered.
    if (Status status = Util::ensure_jni_base(); !status.ok()) {
        tls_env_ = nullptr;
        return status;
    }
    *env = tls_env_;
    return Status::OK();
}

Status Env::GetJniExceptionMsg(JNIEnv* env, bool log_stack, const string& prefix) {
    jthrowable exc = env->ExceptionOccurred();
    Defer def {[&]() { env->DeleteLocalRef(exc); }};
    if (exc == nullptr) {
        return Status::OK();
    }
    env->ExceptionClear();
    DCHECK(throwable_to_string_id_ != nullptr);
    const char* oom_msg_template =
            "$0 threw an unchecked exception. The JVM is likely out "
            "of memory (OOM).";
    jstring msg = static_cast<jstring>(
            env->CallStaticObjectMethod(jni_util_cl_, throwable_to_string_id_, exc));
    if (env->ExceptionOccurred()) {
        env->ExceptionClear();
        string oom_msg = absl::Substitute(oom_msg_template, "throwableToString");
        LOG(WARNING) << oom_msg;
        return Status::JniError(oom_msg);
    }

    std::string return_msg;
    auto* msg_str = env->GetStringUTFChars(msg, nullptr);
    return_msg += msg_str;
    env->ReleaseStringUTFChars(msg, msg_str);

    if (log_stack) {
        jstring stack = static_cast<jstring>(
                env->CallStaticObjectMethod(jni_util_cl_, throwable_to_stack_trace_id_, exc));
        if (env->ExceptionOccurred()) {
            env->ExceptionClear();
            string oom_msg = absl::Substitute(oom_msg_template, "throwableToStackTrace");
            LOG(WARNING) << oom_msg;
            return Status::JniError(oom_msg);
        }

        auto* stask_str = env->GetStringUTFChars(stack, nullptr);
        LOG(WARNING) << stask_str;
        env->ReleaseStringUTFChars(stack, stask_str);
    }

    return Status::JniError("{}{}", prefix, return_msg);
}

jlong Util::max_jvm_heap_memory_size_ = 0;
GlobalClass Util::hashmap_class;
MethodId Util::hashmap_constructor;
MethodId Util::hashmap_put;
GlobalClass Util::mapClass;
MethodId Util::mapEntrySetMethod;
GlobalClass Util::mapEntryClass;
MethodId Util::getEntryKeyMethod;
MethodId Util::getEntryValueMethod;
GlobalClass Util::setClass;
MethodId Util::iteratorSetMethod;
GlobalClass Util::iteratorClass;
MethodId Util::iteratorHasNextMethod;
MethodId Util::iteratorNextMethod;

void Util::_parse_max_heap_memory_size_from_jvm() {
    // The same options the JVM was created from, see JvmLauncher::_build_options().
    std::string java_opts = getenv("JAVA_OPTS") ? getenv("JAVA_OPTS") : "";
    if (java_opts.empty()) {
        java_opts = getenv("LIBHDFS_OPTS") ? getenv("LIBHDFS_OPTS") : "";
    }
    std::istringstream iss(java_opts);
    std::string opt;
    while (iss >> opt) {
        if (opt.find("-Xmx") == 0) {
            std::string xmxValue = opt.substr(4);
            LOG(INFO) << "The max heap vaule is " << xmxValue;
            char unit = xmxValue.back();
            xmxValue.pop_back();
            long long value = std::stoll(xmxValue);
            switch (unit) {
            case 'g':
            case 'G':
                max_jvm_heap_memory_size_ = value * 1024 * 1024 * 1024;
                break;
            case 'm':
            case 'M':
                max_jvm_heap_memory_size_ = value * 1024 * 1024;
                break;
            case 'k':
            case 'K':
                max_jvm_heap_memory_size_ = value * 1024;
                break;
            default:
                max_jvm_heap_memory_size_ = value;
                break;
            }
        }
    }
    if (0 == max_jvm_heap_memory_size_) {
        // Used to be fatal, which was survivable only because it ran while the BE was
        // starting up. It now runs on whichever query first writes to hdfs, and taking the
        // BE down over a missing -Xmx would be out of all proportion: fall back to the
        // same 1g the JVM is created with when nothing else says otherwise.
        max_jvm_heap_memory_size_ = 1024L * 1024 * 1024;
        LOG(WARNING) << "No -Xmx in the JVM options, assuming a max heap of "
                     << max_jvm_heap_memory_size_
                     << " bytes when rate limiting hdfs writes. Set -Xmx in JAVA_OPTS to make "
                        "this exact.";
    }
    LOG(INFO) << "the max_jvm_heap_memory_size_ is " << max_jvm_heap_memory_size_;
}

size_t Util::get_max_jni_heap_memory_size() {
#if defined(USE_LIBHDFS3) || defined(BE_TEST)
    return std::numeric_limits<size_t>::max();
#else
    static std::once_flag _parse_max_heap_memory_size_from_jvm_flag;
    std::call_once(_parse_max_heap_memory_size_from_jvm_flag, _parse_max_heap_memory_size_from_jvm);
    return max_jvm_heap_memory_size_;
#endif
}

Status Util::_init_collect_class() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    // for hashmap
    RETURN_IF_ERROR(find_class(env, "java/util/HashMap", &hashmap_class));
    RETURN_IF_ERROR(hashmap_class.get_method(env, "<init>", "(I)V", &hashmap_constructor));
    RETURN_IF_ERROR(hashmap_class.get_method(
            env, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;", &hashmap_put));

    //for map
    RETURN_IF_ERROR(find_class(env, "java/util/Map", &mapClass));
    RETURN_IF_ERROR(mapClass.get_method(env, "entrySet", "()Ljava/util/Set;", &mapEntrySetMethod));

    //for set
    RETURN_IF_ERROR(find_class(env, "java/util/Set", &setClass));
    RETURN_IF_ERROR(
            setClass.get_method(env, "iterator", "()Ljava/util/Iterator;", &iteratorSetMethod));

    // for iterator
    RETURN_IF_ERROR(find_class(env, "java/util/Iterator", &iteratorClass));
    RETURN_IF_ERROR(iteratorClass.get_method(env, "hasNext", "()Z", &iteratorHasNextMethod));
    RETURN_IF_ERROR(
            iteratorClass.get_method(env, "next", "()Ljava/lang/Object;", &iteratorNextMethod));

    //for map entry
    RETURN_IF_ERROR(find_class(env, "java/util/Map$Entry", &mapEntryClass));
    RETURN_IF_ERROR(
            mapEntryClass.get_method(env, "getKey", "()Ljava/lang/Object;", &getEntryKeyMethod));

    RETURN_IF_ERROR(mapEntryClass.get_method(env, "getValue", "()Ljava/lang/Object;",
                                             &getEntryValueMethod));

    return Status::OK();
}

Status Util::ensure_jni_base() {
    static std::once_flag jni_base_once;
    static Status jni_base_status;
    std::call_once(jni_base_once, []() { jni_base_status = _init_jni_base(); });
    return jni_base_status;
}

Status Util::_init_jni_base() {
    RETURN_IF_ERROR(Env::Init());
    // Before any Java code runs: it links against these natives, and hitting an
    // unregistered one would surface as an UnsatisfiedLinkError deep inside a scanner.
    RETURN_IF_ERROR(_init_register_natives());
    RETURN_IF_ERROR(_init_collect_class());
    // The JVM exists from here on, so its metrics have something to report.
    DorisMetrics::instance()->init_jvm_metrics();
    return Status::OK();
}

Status Util::_init_register_natives() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    // Find JNINativeMethod class and create a global ref.
    jclass local_jni_native_exc_cl =
            env->FindClass("org/apache/doris/common/jni/utils/JNINativeMethod");
    if (local_jni_native_exc_cl == nullptr) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::JniError("Failed to find JNINativeMethod class.");
    }

    static char memory_alloc_name[] = "memoryTrackerMalloc";
    static char memory_alloc_sign[] = "(J)J";
    static char memory_free_name[] = "memoryTrackerFree";
    static char memory_free_sign[] = "(J)V";
    static char memory_alloc_batch_name[] = "memoryTrackerMallocBatch";
    static char memory_alloc_batch_sign[] = "([I)[J";
    static char memory_free_batch_name[] = "memoryTrackerFreeBatch";
    static char memory_free_batch_sign[] = "([J)V";
    static JNINativeMethod java_native_methods[] = {
            {memory_alloc_name, memory_alloc_sign, (void*)&JavaNativeMethods::memoryMalloc},
            {memory_free_name, memory_free_sign, (void*)&JavaNativeMethods::memoryFree},
            {memory_alloc_batch_name, memory_alloc_batch_sign,
             (void*)&JavaNativeMethods::memoryMallocBatch},
            {memory_free_batch_name, memory_free_batch_sign,
             (void*)&JavaNativeMethods::memoryFreeBatch},
    };

    int res = env->RegisterNatives(local_jni_native_exc_cl, java_native_methods,
                                   sizeof(java_native_methods) / sizeof(java_native_methods[0]));
    DCHECK_EQ(res, 0);
    if (res) [[unlikely]] {
        return Status::JniError("Failed to RegisterNatives.");
    }
    return Status::OK();
}

} // namespace Jni
} // namespace doris
