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

#include <fmt/format.h>
#include <glog/logging.h>
#include <jni.h>
#include <jni_md.h>

#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "absl/strings/substitute.h"
#include "common/cast_set.h"
#include "common/config.h"
#include "util/doris_metrics.h"
#include "util/jni_native_method.h"
// #include "util/libjvm_loader.h"

using std::string;

namespace doris {
#include "common/compile_check_begin.h"
namespace Jni {
JavaVM* g_vm;
[[maybe_unused]] std::once_flag g_vm_once;
[[maybe_unused]] std::once_flag g_jvm_conf_once;
__thread JNIEnv* Env::tls_env_ = nullptr;
jclass Env::jni_util_cl_ = nullptr;
jmethodID Env::throwable_to_string_id_ = nullptr;
jmethodID Env::throwable_to_stack_trace_id_ = nullptr;

const std::string GetDorisJNIDefaultClasspath() {
    const auto* doris_home = getenv("DORIS_HOME");
    DCHECK(doris_home) << "Environment variable DORIS_HOME is not set.";

    std::ostringstream out;

    auto add_jars_from_path = [&](const std::string& base_path) {
        if (!std::filesystem::exists(base_path)) {
            return;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(base_path)) {
            if (entry.path().extension() == ".jar") {
                if (!out.str().empty()) {
                    out << ":";
                }
                out << entry.path().string();
            }
        }
    };

    add_jars_from_path(std::string(doris_home) + "/lib");
    add_jars_from_path(std::string(doris_home) + "/custom_lib");

    // Check and add HADOOP_CONF_DIR if it's set
    const auto* hadoop_conf_dir = getenv("HADOOP_CONF_DIR");
    if (hadoop_conf_dir != nullptr && strlen(hadoop_conf_dir) > 0) {
        if (!out.str().empty()) {
            out << ":";
        }
        out << hadoop_conf_dir;
    }

    DCHECK(!out.str().empty()) << "Empty classpath is invalid.";
    return out.str();
}

const std::string GetDorisJNIClasspathOption() {
    const auto* classpath = getenv("DORIS_CLASSPATH");
    if (classpath) {
        return classpath;
    } else {
        return "-Djava.class.path=" + GetDorisJNIDefaultClasspath();
    }
}

const std::string GetKerb5ConfPath() {
    return "-Djava.security.krb5.conf=" + config::kerberos_krb5_conf_path;
}

[[maybe_unused]] void SetEnvIfNecessary() {
    std::string libhdfs_opts = getenv("LIBHDFS_OPTS") ? getenv("LIBHDFS_OPTS") : "";
    CHECK(libhdfs_opts != "") << "LIBHDFS_OPTS is not set";
    libhdfs_opts += fmt::format(" {} ", GetKerb5ConfPath());
    libhdfs_opts += fmt::format(" -Djdk.lang.processReaperUseDefaultStackSize={}",
                                config::jdk_process_reaper_use_default_stack_size);
    setenv("LIBHDFS_OPTS", libhdfs_opts.c_str(), 1);
    LOG(INFO) << "set final LIBHDFS_OPTS: " << libhdfs_opts;
}

// Only used on non-x86 platform
[[maybe_unused]] void FindOrCreateJavaVM() {
    int num_vms;
    int rv = JNI_GetCreatedJavaVMs(&g_vm, 1, &num_vms);
    if (rv == 0) {
        std::vector<std::string> options;

        char* java_opts = getenv("JAVA_OPTS");
        if (java_opts == nullptr) {
            options = {
                    GetDorisJNIClasspathOption(), fmt::format("-Xmx{}", "1g"),
                    fmt::format("-DlogPath={}/log/jni.log", getenv("DORIS_HOME")),
                    fmt::format("-Dsun.java.command={}", "DorisBE"), "-XX:-CriticalJNINatives",
                    fmt::format("-Djdk.lang.processReaperUseDefaultStackSize={}",
                                config::jdk_process_reaper_use_default_stack_size),
#ifdef __APPLE__
                    // On macOS, we should disable MaxFDLimit, otherwise the RLIMIT_NOFILE
                    // will be assigned the minimum of OPEN_MAX (10240) and rlim_cur (See src/hotspot/os/bsd/os_bsd.cpp)
                    // and it can not pass the check performed by storage engine.
                    // The newer JDK has fixed this issue.
                    "-XX:-MaxFDLimit"
#endif
            };
        } else {
            std::istringstream stream(java_opts);
            options = std::vector<std::string>(std::istream_iterator<std::string> {stream},
                                               std::istream_iterator<std::string>());
            options.push_back(GetDorisJNIClasspathOption());
        }
        options.push_back(GetKerb5ConfPath());
        std::unique_ptr<JavaVMOption[]> jvm_options(new JavaVMOption[options.size()]);
        for (int i = 0; i < options.size(); ++i) {
            // To convert a string to a char*, const_cast is used.
            jvm_options[i] = {const_cast<char*>(options[i].c_str()), nullptr};
        }

        JNIEnv* env = nullptr;
        JavaVMInitArgs vm_args;
        vm_args.version = JNI_VERSION_1_8;
        vm_args.options = jvm_options.get();
        vm_args.nOptions = cast_set<int>(options.size());
        // Set it to JNI_FALSE because JNI_TRUE will let JVM ignore the max size config.
        vm_args.ignoreUnrecognized = JNI_FALSE;

        jint res = JNI_CreateJavaVM(&g_vm, (void**)&env, &vm_args);
        if (JNI_OK != res) {
            DCHECK(false) << "Failed to create JVM, code= " << res;
        }

    } else {
        CHECK_EQ(rv, 0) << "Could not find any created Java VM";
        CHECK_EQ(num_vms, 1) << "No VMs returned";
    }
}

Status Env::GetJNIEnvSlowPath(JNIEnv** env) {
    DCHECK(!tls_env_) << "Call GetJNIEnv() fast path";

#ifdef USE_LIBHDFS3
    std::call_once(g_vm_once, FindOrCreateJavaVM);
    int rc = g_vm->GetEnv(reinterpret_cast<void**>(&tls_env_), JNI_VERSION_1_8);
    if (rc == JNI_EDETACHED) {
        rc = g_vm->AttachCurrentThread((void**)&tls_env_, nullptr);
    }
    if (rc != 0 || tls_env_ == nullptr) {
        return Status::JniError("Unable to get JVM: {}", rc);
    }
#else
    // the hadoop libhdfs will do all the stuff
    std::call_once(g_jvm_conf_once, SetEnvIfNecessary);
    tls_env_ = getJNIEnv();
#endif
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
    env->ReleaseStringUTFChars((jstring)msg, msg_str);

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

bool Util::jvm_inited_ = false;

jlong Util::max_jvm_heap_memory_size_ = 0;
GlobalObject Util::jni_scanner_loader_obj_;
MethodId Util::jni_scanner_loader_method_;
MethodId Util::_clean_udf_cache_method_id;
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
    // The start_be.sh would set JAVA_OPTS inside LIBHDFS_OPTS
    std::string java_opts = getenv("LIBHDFS_OPTS") ? getenv("LIBHDFS_OPTS") : "";
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
        LOG(FATAL) << "the max_jvm_heap_memory_size_ is " << max_jvm_heap_memory_size_;
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

Status Util::_init_jni_scanner_loader() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    LocalClass jni_scanner_loader_cls;
    std::string jni_scanner_loader_str = "org/apache/doris/common/classloader/ScannerLoader";
    RETURN_IF_ERROR(find_class(env, jni_scanner_loader_str.c_str(), &jni_scanner_loader_cls));

    MethodId jni_scanner_loader_constructor;
    RETURN_IF_ERROR(jni_scanner_loader_cls.get_method(env, "<init>", "()V",
                                                      &jni_scanner_loader_constructor));

    RETURN_IF_ERROR(jni_scanner_loader_cls.get_method(env, "getLoadedClass",
                                                      "(Ljava/lang/String;)Ljava/lang/Class;",
                                                      &jni_scanner_loader_method_));

    MethodId load_jni_scanner;
    RETURN_IF_ERROR(
            jni_scanner_loader_cls.get_method(env, "loadAllScannerJars", "()V", &load_jni_scanner));

    RETURN_IF_ERROR(jni_scanner_loader_cls.get_method(
            env, "cleanUdfClassLoader", "(Ljava/lang/String;)V", &_clean_udf_cache_method_id));

    RETURN_IF_ERROR(jni_scanner_loader_cls.new_object(env, jni_scanner_loader_constructor)
                            .call(&jni_scanner_loader_obj_));

    RETURN_IF_ERROR(jni_scanner_loader_obj_.call_void_method(env, load_jni_scanner).call());
    return Status::OK();
}

Status Util::clean_udf_class_load_cache(const std::string& function_signature) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    LocalString function_signature_jstr;
    RETURN_IF_ERROR(
            LocalString::new_string(env, function_signature.c_str(), &function_signature_jstr));

    RETURN_IF_ERROR(jni_scanner_loader_obj_.call_void_method(env, _clean_udf_cache_method_id)
                            .with_arg(function_signature_jstr)
                            .call());

    return Status::OK();
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

Status Util::Init() {
    RETURN_IF_ERROR(Env::Init());
    RETURN_IF_ERROR(_init_register_natives());
    RETURN_IF_ERROR(_init_collect_class());
    RETURN_IF_ERROR(_init_jni_scanner_loader());
    jvm_inited_ = true;
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
#include "common/compile_check_end.h"
} // namespace doris
