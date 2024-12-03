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

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "common/config.h"
#include "gutil/strings/substitute.h"
#include "util/doris_metrics.h"
#include "util/jni_native_method.h"
// #include "util/libjvm_loader.h"

using std::string;

namespace doris {

namespace {
JavaVM* g_vm;
[[maybe_unused]] std::once_flag g_vm_once;

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

[[maybe_unused]] void SetEnvIfNecessary() {
    const auto* doris_home = getenv("DORIS_HOME");
    DCHECK(doris_home) << "Environment variable DORIS_HOME is not set.";

    // CLASSPATH
    const std::string original_classpath = getenv("CLASSPATH") ? getenv("CLASSPATH") : "";
    static const std::string classpath = fmt::format(
            "{}/conf:{}:{}", doris_home, GetDorisJNIDefaultClasspath(), original_classpath);
    setenv("CLASSPATH", classpath.c_str(), 0);

    // LIBHDFS_OPTS
    const std::string java_opts = getenv("JAVA_OPTS") ? getenv("JAVA_OPTS") : "";
    std::string libhdfs_opts =
            fmt::format("{} -Djava.library.path={}/lib/hadoop_hdfs/native:{}", java_opts,
                        getenv("DORIS_HOME"), getenv("DORIS_HOME") + std::string("/lib"));

    setenv("LIBHDFS_OPTS", libhdfs_opts.c_str(), 0);
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
        std::unique_ptr<JavaVMOption[]> jvm_options(new JavaVMOption[options.size()]);
        for (int i = 0; i < options.size(); ++i) {
            jvm_options[i] = {const_cast<char*>(options[i].c_str()), nullptr};
        }

        JNIEnv* env = nullptr;
        JavaVMInitArgs vm_args;
        vm_args.version = JNI_VERSION_1_8;
        vm_args.options = jvm_options.get();
        vm_args.nOptions = options.size();
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

} // anonymous namespace

bool JniUtil::jvm_inited_ = false;
__thread JNIEnv* JniUtil::tls_env_ = nullptr;
jclass JniUtil::internal_exc_cl_ = nullptr;
jclass JniUtil::jni_util_cl_ = nullptr;
jclass JniUtil::jni_native_method_exc_cl_ = nullptr;
jmethodID JniUtil::throwable_to_string_id_ = nullptr;
jmethodID JniUtil::throwable_to_stack_trace_id_ = nullptr;
jmethodID JniUtil::get_jvm_metrics_id_ = nullptr;
jmethodID JniUtil::get_jvm_threads_id_ = nullptr;
jmethodID JniUtil::get_jmx_json_ = nullptr;
jobject JniUtil::jni_scanner_loader_obj_ = nullptr;
jmethodID JniUtil::jni_scanner_loader_method_ = nullptr;
jlong JniUtil::max_jvm_heap_memory_size_ = 0;
jmethodID JniUtil::_clean_udf_cache_method_id = nullptr;

Status JniUtfCharGuard::create(JNIEnv* env, jstring jstr, JniUtfCharGuard* out) {
    DCHECK(jstr != nullptr);
    DCHECK(!env->ExceptionCheck());
    jboolean is_copy;
    const char* utf_chars = env->GetStringUTFChars(jstr, &is_copy);
    bool exception_check = static_cast<bool>(env->ExceptionCheck());
    if (utf_chars == nullptr || exception_check) {
        if (exception_check) env->ExceptionClear();
        if (utf_chars != nullptr) env->ReleaseStringUTFChars(jstr, utf_chars);
        auto fail_message = "GetStringUTFChars failed. Probable OOM on JVM side";
        LOG(WARNING) << fail_message;
        return Status::InternalError(fail_message);
    }
    out->env = env;
    out->jstr = jstr;
    out->utf_chars = utf_chars;
    return Status::OK();
}

Status JniLocalFrame::push(JNIEnv* env, int max_local_ref) {
    DCHECK(env_ == nullptr);
    DCHECK_GT(max_local_ref, 0);
    if (env->PushLocalFrame(max_local_ref) < 0) {
        env->ExceptionClear();
        return Status::InternalError("failed to push frame");
    }
    env_ = env;
    return Status::OK();
}

void JniUtil::parse_max_heap_memory_size_from_jvm(JNIEnv* env) {
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

size_t JniUtil::get_max_jni_heap_memory_size() {
#if defined(USE_LIBHDFS3) || defined(BE_TEST)
    return std::numeric_limits<size_t>::max();
#else
    static std::once_flag parse_max_heap_memory_size_from_jvm_flag;
    std::call_once(parse_max_heap_memory_size_from_jvm_flag, parse_max_heap_memory_size_from_jvm,
                   tls_env_);
    return max_jvm_heap_memory_size_;
#endif
}

Status JniUtil::GetJNIEnvSlowPath(JNIEnv** env) {
    DCHECK(!tls_env_) << "Call GetJNIEnv() fast path";

#ifdef USE_LIBHDFS3
    std::call_once(g_vm_once, FindOrCreateJavaVM);
    int rc = g_vm->GetEnv(reinterpret_cast<void**>(&tls_env_), JNI_VERSION_1_8);
    if (rc == JNI_EDETACHED) {
        rc = g_vm->AttachCurrentThread((void**)&tls_env_, nullptr);
    }
    if (rc != 0 || tls_env_ == nullptr) {
        return Status::InternalError("Unable to get JVM: {}", rc);
    }
#else
    // the hadoop libhdfs will do all the stuff
    SetEnvIfNecessary();
    tls_env_ = getJNIEnv();
#endif
    *env = tls_env_;
    return Status::OK();
}

Status JniUtil::GetJniExceptionMsg(JNIEnv* env, bool log_stack, const string& prefix) {
    jthrowable exc = env->ExceptionOccurred();
    if (exc == nullptr) {
        return Status::OK();
    }
    env->ExceptionClear();
    DCHECK(throwable_to_string_id() != nullptr);
    const char* oom_msg_template =
            "$0 threw an unchecked exception. The JVM is likely out "
            "of memory (OOM).";
    jstring msg = static_cast<jstring>(
            env->CallStaticObjectMethod(jni_util_class(), throwable_to_string_id(), exc));
    if (env->ExceptionOccurred()) {
        env->ExceptionClear();
        string oom_msg = strings::Substitute(oom_msg_template, "throwableToString");
        LOG(WARNING) << oom_msg;
        return Status::InternalError(oom_msg);
    }
    JniUtfCharGuard msg_str_guard;
    RETURN_IF_ERROR(JniUtfCharGuard::create(env, msg, &msg_str_guard));
    if (log_stack) {
        jstring stack = static_cast<jstring>(
                env->CallStaticObjectMethod(jni_util_class(), throwable_to_stack_trace_id(), exc));
        if (env->ExceptionOccurred()) {
            env->ExceptionClear();
            string oom_msg = strings::Substitute(oom_msg_template, "throwableToStackTrace");
            LOG(WARNING) << oom_msg;
            return Status::InternalError(oom_msg);
        }
        JniUtfCharGuard c_stack_guard;
        RETURN_IF_ERROR(JniUtfCharGuard::create(env, stack, &c_stack_guard));
        LOG(WARNING) << c_stack_guard.get();
    }

    env->DeleteLocalRef(exc);
    return Status::InternalError("{}{}", prefix, msg_str_guard.get());
}

jobject JniUtil::convert_to_java_map(JNIEnv* env, const std::map<std::string, std::string>& map) {
    jclass hashmap_class = env->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = env->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = env->NewObject(hashmap_class, hashmap_constructor, map.size());
    jmethodID hashmap_put = env->GetMethodID(
            hashmap_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    for (const auto& it : map) {
        jstring key = env->NewStringUTF(it.first.c_str());
        jstring value = env->NewStringUTF(it.second.c_str());
        env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
    }
    env->DeleteLocalRef(hashmap_class);
    return hashmap_object;
}

std::map<std::string, std::string> JniUtil::convert_to_cpp_map(JNIEnv* env, jobject map) {
    std::map<std::string, std::string> resultMap;

    // Get the class and method ID of the java.util.Map interface
    jclass mapClass = env->FindClass("java/util/Map");
    jmethodID entrySetMethod = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");

    // Get the class and method ID of the java.util.Set interface
    jclass setClass = env->FindClass("java/util/Set");
    jmethodID iteratorSetMethod = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");

    // Get the class and method ID of the java.util.Iterator interface
    jclass iteratorClass = env->FindClass("java/util/Iterator");
    jmethodID hasNextMethod = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID nextMethod = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");

    // Get the class and method ID of the java.util.Map.Entry interface
    jclass entryClass = env->FindClass("java/util/Map$Entry");
    jmethodID getKeyMethod = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    jmethodID getValueMethod = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

    // Call the entrySet method to get the set of key-value pairs
    jobject entrySet = env->CallObjectMethod(map, entrySetMethod);

    // Call the iterator method on the set to iterate over the key-value pairs
    jobject iteratorSet = env->CallObjectMethod(entrySet, iteratorSetMethod);

    // Iterate over the key-value pairs
    while (env->CallBooleanMethod(iteratorSet, hasNextMethod)) {
        // Get the current entry
        jobject entry = env->CallObjectMethod(iteratorSet, nextMethod);

        // Get the key and value from the entry
        jobject javaKey = env->CallObjectMethod(entry, getKeyMethod);
        jobject javaValue = env->CallObjectMethod(entry, getValueMethod);

        // Convert the key and value to C++ strings
        const char* key = env->GetStringUTFChars(static_cast<jstring>(javaKey), nullptr);
        const char* value = env->GetStringUTFChars(static_cast<jstring>(javaValue), nullptr);

        // Store the key-value pair in the map
        resultMap[key] = value;

        // Release the string references
        env->ReleaseStringUTFChars(static_cast<jstring>(javaKey), key);
        env->ReleaseStringUTFChars(static_cast<jstring>(javaValue), value);

        // Delete local references
        env->DeleteLocalRef(entry);
        env->DeleteLocalRef(javaKey);
        env->DeleteLocalRef(javaValue);
    }

    // Delete local references
    env->DeleteLocalRef(iteratorSet);
    env->DeleteLocalRef(entrySet);
    env->DeleteLocalRef(mapClass);
    env->DeleteLocalRef(setClass);
    env->DeleteLocalRef(iteratorClass);
    env->DeleteLocalRef(entryClass);

    return resultMap;
}

Status JniUtil::GetGlobalClassRef(JNIEnv* env, const char* class_str, jclass* class_ref) {
    *class_ref = NULL;
    jclass local_cl = env->FindClass(class_str);
    RETURN_ERROR_IF_EXC(env);
    RETURN_IF_ERROR(LocalToGlobalRef(env, local_cl, reinterpret_cast<jobject*>(class_ref)));
    env->DeleteLocalRef(local_cl);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniUtil::LocalToGlobalRef(JNIEnv* env, jobject local_ref, jobject* global_ref) {
    *global_ref = env->NewGlobalRef(local_ref);
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniUtil::init_jni_scanner_loader(JNIEnv* env) {
    // Get scanner loader;
    jclass jni_scanner_loader_cls;
    std::string jni_scanner_loader_str = "org/apache/doris/common/classloader/ScannerLoader";
    RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(env, jni_scanner_loader_str.c_str(),
                                               &jni_scanner_loader_cls));
    jmethodID jni_scanner_loader_constructor =
            env->GetMethodID(jni_scanner_loader_cls, "<init>", "()V");
    RETURN_ERROR_IF_EXC(env);
    jni_scanner_loader_method_ = env->GetMethodID(jni_scanner_loader_cls, "getLoadedClass",
                                                  "(Ljava/lang/String;)Ljava/lang/Class;");
    if (jni_scanner_loader_method_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find ScannerLoader.getLoadedClass method.");
    }
    RETURN_ERROR_IF_EXC(env);
    jmethodID load_jni_scanner =
            env->GetMethodID(jni_scanner_loader_cls, "loadAllScannerJars", "()V");
    RETURN_ERROR_IF_EXC(env);

    jni_scanner_loader_obj_ =
            env->NewObject(jni_scanner_loader_cls, jni_scanner_loader_constructor);
    RETURN_ERROR_IF_EXC(env);
    if (jni_scanner_loader_obj_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to create ScannerLoader object.");
    }
    env->CallVoidMethod(jni_scanner_loader_obj_, load_jni_scanner);
    RETURN_ERROR_IF_EXC(env);

    _clean_udf_cache_method_id = env->GetMethodID(jni_scanner_loader_cls, "cleanUdfClassLoader",
                                                  "(Ljava/lang/String;)V");
    if (_clean_udf_cache_method_id == nullptr) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::InternalError("Failed to find removeUdfClassLoader method.");
    }
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniUtil::clean_udf_class_load_cache(const std::string& function_signature) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    env->CallVoidMethod(jni_scanner_loader_obj_, _clean_udf_cache_method_id,
                        env->NewStringUTF(function_signature.c_str()));
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniUtil::get_jni_scanner_class(JNIEnv* env, const char* classname,
                                      jclass* jni_scanner_class) {
    // Get JNI scanner class by class name;
    jobject loaded_class_obj = env->CallObjectMethod(
            jni_scanner_loader_obj_, jni_scanner_loader_method_, env->NewStringUTF(classname));
    RETURN_ERROR_IF_EXC(env);
    *jni_scanner_class = reinterpret_cast<jclass>(env->NewGlobalRef(loaded_class_obj));
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status JniUtil::Init() {
    // RETURN_IF_ERROR(LibJVMLoader::instance().load());

    // Get the JNIEnv* corresponding to current thread.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));

    if (env == NULL) return Status::InternalError("Failed to get/create JVM");
    // Find JniUtil class and create a global ref.
    jclass local_jni_util_cl = env->FindClass("org/apache/doris/common/jni/utils/JniUtil");
    if (local_jni_util_cl == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil class.");
    }
    jni_util_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_jni_util_cl));
    if (jni_util_cl_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to create global reference to JniUtil class.");
    }
    env->DeleteLocalRef(local_jni_util_cl);
    if (env->ExceptionOccurred()) {
        return Status::InternalError("Failed to delete local reference to JniUtil class.");
    }

    // Find InternalException class and create a global ref.
    jclass local_internal_exc_cl =
            env->FindClass("org/apache/doris/common/exception/InternalException");
    if (local_internal_exc_cl == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil class.");
    }
    internal_exc_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_internal_exc_cl));
    if (internal_exc_cl_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to create global reference to JniUtil class.");
    }
    env->DeleteLocalRef(local_internal_exc_cl);
    if (env->ExceptionOccurred()) {
        return Status::InternalError("Failed to delete local reference to JniUtil class.");
    }

    // Find JNINativeMethod class and create a global ref.
    jclass local_jni_native_exc_cl =
            env->FindClass("org/apache/doris/common/jni/utils/JNINativeMethod");
    if (local_jni_native_exc_cl == nullptr) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::InternalError("Failed to find JNINativeMethod class.");
    }
    jni_native_method_exc_cl_ =
            reinterpret_cast<jclass>(env->NewGlobalRef(local_jni_native_exc_cl));
    if (jni_native_method_exc_cl_ == nullptr) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::InternalError("Failed to create global reference to JNINativeMethod class.");
    }
    env->DeleteLocalRef(local_jni_native_exc_cl);
    if (env->ExceptionOccurred()) {
        return Status::InternalError("Failed to delete local reference to JNINativeMethod class.");
    }
    std::string resize_column_name = "resizeStringColumn";
    std::string resize_column_sign = "(JI)J";
    std::string memory_alloc_name = "memoryTrackerMalloc";
    std::string memory_alloc_sign = "(J)J";
    std::string memory_free_name = "memoryTrackerFree";
    std::string memory_free_sign = "(J)V";
    static JNINativeMethod java_native_methods[] = {
            {const_cast<char*>(resize_column_name.c_str()),
             const_cast<char*>(resize_column_sign.c_str()),
             (void*)&JavaNativeMethods::resizeStringColumn},
            {const_cast<char*>(memory_alloc_name.c_str()),
             const_cast<char*>(memory_alloc_sign.c_str()), (void*)&JavaNativeMethods::memoryMalloc},
            {const_cast<char*>(memory_free_name.c_str()),
             const_cast<char*>(memory_free_sign.c_str()), (void*)&JavaNativeMethods::memoryFree},
    };

    int res = env->RegisterNatives(jni_native_method_exc_cl_, java_native_methods,
                                   sizeof(java_native_methods) / sizeof(java_native_methods[0]));
    DCHECK_EQ(res, 0);

    // Throwable toString()
    throwable_to_string_id_ = env->GetStaticMethodID(jni_util_cl_, "throwableToString",
                                                     "(Ljava/lang/Throwable;)Ljava/lang/String;");
    if (throwable_to_string_id_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil.throwableToString method.");
    }

    // throwableToStackTrace()
    throwable_to_stack_trace_id_ = env->GetStaticMethodID(
            jni_util_cl_, "throwableToStackTrace", "(Ljava/lang/Throwable;)Ljava/lang/String;");
    if (throwable_to_stack_trace_id_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil.throwableToFullStackTrace method.");
    }

    get_jvm_metrics_id_ = env->GetStaticMethodID(jni_util_cl_, "getJvmMemoryMetrics", "()[B");
    if (get_jvm_metrics_id_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil.getJvmMemoryMetrics method.");
    }

    get_jvm_threads_id_ = env->GetStaticMethodID(jni_util_cl_, "getJvmThreadsInfo", "([B)[B");
    if (get_jvm_threads_id_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil.getJvmThreadsInfo method.");
    }

    get_jmx_json_ = env->GetStaticMethodID(jni_util_cl_, "getJMXJson", "()[B");
    if (get_jmx_json_ == NULL) {
        if (env->ExceptionOccurred()) env->ExceptionDescribe();
        return Status::InternalError("Failed to find JniUtil.getJMXJson method.");
    }
    RETURN_IF_ERROR(init_jni_scanner_loader(env));
    jvm_inited_ = true;
    DorisMetrics::instance()->init_jvm_metrics(env);
    return Status::OK();
}

} // namespace doris
