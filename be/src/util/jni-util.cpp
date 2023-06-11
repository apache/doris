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
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/jni_native_method.h"
#include "util/libjvm_loader.h"

using std::string;

namespace doris {

namespace {
JavaVM* g_vm;
[[maybe_unused]] std::once_flag g_vm_once;

const std::string GetDorisJNIDefaultClasspath() {
    const auto* doris_home = getenv("DORIS_HOME");
    DCHECK(doris_home) << "Environment variable DORIS_HOME is not set.";

    std::ostringstream out;
    std::string path(doris_home);
    path += "/lib";
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (entry.path().extension() != ".jar") {
            continue;
        }
        if (out.str().empty()) {
            out << entry.path().string();
        } else {
            out << ":" << entry.path().string();
        }
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
    static const std::string classpath =
            fmt::format("{}/conf:{}", doris_home, GetDorisJNIDefaultClasspath());
    setenv("CLASSPATH", classpath.c_str(), 0);

    // LIBHDFS_OPTS
    setenv("LIBHDFS_OPTS",
           fmt::format("-Djava.library.path={}/lib/hadoop_hdfs/native", getenv("DORIS_HOME"))
                   .c_str(),
           0);
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

        JNIEnv* env;
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
jclass JniUtil::internal_exc_cl_ = NULL;
jclass JniUtil::jni_util_cl_ = NULL;
jclass JniUtil::jni_native_method_exc_cl_ = nullptr;
jmethodID JniUtil::throwable_to_string_id_ = NULL;
jmethodID JniUtil::throwable_to_stack_trace_id_ = NULL;
jmethodID JniUtil::get_jvm_metrics_id_ = NULL;
jmethodID JniUtil::get_jvm_threads_id_ = NULL;
jmethodID JniUtil::get_jmx_json_ = NULL;

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
    DCHECK(env_ == NULL);
    DCHECK_GT(max_local_ref, 0);
    if (env->PushLocalFrame(max_local_ref) < 0) {
        env->ExceptionClear();
        return Status::InternalError("failed to push frame");
    }
    env_ = env;
    return Status::OK();
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

Status JniUtil::Init() {
    RETURN_IF_ERROR(LibJVMLoader::instance().load());

    // Get the JNIEnv* corresponding to current thread.
    JNIEnv* env;
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
    jvm_inited_ = true;
    return Status::OK();
}

} // namespace doris
