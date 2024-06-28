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

#include <butil/macros.h>
#include <jni.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>

#include <string>
#include <unordered_map>

#include "common/status.h"
#include "jni_md.h"
#include "util/thrift_util.h"

#ifdef USE_HADOOP_HDFS
// defined in hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/jni_helper.c
extern "C" JNIEnv* getJNIEnv(void);
#endif

namespace doris {
class JniUtil;

#define RETURN_ERROR_IF_EXC(env)                                     \
    do {                                                             \
        jthrowable exc = (env)->ExceptionOccurred();                 \
        if (exc != nullptr) return JniUtil::GetJniExceptionMsg(env); \
    } while (false)

class JniUtil {
public:
    static Status Init() WARN_UNUSED_RESULT;

    static jmethodID throwable_to_string_id() { return throwable_to_string_id_; }

    static Status GetJNIEnv(JNIEnv** env) {
        if (tls_env_) {
            *env = tls_env_;
        } else {
            Status status = GetJNIEnvSlowPath(env);
            if (!status.ok()) {
                return status;
            }
        }
        if (*env == nullptr) {
            return Status::RuntimeError("Failed to get JNIEnv: it is nullptr.");
        }
        return Status::OK();
    }

    static Status GetGlobalClassRef(JNIEnv* env, const char* class_str,
                                    jclass* class_ref) WARN_UNUSED_RESULT;

    static Status LocalToGlobalRef(JNIEnv* env, jobject local_ref,
                                   jobject* global_ref) WARN_UNUSED_RESULT;

    static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
                                     const std::string& prefix = "") WARN_UNUSED_RESULT;

    static jclass jni_util_class() { return jni_util_cl_; }
    static jmethodID throwable_to_stack_trace_id() { return throwable_to_stack_trace_id_; }

    static const int64_t INITIAL_RESERVED_BUFFER_SIZE = 1024;
    // TODO: we need a heuristic strategy to increase buffer size for variable-size output.
    static inline int64_t IncreaseReservedBufferSize(int n) {
        return INITIAL_RESERVED_BUFFER_SIZE << n;
    }
    static Status get_jni_scanner_class(JNIEnv* env, const char* classname, jclass* loaded_class);
    static jobject convert_to_java_map(JNIEnv* env, const std::map<std::string, std::string>& map);
    static std::map<std::string, std::string> convert_to_cpp_map(JNIEnv* env, jobject map);
    static size_t get_max_jni_heap_memory_size();
    static Status clean_udf_class_load_cache(const std::string& function_signature);

private:
    static void parse_max_heap_memory_size_from_jvm(JNIEnv* env);
    static Status GetJNIEnvSlowPath(JNIEnv** env);
    static Status init_jni_scanner_loader(JNIEnv* env);

    static bool jvm_inited_;
    static jclass internal_exc_cl_;
    static jclass jni_native_method_exc_cl_;
    static jclass jni_util_cl_;
    static jmethodID throwable_to_string_id_;
    static jmethodID throwable_to_stack_trace_id_;
    static jmethodID get_jvm_metrics_id_;
    static jmethodID get_jvm_threads_id_;
    static jmethodID get_jmx_json_;
    // JNI scanner loader
    static jobject jni_scanner_loader_obj_;
    static jmethodID jni_scanner_loader_method_;
    // Thread-local cache of the JNIEnv for this thread.
    static __thread JNIEnv* tls_env_;
    static jlong max_jvm_heap_memory_size_;
    static jmethodID _clean_udf_cache_method_id;
};

/// Helper class for lifetime management of chars from JNI, releasing JNI chars when
/// destructed
class JniUtfCharGuard {
public:
    /// Construct a JniUtfCharGuards holding nothing
    JniUtfCharGuard() : utf_chars(nullptr) {}

    /// Release the held char sequence if there is one.
    ~JniUtfCharGuard() {
        if (utf_chars != nullptr) env->ReleaseStringUTFChars(jstr, utf_chars);
    }

    /// Try to get chars from jstr. If error is returned, utf_chars and get() remain
    /// to be nullptr, otherwise they point to a valid char sequence. The char sequence
    /// lives as long as this guard. jstr should not be null.
    static Status create(JNIEnv* env, jstring jstr, JniUtfCharGuard* out);

    /// Get the char sequence. Returns nullptr if the guard does hold a char sequence.
    const char* get() { return utf_chars; }

private:
    JNIEnv* env = nullptr;
    jstring jstr;
    const char* utf_chars = nullptr;
    DISALLOW_COPY_AND_ASSIGN(JniUtfCharGuard);
};

class JniLocalFrame {
public:
    JniLocalFrame() : env_(nullptr) {}
    ~JniLocalFrame() {
        if (env_ != nullptr) env_->PopLocalFrame(nullptr);
    }

    JniLocalFrame(JniLocalFrame&& other) noexcept : env_(other.env_) { other.env_ = nullptr; }

    /// Pushes a new JNI local frame. The frame can support max_local_ref local references.
    /// The number of local references created inside the frame might exceed max_local_ref,
    /// but there is no guarantee that memory will be available.
    /// Push should be called at most once.
    Status push(JNIEnv* env, int max_local_ref = 10) WARN_UNUSED_RESULT;

private:
    DISALLOW_COPY_AND_ASSIGN(JniLocalFrame);

    JNIEnv* env_ = nullptr;
};

template <class T>
Status SerializeThriftMsg(JNIEnv* env, T* msg, jbyteArray* serialized_msg) {
    int buffer_size = 100 * 1024; // start out with 100KB
    ThriftSerializer serializer(false, buffer_size);

    uint8_t* buffer = nullptr;
    uint32_t size = 0;
    RETURN_IF_ERROR(serializer.serialize(msg, &size, &buffer));

    // Make sure that 'size' is within the limit of INT_MAX as the use of
    // 'size' below takes int.
    if (size > INT_MAX) {
        return Status::InternalError(
                "The length of the serialization buffer ({} bytes) exceeds the limit of {} bytes",
                size, INT_MAX);
    }

    /// create jbyteArray given buffer
    *serialized_msg = env->NewByteArray(size);
    RETURN_ERROR_IF_EXC(env);
    if (*serialized_msg == NULL) return Status::InternalError("couldn't construct jbyteArray");
    env->SetByteArrayRegion(*serialized_msg, 0, size, reinterpret_cast<jbyte*>(buffer));
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

} // namespace doris
