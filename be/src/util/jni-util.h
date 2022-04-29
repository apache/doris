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

#ifdef LIBJVM
#include <hdfs/hdfs.h>
#include <jni.h>

#include "common/status.h"
#include "gutil/macros.h"
#include "gutil/strings/substitute.h"
#include "util//thrift_util.h"

namespace doris {

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
            return Status::OK();
        }
        return GetJNIEnvSlowPath(env);
    }

    static Status GetGlobalClassRef(JNIEnv* env, const char* class_str,
                                    jclass* class_ref) WARN_UNUSED_RESULT;

    static Status LocalToGlobalRef(JNIEnv* env, jobject local_ref,
                                   jobject* global_ref) WARN_UNUSED_RESULT;

    static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
                                     const std::string& prefix = "") WARN_UNUSED_RESULT;

    static jclass jni_util_class() { return jni_util_cl_; }
    static jmethodID throwable_to_stack_trace_id() { return throwable_to_stack_trace_id_; }

private:
    static Status GetJNIEnvSlowPath(JNIEnv** env);

    static bool jvm_inited_;
    static jclass internal_exc_cl_;
    static jclass jni_util_cl_;
    static jmethodID throwable_to_string_id_;
    static jmethodID throwable_to_stack_trace_id_;
    static jmethodID get_jvm_metrics_id_;
    static jmethodID get_jvm_threads_id_;
    static jmethodID get_jmx_json_;

    // Thread-local cache of the JNIEnv for this thread.
    static __thread JNIEnv* tls_env_;
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
    JNIEnv* env;
    jstring jstr;
    const char* utf_chars;
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

    JNIEnv* env_;
};

template <class T>
Status SerializeThriftMsg(JNIEnv* env, T* msg, jbyteArray* serialized_msg) {
    int buffer_size = 100 * 1024; // start out with 100KB
    ThriftSerializer serializer(false, buffer_size);

    uint8_t* buffer = NULL;
    uint32_t size = 0;
    RETURN_IF_ERROR(serializer.serialize(msg, &size, &buffer));

    // Make sure that 'size' is within the limit of INT_MAX as the use of
    // 'size' below takes int.
    if (size > INT_MAX) {
        return Status::InternalError(strings::Substitute(
                "The length of the serialization buffer ($0 bytes) exceeds the limit of $1 bytes",
                size, INT_MAX));
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

#endif
