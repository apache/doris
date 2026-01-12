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

#include "common/status.h"
#include "jni_md.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

#ifdef USE_HADOOP_HDFS
// defined in hadoop/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs/jni_helper.c
extern "C" JNIEnv* getJNIEnv(void);
#endif

namespace doris {

#define RETURN_ERROR_IF_EXC(env)                      \
    do {                                              \
        if (env->ExceptionCheck()) [[unlikely]]       \
            return Jni::Env::GetJniExceptionMsg(env); \
    } while (false)

//In order to reduce the potential risks caused by not handling exceptions,
// you need to refer to  https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/functions.html
// to confirm whether the jni method will throw an exception.

namespace Jni {
class Env {
public:
    static Status Get(JNIEnv** env) {
        if (tls_env_) {
            *env = tls_env_;
        } else {
            Status status = GetJNIEnvSlowPath(env);
            if (!status.ok()) {
                return status;
            }
        }
        if (*env == nullptr) [[unlikely]] {
            return Status::JniError("Failed to get JNIEnv: it is nullptr.");
        }
        return Status::OK();
    }

    static Status Init() { return init_throw_exception(); }

    static Status GetJniExceptionMsg(JNIEnv* env, bool log_stack = true,
                                     const std::string& prefix = "") WARN_UNUSED_RESULT;

private:
    static Status GetJNIEnvSlowPath(JNIEnv** env);
    static Status init_throw_exception() {
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(Jni::Env::Get(&env));

        // Find JniUtil class and create a global ref.
        jclass local_jni_util_cl = env->FindClass("org/apache/doris/common/jni/utils/JniUtil");
        if (local_jni_util_cl == nullptr) {
            if (env->ExceptionOccurred()) {
                env->ExceptionDescribe();
            }
            return Status::JniError("Failed to find JniUtil class.");
        }
        jni_util_cl_ = reinterpret_cast<jclass>(env->NewGlobalRef(local_jni_util_cl));
        env->DeleteLocalRef(local_jni_util_cl);
        if (jni_util_cl_ == nullptr) {
            if (env->ExceptionOccurred()) {
                env->ExceptionDescribe();
            }
            return Status::JniError("Failed to create global reference to JniUtil class.");
        }
        if (env->ExceptionOccurred()) {
            return Status::JniError("Failed to delete local reference to JniUtil class.");
        }

        // Throwable toString()
        throwable_to_string_id_ = env->GetStaticMethodID(
                jni_util_cl_, "throwableToString", "(Ljava/lang/Throwable;)Ljava/lang/String;");
        if (throwable_to_string_id_ == nullptr) {
            if (env->ExceptionOccurred()) {
                env->ExceptionDescribe();
            }
            return Status::JniError("Failed to find JniUtil.throwableToString method.");
        }

        // throwableToStackTrace()
        throwable_to_stack_trace_id_ = env->GetStaticMethodID(
                jni_util_cl_, "throwableToStackTrace", "(Ljava/lang/Throwable;)Ljava/lang/String;");
        if (throwable_to_stack_trace_id_ == nullptr) {
            if (env->ExceptionOccurred()) {
                env->ExceptionDescribe();
            }
            return Status::JniError("Failed to find JniUtil.throwableToFullStackTrace method.");
        }
        return Status::OK();
    }

private:
    // Thread-local cache of the JNIEnv for this thread.
    static __thread JNIEnv* tls_env_;

    //for exception
    static jclass jni_util_cl_;
    static jmethodID throwable_to_string_id_;
    static jmethodID throwable_to_stack_trace_id_;
};

enum RefType { Local, Global };

enum BufferType { Chars, ByteArray };

template <RefType Ref>
struct RefHelper {};

template <>
struct RefHelper<Local> {
    static jobject create(JNIEnv* env, jobject obj) { return env->NewLocalRef(obj); }

    static void destroy(JNIEnv* env, jobject obj) { env->DeleteLocalRef(obj); }

    static Status get_env(JNIEnv** env) {
        // Get the JNIEnv* corresponding to current thread.
        return Jni::Env::Get(env);
    }
};

template <>
struct RefHelper<Global> {
    static jobject create(JNIEnv* env, jobject obj) { return env->NewGlobalRef(obj); }

    static void destroy(JNIEnv* env, jobject obj) { env->DeleteGlobalRef(obj); }

    static Status get_env(JNIEnv** env) { return Jni::Env::Get(env); }
};

template <RefType Ref>
class Object;

template <RefType Ref>
class Class;

class MethodId {
public:
    MethodId() = default;
    bool uninitialized() const { return _id == nullptr; }

    template <RefType U>
    friend class Object;

    template <RefType U>
    friend class Class;

private:
    jmethodID _id = nullptr;
};

class FieldId {
public:
    FieldId() = default;
    bool uninitialized() const { return _id == nullptr; }

    template <RefType U>
    friend class Object;

    template <RefType U>
    friend class Class;

private:
    jfieldID _id = nullptr;
};

enum CallTag {
    ObjectMethod,
    IntMethod,
    LongMethod,
    VoidMethod,
    BooleanMethod,

    StaticObjectMethod,
    StaticIntMethod,
    StaticLongMethod,
    StaticVoidMethod,

    NewObject,

    NonvirtualVoidMethod,
    NonvirtualObjectMethod,
    NonvirtualIntMethod,
    NonvirtualBooleanMethod,
};

template <CallTag Tag>
struct CallHelper {};

template <>
struct CallHelper<ObjectMethod> {
    static jobject call_impl(JNIEnv* env, jobject obj, jmethodID methodID, const jvalue* args) {
        return env->CallObjectMethodA(obj, methodID, args);
    }
    using BASE_TYPE = jobject;
    using RETURN_TYPE = jobject;
};

template <>
struct CallHelper<IntMethod> {
    static jint call_impl(JNIEnv* env, jobject obj, jmethodID methodID, const jvalue* args) {
        return env->CallIntMethodA(obj, methodID, args);
    }
    using BASE_TYPE = jobject;
    using RETURN_TYPE = jint;
};

template <>
struct CallHelper<LongMethod> {
    static jlong call_impl(JNIEnv* env, jobject obj, jmethodID methodID, const jvalue* args) {
        return env->CallLongMethodA(obj, methodID, args);
    }
    using BASE_TYPE = jobject;
    using RETURN_TYPE = jlong;
};

template <>
struct CallHelper<VoidMethod> {
    static void call_impl(JNIEnv* env, jobject obj, jmethodID methodID, const jvalue* args) {
        env->CallVoidMethodA(obj, methodID, args);
    }
    using BASE_TYPE = jobject;
    using RETURN_TYPE = void;
};

template <>
struct CallHelper<BooleanMethod> {
    static jboolean call_impl(JNIEnv* env, jobject obj, jmethodID methodID, const jvalue* args) {
        return env->CallBooleanMethodA(obj, methodID, args);
    }
    using BASE_TYPE = jobject;
    using RETURN_TYPE = jboolean;
};

template <>
struct CallHelper<StaticObjectMethod> {
    static jobject call_impl(JNIEnv* env, jclass cls, jmethodID methodID, const jvalue* args) {
        return env->CallStaticObjectMethodA(cls, methodID, args);
    }
    using BASE_TYPE = jclass;
    using RETURN_TYPE = jobject;
};

template <>
struct CallHelper<StaticIntMethod> {
    static jint call_impl(JNIEnv* env, jclass cls, jmethodID methodID, const jvalue* args) {
        return env->CallStaticIntMethodA(cls, methodID, args);
    }
    using BASE_TYPE = jclass;
    using RETURN_TYPE = jint;
};

template <>
struct CallHelper<StaticLongMethod> {
    static jlong call_impl(JNIEnv* env, jclass cls, jmethodID methodID, const jvalue* args) {
        return env->CallStaticLongMethodA(cls, methodID, args);
    }
    using BASE_TYPE = jclass;
    using RETURN_TYPE = jlong;
};

template <>
struct CallHelper<StaticVoidMethod> {
    static void call_impl(JNIEnv* env, jclass cls, jmethodID methodID, const jvalue* args) {
        return env->CallStaticVoidMethodA(cls, methodID, args);
    }

    using BASE_TYPE = jclass;
    using RETURN_TYPE = void;
};

template <>
struct CallHelper<NewObject> {
    static jobject call_impl(JNIEnv* env, jclass cls, jmethodID methodID, const jvalue* args) {
        return env->NewObjectA(cls, methodID, args);
    }

    using BASE_TYPE = jclass;
    using RETURN_TYPE = jobject;
};

template <>
struct CallHelper<NonvirtualVoidMethod> {
    static void call_impl(JNIEnv* env, jobject obj, jclass clazz, jmethodID methodID,
                          const jvalue* args) {
        return env->CallNonvirtualVoidMethodA(obj, clazz, methodID, args);
    }

    using BASE_TYPE = jobject;
    using RETURN_TYPE = void;
};

template <>
struct CallHelper<NonvirtualObjectMethod> {
    static jobject call_impl(JNIEnv* env, jobject obj, jclass clazz, jmethodID methodID,
                             const jvalue* args) {
        return env->CallNonvirtualObjectMethodA(obj, clazz, methodID, args);
    }

    using BASE_TYPE = jobject;
    using RETURN_TYPE = jobject;
};

template <>
struct CallHelper<NonvirtualIntMethod> {
    static jint call_impl(JNIEnv* env, jobject obj, jclass clazz, jmethodID methodID,
                          const jvalue* args) {
        return env->CallNonvirtualIntMethodA(obj, clazz, methodID, args);
    }

    using BASE_TYPE = jobject;
    using RETURN_TYPE = jint;
};

template <>
struct CallHelper<NonvirtualBooleanMethod> {
    static jboolean call_impl(JNIEnv* env, jobject obj, jclass clazz, jmethodID methodID,
                              const jvalue* args) {
        return env->CallNonvirtualBooleanMethodA(obj, clazz, methodID, args);
    }

    using BASE_TYPE = jobject;
    using RETURN_TYPE = jboolean;
};

template <CallTag tag>
class FunctionCall {
public:
    FunctionCall(FunctionCall&& other) noexcept = default;

    static FunctionCall instance(JNIEnv* env, typename CallHelper<tag>::BASE_TYPE base,
                                 jmethodID method_id) {
        return FunctionCall(env, base, method_id);
    }

    /// Pass a primitive arg (eg an integer).
    /// Multiple arguments may be passed by repeated calls.
    template <class T>
        requires std::disjunction_v<std::is_same<T, jboolean>, std::is_same<T, jbyte>,
                                    std::is_same<T, jchar>, std::is_same<T, jshort>,
                                    std::is_same<T, jint>, std::is_same<T, jlong>,
                                    std::is_same<T, jfloat>, std::is_same<T, jdouble>>
    FunctionCall& with_arg(T arg) {
        jvalue v;
        std::memset(&v, 0, sizeof(v));
        if constexpr (std::is_same_v<T, jboolean>) {
            v.z = arg;
        } else if constexpr (std::is_same_v<T, jbyte>) {
            v.b = arg;
        } else if constexpr (std::is_same_v<T, jchar>) {
            v.c = arg;
        } else if constexpr (std::is_same_v<T, jshort>) {
            v.s = arg;
        } else if constexpr (std::is_same_v<T, jint>) {
            v.i = arg;
        } else if constexpr (std::is_same_v<T, jlong>) {
            v.j = arg;
        } else if constexpr (std::is_same_v<T, jfloat>) {
            v.f = arg;
        } else if constexpr (std::is_same_v<T, jdouble>) {
            v.d = arg;
        } else {
            static_assert(false);
        }
        _args.push_back(v);
        return *this;
    }

    template <RefType Ref>
    FunctionCall& with_arg(const Object<Ref>& obj) WARN_UNUSED_RESULT;

    template <typename ReturnType>
        requires(std::is_same_v<typename CallHelper<tag>::RETURN_TYPE, ReturnType>)
    Status call(ReturnType* result) {
        *result = CallHelper<tag>::call_impl(_env, _base, _method, _args.data());
        RETURN_ERROR_IF_EXC(_env);
        return Status::OK();
    }

    Status call(Object<Local>* result);

    Status call(Object<Global>* result);

    Status call() {
        using return_type = typename CallHelper<tag>::RETURN_TYPE;
        if constexpr (std::disjunction_v<
                              std::is_same<return_type, jboolean>, std::is_same<return_type, jbyte>,
                              std::is_same<return_type, jchar>, std::is_same<return_type, jshort>,
                              std::is_same<return_type, jint>, std::is_same<return_type, jlong>,
                              std::is_same<return_type, jfloat>, std::is_same<return_type, jdouble>,
                              std::is_same<return_type, void>>) {
            CallHelper<tag>::call_impl(_env, _base, _method, _args.data());
            RETURN_ERROR_IF_EXC(_env);
        } else if constexpr (std::is_same_v<return_type, jobject>) {
            jobject tmp = CallHelper<tag>::call_impl(_env, _base, _method, _args.data());
            _env->DeleteLocalRef(tmp);
            RETURN_ERROR_IF_EXC(_env);
        } else {
            static_assert(false);
        }
        return Status::OK();
    }

protected:
    explicit FunctionCall(JNIEnv* env, typename CallHelper<tag>::BASE_TYPE base,
                          jmethodID method_id)
            : _env(env), _base(base), _method(method_id) {}

    JNIEnv* _env = nullptr;
    CallHelper<tag>::BASE_TYPE _base; // is jobject/jclass  not need new/delete local/global ref.
    const jmethodID _method = nullptr;
    std::vector<jvalue> _args;
    Status _st = Status::OK();
    DISALLOW_COPY_AND_ASSIGN(FunctionCall);
};

template <CallTag tag>
class NonvirtualFunctionCall : public FunctionCall<tag> {
public:
    NonvirtualFunctionCall(NonvirtualFunctionCall&& other) noexcept = default;

    static NonvirtualFunctionCall instance(JNIEnv* env, typename CallHelper<tag>::BASE_TYPE base,
                                           jclass cls, jmethodID method_id) {
        return NonvirtualFunctionCall(env, base, cls, method_id);
    }

    // no override
    template <class T>
        requires std::disjunction_v<std::is_same<T, jboolean>, std::is_same<T, jbyte>,
                                    std::is_same<T, jchar>, std::is_same<T, jshort>,
                                    std::is_same<T, jint>, std::is_same<T, jlong>,
                                    std::is_same<T, jfloat>, std::is_same<T, jdouble>>
    NonvirtualFunctionCall& with_arg(T arg) {
        jvalue v;
        std::memset(&v, 0, sizeof(v));
        if constexpr (std::is_same_v<T, jboolean>) {
            v.z = arg;
        } else if constexpr (std::is_same_v<T, jbyte>) {
            v.b = arg;
        } else if constexpr (std::is_same_v<T, jchar>) {
            v.c = arg;
        } else if constexpr (std::is_same_v<T, jshort>) {
            v.s = arg;
        } else if constexpr (std::is_same_v<T, jint>) {
            v.i = arg;
        } else if constexpr (std::is_same_v<T, jlong>) {
            v.j = arg;
        } else if constexpr (std::is_same_v<T, jfloat>) {
            v.f = arg;
        } else if constexpr (std::is_same_v<T, jdouble>) {
            v.d = arg;
        } else {
            static_assert(false);
        }
        this->_args.push_back(v);
        return *this;
    }

    template <RefType Ref>
    NonvirtualFunctionCall& with_arg(const Object<Ref>& obj) WARN_UNUSED_RESULT;

    // no override
    Status call() {
        using return_type = typename CallHelper<tag>::RETURN_TYPE;
        if constexpr (std::disjunction_v<
                              std::is_same<return_type, jboolean>, std::is_same<return_type, jbyte>,
                              std::is_same<return_type, jchar>, std::is_same<return_type, jshort>,
                              std::is_same<return_type, jint>, std::is_same<return_type, jlong>,
                              std::is_same<return_type, jfloat>, std::is_same<return_type, jdouble>,
                              std::is_same<return_type, void>>) {
            CallHelper<tag>::call_impl(this->_env, this->_base, _cls, this->_method,
                                       this->_args.data());
            RETURN_ERROR_IF_EXC(this->_env);
        } else if constexpr (std::is_same_v<return_type, jobject>) {
            jobject tmp = CallHelper<tag>::call_impl(this->_env, this->_base, _cls, this->_method,
                                                     this->_args.data());
            RETURN_ERROR_IF_EXC(this->_env);
            this->_env->DeleteLocalRef(tmp);
        } else {
            static_assert(false);
        }
        return Status::OK();
    }

    Status call(Object<Local>* result);

    template <typename ReturnType>
        requires(std::is_same_v<typename CallHelper<tag>::RETURN_TYPE, ReturnType>)
    Status call(ReturnType* result) {
        *result = CallHelper<tag>::call_impl(this->_env, this->_base, _cls, this->_method,
                                             this->_args.data());
        RETURN_ERROR_IF_EXC(this->_env);
        return Status::OK();
    }

private:
    explicit NonvirtualFunctionCall(JNIEnv* env, typename CallHelper<tag>::BASE_TYPE base,
                                    jclass cls, jmethodID method_id)
            : FunctionCall<tag>(env, base, method_id), _cls(cls) {}
    jclass _cls;
    DISALLOW_COPY_AND_ASSIGN(NonvirtualFunctionCall);
};

/**
 * When writing JNI code, developers usually need to pay extra attention to several error-prone aspects, including:
 * 1. The reference type of jobject (local vs. global)
 * 2. The lifetime and scope of JNI references
 * 3. Proper release of references after use
 * 4. Explicit exception checking after JNI calls
 * Because these concerns are verbose and easy to overlook, they often lead to bugs or inconsistent code. To simplify this, 
 * we provide a wrapper framework around raw JNI APIs. The following describes how to use it (assuming the user already 
 * understands the basic JNI programming model).
 *
 * 0. Get JNIEnv* env: `Status st = Jni::Env::Get(&env)`
 * 1. Choose the reference type
 *   First, determine whether the JNI object should be a local or global reference. Based on this, create the corresponding C++ wrapper object:
 *   LocalObject / GlobalObject. If the exact JNI type is known, use specialized wrappers such as <Local/Global><Array/String/Class>. 
 * 2. Initialize the object
 *   For `jclass`, typically use: `Status st = Jni::Util::find_class(xxx);`
 *   For other object types, they are usually initialized via: `Status st = clazz.new_object(xxx).with_arg(xxx).call(&object) or by calling methods on existing objects.
 * 3. Call methods and retrieve results
 *   To invoke a method and obtain a return value, use: `Status st = object.call_<return_type>_method(xxx).call(&result);` 
 * 
 * Notes
 * 1. All JNI references are automatically released in the wrapper’s destructor, ensuring safe and deterministic cleanup.
 * 2. All framework method invocations return a Status.
 * The actual JNI return value is written to the address passed to call().
 * 
 * Example: be/test/util/jni_util_test.cpp
*/
template <RefType Ref>
class Object {
    // env->GetObjectRefType
public:
    Object() = default;

    template <RefType U>
    friend class Object;

    template <RefType U>
    friend class Class;

    template <RefType U>
    friend class String;

    template <RefType U>
    friend class Array;

    template <BufferType bufferfType, RefType U>
    friend class BufferGuard;

    template <CallTag tag>
    friend class FunctionCall;

    template <CallTag tag>
    friend class NonvirtualFunctionCall;

    virtual ~Object() {
        if (_obj != nullptr) [[likely]] {
            JNIEnv* env = nullptr;
            if (Status st = RefHelper<Ref>::get_env(&env); !st.ok()) [[unlikely]] {
                LOG(WARNING) << "Can't destroy Jni Ref : " << st.msg();
                return;
            }
            RefHelper<Ref>::destroy(env, _obj);
        }
    }

    template <RefType R>
    static Status create(JNIEnv* env, const Object<R>& other, Object<Ref>* result) {
        DCHECK(!other.uninitialized());
        DCHECK(result->uninitialized());

        result->_obj = RefHelper<Ref>::create(env, other._obj);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    bool uninitialized() const { return _obj == nullptr; }

    template <RefType T>
    bool equal(JNIEnv* env, const Object<T>& other) {
        DCHECK(!uninitialized());
        DCHECK(!other.uninitialized());
        return env->IsSameObject(this->_obj, other._obj); //assume not throw exception.
    }

    FunctionCall<ObjectMethod> call_object_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<ObjectMethod>::instance(env, _obj, method_id._id);
    }

    FunctionCall<IntMethod> call_int_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<IntMethod>::instance(env, _obj, method_id._id);
    }

    FunctionCall<LongMethod> call_long_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<LongMethod>::instance(env, _obj, method_id._id);
    }

    FunctionCall<VoidMethod> call_void_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<VoidMethod>::instance(env, _obj, method_id._id);
    }

    FunctionCall<BooleanMethod> call_boolean_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<BooleanMethod>::instance(env, _obj, method_id._id);
    }

    template <RefType R>
    NonvirtualFunctionCall<NonvirtualVoidMethod> call_nonvirtual_void_method(
            JNIEnv* env, const Class<R>& clazz, MethodId method_id) const;

    template <RefType R>
    NonvirtualFunctionCall<NonvirtualObjectMethod> call_nonvirtual_object_method(
            JNIEnv* env, const Class<R>& clazz, MethodId method_id) const;

    template <RefType R>
    NonvirtualFunctionCall<NonvirtualIntMethod> call_nonvirtual_int_method(
            JNIEnv* env, const Class<R>& clazz, MethodId method_id) const;

    template <RefType R>
    NonvirtualFunctionCall<NonvirtualBooleanMethod> call_nonvirtual_boolean_method(
            JNIEnv* env, const Class<R>& clazz, MethodId method_id) const;

protected:
    jobject _obj = nullptr;
    DISALLOW_COPY_AND_ASSIGN(Object);
};

using LocalObject = Object<Local>;
using GlobalObject = Object<Global>;

static inline Status local_to_global_ref(JNIEnv* env, const LocalObject& local_ref,
                                         GlobalObject* global_ref) {
    return Object<Global>::create(env, local_ref, global_ref);
}

// auto ReleaseStringUTFChars ReleaseByteArrayElements ...
template <BufferType bufferfType, RefType Ref>
class BufferGuard {
public:
    BufferGuard() = default;

    template <RefType R>
    static Status create(JNIEnv* env, const Object<R>& object,
                         BufferGuard<bufferfType, Ref>* result, jboolean* isCopy) {
        DCHECK(result->_buffer == nullptr && result->_object.uninitialized());

        RETURN_IF_ERROR(Object<Ref>::create(env, object, &result->_object));

        if constexpr (bufferfType == BufferType::Chars) {
            result->_buffer = env->GetStringUTFChars((jstring)result->_object._obj, isCopy);
        } else if constexpr (bufferfType == BufferType::ByteArray) {
            result->_buffer =
                    (char*)env->GetByteArrayElements((jbyteArray)result->_object._obj, isCopy);
        } else {
            static_assert(false);
        }

        RETURN_ERROR_IF_EXC(env);
        if (result->_buffer == nullptr) [[unlikely]] {
            return Status::JniError("GetStringUTFChars/GetByteArrayElements fail.");
        }

        return Status::OK();
    }

    ~BufferGuard() {
        if (_object.uninitialized() || _buffer == nullptr) [[unlikely]] {
            return;
        }
        JNIEnv* env = nullptr;

        if (auto st = RefHelper<Ref>::get_env(&env); !st.ok()) [[unlikely]] {
            LOG(WARNING) << "BufferGuard release fail: " << st;
            return;
        }

        if constexpr (bufferfType == BufferType::Chars) {
            env->ReleaseStringUTFChars((jstring)_object._obj, _buffer);
        } else if constexpr (bufferfType == BufferType::ByteArray) {
            env->ReleaseByteArrayElements((jbyteArray)_object._obj, (jbyte*)_buffer, JNI_ABORT);
        }
    }

    const char* get() const { return _buffer; }

private:
    Object<Ref> _object;
    const char* _buffer = nullptr;

    DISALLOW_COPY_AND_ASSIGN(BufferGuard);
};

template <RefType Ref>
using StringBufferGuard = BufferGuard<Chars, Ref>;
using LocalStringBufferGuard = BufferGuard<Chars, Local>;
using GlobalStringBufferGuard = BufferGuard<Chars, Global>;

template <RefType Ref>
using ByteArrayBufferGuard = BufferGuard<ByteArray, Ref>;
using LocalByteArrayBufferGuard = BufferGuard<ByteArray, Local>;
using GlobalByteArrayBufferGuard = BufferGuard<ByteArray, Global>;

template <RefType Ref>
class String : public Object<Ref> {
public:
    String() = default;

    static Status new_string(JNIEnv* env, const char* utf_chars, String<Ref>* result) {
        DCHECK(result->uninitialized());

        if constexpr (Ref == Local) {
            result->_obj = env->NewStringUTF(utf_chars);
            RETURN_ERROR_IF_EXC(env);
        } else if constexpr (Ref == Global) {
            String local_result;
            local_result->_obj = env->NewStringUTF(utf_chars);
            RETURN_ERROR_IF_EXC(env);
            RETURN_IF_ERROR(local_to_global_ref(env, local_result, result));
        } else {
            static_assert(false);
        }
        return Status::OK();
    }

    Status get_string_chars(JNIEnv* env, StringBufferGuard<Ref>* jni_chars) const {
        return StringBufferGuard<Ref>::create(env, *this, jni_chars, nullptr);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(String);
};

template <RefType Ref>
class Array : public Object<Ref> {
public:
    Array() = default;

    Status get_length(JNIEnv* env, jsize* result) const {
        DCHECK(!this->uninitialized());

        *result = env->GetArrayLength((jarray)this->_obj);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_object_array_element(JNIEnv* env, jsize index, Jni::LocalObject* result) {
        DCHECK(!this->uninitialized());
        DCHECK(result->uninitialized());
        result->_obj = env->GetObjectArrayElement((jobjectArray)this->_obj, index);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_byte_elements(JNIEnv* env, ByteArrayBufferGuard<Ref>* jni_bytes) const {
        DCHECK(!this->uninitialized());
        return ByteArrayBufferGuard<Ref>::create(env, *this, jni_bytes, nullptr);
    }

    Status get_byte_elements(JNIEnv* env, jsize start, jsize len, jbyte* buffer) {
        DCHECK(!this->uninitialized());
        env->GetByteArrayRegion((jbyteArray)this->_obj, start, len,
                                reinterpret_cast<jbyte*>(buffer));
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    static Status WriteBufferToByteArray(JNIEnv* env, const jbyte* buffer, jint size,
                                         Array<Local>* serialized_msg) {
        DCHECK(serialized_msg->uninitialized());
        /// create jbyteArray given buffer
        serialized_msg->_obj = env->NewByteArray(size);
        RETURN_ERROR_IF_EXC(env);
        if (serialized_msg->_obj == nullptr) [[unlikely]] {
            return Status::JniError("couldn't construct jbyteArray");
        }
        env->SetByteArrayRegion((jbyteArray)serialized_msg->_obj, 0, size, buffer);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    template <class T>
    static Status SerializeThriftMsg(JNIEnv* env, T* msg, Array<Local>* serialized_msg) {
        int buffer_size = 100 * 1024; // start out with 100KB
        ThriftSerializer serializer(false, buffer_size);

        uint8_t* buffer = nullptr;
        uint32_t size = 0;
        RETURN_IF_ERROR(serializer.serialize(msg, &size, &buffer));

        // Make sure that 'size' is within the limit of INT_MAX as the use of
        // 'size' below takes int.
        if (size > INT_MAX) [[unlikely]] {
            return Status::JniError(
                    "The length of the serialization buffer ({} bytes) exceeds the limit of {} "
                    "bytes",
                    size, INT_MAX);
        }
        RETURN_IF_ERROR(WriteBufferToByteArray(env, (jbyte*)buffer, size, serialized_msg));
        return Status::OK();
    }

private:
    DISALLOW_COPY_AND_ASSIGN(Array);
};

template <RefType Ref>
class Class : public Object<Ref> {
public:
    Class() = default;

    static Status find_class(JNIEnv* env, const char* class_str, Class<Ref>* result) {
        DCHECK(result->uninitialized());
        if constexpr (Ref == Local) {
            result->_obj = env->FindClass(class_str);
            RETURN_ERROR_IF_EXC(env);
            return Status::OK();
        } else if constexpr (Ref == Global) {
            Class<Local> local_class;
            local_class._obj = env->FindClass(class_str);
            RETURN_ERROR_IF_EXC(env);
            return local_to_global_ref(env, local_class, result);
        } else {
            static_assert(false);
        }
    }

    Status get_static_method(JNIEnv* env, const char* method_str, const char* method_signature,
                             MethodId* method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(method_id->uninitialized());
        method_id->_id = env->GetStaticMethodID((jclass)this->_obj, method_str, method_signature);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_method(JNIEnv* env, const char* method_str, const char* method_signature,
                      MethodId* method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(method_id->uninitialized());
        method_id->_id = env->GetMethodID((jclass)this->_obj, method_str, method_signature);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_static_fieldId(JNIEnv* env, const char* name, const char* signature,
                              FieldId* field_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(field_id->uninitialized());
        field_id->_id = env->GetStaticFieldID((jclass)this->_obj, name, signature);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_static_object_field(JNIEnv* env, const FieldId& field_id,
                                   Object<Local>* result) const {
        DCHECK(!this->uninitialized());
        DCHECK(!field_id.uninitialized());
        result->_obj = env->GetStaticObjectField((jclass)this->_obj, field_id._id);
        RETURN_ERROR_IF_EXC(env);
        return Status::OK();
    }

    Status get_static_object_field(JNIEnv* env, const FieldId& field_id,
                                   Object<Global>* global_result) const {
        DCHECK(!this->uninitialized());
        DCHECK(!field_id.uninitialized());
        Object<Local> local_result;
        local_result._obj = env->GetStaticObjectField((jclass)this->_obj, field_id._id);
        RETURN_ERROR_IF_EXC(env);
        return local_to_global_ref(env, local_result, global_result);
    }

    Status get_static_object_field(JNIEnv* env, const char* name, const char* signature,
                                   Object<Global>* global_result) const {
        Jni::FieldId tmpFieldID;
        RETURN_IF_ERROR(get_static_fieldId(env, name, signature, &tmpFieldID));
        RETURN_IF_ERROR(get_static_object_field(env, tmpFieldID, global_result));
        return Status::OK();
    }

    FunctionCall<NewObject> new_object(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<NewObject>::instance(env, (jclass)this->_obj, method_id._id);
    }

    FunctionCall<StaticObjectMethod> call_static_object_method(JNIEnv* env,
                                                               MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<StaticObjectMethod>::instance(env, (jclass)this->_obj, method_id._id);
    }

    FunctionCall<StaticVoidMethod> call_static_void_method(JNIEnv* env, MethodId method_id) const {
        DCHECK(!this->uninitialized());
        DCHECK(!method_id.uninitialized());
        return FunctionCall<StaticVoidMethod>::instance(env, (jclass)this->_obj, method_id._id);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(Class);
};

using LocalClass = Class<Local>;
using GlobalClass = Class<Global>;

using LocalArray = Array<Local>;
using GlobalArray = Array<Global>;

using LocalString = String<Local>;
using GlobalString = String<Global>;

template <CallTag tag>
template <RefType Ref>
FunctionCall<tag>& FunctionCall<tag>::with_arg(const Object<Ref>& obj) {
    jvalue v;
    std::memset(&v, 0, sizeof(v));
    v.l = obj._obj;
    _args.push_back(v);
    return *this;
}

template <CallTag tag>
template <RefType Ref>
NonvirtualFunctionCall<tag>& NonvirtualFunctionCall<tag>::with_arg(const Object<Ref>& obj) {
    jvalue v;
    std::memset(&v, 0, sizeof(v));
    v.l = obj._obj;
    this->_args.push_back(v);
    return *this;
}

template <CallTag tag>
Status FunctionCall<tag>::call(Object<Local>* result) {
    DCHECK(result->uninitialized());
    result->_obj = CallHelper<tag>::call_impl(_env, _base, _method, _args.data());
    RETURN_ERROR_IF_EXC(this->_env);
    return Status::OK();
}

template <CallTag tag>
Status FunctionCall<tag>::call(Object<Global>* result) {
    DCHECK(result->uninitialized());
    Object<Local> local_result;
    local_result._obj = CallHelper<tag>::call_impl(_env, _base, _method, _args.data());
    RETURN_ERROR_IF_EXC(this->_env);
    return local_to_global_ref(_env, local_result, result);
}

template <CallTag tag>
Status NonvirtualFunctionCall<tag>::call(Object<Local>* result) {
    DCHECK(result->uninitialized());
    result->_obj = CallHelper<tag>::call_impl(this->_env, this->_base, _cls, this->_method,
                                              this->_args.data());
    RETURN_ERROR_IF_EXC(this->_env);
    return Status::OK();
}

template <RefType Ref>
template <RefType R>
NonvirtualFunctionCall<NonvirtualObjectMethod> Object<Ref>::call_nonvirtual_object_method(
        JNIEnv* env, const Class<R>& clazz, MethodId method_id) const {
    DCHECK(!this->uninitialized());
    DCHECK(!method_id.uninitialized());

    return NonvirtualFunctionCall<NonvirtualObjectMethod>::instance(env, _obj, (jclass)clazz._obj,
                                                                    method_id._id);
}

template <RefType Ref>
template <RefType R>
NonvirtualFunctionCall<NonvirtualVoidMethod> Object<Ref>::call_nonvirtual_void_method(
        JNIEnv* env, const Class<R>& clazz, MethodId method_id) const {
    DCHECK(!this->uninitialized());
    DCHECK(!method_id.uninitialized());
    return NonvirtualFunctionCall<NonvirtualVoidMethod>::instance(env, _obj, (jclass)clazz._obj,
                                                                  method_id._id);
}

template <RefType Ref>
template <RefType R>
NonvirtualFunctionCall<NonvirtualIntMethod> Object<Ref>::call_nonvirtual_int_method(
        JNIEnv* env, const Class<R>& clazz, MethodId method_id) const {
    DCHECK(!this->uninitialized());
    DCHECK(!method_id.uninitialized());
    return NonvirtualFunctionCall<NonvirtualIntMethod>::instance(env, _obj, (jclass)clazz._obj,
                                                                 method_id._id);
}

template <RefType Ref>
template <RefType R>
NonvirtualFunctionCall<NonvirtualBooleanMethod> Object<Ref>::call_nonvirtual_boolean_method(
        JNIEnv* env, const Class<R>& clazz, MethodId method_id) const {
    DCHECK(!this->uninitialized());
    DCHECK(!method_id.uninitialized());
    return NonvirtualFunctionCall<NonvirtualBooleanMethod>::instance(env, _obj, (jclass)clazz._obj,
                                                                     method_id._id);
}

class Util {
public:
    static size_t get_max_jni_heap_memory_size();

    template <RefType Ref>
    static Status find_class(JNIEnv* env, const char* class_str, Class<Ref>* result) {
        return Class<Ref>::find_class(env, class_str, result);
    }

    template <RefType Ref>
    static Status WriteBufferToByteArray(JNIEnv* env, const jbyte* buffer, jint size,
                                         Array<Ref>* serialized_msg) {
        if constexpr (Ref == Local) {
            return Array<Local>::WriteBufferToByteArray(env, buffer, size, serialized_msg);
        } else if constexpr (Ref == Global) {
            Array<Local> local_obj;
            RETURN_IF_ERROR(Array<Local>::WriteBufferToByteArray(env, buffer, size, &local_obj));
            return local_to_global_ref(env, local_obj, serialized_msg);
        } else {
            static_assert(false);
        }
    }

    template <class T, RefType Ref>
    static Status SerializeThriftMsg(JNIEnv* env, T* msg, Array<Ref>* serialized_msg) {
        if constexpr (Ref == Local) {
            return Array<Local>::SerializeThriftMsg(env, msg, serialized_msg);
        } else if (Ref == Global) {
            Array<Local> local_obj;
            RETURN_IF_ERROR(Array<Local>::SerializeThriftMsg(env, msg, local_obj));
            return local_to_global_ref(env, local_obj, serialized_msg);
        } else {
            static_assert(false);
        }
    }

    template <RefType Ref>
    static Status get_jni_scanner_class(JNIEnv* env, const char* classname,
                                        Object<Ref>* jni_scanner_class) {
        // Get JNI scanner class by class name;
        LocalString class_name_str;
        RETURN_IF_ERROR(LocalString::new_string(env, classname, &class_name_str));
        return jni_scanner_loader_obj_.call_object_method(env, jni_scanner_loader_method_)
                .with_arg(class_name_str)
                .call(jni_scanner_class);
    }

    template <RefType Ref>
    static Status convert_to_java_map(JNIEnv* env, const std::map<std::string, std::string>& map,
                                      Object<Ref>* hashmap_object) {
        RETURN_IF_ERROR(hashmap_class.new_object(env, hashmap_constructor)
                                .with_arg((jint)map.size())
                                .call(hashmap_object));

        for (const auto& it : map) {
            LocalString key;
            RETURN_IF_ERROR(String<Local>::new_string(env, it.first.c_str(), &key));

            LocalString value;
            RETURN_IF_ERROR(String<Local>::new_string(env, it.second.c_str(), &value));

            LocalObject result;
            RETURN_IF_ERROR(hashmap_object->call_object_method(env, hashmap_put)
                                    .with_arg(key)
                                    .with_arg(value)
                                    .call());
        }
        return Status::OK();
    }

    template <RefType Ref>
    static Status convert_to_cpp_map(JNIEnv* env, const Object<Ref>& map,
                                     std::map<std::string, std::string>* resultMap) {
        LocalObject entrySet;
        RETURN_IF_ERROR(map.call_object_method(env, mapEntrySetMethod).call(&entrySet));

        // Call the iterator method on the set to iterate over the key-value pairs
        LocalObject iteratorSet;
        RETURN_IF_ERROR(entrySet.call_object_method(env, iteratorSetMethod).call(&iteratorSet));

        while (true) {
            jboolean hasNext = false;
            RETURN_IF_ERROR(
                    iteratorSet.call_boolean_method(env, iteratorHasNextMethod).call(&hasNext));
            if (!hasNext) {
                break;
            }

            LocalObject entry;
            RETURN_IF_ERROR(iteratorSet.call_object_method(env, iteratorNextMethod).call(&entry));

            LocalString javaKey;
            RETURN_IF_ERROR(entry.call_object_method(env, getEntryKeyMethod).call(&javaKey));

            LocalString javaValue;
            RETURN_IF_ERROR(entry.call_object_method(env, getEntryValueMethod).call(&javaValue));

            LocalStringBufferGuard key;
            RETURN_IF_ERROR(javaKey.get_string_chars(env, &key));

            LocalStringBufferGuard value;
            RETURN_IF_ERROR(javaValue.get_string_chars(env, &value));

            // Store the key-value pair in the map
            (*resultMap)[key.get()] = value.get();
        }
        return Status::OK();
    }

    static Status clean_udf_class_load_cache(const std::string& function_signature);

    static Status Init();

private:
    static void _parse_max_heap_memory_size_from_jvm();

    static Status _init_collect_class() WARN_UNUSED_RESULT;
    static Status _init_register_natives() WARN_UNUSED_RESULT;
    static Status _init_jni_scanner_loader() WARN_UNUSED_RESULT;

    static bool jvm_inited_;

    // for jvm heap
    static jlong max_jvm_heap_memory_size_;

    // for JNI scanner loader
    static GlobalObject jni_scanner_loader_obj_;
    static MethodId jni_scanner_loader_method_;

    // for clean udf cache
    static MethodId _clean_udf_cache_method_id;

    //for hashmap
    static GlobalClass hashmap_class;
    static MethodId hashmap_constructor;
    static MethodId hashmap_put;

    //for map
    static GlobalClass mapClass;
    static MethodId mapEntrySetMethod;

    // for map entry
    static GlobalClass mapEntryClass;
    static MethodId getEntryKeyMethod;
    static MethodId getEntryValueMethod;

    //for set
    static GlobalClass setClass;
    static MethodId iteratorSetMethod;

    // for iterator
    static GlobalClass iteratorClass;
    static MethodId iteratorHasNextMethod;
    static MethodId iteratorNextMethod;
};
}; // namespace Jni

} // namespace doris
