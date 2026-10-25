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

#include "io/hdfs_tvf_policy.h"

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <thread>

#include "io/hdfs_util.h"
#include "runtime/thread_context.h"
#include "util/thread.h"
#ifdef USE_HADOOP_HDFS
#include "util/jni-util.h"
#endif

namespace doris::io {

// Hadoop JNI must execute on an OS thread, including checks on cached connections.
Status HdfsTvfPolicy::on_pthread(const std::function<Status()>& operation) {
    if (bthread_self() == 0) {
        return operation();
    }
    Status status;
    bthread::CountdownEvent complete(1);
    std::thread worker([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        Thread::set_self_name("hdfs_tvf_io");
        status = operation();
        complete.signal();
    });
    complete.wait();
    worker.join();
    return status;
}

bool HdfsTvfPolicy::requested(const THdfsParams& params) {
    return std::ranges::any_of(params.hdfs_conf, [](const auto& conf) {
        return conf.key == POLICY_KEY && (conf.value == "native" || conf.value == "native-write");
    });
}

#ifdef USE_HADOOP_HDFS
struct HdfsTvfPolicy::Impl {
    Jni::GlobalObject context;
    Jni::MethodId check_path;
    bool direct_remote = false;
    std::string profile;
    std::string fs_name;
};

Status HdfsTvfPolicy::clear_runtime() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    Jni::LocalClass guard;
    RETURN_IF_ERROR(
            Jni::LocalClass::find_class(env, "org/apache/hadoop/fs/FileSystem$TvfGuard", &guard));
    Jni::MethodId clear;
    RETURN_IF_ERROR(guard.get_static_method(env, "clearNativeContext", "()V", &clear));
    return guard.call_static_void_method(env, clear).call();
}

Status HdfsTvfPolicy::prepare_runtime() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    jclass clazz = env->FindClass("org/apache/hadoop/fs/FileSystem");
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
    if (clazz == nullptr) {
        return Status::NotSupported("TVF requires the patched Hadoop FileSystem runtime");
    }
    jmethodID method = env->GetStaticMethodID(clazz, "getDorisTvfGuardVersion", "()I");
    if (env->ExceptionCheck()) {
        env->ExceptionClear();
    }
    if (method == nullptr) {
        env->DeleteLocalRef(clazz);
        return Status::NotSupported("TVF requires the patched Hadoop FileSystem runtime");
    }
    jint version = env->CallStaticIntMethod(clazz, method);
    bool failed = env->ExceptionCheck();
    if (failed) {
        env->ExceptionClear();
    }
    env->DeleteLocalRef(clazz);
    if (failed || version != 1) {
        return Status::NotSupported("TVF Hadoop runtime policy version mismatch");
    }
    return clear_runtime();
}

Status HdfsTvfPolicy::capture(const std::string& fs_name, std::shared_ptr<HdfsTvfPolicy>* result) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    Jni::LocalClass guard;
    RETURN_IF_ERROR(
            Jni::LocalClass::find_class(env, "org/apache/hadoop/fs/FileSystem$TvfGuard", &guard));
    Jni::MethodId take;
    RETURN_IF_ERROR(guard.get_static_method(env, "takeNativeContext",
                                            "()Lorg/apache/hadoop/fs/FileSystem$TvfGuard$Context;",
                                            &take));
    Jni::LocalObject context;
    RETURN_IF_ERROR(guard.call_static_object_method(env, take).call(&context));
    if (context.uninitialized()) {
        return Status::NotSupported("Hadoop runtime did not capture the TVF configuration");
    }
    auto impl = std::make_unique<Impl>();
    RETURN_IF_ERROR(Jni::GlobalObject::create(env, context, &impl->context));
    Jni::LocalClass context_class;
    RETURN_IF_ERROR(Jni::LocalClass::find_class(
            env, "org/apache/hadoop/fs/FileSystem$TvfGuard$Context", &context_class));
    RETURN_IF_ERROR(context_class.get_method(
            env, "checkNative", "(Ljava/lang/String;Ljava/lang/String;)V", &impl->check_path));
    Jni::MethodId direct;
    RETURN_IF_ERROR(context_class.get_method(env, "isDirectRemote", "()Z", &direct));
    jboolean direct_remote;
    RETURN_IF_ERROR(context.call_boolean_method(env, direct).call(&direct_remote));
    impl->direct_remote = direct_remote;
    impl->fs_name = fs_name;
    Jni::MethodId profile;
    RETURN_IF_ERROR(context_class.get_method(env, "profile", "()Ljava/lang/String;", &profile));
    Jni::LocalString text;
    RETURN_IF_ERROR(context.call_object_method(env, profile).call(&text));
    const char* chars = env->GetStringUTFChars(static_cast<jstring>(text.get()), nullptr);
    RETURN_ERROR_IF_EXC(env);
    impl->profile = chars;
    env->ReleaseStringUTFChars(static_cast<jstring>(text.get()), chars);
    *result = std::shared_ptr<HdfsTvfPolicy>(new HdfsTvfPolicy(std::move(impl)));
    return Status::OK();
}

bool HdfsTvfPolicy::requires_jni() const {
    return !_impl->direct_remote;
}

Status HdfsTvfPolicy::check(const std::string& fs_name, const std::string& native_path) {
    if (fs_name != _impl->fs_name) {
        return Status::InvalidArgument("TVF Hadoop connection identity mismatch");
    }
    if (_impl->direct_remote) {
        return Status::OK();
    }
    // libhdfs consumes C strings, so bytes after the first NUL are not part of its path.
    const auto path = native_path.substr(0, native_path.find('\0'));
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    Jni::LocalString root;
    Jni::LocalString file;
    RETURN_IF_ERROR(Jni::LocalString::new_string(env, fs_name.c_str(), &root));
    RETURN_IF_ERROR(Jni::LocalString::new_string(env, path.c_str(), &file));
    RETURN_IF_ERROR(_impl->context.call_void_method(env, _impl->check_path)
                            .with_arg(root)
                            .with_arg(file)
                            .call());
    return Status::OK();
}
#else
struct HdfsTvfPolicy::Impl {
    std::string profile;
};
Status HdfsTvfPolicy::prepare_runtime() {
    return Status::NotSupported("Hadoop JVM is not enabled");
}
Status HdfsTvfPolicy::clear_runtime() {
    return Status::OK();
}
Status HdfsTvfPolicy::capture(const std::string&, std::shared_ptr<HdfsTvfPolicy>*) {
    return Status::NotSupported("Hadoop JVM is not enabled");
}
bool HdfsTvfPolicy::requires_jni() const {
    return false;
}
Status HdfsTvfPolicy::check(const std::string&, const std::string&) {
    return Status::OK();
}
#endif

std::string HdfsTvfPolicy::storage_cache_identity(const HdfsHandler& handler,
                                                  const std::string& fs_name) {
    if (!handler.tvf_params) {
        return fs_name;
    }
    return "tvf:" + (handler.tvf_context ? handler.tvf_context->_impl->profile : "native-hdfs3") +
           ":" + fs_name;
}

HdfsTvfPolicy::HdfsTvfPolicy(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
HdfsTvfPolicy::~HdfsTvfPolicy() {
    // A collision-only connection can lose its last reader on a bthread. Release its Java
    // global reference on an OS thread, just like creation and route checks.
    if (_impl) {
        static_cast<void>(on_pthread([&] {
            _impl.reset();
            return Status::OK();
        }));
    }
}

} // namespace doris::io
