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

#include "util/jvm_launcher.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <mutex>
#include <sstream>

#include "common/cast_set.h"
#include "common/config.h"
#include "util/defer_op.h"
#include "util/jni-util.h"

namespace doris::Jni {

JavaVM* JvmLauncher::_vm = nullptr;
pthread_key_t JvmLauncher::_detach_key;

namespace {

std::string env_value(const char* name) {
    const char* value = getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

// Splits an option string on whitespace, the way libhdfs splits LIBHDFS_OPTS.
std::vector<std::string> split_options(const std::string& options) {
    std::istringstream stream(options);
    return {std::istream_iterator<std::string>(stream), std::istream_iterator<std::string>()};
}

std::string join_options(const std::vector<std::string>& options) {
    std::string joined;
    for (const auto& option : options) {
        if (!joined.empty()) {
            joined += " ";
        }
        joined += option;
    }
    return joined;
}

// Every jar below `base_path`, in no particular order.
void collect_jars(const std::string& base_path, std::string* class_path) {
    std::error_code ec;
    if (!std::filesystem::exists(base_path, ec)) {
        return;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(base_path, ec)) {
        if (entry.path().extension() == ".jar") {
            if (!class_path->empty()) {
                *class_path += ":";
            }
            *class_path += entry.path().string();
        }
    }
}

// Last resort for processes that were not started by start_be.sh, e.g. tests.
std::string scan_class_path() {
    const std::string doris_home = env_value("DORIS_HOME");
    if (doris_home.empty()) {
        return "";
    }
    std::string class_path = doris_home + "/conf/";
    collect_jars(doris_home + "/lib", &class_path);
    collect_jars(doris_home + "/custom_lib", &class_path);

    const std::string hadoop_conf_dir = env_value("HADOOP_CONF_DIR");
    if (!hadoop_conf_dir.empty()) {
        class_path += ":" + hadoop_conf_dir;
    }
    return class_path;
}

} // namespace

std::string JvmLauncher::_class_path_option() {
    // start_be.sh publishes one and the same class path under two names: CLASSPATH, which
    // libhdfs used to build the JVM from, and DORIS_CLASSPATH, which is the same list
    // already spelled as a JVM option. CLASSPATH wins because it is the one the JVM of
    // every running BE was created from, and the only one that carries conf/.
    std::string class_path = env_value("CLASSPATH");
    if (class_path.empty()) {
        static constexpr std::string_view kOptionPrefix = "-Djava.class.path=";
        std::string doris_class_path = env_value("DORIS_CLASSPATH");
        if (doris_class_path.starts_with(kOptionPrefix)) {
            doris_class_path = doris_class_path.substr(kOptionPrefix.size());
        }
        class_path = doris_class_path;
    }
    if (class_path.empty()) {
        class_path = scan_class_path();
    }
    return "-Djava.class.path=" + class_path;
}

std::vector<std::string> JvmLauncher::_build_options() {
    // start_be.sh exports the very same options as JAVA_OPTS and as LIBHDFS_OPTS; reading
    // both means a deployment that only sets one of them still gets its own settings.
    std::string java_opts = env_value("JAVA_OPTS");
    if (java_opts.empty()) {
        java_opts = env_value("LIBHDFS_OPTS");
    }

    std::vector<std::string> options;
    if (!java_opts.empty()) {
        options = split_options(java_opts);
    } else {
        const std::string doris_home = env_value("DORIS_HOME");
        options = {
                "-Xmx1g",
                fmt::format("-DlogPath={}/log/jni.log", doris_home),
                "-Dsun.java.command=DorisBE",
                "-XX:-CriticalJNINatives",
#ifdef __APPLE__
                // On macOS, we should disable MaxFDLimit, otherwise the RLIMIT_NOFILE
                // will be assigned the minimum of OPEN_MAX (10240) and rlim_cur (See
                // src/hotspot/os/bsd/os_bsd.cpp) and it can not pass the check performed
                // by storage engine. The newer JDK has fixed this issue.
                "-XX:-MaxFDLimit",
#endif
        };
    }

    options.push_back(_class_path_option());
    options.push_back("-Djava.security.krb5.conf=" + config::kerberos_krb5_conf_path);
    options.push_back(fmt::format("-Djdk.lang.processReaperUseDefaultStackSize={}",
                                  config::jdk_process_reaper_use_default_stack_size));
    // Where PluginRegistry looks for plugins. It travels as a system property rather than
    // through the startup script because the value is a BE config and this is the one place
    // that turns BE config into JVM options; passing it from the script as well would be two
    // sources for one path.
    options.push_back("-Ddoris.jni.plugin.dir=" + config::java_plugin_dir);
    return options;
}

// libhdfs runs this right after it creates the VM, and hadoop's FileSystem remembers the
// service-loaded implementations it finds in a static field, populated exactly once with
// whatever context class loader the first caller happens to carry. Doing it here keeps
// that first caller the BE itself rather than some scanner thread whose context class
// loader belongs to a plugin.
void JvmLauncher::_load_file_systems(JNIEnv* env) {
    jclass file_system_cl = env->FindClass("org/apache/hadoop/fs/FileSystem");
    if (file_system_cl == nullptr) {
        env->ExceptionClear();
        LOG(INFO) << "hadoop FileSystem is not on the class path, skip loading its file systems";
        return;
    }
    Defer defer {[&]() { env->DeleteLocalRef(file_system_cl); }};

    jmethodID load_file_systems = env->GetStaticMethodID(file_system_cl, "loadFileSystems", "()V");
    if (load_file_systems == nullptr) {
        env->ExceptionClear();
        LOG(WARNING) << "hadoop FileSystem has no loadFileSystems method, skip loading its file "
                        "systems";
        return;
    }

    env->CallStaticVoidMethod(file_system_cl, load_file_systems);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(WARNING) << "failed to load the hadoop file systems, hdfs access may not work";
    }
}

Status JvmLauncher::_create_jvm() {
    JavaVM* created_vms[1] = {nullptr};
    jsize num_vms = 0;
    jint rv = JNI_GetCreatedJavaVMs(created_vms, 1, &num_vms);
    if (rv != JNI_OK) {
        return Status::JniError("Failed to look up the JVM of this process, code={}", rv);
    }
    if (num_vms > 0) {
        // Somebody got there first - in practice libhdfs, whose entry points create a JVM
        // of their own when they do not find one. Its options are the same ones we would
        // have passed, so the VM is usable as it is.
        _vm = created_vms[0];
        LOG(INFO) << "Reuse the JVM that already exists in this process.";
        return Status::OK();
    }

    const std::vector<std::string> options = _build_options();
    std::vector<JavaVMOption> jvm_options(options.size());
    for (size_t i = 0; i < options.size(); ++i) {
        jvm_options[i] = {const_cast<char*>(options[i].c_str()), nullptr};
    }

    JavaVMInitArgs vm_args;
    vm_args.version = JNI_VERSION_1_8;
    vm_args.options = jvm_options.data();
    vm_args.nOptions = cast_set<jint>(options.size());
    // Unknown options are dropped rather than rejected, which is how libhdfs created the
    // JVM of every deployed BE so far. Rejecting them would turn one stale JVM option in
    // be.conf into a BE without any Java feature at all. The options that are dropped
    // this way show up in the log line below.
    vm_args.ignoreUnrecognized = JNI_TRUE;

    JNIEnv* env = nullptr;
    jint res = JNI_CreateJavaVM(&_vm, reinterpret_cast<void**>(&env), &vm_args);
    if (res != JNI_OK) {
        _vm = nullptr;
        // libhdfs may have created one in the window since the look-up above, in which
        // case there is nothing wrong - we just lost the race for who configures it.
        if (JNI_GetCreatedJavaVMs(created_vms, 1, &num_vms) == JNI_OK && num_vms > 0) {
            _vm = created_vms[0];
            LOG(INFO) << "Reuse the JVM another thread created while we were creating ours.";
            return Status::OK();
        }
        return Status::JniError("Failed to create the JVM, code={}, options=[{}]", res,
                                join_options(options));
    }
    LOG(INFO) << "Created the JVM with options: " << join_options(options);

    _load_file_systems(env);
    return Status::OK();
}

Status JvmLauncher::_bootstrap() {
    RETURN_IF_ERROR(_create_jvm());

#ifdef USE_HADOOP_HDFS
    // Hand libhdfs its thread-local key before we create ours. Both of us detach a thread
    // when it exits, and a thread that reads hdfs files and runs a JNI scanner ends up
    // registered with both. libhdfs' destructor dereferences the JNIEnv it cached for the
    // thread (jni_helper.c: `(*env)->GetJavaVM(env, &vm)`, and more JNI calls if the
    // detach fails), which DetachCurrentThread has freed by then - so it has to run
    // first. Destructors run in ascending key order, so creating its key first is what
    // orders them. Dropping this call is safe only once libhdfs stops touching the env it
    // cached.
    if (getJNIEnv() == nullptr) {
        LOG(WARNING) << "libhdfs failed to attach the bootstrap thread to the JVM; its thread "
                        "exit hook may now run after ours";
    }
#endif

    if (int rc = pthread_key_create(&_detach_key, &JvmLauncher::_detach_current_thread); rc != 0) {
        return Status::JniError("Failed to register the JVM thread exit hook, errno={}", rc);
    }
    return Status::OK();
}

Status JvmLauncher::ensure_jvm() {
    if (!config::enable_java_support) {
        return Status::InternalError(
                "Java support is disabled, you can change be config enable_java_support to true "
                "and restart be.");
    }

    static std::once_flag jvm_once;
    // The JVM is created at most once, so the outcome of that attempt is the answer for
    // every caller that follows.
    static Status jvm_status;
    std::call_once(jvm_once, []() { jvm_status = _bootstrap(); });
    return jvm_status;
}

Status JvmLauncher::attach_current_thread(JNIEnv** env) {
    RETURN_IF_ERROR(ensure_jvm());

    JNIEnv* thread_env = nullptr;
    jint rc = _vm->GetEnv(reinterpret_cast<void**>(&thread_env), JNI_VERSION_1_8);
    if (rc == JNI_EDETACHED) {
        rc = _vm->AttachCurrentThread(reinterpret_cast<void**>(&thread_env), nullptr);
    }
    if (rc != JNI_OK || thread_env == nullptr) {
        return Status::JniError("Failed to attach the current thread to the JVM, code={}", rc);
    }

    // Arm the thread exit hook, also for a thread that arrived already attached - creating
    // the JVM attaches whoever did it, and libhdfs attaches whoever calls it first. Any
    // non-null value makes the hook run; the env is the honest one to store even though
    // the hook must not dereference it.
    if (pthread_getspecific(_detach_key) == nullptr) {
        if (int err = pthread_setspecific(_detach_key, thread_env); err != 0) {
            LOG(WARNING) << "Failed to arm the JVM thread exit hook, this thread stays "
                            "attached to the JVM after it exits, errno="
                         << err;
        }
    }
    *env = thread_env;
    return Status::OK();
}

void JvmLauncher::_detach_current_thread(void* /*attached_env*/) {
    // Deliberately not touching the JNIEnv handed out for this thread: detaching frees it,
    // and this hook is exactly the place where a freed env turns into a crash inside the
    // JVM - see the ordering note in _bootstrap().
    Env::reset_tls_env();
    if (_vm != nullptr) {
        // JNI_EDETACHED here just means libhdfs' hook already detached this thread.
        static_cast<void>(_vm->DetachCurrentThread());
    }
}

} // namespace doris::Jni
