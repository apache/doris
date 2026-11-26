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

#include <bthread/bthread.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <mutex>
#include <sstream>
#include <thread>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/jni-util.h"
#include "util/thread.h"

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
    // Named separately because it is the one thing libhdfs needs that does NOT live under lib/:
    // the third-party filesystem jars moved to plugins/jni_fs when the Java plugins started
    // reading them too. Leaving it out would silently drop oss-hdfs:// and jfs:// for exactly the
    // processes this branch serves. Read from the config rather than spelled out, because this
    // runs from _build_options() - well after config is loaded - and an operator who points
    // jni_plugin_fs_dir somewhere else must not end up with the Java plugins reading one
    // directory and the native reader another.
    collect_jars(config::jni_plugin_fs_dir, &class_path);

    const std::string hadoop_conf_dir = env_value("HADOOP_CONF_DIR");
    if (!hadoop_conf_dir.empty()) {
        class_path += ":" + hadoop_conf_dir;
    }
    return class_path;
}

// The signals the BE keeps for itself and installs its own handlers for in init_signals():
// SIGINT and SIGTERM ask it to shut down - the handler raises the flag main() waits on, so
// the shutdown stays orderly: running queries are drained and the process leaves through
// _exit(), which is what keeps the global destructors from running underneath threads that
// are still working - and SIGQUIT is held so that it does nothing, because its default
// action is to terminate the process and dump core (see init_signals()).
//
// A starting JVM installs handlers of its own for all three, and its handler turns a
// shutdown signal into a Java Shutdown.exit() - an ::exit() call made from a JVM thread.
// That runs the global destructors while the BE is still serving, and BE singletons have
// been seen freed out from under a live compaction thread this way. The BE has always kept
// SIGINT and SIGTERM for itself; it used to do so by creating the JVM before
// init_signals(), which no longer holds now that the JVM is created on demand.
//
// Our own JVM is created with -Xrs, so it never installs them in the first place. This guard
// is for the JVM we do not configure: libhdfs creates one from LIBHDFS_OPTS when it finds no
// VM, and the options in there are the operator's. Saving the handlers and putting them back
// leaves a window in between, so the guard is wrapped as tightly as possible around the two
// calls that can create a VM - it used to span the whole bootstrap, seconds of hadoop
// ServiceLoader scanning included.
//
// Only these two. The JVM needs SIGSEGV, SIGBUS, SIGILL and SIGFPE for its implicit null
// checks and safepoint polls; taking those back would break it. SIGHUP is left to the JVM as
// well, which is what the eager-JVM order did too.
constexpr int BE_OWNED_SIGNALS[] = {SIGINT, SIGTERM, SIGQUIT};
constexpr size_t BE_OWNED_SIGNAL_COUNT = std::size(BE_OWNED_SIGNALS);

class BeOwnedSignalGuard {
public:
    BeOwnedSignalGuard() {
        for (size_t i = 0; i < BE_OWNED_SIGNAL_COUNT; ++i) {
            _saved_valid[i] = sigaction(BE_OWNED_SIGNALS[i], nullptr, &_saved[i]) == 0;
            if (!_saved_valid[i]) {
                LOG(WARNING) << "failed to read the handler of signal " << BE_OWNED_SIGNALS[i]
                             << " before starting the JVM, errno=" << errno
                             << "; the JVM is about to take this signal over";
            }
        }
    }

    ~BeOwnedSignalGuard() {
        for (size_t i = 0; i < BE_OWNED_SIGNAL_COUNT; ++i) {
            if (!_saved_valid[i]) {
                continue;
            }
            if (sigaction(BE_OWNED_SIGNALS[i], &_saved[i], nullptr) != 0) {
                LOG(WARNING) << "failed to restore the handler of signal " << BE_OWNED_SIGNALS[i]
                             << " after starting the JVM, errno=" << errno
                             << "; the JVM now owns this signal and shutting the BE down with "
                                "it will not be graceful";
            }
        }
    }

    BeOwnedSignalGuard(const BeOwnedSignalGuard&) = delete;
    BeOwnedSignalGuard& operator=(const BeOwnedSignalGuard&) = delete;

private:
    struct sigaction _saved[BE_OWNED_SIGNAL_COUNT] = {};
    bool _saved_valid[BE_OWNED_SIGNAL_COUNT] = {};
};

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
    // Where PluginRegistry looks for plugins, where it lets them read hadoop configuration files
    // from, and where it finds the third-party filesystem jars every plugin may need. All three
    // travel as system properties rather than through the startup script because the values are BE
    // configs and this is the one place that turns BE config into JVM options; passing them from
    // the script as well would be two sources for one path.
    options.push_back("-Ddoris.jni.plugin.dir=" + config::jni_plugin_dir);
    options.push_back("-Ddoris.jni.hadoop.conf.dir=" + config::jni_plugin_hadoop_conf_dir);
    options.push_back("-Ddoris.jni.fs.dir=" + config::jni_plugin_fs_dir);
    // The JVM must not install handlers for SIGINT/SIGTERM/SIGHUP: its handler turns a shutdown
    // signal into Java's Shutdown.exit(), an ::exit() from a JVM thread that runs the global
    // destructors while the BE is still serving. Restoring the handlers around bootstrap (see
    // BeOwnedSignalGuard) only narrows that window - this closes it, for the whole life of the
    // process. Appended last so that it also wins over a JAVA_OPTS that says otherwise.
    //
    // What this costs: no Java shutdown hooks on those signals, which is the point, and no JVM
    // thread dump from SIGQUIT - the JVM does not install its handler for that one either. That
    // is why the BE installs one of its own in init_signals(): SIGQUIT's default action is to
    // terminate the process and dump core, so leaving it at SIG_DFL would turn the operator's
    // habitual `kill -3 <be_pid>` from "print the thread dump" into "kill the BE". jcmd and
    // jstack, which attach instead of signalling, are the way to get a Java thread dump now.
    options.push_back("-Xrs");
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
        // Somebody got there first - in practice libhdfs, whose entry points create a JVM of
        // their own when they do not find one, out of CLASSPATH and LIBHDFS_OPTS. Those are the
        // same two the options below are built from, so the VM is usable, but it is NOT
        // identically configured: the six settings this launcher adds - -Xrs, the krb5 conf
        // path, the three doris.jni.* directories and the process-reaper stack size - are missing
        // from it. The branch is unreachable today, because every libhdfs entry point in the BE
        // runs behind ensure_jvm(); it is here for the case where that stops being true.
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
    // Options the JVM does not recognize at all are dropped rather than rejected, which is
    // how libhdfs created the JVM of every deployed BE so far - it passed JNI_TRUE too, so
    // this is not a change for any running deployment. It only covers unrecognized -X and
    // _ options: one HotSpot recognizes but cannot parse the value of, a mistyped -Xmx8gb
    // among them, still fails JNI_CreateJavaVM with JNI_EINVAL. That failure is cached for
    // the life of the process (see ensure_jvm), which disables every Java feature and every
    // HDFS access with it, so it is reported at ERROR below rather than as a warning.
    vm_args.ignoreUnrecognized = JNI_TRUE;

    JNIEnv* env = nullptr;
    jint res = JNI_OK;
    {
        BeOwnedSignalGuard be_owned_signals;
        res = JNI_CreateJavaVM(&_vm, reinterpret_cast<void**>(&env), &vm_args);
    }
    if (res != JNI_OK) {
        _vm = nullptr;
        // libhdfs may have created one in the window since the look-up above, in which
        // case there is nothing wrong - we just lost the race for who configures it.
        if (JNI_GetCreatedJavaVMs(created_vms, 1, &num_vms) == JNI_OK && num_vms > 0) {
            _vm = created_vms[0];
            LOG(INFO) << "Reuse the JVM another thread created while we were creating ours.";
            return Status::OK();
        }
        Status failure = Status::JniError(
                "Failed to create the JVM, code={}, options=[{}]. Every Java feature of this BE "
                "and all HDFS access are disabled until it is restarted; a JVM option this "
                "process cannot parse - a mistyped -Xmx among them - is the usual cause.",
                res, join_options(options));
        LOG(ERROR) << failure;
        return failure;
    }
    LOG(INFO) << "Created the JVM with options: " << join_options(options);

    _load_file_systems(env);
    return Status::OK();
}

Status JvmLauncher::_bootstrap() {
    RETURN_IF_ERROR(_create_jvm());

    // Hand libhdfs its thread-local key before we create ours. Both of us detach a thread
    // when it exits, and a thread that reads hdfs files and runs a JNI scanner ends up
    // registered with both. libhdfs' destructor dereferences the JNIEnv it cached for the
    // thread (jni_helper.c: `(*env)->GetJavaVM(env, &vm)`, and more JNI calls if the
    // detach fails), which DetachCurrentThread has freed by then - so it has to run
    // first. Destructors run in ascending key order, so creating its key first is what
    // orders them. Dropping this call is safe only once libhdfs stops touching the env it
    // cached.
    {
        // Guarded as well: getJNIEnv() is libhdfs' own entry point for creating a VM when it
        // finds none. Ours exists by now, so this is belt and braces.
        BeOwnedSignalGuard be_owned_signals;
        if (getJNIEnv() == nullptr) {
            LOG(WARNING) << "libhdfs failed to attach the bootstrap thread to the JVM; its thread "
                            "exit hook may now run after ours";
        }
    }

    if (int rc = pthread_key_create(&_detach_key, &JvmLauncher::_detach_current_thread); rc != 0) {
        return Status::JniError("Failed to register the JVM thread exit hook, errno={}", rc);
    }

    // The JNI base belongs to the JVM this just created, so it is resolved here rather than on
    // whichever thread first asks for a JNIEnv: the native methods Java code links against and
    // the cached classes both come out of doris-jni-spi.jar, and resolving them once, here, is
    // what keeps every later Env::Get() on its fast path.
    //
    // Attached and cached by hand rather than through attach_current_thread(): that one calls
    // ensure_jvm(), and we are inside its call_once. Priming the thread-local env is also what
    // keeps Env::Get() below on its fast path, for the same reason.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_attach_current_thread(&env));
    Env::set_tls_env(env);
    Status base = Util::ensure_jni_base();

    // The jvm_* metrics describe the JVM and nothing else - JvmStats::init() reaches only for
    // java.lang.management classes - so they are published whenever a JVM exists, whoever brought
    // it up and whether or not the plugin SPI resolved. Publishing them anywhere but here would
    // mean a BE that has a JVM but reached it through libhdfs (an HDFS catalog served by the
    // native reader, no JNI table format and no Java UDF anywhere) exports no jvm_* series at all
    // and has its whole Java heap counted as untracked memory.
    //
    // Reached from inside this call_once, so it must not touch anything that leads back to
    // ensure_jvm(). What keeps it off that path is ScopedVmEnv, which JvmStats::init() takes and
    // holds across its whole body; the primed env above is not what makes this safe and neither
    // is the position before reset_tls_env(), because the metrics daemon reruns the same code
    // later on a thread that was never primed at all.
    DorisMetrics::instance()->init_jvm_metrics();

    if (!base.ok()) {
        // The env stays valid, but nothing that reads it may run without the base - so leave
        // this thread looking unattached, exactly as GetJNIEnvSlowPath() does on failure.
        Env::reset_tls_env();
        // Deliberately NOT this function's answer. A failure here means doris-jni-spi.jar is
        // missing or unreadable, which disables every Java feature - but the JVM is up, and
        // libhdfs needs nothing else from it. Returning the failure used to make ensure_jvm()
        // fail as well, and since hdfs_file_system.cpp and hdfs_mgr.cpp both go through
        // ensure_jvm(), a half-finished upgrade that had not yet laid down lib/jni/spi took every
        // HDFS read down with it, once per query. Java entry points get this same cached status
        // from Env::GetJNIEnvSlowPath(), which is the only door they come through.
        LOG(ERROR) << "The JVM is up but the Java plugin SPI could not be resolved, so no Java "
                      "code can run in this process (HDFS through libhdfs is unaffected): "
                   << base;
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
    std::call_once(jvm_once, []() { jvm_status = _bootstrap_on_pthread(); });
    return jvm_status;
}

// https://brpc.apache.org/docs/server/basics/
// JNI code checks the stack layout and cannot run on a bthread, and the bootstrap below is all
// JNI: JNI_CreateJavaVM, FindClass, libhdfs' getJNIEnv. The switch belongs here rather than at
// the call sites because a caller is whichever query first touches Java - HdfsMgr makes the same
// switch for the connection it goes on to open, but by then the JVM has already been created,
// and every future caller would have to know to repeat it.
//
// Joining does park the calling bthread's worker for as long as the JVM takes to come up. It
// happens once per process, and the alternative - a butex wait - would put brpc internals into
// the launcher. (Before the JVM became lazy there was nothing to park for: it was created in
// main(), long before any bthread existed.)
Status JvmLauncher::_bootstrap_on_pthread() {
    if (bthread_self() == 0) { // already a pthread
        return _bootstrap_guarded();
    }

    Status status;
    std::thread bootstrap([&status]() {
        SCOPED_INIT_THREAD_CONTEXT();
        Thread::set_self_name("jvm_bootstrap");
        status = _bootstrap_guarded();
    });
    bootstrap.join();
    return status;
}

// The bootstrap walks the plugin and lib directories: collect_jars() passes an error_code to
// recursive_directory_iterator's constructor but not to its operator++, which throws when a
// directory becomes unreadable mid-iteration. On the std::thread branch above an exception leaving
// the body is std::terminate, which is what this started out guarding - but the throwing path is
// reachable only when neither CLASSPATH nor DORIS_CLASSPATH is set, i.e. for a process NOT started
// by start_be.sh, and such a process is a plain pthread and takes the other branch. So the guard
// belongs here, around the one function, where it covers both. Same reason as the plugin warmup
// thread in doris_main.cpp.
Status JvmLauncher::_bootstrap_guarded() {
    try {
        return _bootstrap();
    } catch (const std::exception& e) {
        return Status::JniError("Failed to create the JVM: {}", e.what());
    } catch (...) {
        return Status::JniError("Failed to create the JVM: unknown exception");
    }
}

Status JvmLauncher::attach_current_thread(JNIEnv** env) {
    RETURN_IF_ERROR(ensure_jvm());
    return _attach_current_thread(env);
}

JNIEnv* JvmLauncher::_tls_env() {
    return Env::tls_env();
}

void JvmLauncher::_set_tls_env(JNIEnv* env) {
    Env::set_tls_env(env);
}

Status JvmLauncher::ScopedVmEnv::attach(JNIEnv** env) {
    DCHECK(!_primed) << "ScopedVmEnv::attach() called twice on one guard";
    if (_vm == nullptr) {
        return Status::JniError(
                "This process has no JVM, so there is no JNIEnv to look at. ScopedVmEnv "
                "deliberately does not create one.");
    }
    JNIEnv* attached = nullptr;
    RETURN_IF_ERROR(_attach_current_thread(&attached));
    // Read before the write, and put back rather than cleared in the destructor: this runs on the
    // bootstrap thread too, which primed the cache itself before calling in and still needs it
    // afterwards to finish resolving the JNI base.
    _previous = _tls_env();
    _set_tls_env(attached);
    _primed = true;
    *env = attached;
    return Status::OK();
}

JvmLauncher::ScopedVmEnv::~ScopedVmEnv() {
    if (_primed) {
        _set_tls_env(_previous);
    }
}

Status JvmLauncher::_attach_current_thread(JNIEnv** env) {
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
