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
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>

#include "absl/strings/substitute.h"
#include "common/config.h"
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

    // Three steps, in this order, and the order is the point.
    //
    // The JVM first, because the base is resolved inside its bootstrap - once per process, on the
    // thread that brought the JVM up, with that thread's env primed so this function is not
    // re-entered while ensure_jni_base()'s call_once is held. Asking for the base before there is
    // a JVM would run that resolution HERE instead: Env::Init() calls Env::Get(), which lands back
    // in this slow path and re-enters the same call_once on the same thread.
    RETURN_IF_ERROR(JvmLauncher::ensure_jvm());

    // Then the base, asked for rather than assumed, because the bootstrap no longer fails when it
    // cannot resolve it: a JVM whose plugin SPI is missing still serves libhdfs, and taking HDFS
    // down with a Java deployment problem is not this file's call to make. This is where that gate
    // lives instead - every caller that is about to run Java code comes through here, and nothing
    // on the libhdfs path does. By now it is the cached outcome of the attempt the bootstrap
    // already made; it never re-runs the resolution.
    //
    // Before the attach, not after: attach_current_thread() both attaches the thread and registers
    // the pthread-key hook that detaches it again, and neither is undone by clearing tls_env_. A
    // rejected caller that had already attached would stay bound to the JVM - one JVM thread
    // structure and one stack reservation each - for the rest of its life, on a deployment where
    // it can never run a line of Java. With the base broken and load arriving, that is every
    // scanner thread and every pipeline thread. Not the metrics daemons: the jvm_* metrics are
    // published whenever a JVM exists, base or no base, so they take their env from
    // JvmLauncher::ScopedVmEnv and never come through here.
    //
    // Never the call that RUNS the resolution, only the one that reads its answer: ensure_jvm()
    // above returned OK, which means _bootstrap() ran to its end on some thread and its last step
    // was ensure_jni_base(). That is what keeps this line from re-entering a call_once from an
    // unattached thread, and it is the whole reason the order above is the order above - so it is
    // asserted rather than left to be rediscovered.
    DCHECK(Util::jni_base_outcome().has_value())
            << "the JNI base has no outcome yet, so this call would run the resolution instead of "
               "reading it - see the ordering note above";
    RETURN_IF_ERROR(Util::ensure_jni_base());

    // Finally the attach. On failure tls_env_ is left untouched, so the thread still looks
    // unattached and the next call retries and reports the same error.
    RETURN_IF_ERROR(JvmLauncher::attach_current_thread(&tls_env_));
    *env = tls_env_;
    return Status::OK();
}

Status Env::GetJniExceptionMsg(JNIEnv* env, bool log_stack, const string& prefix) {
    jthrowable exc = env->ExceptionOccurred();
    Defer def {[&]() { env->DeleteLocalRef(exc); }};
    if (exc == nullptr) {
        return Status::OK();
    }
    // Not a DCHECK, and before the ExceptionClear() below so ExceptionDescribe() still has
    // something to print: these two are set by Env::Init(), the first step of the JNI base, and
    // since the base stopped being a precondition for having a JVM there is a reachable state
    // where a JVM exists and they are still null. JvmStats::init() runs in exactly that state -
    // the jvm_* metrics are published whenever a JVM exists, base or no base - so a JNI call of
    // its own throwing would land here with no way to render the throwable, and a DCHECK is a
    // no-op in a release build. Degrade to a plain message rather than dereference null: this
    // function exists to turn a Java exception into a Status, and it can still do that much.
    //
    // All THREE, not just the two this function uses first: init_throw_exception() assigns them
    // in order, so a failure at its last step leaves the class and toString set and
    // toStackTrace null, and the log_stack branch below - log_stack defaults to true - would then
    // call CallStaticObjectMethod with a null jmethodID, which is the very SIGSEGV this guard is
    // here to prevent.
    if (jni_util_cl_ == nullptr || throwable_to_string_id_ == nullptr ||
        throwable_to_stack_trace_id_ == nullptr) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return Status::JniError(
                "{}a Java exception was thrown before the Java plugin SPI was resolved, so it "
                "cannot be rendered here and was written to stderr instead. That means "
                "doris-jni-spi.jar is missing or unreadable - see the error logged at startup.",
                prefix);
    }
    env->ExceptionClear();
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

jlong Util::_parse_xmx(const std::string& options) {
    jlong parsed = 0;
    std::istringstream iss(options);
    std::string opt;
    while (iss >> opt) {
        if (!opt.starts_with("-Xmx")) {
            continue;
        }
        std::string value = opt.substr(4);
        jlong multiplier = 1;
        if (!value.empty()) {
            switch (value.back()) {
            case 'g':
            case 'G':
                multiplier = 1024L * 1024 * 1024;
                value.pop_back();
                break;
            case 'm':
            case 'M':
                multiplier = 1024L * 1024;
                value.pop_back();
                break;
            case 'k':
            case 'K':
                multiplier = 1024L;
                value.pop_back();
                break;
            default:
                break;
            }
        }
        // Checked rather than handed to std::stoll, which throws on "-Xmxbig" and on a bare
        // "-Xmx", and which would silently accept "-Xmx12big". The JVM was started with these
        // options and ignored the malformed one, so neither may this: the value is only used to
        // rate limit hdfs writes, and an exception here would come out of a user's write.
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos) {
            LOG(WARNING) << "Ignoring the JVM option '" << opt << "': not a heap size";
            continue;
        }
        try {
            jlong digits = std::stoll(value);
            // The multiply, too: "-Xmx99999999999g" survives stoll and overflows a signed 64-bit
            // here, which is UB and which an ASAN build (-fsanitize=undefined) reports. Treated
            // like the malformed case three lines up - the JVM would have rejected such an -Xmx,
            // so this can only be a value nothing is running with.
            if (digits > std::numeric_limits<jlong>::max() / multiplier) {
                LOG(WARNING) << "Ignoring the JVM option '" << opt << "': heap size out of range";
                continue;
            }
            // The last one wins, as it does for the JVM itself.
            parsed = digits * multiplier;
        } catch (const std::exception& e) {
            LOG(WARNING) << "Ignoring the JVM option '" << opt << "': " << e.what();
        }
    }
    return parsed;
}

void Util::_parse_max_heap_memory_size_from_jvm() {
    // The same options the JVM was created from, see JvmLauncher::_build_options().
    std::string java_opts = getenv("JAVA_OPTS") ? getenv("JAVA_OPTS") : "";
    if (java_opts.empty()) {
        java_opts = getenv("LIBHDFS_OPTS") ? getenv("LIBHDFS_OPTS") : "";
    }
    max_jvm_heap_memory_size_ = _parse_xmx(java_opts);
    if (0 == max_jvm_heap_memory_size_) {
        // Used to be LOG(FATAL). This runs on whichever query first writes to hdfs - it did
        // before this change too - and taking the BE down over a missing -Xmx is out of all
        // proportion to what the value is for: rate limiting hdfs writes against the JVM heap.
        // Fall back to the same 1g the JVM is created with when nothing else says otherwise.
        max_jvm_heap_memory_size_ = 1024L * 1024 * 1024;
        LOG(WARNING) << "No -Xmx in the JVM options, assuming a max heap of "
                     << max_jvm_heap_memory_size_
                     << " bytes when rate limiting hdfs writes. Set -Xmx in JAVA_OPTS to make "
                        "this exact.";
    }
    LOG(INFO) << "the max_jvm_heap_memory_size_ is " << max_jvm_heap_memory_size_;
}

size_t Util::get_max_jni_heap_memory_size() {
#ifdef BE_TEST
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

// Driven by JvmLauncher::_bootstrap(), right after the JVM it describes exists, and re-asked by
// Env::GetJNIEnvSlowPath() on every thread that is about to run Java code: the bootstrap does not
// fail when this fails, so somebody has to keep Java callers out afterwards. Only the first call
// resolves anything; the rest get its outcome.
namespace {
std::once_flag jni_base_once;
Status jni_base_status;
// Published after jni_base_status is written, and read before it, so that jni_base_outcome()
// below never reads a Status another thread is still assigning. call_once alone does not order
// that read: a thread that never calls ensure_jni_base() takes no part in its synchronization.
std::atomic<bool> jni_base_resolved {false};
} // namespace

Status Util::ensure_jni_base() {
    std::call_once(jni_base_once, []() {
        jni_base_status = _init_jni_base();
        jni_base_resolved.store(true, std::memory_order_release);
    });
    return jni_base_status;
}

std::optional<Status> Util::jni_base_outcome() {
    if (!jni_base_resolved.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    return jni_base_status;
}

Status Util::_init_jni_base() {
    RETURN_IF_ERROR(Env::Init());
    // Before any Java code runs: it links against these natives, and hitting an
    // unregistered one would surface as an UnsatisfiedLinkError deep inside a scanner.
    RETURN_IF_ERROR(_init_register_natives());
    RETURN_IF_ERROR(_init_collect_class());
    return Status::OK();
}

Status Util::_init_register_natives() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    // Find JNINativeMethod class and create a global ref.
    jclass local_jni_native_exc_cl =
            env->FindClass("org/apache/doris/jni/spi/utils/JNINativeMethod");
    if (local_jni_native_exc_cl == nullptr) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        // Same deployment failure as the JniUtil lookup, and the same answer - see the note
        // there.
        return Status::JniError(
                "Failed to find the JNINativeMethod class of the Java plugin SPI. It ships in "
                "doris-jni-spi.jar under DORIS_HOME/lib/jni/spi, which bin/start_be.sh puts on "
                "the class path.");
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
