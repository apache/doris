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

#include <jni.h>
#include <pthread.h>

#include <string>
#include <vector>

#include "common/status.h"

namespace doris::Jni {

// Owns the JVM of this process: creates it and attaches threads to it.
//
// The JVM used to be bootstrapped by hadoop's libhdfs. Its getJNIEnv() created the VM out
// of the CLASSPATH and LIBHDFS_OPTS environment variables on first use, and registered a
// thread-local destructor that detached the thread again. Every Java feature of the BE -
// JNI table formats, Java UDFs, the plugin registry - therefore only worked because a
// file system client happened to be linked in, and the JVM was configured by that client
// rather than by the BE. This class takes the job over so that the two are independent:
// builds without libhdfs keep their Java features, and the BE decides how the JVM looks.
//
// The JVM is created on first use, never at startup: a BE that touches no Java feature
// pays for no JVM at all.
class JvmLauncher {
public:
    // Makes sure this process has a JVM, creating one if needed. Succeeding means exactly that
    // and nothing more: the jvm_* metrics are published along the way, because they describe the
    // JVM itself and a BE that reached one only through libhdfs must still export them, but
    // resolving the plugin SPI is NOT part of the answer. That comes out of doris-jni-spi.jar,
    // and a deployment missing it has a Java problem, not an HDFS one - both hdfs_file_system.cpp
    // and hdfs_mgr.cpp gate on this call. Java callers get that second failure from
    // Jni::Env::Get(), the door they all come through.
    //
    // Thread-safe; the JVM is created at most once and the outcome of that single
    // attempt is what every later call returns. Fails with a clear message when Java support
    // is turned off, which is what every Java entry point of the BE reports to the user.
    //
    // Safe to call from a bthread: creating the JVM is JNI code, which cannot run on one, so
    // this switches to a pthread itself when it has to. Callers need no switch of their own.
    static Status ensure_jvm();

    // Attaches the calling thread to the JVM and hands out its JNIEnv, arranging for the
    // thread to be detached again when it exits. Implies ensure_jvm(), and like it says nothing
    // about the plugin SPI - Jni::Env::Get() is the entry point that checks that too.
    static Status attach_current_thread(JNIEnv** env);

    // The VM of this process, nullptr until this process has one. Deliberately not "until
    // ensure_jvm() has succeeded": _bootstrap() has two error paths after JNI_CreateJavaVM
    // returns, so a failed ensure_jvm() can leave a live VM behind. "Does a JVM exist" is both
    // what this can actually answer and what its callers - the tests asserting that a code path
    // creates none - are asking.
    static JavaVM* vm() { return _vm; }

private:
    static Status _bootstrap_on_pthread();
    // _bootstrap() with the directory-walk exceptions turned into a Status, for both branches
    // of _bootstrap_on_pthread().
    static Status _bootstrap_guarded();
    static Status _bootstrap();
    // attach_current_thread() without the ensure_jvm(), for the bootstrap itself: it runs
    // inside that call_once and would deadlock on it.
    static Status _attach_current_thread(JNIEnv** env);
    static Status _create_jvm();
    static std::vector<std::string> _build_options();
    static std::string _class_path_option();
    static void _load_file_systems(JNIEnv* env);
    static void _detach_current_thread(void* attached_env);

    static JavaVM* _vm;
    // Key whose only purpose is to get _detach_current_thread() called on thread exit.
    static pthread_key_t _detach_key;
};

} // namespace doris::Jni
