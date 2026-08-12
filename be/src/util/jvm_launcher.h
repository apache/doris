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
    // Makes sure this process has a JVM, creating one if needed. Thread-safe; the JVM is
    // created at most once and the outcome of that single attempt is what every later
    // call returns. Fails with a clear message when Java support is turned off, which is
    // what every Java entry point of the BE reports to the user.
    static Status ensure_jvm();

    // Attaches the calling thread to the JVM and hands out its JNIEnv, arranging for the
    // thread to be detached again when it exits. Implies ensure_jvm().
    static Status attach_current_thread(JNIEnv** env);

    // The VM of this process, nullptr until ensure_jvm() has succeeded.
    static JavaVM* vm() { return _vm; }

private:
    static Status _bootstrap();
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
