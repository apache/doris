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

#include "jni_md.h"

namespace doris {

/**
 * Java native methods for org.apache.doris.common.jni.utils.JNINativeMethod.
 */
struct JavaNativeMethods {
    /**
     * Resize string column and return the new column address.
     */
    static jlong resizeStringColumn(JNIEnv* env, jclass clazz, jlong columnAddr, jint length);

    /**
     * Allocate memory, which will be tracked by memory tracker.
     */
    static jlong memoryMalloc(JNIEnv* env, jclass clazz, jlong bytes);

    /**
     * Free memory, which will be tracked by memory tracker.
     */
    static void memoryFree(JNIEnv* env, jclass clazz, jlong address);
};

} // namespace doris
