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

#include "jni_native_method.h"

#include <glog/logging.h>

#include <cstdlib>
#include <vector>

#include "jni.h"
#include "util/defer_op.h"

namespace doris {

jlong JavaNativeMethods::memoryMalloc(JNIEnv* env, jclass clazz, jlong bytes) {
    return reinterpret_cast<long>(malloc(bytes));
}

void JavaNativeMethods::memoryFree(JNIEnv* env, jclass clazz, jlong address) {
    free(reinterpret_cast<void*>(address));
}

jlongArray JavaNativeMethods::memoryMallocBatch(JNIEnv* env, jclass clazz, jintArray sizes) {
    DCHECK(sizes != nullptr);
    jsize n = env->GetArrayLength(sizes);
    DCHECK(n > 0);
    jint* elems = env->GetIntArrayElements(sizes, nullptr);
    if (elems == nullptr) {
        return nullptr;
    }
    DEFER({
        if (elems != nullptr) {
            env->ReleaseIntArrayElements(sizes, elems, JNI_ABORT);
        }
    });

    jlongArray result = env->NewLongArray(n);
    if (result == nullptr) {
        return nullptr;
    }

    std::vector<void*> allocated;
    allocated.reserve(n);

    // sizes are validated on Java side: n > 0 and each size > 0
    bool failed = false;
    for (jsize i = 0; i < n; ++i) {
        auto sz = static_cast<size_t>(elems[i]);
        void* p = malloc(sz);
        if (p == nullptr) {
            failed = true;
            break;
        }
        allocated.push_back(p);
    }

    if (failed) {
        for (void* p : allocated) {
            if (p != nullptr) {
                free(p);
            }
        }
        return nullptr;
    }

    std::vector<jlong> addrs(n);
    for (jsize i = 0; i < n; ++i) {
        addrs[i] = reinterpret_cast<jlong>(allocated[i]);
    }
    env->SetLongArrayRegion(result, 0, n, addrs.data());
    return result;
}

void JavaNativeMethods::memoryFreeBatch(JNIEnv* env, jclass clazz, jlongArray addrs) {
    if (addrs == nullptr) {
        return;
    }
    jsize n = env->GetArrayLength(addrs);
    if (n <= 0) {
        return;
    }
    jlong* elems = env->GetLongArrayElements(addrs, nullptr);
    if (elems == nullptr) {
        return;
    }
    for (jsize i = 0; i < n; ++i) {
        if (elems[i] != 0) {
            free(reinterpret_cast<void*>(elems[i]));
        }
    }
    env->ReleaseLongArrayElements(addrs, elems, JNI_ABORT);
}

} // namespace doris
