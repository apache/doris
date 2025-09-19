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

#include <stdlib.h>

#include <vector>

#include "jni.h"
#include "vec/columns/column_string.h"

namespace doris {

jlong JavaNativeMethods::resizeStringColumn(JNIEnv* env, jclass clazz, jlong columnAddr,
                                            jint length) {
    auto* column = reinterpret_cast<vectorized::ColumnString::Chars*>(columnAddr);
    column->resize(length);
    return reinterpret_cast<jlong>(column->data());
}

jlong JavaNativeMethods::memoryMalloc(JNIEnv* env, jclass clazz, jlong bytes) {
    return reinterpret_cast<long>(malloc(bytes));
}

void JavaNativeMethods::memoryFree(JNIEnv* env, jclass clazz, jlong address) {
    free(reinterpret_cast<void*>(address));
}

jlongArray JavaNativeMethods::memoryMallocBatch(JNIEnv* env, jclass clazz, jintArray sizes) {
    if (sizes == nullptr) {
        return env->NewLongArray(0);
    }
    jsize n = env->GetArrayLength(sizes);
    if (n <= 0) {
        return env->NewLongArray(0);
    }

    jint* elems = env->GetIntArrayElements(sizes, nullptr);
    if (elems == nullptr) {
        return nullptr; // OOM
    }

    jlongArray result = env->NewLongArray(n);
    if (result == nullptr) {
        env->ReleaseIntArrayElements(sizes, elems, JNI_ABORT);
        return nullptr;
    }

    std::vector<void*> allocated;
    allocated.reserve(n);

    bool failed = false;
    for (jsize i = 0; i < n; ++i) {
        jint sz = elems[i];
        if (sz <= 0) {
            allocated.push_back(nullptr);
            continue;
        }
        void* p = malloc(static_cast<size_t>(sz));
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
        env->ReleaseIntArrayElements(sizes, elems, JNI_ABORT);
        return nullptr;
    }

    std::vector<jlong> addrs(n);
    for (jsize i = 0; i < n; ++i) {
        addrs[i] = reinterpret_cast<jlong>(allocated[i]);
    }
    env->SetLongArrayRegion(result, 0, n, addrs.data());
    env->ReleaseIntArrayElements(sizes, elems, JNI_ABORT);
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
