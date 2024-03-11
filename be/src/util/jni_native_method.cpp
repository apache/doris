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

#include "jni.h"
#include "vec/columns/column_string.h"

namespace doris {

jlong JavaNativeMethods::resizeStringColumn(JNIEnv* env, jclass clazz, jlong columnAddr,
                                            jint length) {
    auto column = reinterpret_cast<vectorized::ColumnString::Chars*>(columnAddr);
    column->resize((size_t)length);
    return reinterpret_cast<jlong>(column->data());
}

jlong JavaNativeMethods::memoryMalloc(JNIEnv* env, jclass clazz, jlong bytes) {
    return reinterpret_cast<long>(malloc((size_t)bytes));
}

void JavaNativeMethods::memoryFree(JNIEnv* env, jclass clazz, jlong address) {
    free(reinterpret_cast<void*>(address));
}

} // namespace doris
