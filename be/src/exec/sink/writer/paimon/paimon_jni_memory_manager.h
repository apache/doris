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

#include <cstdint>
#include <memory>

#include "common/status.h"

namespace doris {

class RuntimeState;

/// Owns native pages used by one Java Paimon writer and accounts Java Arrow
/// direct memory reported through JNI callbacks.
///
/// All instances share one process-wide hard limit. The limit is deliberately
/// simple: BE JVM -Xmx * configured ratio. Allocation
/// beyond that limit fails the current Paimon write instead of attempting
/// fairness, borrowing, revocation, or background recovery.
class PaimonJniMemoryManager {
public:
    ~PaimonJniMemoryManager();

    static Status create(RuntimeState* state, std::unique_ptr<PaimonJniMemoryManager>* manager);
    static Status register_natives(JNIEnv* env, jclass writer_class);

    jobject allocate_page(JNIEnv* env, jint bytes);

    bool try_reserve_java_arrow(jlong bytes);
    void release_java_arrow(jlong bytes);

    /// Process-wide effective limit used as the local Java allocator ceiling.
    int64_t memory_limit() const;

    int64_t native_allocated_bytes() const;
    int64_t native_peak_allocated_bytes() const;

    int64_t global_current_bytes() const;
    int64_t global_peak_bytes() const;
    int64_t global_rejected_allocations() const;

private:
    class Impl;

    explicit PaimonJniMemoryManager(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> _impl;
};

} // namespace doris
