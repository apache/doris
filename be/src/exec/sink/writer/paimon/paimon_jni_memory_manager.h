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

/// Owns the Doris-side native memory used by one Java Paimon writer.
///
/// Paimon's sort/merge buffers are Java objects, but their page storage is
/// requested through a JNI callback.  This manager is the bridge for that
/// callback: it allocates each page with Doris' allocator, exposes the page as
/// a direct ByteBuffer, tracks it until the writer is closed, and releases all
/// pages in its destructor.  The native writer/backend therefore keeps this
/// manager alive for at least as long as the Java writer can access its
/// callback handle.
///
/// The limit is a per-writer budget.  It is derived from the query memory
/// limit and the number of local sink instances, then capped by the global
/// Paimon JNI configuration.  The manager accounts only for pages allocated
/// by this callback; Java heap and other Paimon-managed memory remain under
/// their respective runtimes.
class PaimonJniMemoryManager {
public:
    ~PaimonJniMemoryManager();

    /// Construct a manager whose budget is sized from the query context.
    ///
    /// The query must provide both a memory tracker and QueryContext.  The
    /// latter supplies the ResourceContext used whenever allocation/freeing
    /// crosses into a JNI-created thread.
    static Status create(RuntimeState* state, std::unique_ptr<PaimonJniMemoryManager>* manager);
    /// Register the static JNI callback used by PaimonJniWriter.
    static Status register_natives(JNIEnv* env, jclass writer_class);

    /// Allocate one native page and return it as a direct ByteBuffer.
    ///
    /// Workload-group and process pressure block the calling pipeline task until memory becomes
    /// available or the query is cancelled. Query-local hard-limit failures return immediately
    /// because waiting inside the same synchronous write could deadlock. On failure this method
    /// leaves no accounting entry behind and reports the error through the JNI environment. The
    /// returned buffer remains
    /// valid until the manager is destroyed (or allocation of that page is
    /// rolled back because NewDirectByteBuffer failed).
    jobject allocate_page(JNIEnv* env, jint bytes);

    /// Return the immutable per-writer native page budget in bytes.
    int64_t memory_limit() const;

    /// Return the high-water mark of native pages allocated by this manager.
    int64_t native_peak_allocated_bytes() const;

private:
    class Impl;

    explicit PaimonJniMemoryManager(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> _impl;
};

} // namespace doris
