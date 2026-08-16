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

#include <gen_cpp/DataSinks_types.h>
#include <jni.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"
#include "format/parquet/arrow_memory_pool.h"
#include "runtime/runtime_profile.h"

namespace arrow {
class Schema;
}

namespace doris {

class RuntimeState;

extern const char* const PAIMON_JNI_WRITER_OPEN_SIGNATURE;

struct PaimonJniWriterOpenMode {
    jboolean overwrite;
    jboolean changelog;

    static PaimonJniWriterOpenMode from_write_mode(TPaimonWriteMode::type write_mode);
};

/// JNI backend that owns the Java PaimonJniWriter object and its JNI method
/// handles. Creates lightweight JniPaimonWriter adapters that share this
/// backend's JVM connection.
///
/// Each JniPaimonWriteBackend corresponds to one Java PaimonJniWriter
/// instance; the JniPaimonWriter adapters are thin wrappers that delegate
/// write/prepare_commit/abort calls through the cached JNI method IDs. JNI-only
/// memory ownership and Profile counters stay here and are not part of the
/// common backend contract.
class JniPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    ~JniPaimonWriteBackend() override;

    Status open(const TPaimonTableSink& sink, RuntimeState* state,
                RuntimeProfile* profile) override;
    Status create_writer(std::unique_ptr<IPaimonWriter>* writer) override;
    Status close() override;
    PaimonBackendType type() const override { return PaimonBackendType::JNI; }

private:
    Status _check_jni_exception(JNIEnv* env, const std::string& method_name);
    Status _load_writer_class(JNIEnv* env, jclass* writer_class);
    Status _load_writer_memory_limits(JNIEnv* env);
    void _refresh_memory_profile(JNIEnv* env);

    // JNI global references — live for the duration of this backend.
    jclass _jni_writer_cls = nullptr;
    jobject _jni_writer_obj = nullptr;

    // Cached JNI method IDs for the PaimonJniWriter Java methods.
    jmethodID _write_id = nullptr;
    jmethodID _prepare_commit_id = nullptr;
    jmethodID _abort_id = nullptr;
    jmethodID _close_id = nullptr;
    jmethodID _get_arrow_memory_current_id = nullptr;
    jmethodID _get_arrow_memory_peak_id = nullptr;
    jmethodID _get_arrow_memory_limit_id = nullptr;
    jmethodID _get_paimon_page_memory_limit_id = nullptr;
    jmethodID _consume_last_memory_error_id = nullptr;

    TPaimonTableSink _sink;
    std::unique_ptr<PaimonJniMemoryManager> _memory_manager;
    RuntimeProfile::Counter* _writer_memory_limit = nullptr;
    RuntimeProfile::Counter* _native_page_memory_limit = nullptr;
    RuntimeProfile::Counter* _native_page_memory_peak = nullptr;
    RuntimeProfile::Counter* _arrow_memory_limit = nullptr;
    RuntimeProfile::Counter* _arrow_memory_current = nullptr;
    RuntimeProfile::Counter* _arrow_memory_peak = nullptr;
    RuntimeProfile::Counter* _arrow_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _paimon_page_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _jvm_heap_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _commit_payload_memory_exceeded = nullptr;
    RuntimeProfile* _jni_profile = nullptr;
    int64_t _arrow_memory_limit_bytes = 0;
    bool _opened = false;
};

/// Lightweight C++ adapter that delegates to the shared JNI backend.
///
/// Owns the Arrow memory pool used for Block → Arrow IPC conversion.
/// Each JniPaimonWriter is created by JniPaimonWriteBackend::create_writer()
/// and shares the backend's JNI method IDs and Java writer object reference.
class JniPaimonWriter final : public IPaimonWriter {
public:
    JniPaimonWriter(jobject jni_writer_obj, jmethodID write_id, jmethodID prepare_commit_id,
                    jmethodID abort_id, jmethodID consume_last_memory_error_id,
                    std::unique_ptr<ArrowMemoryPool<>> arrow_pool, TPaimonTableSink sink,
                    int64_t arrow_memory_limit_bytes, RuntimeProfile* profile);

    Status write(RuntimeState* state, Block& block) override;
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override;
    Status abort() override;

private:
    /// Convert Block → Arrow RecordBatch → IPC Stream, then pass to Java via JNI direct buffer.
    Status _write_projected_block(RuntimeState* state, Block& block);
    Status _write_row_range(RuntimeState* state, const Block& block,
                            const std::shared_ptr<arrow::Schema>& arrow_schema, size_t start_row,
                            size_t end_row, size_t estimated_ipc_bytes);
    Status _get_jni_call_status(JNIEnv* env, Status status);

    // Shared JNI state (owned by JniPaimonWriteBackend, not this adapter).
    jobject _jni_writer_obj;
    jmethodID _write_id;
    jmethodID _prepare_commit_id;
    jmethodID _abort_id;
    jmethodID _consume_last_memory_error_id;

    // Arrow resources owned by this writer adapter.
    std::unique_ptr<ArrowMemoryPool<>> _arrow_pool;
    TPaimonTableSink _sink;
    int64_t _arrow_memory_limit_bytes;
    RuntimeProfile::Counter* _cpp_arrow_memory_peak = nullptr;
    RuntimeProfile::Counter* _arrow_ipc_batch_count = nullptr;
    RuntimeProfile::Counter* _arrow_ipc_bytes = nullptr;
    RuntimeProfile::Counter* _arrow_ipc_batch_bytes_peak = nullptr;
    RuntimeProfile::Counter* _arrow_batch_rows_peak = nullptr;
    RuntimeProfile::Counter* _cpp_arrow_memory_error = nullptr;
    RuntimeProfile::Counter* _arrow_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _paimon_page_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _jvm_heap_memory_exceeded = nullptr;
    RuntimeProfile::Counter* _commit_payload_memory_exceeded = nullptr;
};

} // namespace doris
