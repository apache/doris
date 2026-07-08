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

#include <memory>
#include <string>

#include "common/status.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"

namespace doris {

class RuntimeState;

/// JNI-based Paimon write backend.
///
/// Bridges Doris BE (C++) to the Paimon Java SDK via JNI.
/// On open() it loads the Java class `PaimonJniWriter` and caches
/// method IDs. On create_writer() it instantiates a Java
/// BatchTableWrite for a given (partition, bucket) pair.
///
/// Data path:
///   Block → Arrow RecordBatch → IPC Stream → JNI direct buffer →
///   Java ArrowStreamReader → Paimon InternalRow → BatchTableWrite
///
/// Commit path:
///   Java prepareCommit() → CommitMessageSerializer → byte[][]
///   → C++ TPaimonCommitMessage → RuntimeState → FE Coordinator
class JniPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    JniPaimonWriteBackend() = default;
    ~JniPaimonWriteBackend() override;

    Status open(const TPaimonTableSink& sink, RuntimeState* state) override;

    Status create_writer(const std::string& partition_bytes, int32_t bucket,
                         std::unique_ptr<IPaimonWriter>* writer) override;

    Status create_committer(std::unique_ptr<IPaimonCommitter>* committer) override;

    PaimonBackendType type() const override { return PaimonBackendType::JNI; }

    bool supports_compaction() const override { return true; }
    bool supports_lookup_changelog_producer() const override { return true; }
    bool supports_full_compaction_changelog_producer() const override { return true; }
    bool supports_partial_update_with_dv() const override { return true; }
    bool supports_aggregation_with_dv() const override { return true; }

private:
    Status _get_jni_env(JNIEnv** env);
    Status _check_jni_exception(JNIEnv* env, const std::string& method_name);

    // JNI global references
    jclass _jni_writer_cls = nullptr;
    jobject _jni_writer_obj = nullptr;

    // Cached method IDs for the Java PaimonJniWriter class
    jmethodID _open_id = nullptr;
    jmethodID _write_id = nullptr;
    jmethodID _prepare_commit_id = nullptr;
    jmethodID _abort_id = nullptr;
    jmethodID _close_id = nullptr;

    // Arrow memory pool for Block → Arrow conversion
    std::unique_ptr<ArrowMemoryPool<>> _arrow_pool;

    // Parsed sink configuration
    TPaimonTableSink _sink;

    // Whether the JNI writer has been opened
    bool _opened = false;
};

/// Per-(partition, bucket) writer backed by the shared JNI context.
class JniPaimonWriter final : public IPaimonWriter {
public:
    JniPaimonWriter(JNIEnv* env, jobject jni_writer_obj, jmethodID write_id,
                    jmethodID prepare_commit_id, jmethodID abort_id,
                    std::unique_ptr<ArrowMemoryPool<>> arrow_pool, const TPaimonTableSink& sink);
    ~JniPaimonWriter() override;

    Status write(RuntimeState* state, Block& block) override;
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override;
    Status compact(bool full_compaction) override;
    Status abort() override;

private:
    Status _write_projected_block(Block& block);

    JNIEnv* _env;
    jobject _jni_writer_obj;
    jmethodID _write_id;
    jmethodID _prepare_commit_id;
    jmethodID _abort_id;
    std::unique_ptr<ArrowMemoryPool<>> _arrow_pool;
    TPaimonTableSink _sink;
    int64_t _row_count = 0;
};

/// Committer that delegates to the Java Paimon SDK.
class JniPaimonCommitter final : public IPaimonCommitter {
public:
    explicit JniPaimonCommitter(const TPaimonTableSink& sink);
    ~JniPaimonCommitter() override = default;

    Status commit(const std::vector<TPaimonCommitMessage>& messages) override;
    Status overwrite(const std::vector<TPaimonCommitMessage>& messages,
                     const std::map<std::string, std::string>& static_partition) override;
    Status truncate_table() override;
    Status truncate_partitions(
            const std::vector<std::map<std::string, std::string>>& partitions) override;
    Status abort(const std::vector<TPaimonCommitMessage>& messages) override;

private:
    Status _commit_impl(const std::vector<TPaimonCommitMessage>& messages, bool overwrite_mode,
                        const std::map<std::string, std::string>* static_partition);

    TPaimonTableSink _sink;
};

} // namespace doris
