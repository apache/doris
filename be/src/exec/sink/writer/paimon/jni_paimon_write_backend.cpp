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

#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "common/logging.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"

namespace doris {

// ────────────────────────────────────────────────────────────
// JniPaimonWriteBackend
// ────────────────────────────────────────────────────────────

static constexpr const char* PAIMON_JNI_WRITER_CLASS = "org/apache/doris/paimon/PaimonJniWriter";
static constexpr const char* PAIMON_OUTPUT_COLUMN_NAMES_KEY = "doris.output_column_names";
static constexpr char PAIMON_COLUMN_NAME_SEPARATOR = '\x01';

JniPaimonWriteBackend::~JniPaimonWriteBackend() {
    JNIEnv* env = nullptr;
    if (_get_jni_env(&env).ok()) {
        if (_jni_writer_obj != nullptr) {
            env->DeleteGlobalRef(_jni_writer_obj);
            _jni_writer_obj = nullptr;
        }
        if (_jni_writer_cls != nullptr) {
            env->DeleteGlobalRef(_jni_writer_cls);
            _jni_writer_cls = nullptr;
        }
    }
}

Status JniPaimonWriteBackend::_get_jni_env(JNIEnv** env) {
    JavaVM* jvm = nullptr;
    jsize n_vms = 0;
    jint result = JNI_GetCreatedJavaVMs(&jvm, 1, &n_vms);
    if (result != JNI_OK || n_vms == 0) {
        return Status::InternalError("Failed to get created JavaVM");
    }
    result = jvm->GetEnv(reinterpret_cast<void**>(env), JNI_VERSION_1_8);
    if (result == JNI_EDETACHED) {
        result = jvm->AttachCurrentThread(reinterpret_cast<void**>(env), nullptr);
        if (result != JNI_OK) {
            return Status::InternalError("Failed to attach current thread to JVM");
        }
    } else if (result != JNI_OK) {
        return Status::InternalError("Failed to get JNIEnv");
    }
    return Status::OK();
}

Status JniPaimonWriteBackend::_check_jni_exception(JNIEnv* env, const std::string& method_name) {
    if (env->ExceptionCheck()) {
        Status st =
                Jni::Env::GetJniExceptionMsg(env, true, "JNI exception in " + method_name + ": ");
        LOG(WARNING) << st.to_string();
        return st;
    }
    return Status::OK();
}

static std::vector<std::string> _split_column_names(const std::string& column_names_str) {
    std::vector<std::string> names;
    size_t begin = 0;
    while (begin <= column_names_str.size()) {
        size_t end = column_names_str.find(PAIMON_COLUMN_NAME_SEPARATOR, begin);
        if (end == std::string::npos) {
            names.emplace_back(column_names_str.substr(begin));
            break;
        }
        names.emplace_back(column_names_str.substr(begin, end - begin));
        begin = end + 1;
    }
    return names;
}

static jobject _to_java_options(JNIEnv* env, const std::map<std::string, std::string>& options) {
    jclass map_cls = env->FindClass("java/util/HashMap");
    jmethodID map_ctor = env->GetMethodID(map_cls, "<init>", "()V");
    jmethodID put_method = env->GetMethodID(
            map_cls, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    jobject map_obj = env->NewObject(map_cls, map_ctor);
    for (const auto& kv : options) {
        jstring key = env->NewStringUTF(kv.first.c_str());
        jstring val = env->NewStringUTF(kv.second.c_str());
        env->CallObjectMethod(map_obj, put_method, key, val);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(val);
    }
    env->DeleteLocalRef(map_cls);
    return map_obj;
}

Status JniPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state) {
    _sink = sink;
    _arrow_pool = std::make_unique<ArrowMemoryPool<>>();

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    // Find the Java writer class
    jclass local_cls = env->FindClass(PAIMON_JNI_WRITER_CLASS);
    RETURN_IF_ERROR(_check_jni_exception(env, "FindClass"));
    if (local_cls == nullptr) {
        return Status::InternalError("Failed to find Java class: {}", PAIMON_JNI_WRITER_CLASS);
    }
    _jni_writer_cls = static_cast<jclass>(env->NewGlobalRef(local_cls));
    env->DeleteLocalRef(local_cls);

    // Cache method IDs
    _open_id = env->GetMethodID(_jni_writer_cls, "open",
                                "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;)V");
    _write_id = env->GetMethodID(_jni_writer_cls, "write", "(JI)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _close_id = env->GetMethodID(_jni_writer_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "GetMethodID"));

    // Create the JNI writer object
    jmethodID ctor_id = env->GetMethodID(_jni_writer_cls, "<init>", "()V");
    jobject local_obj = env->NewObject(_jni_writer_cls, ctor_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "NewObject"));
    _jni_writer_obj = env->NewGlobalRef(local_obj);
    env->DeleteLocalRef(local_obj);

    // Build options map
    std::map<std::string, std::string> jni_options;
    if (sink.__isset.paimon_options) {
        jni_options.insert(sink.paimon_options.begin(), sink.paimon_options.end());
    }
    if (sink.__isset.hadoop_config) {
        for (const auto& [key, value] : sink.hadoop_config) {
            jni_options[key] = value;
        }
    }
    jni_options["db_name"] = sink.db_name;
    jni_options["table_name"] = sink.tb_name;
    if (sink.__isset.serialized_table && !sink.serialized_table.empty()) {
        jni_options["serialized_table"] = sink.serialized_table;
    }

    // Build column names array
    jstring j_location = env->NewStringUTF(sink.table_location.c_str());
    jobject j_options = _to_java_options(env, jni_options);

    jclass string_cls = env->FindClass("java/lang/String");
    jobjectArray j_cols = nullptr;
    if (sink.__isset.column_names && !sink.column_names.empty()) {
        j_cols = env->NewObjectArray(static_cast<jsize>(sink.column_names.size()), string_cls,
                                     nullptr);
        for (size_t i = 0; i < sink.column_names.size(); ++i) {
            jstring str = env->NewStringUTF(sink.column_names[i].c_str());
            env->SetObjectArrayElement(j_cols, static_cast<jsize>(i), str);
            env->DeleteLocalRef(str);
        }
    } else {
        j_cols = env->NewObjectArray(0, string_cls, nullptr);
    }

    // Call Java open()
    env->CallVoidMethod(_jni_writer_obj, _open_id, j_location, j_options, j_cols);
    Status st = _check_jni_exception(env, "open");

    env->DeleteLocalRef(j_location);
    env->DeleteLocalRef(j_options);
    env->DeleteLocalRef(j_cols);
    env->DeleteLocalRef(string_cls);

    if (st.ok()) {
        _opened = true;
    }
    return st;
}

Status JniPaimonWriteBackend::create_writer(const std::string& partition_bytes, int32_t bucket,
                                            std::unique_ptr<IPaimonWriter>* writer) {
    DCHECK(_opened) << "Backend must be opened before creating writers";
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    // For the JNI backend, each (partition, bucket) shares the same
    // underlying Java BatchTableWrite instance. We create a lightweight
    // wrapper that delegates to the shared Java object.
    auto jni_writer = std::make_unique<JniPaimonWriter>(
            env, _jni_writer_obj, _write_id, _prepare_commit_id, _abort_id,
            std::make_unique<ArrowMemoryPool<>>(), _sink);
    *writer = std::move(jni_writer);
    return Status::OK();
}

Status JniPaimonWriteBackend::create_committer(std::unique_ptr<IPaimonCommitter>* committer) {
    auto c = std::make_unique<JniPaimonCommitter>(_sink);
    *committer = std::move(c);
    return Status::OK();
}

// ────────────────────────────────────────────────────────────
// JniPaimonWriter
// ────────────────────────────────────────────────────────────

JniPaimonWriter::JniPaimonWriter(JNIEnv* env, jobject jni_writer_obj, jmethodID write_id,
                                 jmethodID prepare_commit_id, jmethodID abort_id,
                                 std::unique_ptr<ArrowMemoryPool<>> arrow_pool,
                                 const TPaimonTableSink& sink)
        : _env(env),
          _jni_writer_obj(jni_writer_obj),
          _write_id(write_id),
          _prepare_commit_id(prepare_commit_id),
          _abort_id(abort_id),
          _arrow_pool(std::move(arrow_pool)),
          _sink(sink) {}

JniPaimonWriter::~JniPaimonWriter() = default;

Status JniPaimonWriter::_write_projected_block(Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    _row_count += block.rows();

    // Determine output column names
    if (_sink.__isset.paimon_options) {
        auto it = _sink.paimon_options.find(PAIMON_OUTPUT_COLUMN_NAMES_KEY);
        if (it != _sink.paimon_options.end()) {
            auto output_names = _split_column_names(it->second);
            DCHECK_EQ(output_names.size(), block.columns());
            for (size_t i = 0; i < output_names.size(); ++i) {
                block.get_by_position(i).name = output_names[i];
            }
        }
    }

    // Convert Block → Arrow RecordBatch → IPC Stream
    std::shared_ptr<arrow::Schema> arrow_schema;
    RETURN_IF_ERROR(get_arrow_schema_from_block(block, &arrow_schema, "UTC"));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow_schema, _arrow_pool.get(), &record_batch,
                                           cctz::utc_time_zone()));

    auto out_stream_res = arrow::io::BufferOutputStream::Create();
    if (!out_stream_res.ok()) {
        return Status::InternalError("Arrow BufferOutputStream create failed: {}",
                                     out_stream_res.status().ToString());
    }
    auto out_stream = *out_stream_res;

    auto writer_res = arrow::ipc::MakeStreamWriter(out_stream, arrow_schema);
    if (!writer_res.ok()) {
        return Status::InternalError("Arrow StreamWriter create failed: {}",
                                     writer_res.status().ToString());
    }
    auto ipc_writer = *writer_res;
    if (!ipc_writer->WriteRecordBatch(*record_batch).ok()) {
        return Status::InternalError("Arrow WriteRecordBatch failed");
    }
    if (!ipc_writer->Close().ok()) {
        return Status::InternalError("Arrow StreamWriter close failed");
    }

    auto buffer_res = out_stream->Finish();
    if (!buffer_res.ok()) {
        return Status::InternalError("Arrow output stream finish failed: {}",
                                     buffer_res.status().ToString());
    }
    std::shared_ptr<arrow::Buffer> buffer = *buffer_res;

    // Pass to Java via JNI
    JNIEnv* env = nullptr;
    {
        JavaVM* jvm = nullptr;
        jsize n_vms = 0;
        JNI_GetCreatedJavaVMs(&jvm, 1, &n_vms);
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
        if (env == nullptr) {
            jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        }
    }

    auto address = reinterpret_cast<jlong>(buffer->data());
    jint length = static_cast<jint>(buffer->size());
    env->CallVoidMethod(_jni_writer_obj, _write_id, address, length);
    RETURN_IF_ERROR(
            Jni::Env::GetJniExceptionMsg(env, false, "JNI exception in JniPaimonWriter::write: "));
    return Status::OK();
}

Status JniPaimonWriter::write(RuntimeState* state, Block& block) {
    return _write_projected_block(block);
}

Status JniPaimonWriter::prepare_commit(std::vector<TPaimonCommitMessage>& messages) {
    JNIEnv* env = nullptr;
    {
        JavaVM* jvm = nullptr;
        jsize n_vms = 0;
        JNI_GetCreatedJavaVMs(&jvm, 1, &n_vms);
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
        if (env == nullptr) {
            jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        }
    }

    jobject j_payloads_obj = env->CallObjectMethod(_jni_writer_obj, _prepare_commit_id);
    Status st = Jni::Env::GetJniExceptionMsg(env, false, "JNI exception in prepareCommit: ");
    if (!st.ok()) {
        return st;
    }

    if (j_payloads_obj == nullptr) {
        return Status::OK(); // no data written
    }

    auto* j_payloads = static_cast<jobjectArray>(j_payloads_obj);
    jsize num_payloads = env->GetArrayLength(j_payloads);

    for (jsize i = 0; i < num_payloads; ++i) {
        jbyteArray j_bytes = static_cast<jbyteArray>(env->GetObjectArrayElement(j_payloads, i));
        if (j_bytes == nullptr) {
            continue;
        }
        jsize len = env->GetArrayLength(j_bytes);
        if (len > 0) {
            jbyte* bytes = env->GetByteArrayElements(j_bytes, nullptr);
            if (bytes != nullptr) {
                std::string payload(reinterpret_cast<char*>(bytes), static_cast<size_t>(len));
                TPaimonCommitMessage msg;
                msg.__set_payload(payload);
                messages.emplace_back(std::move(msg));
                env->ReleaseByteArrayElements(j_bytes, bytes, JNI_ABORT);
            }
        }
        env->DeleteLocalRef(j_bytes);
    }
    env->DeleteLocalRef(j_payloads);
    return Status::OK();
}

Status JniPaimonWriter::compact(bool full_compaction) {
    // Compaction is triggered by Java's prepareCommit, not as a separate call
    // from BE. For explicit compaction, we would need an additional JNI call.
    return Status::OK();
}

Status JniPaimonWriter::abort() {
    JNIEnv* env = nullptr;
    {
        JavaVM* jvm = nullptr;
        jsize n_vms = 0;
        JNI_GetCreatedJavaVMs(&jvm, 1, &n_vms);
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
        if (env == nullptr) {
            jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        }
    }
    env->CallVoidMethod(_jni_writer_obj, _abort_id);
    return Jni::Env::GetJniExceptionMsg(env, true, "JNI exception in abort: ");
}

// ────────────────────────────────────────────────────────────
// JniPaimonCommitter
// ────────────────────────────────────────────────────────────

JniPaimonCommitter::JniPaimonCommitter(const TPaimonTableSink& sink) : _sink(sink) {}

Status JniPaimonCommitter::_commit_impl(
        const std::vector<TPaimonCommitMessage>& messages, bool overwrite_mode,
        const std::map<std::string, std::string>* static_partition) {
    // The actual commit is performed by FE (PaimonTransaction).
    // BE only collects and forwards the CommitMessages.
    // The commit logic is implemented in FE because it requires
    // Paimon SDK classes (StreamTableCommit) and the coordinator role.
    //
    // This method exists for future use when we implement
    // commit-via-CommitBE pattern (see design doc Section 5.3, Option 3).
    return Status::NotSupported(
            "Commit is handled by FE Coordinator (PaimonTransaction); "
            "JniPaimonCommitter is reserved for future Commit-BE pattern");
}

Status JniPaimonCommitter::commit(const std::vector<TPaimonCommitMessage>& messages) {
    return _commit_impl(messages, false, nullptr);
}

Status JniPaimonCommitter::overwrite(const std::vector<TPaimonCommitMessage>& messages,
                                     const std::map<std::string, std::string>& static_partition) {
    return _commit_impl(messages, true, &static_partition);
}

Status JniPaimonCommitter::truncate_table() {
    return Status::NotSupported("truncate_table is handled by FE Coordinator");
}

Status JniPaimonCommitter::truncate_partitions(
        const std::vector<std::map<std::string, std::string>>& partitions) {
    return Status::NotSupported("truncate_partitions is handled by FE Coordinator");
}

Status JniPaimonCommitter::abort(const std::vector<TPaimonCommitMessage>& messages) {
    // Abort is best-effort: delete the data files.
    // In the current architecture, FE handles abort through PaimonTransaction.
    return Status::NotSupported("abort is handled by FE Coordinator");
}

} // namespace doris
