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

#include <map>
#include <string_view>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "util/string_util.h"

namespace doris {

namespace {
constexpr std::string_view PAIMON_JNI_WRITER_IO_TMP_DIR = "paimon_jni_writer_io_tmp";
} // namespace

// ────────────────────────────────────────────────────────────
// JNI helpers — JVM attachment and class loading
// ────────────────────────────────────────────────────────────

static constexpr const char* PAIMON_JNI_WRITER_CLASS = "org/apache/doris/paimon/PaimonJniWriter";
static constexpr const char* SCANNER_LOADER_CLASS =
        "org/apache/doris/common/classloader/ScannerLoader";

/// Attach the current native thread to the JVM if not already attached,
/// and return a valid JNIEnv pointer.
static Status _get_jni_env(JNIEnv** env) {
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

JniPaimonWriteBackend::~JniPaimonWriteBackend() {
    JNIEnv* env = nullptr;
    if (_get_jni_env(&env).ok()) {
        if (_jni_writer_obj != nullptr) {
            DCHECK(_close_id != nullptr);
            env->CallVoidMethod(_jni_writer_obj, _close_id);
            Status close_status = _check_jni_exception(env, "close PaimonJniWriter");
            if (!close_status.ok()) {
                LOG(WARNING) << "Failed to close PaimonJniWriter: " << close_status.to_string();
            }
            env->DeleteGlobalRef(_jni_writer_obj);
            _jni_writer_obj = nullptr;
        }
        if (_jni_writer_cls != nullptr) {
            env->DeleteGlobalRef(_jni_writer_cls);
            _jni_writer_cls = nullptr;
        }
    }
    _opened = false;
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

Status JniPaimonWriteBackend::_load_writer_class(JNIEnv* env, jclass* writer_class) {
    jclass loader_class = env->FindClass(SCANNER_LOADER_CLASS);
    RETURN_IF_ERROR(_check_jni_exception(env, "find ScannerLoader"));

    jmethodID loader_constructor = env->GetMethodID(loader_class, "<init>", "()V");
    jmethodID get_loaded_class = env->GetMethodID(loader_class, "getLoadedClass",
                                                  "(Ljava/lang/String;)Ljava/lang/Class;");
    RETURN_IF_ERROR(_check_jni_exception(env, "resolve ScannerLoader methods"));

    jobject loader = env->NewObject(loader_class, loader_constructor);
    jstring class_name = env->NewStringUTF(PAIMON_JNI_WRITER_CLASS);
    auto* loaded_class =
            static_cast<jclass>(env->CallObjectMethod(loader, get_loaded_class, class_name));
    RETURN_IF_ERROR(_check_jni_exception(env, "load PaimonJniWriter"));

    *writer_class = loaded_class;
    env->DeleteLocalRef(class_name);
    env->DeleteLocalRef(loader);
    env->DeleteLocalRef(loader_class);
    return Status::OK();
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
    DORIS_CHECK(sink.__isset.column_names);
    DORIS_CHECK(sink.__isset.write_mode);
    DORIS_CHECK(sink.__isset.serialized_table);
    DORIS_CHECK(!sink.serialized_table.empty());
    DORIS_CHECK(sink.__isset.transaction_id);
    DORIS_CHECK(sink.transaction_id > 0);
    DORIS_CHECK(sink.__isset.commit_user);
    DORIS_CHECK(!sink.commit_user.empty());

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    // Step 1: Load PaimonJniWriter class through ScannerLoader (Paimon jars are
    // not on the default application classpath, so FindClass won't work).
    jclass local_cls = nullptr;
    RETURN_IF_ERROR(_load_writer_class(env, &local_cls));
    _jni_writer_cls = static_cast<jclass>(env->NewGlobalRef(local_cls));
    env->DeleteLocalRef(local_cls);

    // Step 2: Cache JNI method IDs for write, prepareCommit, abort, close.
    jmethodID open_id = env->GetMethodID(
            _jni_writer_cls, "open",
            "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;JLjava/lang/String;ZLjava/lang/"
            "String;Ljava/lang/String;)V");
    _write_id = env->GetMethodID(_jni_writer_cls, "write", "(Ljava/nio/ByteBuffer;)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _close_id = env->GetMethodID(_jni_writer_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "GetMethodID"));

    // Step 3: Create the Java PaimonJniWriter instance.
    jmethodID ctor_id = env->GetMethodID(_jni_writer_cls, "<init>", "()V");
    jobject local_obj = env->NewObject(_jni_writer_cls, ctor_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "NewObject"));
    _jni_writer_obj = env->NewGlobalRef(local_obj);
    env->DeleteLocalRef(local_obj);

    // Step 4: Build Java arguments and call PaimonJniWriter.open().
    const std::map<std::string, std::string> empty_config;
    jstring j_serialized_table = env->NewStringUTF(sink.serialized_table.c_str());
    jobject j_hadoop_config =
            _to_java_options(env, sink.__isset.hadoop_config ? sink.hadoop_config : empty_config);
    jstring j_commit_user = env->NewStringUTF(sink.commit_user.c_str());
    jstring j_time_zone = env->NewStringUTF(state->timezone().c_str());
    std::vector<std::string> spill_directories;
    for (const auto& store_path : state->exec_env()->store_paths()) {
        spill_directories.push_back(store_path.path + "/" +
                                    std::string(PAIMON_JNI_WRITER_IO_TMP_DIR));
    }
    DORIS_CHECK(!spill_directories.empty());
    jstring j_spill_directories = env->NewStringUTF(join(spill_directories, ":").c_str());

    jclass string_cls = env->FindClass("java/lang/String");
    jobjectArray j_cols =
            env->NewObjectArray(static_cast<jsize>(sink.column_names.size()), string_cls, nullptr);
    for (size_t i = 0; i < sink.column_names.size(); ++i) {
        jstring str = env->NewStringUTF(sink.column_names[i].c_str());
        env->SetObjectArrayElement(j_cols, static_cast<jsize>(i), str);
        env->DeleteLocalRef(str);
    }

    env->CallVoidMethod(_jni_writer_obj, open_id, j_serialized_table, j_hadoop_config, j_cols,
                        static_cast<jlong>(sink.transaction_id), j_commit_user,
                        static_cast<jboolean>(sink.write_mode == TPaimonWriteMode::OVERWRITE),
                        j_time_zone, j_spill_directories);
    Status st = _check_jni_exception(env, "open");

    env->DeleteLocalRef(j_serialized_table);
    env->DeleteLocalRef(j_hadoop_config);
    env->DeleteLocalRef(j_commit_user);
    env->DeleteLocalRef(j_time_zone);
    env->DeleteLocalRef(j_spill_directories);
    env->DeleteLocalRef(j_cols);
    env->DeleteLocalRef(string_cls);

    if (st.ok()) {
        _opened = true;
    }
    return st;
}

// Writer creation stays non-const because the backend interface also supports future stateful FFI
// implementations.
Status JniPaimonWriteBackend::create_writer( // NOLINT(readability-make-member-function-const)
        std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_opened);
    *writer = std::make_unique<JniPaimonWriter>(_jni_writer_obj, _write_id, _prepare_commit_id,
                                                _abort_id, std::make_unique<ArrowMemoryPool<>>(),
                                                _sink);
    return Status::OK();
}

JniPaimonWriter::JniPaimonWriter(jobject jni_writer_obj, jmethodID write_id,
                                 jmethodID prepare_commit_id, jmethodID abort_id,
                                 std::unique_ptr<ArrowMemoryPool<>> arrow_pool,
                                 TPaimonTableSink sink)
        : _jni_writer_obj(jni_writer_obj),
          _write_id(write_id),
          _prepare_commit_id(prepare_commit_id),
          _abort_id(abort_id),
          _arrow_pool(std::move(arrow_pool)),
          _sink(std::move(sink)) {}

Status JniPaimonWriter::_write_projected_block(RuntimeState* state, Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Use Thrift column_names as the authoritative schema source for both
    // Arrow schema construction and Java-side write type derivation.
    DORIS_CHECK(_sink.__isset.column_names);
    DORIS_CHECK_EQ(_sink.column_names.size(), block.columns());
    for (size_t i = 0; i < _sink.column_names.size(); ++i) {
        block.get_by_position(i).name = _sink.column_names[i];
    }

    // Pipeline: Doris Block → Arrow Schema → Arrow RecordBatch → IPC Stream → JNI direct buffer
    //
    // Step 1: Build Arrow schema from the projected Block.
    // Paimon write timestamps are transported as civil-time fields. The Java writer uses the
    // pinned Paimon target type to preserve NTZ values or convert LTZ values with the session zone.
    std::shared_ptr<arrow::Schema> arrow_schema;
    RETURN_IF_ERROR(get_arrow_schema_from_block(block, &arrow_schema, ""));

    // Step 2: Convert Doris Block columns to an Arrow RecordBatch.
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow_schema, _arrow_pool.get(), &record_batch,
                                           state->timezone_obj()));

    // Step 3: Serialize the RecordBatch to Arrow IPC Stream format in memory.
    auto out_stream_res = arrow::io::BufferOutputStream::Create(4096, _arrow_pool.get());
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

    // Step 4: Wrap the IPC buffer in a JNI direct ByteBuffer (zero-copy) and
    // call PaimonJniWriter.write(ByteBuffer). Java side reads the Arrow IPC
    // stream via ArrowStreamReader.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    jobject direct_buffer =
            env->NewDirectByteBuffer(buffer->mutable_data(), static_cast<jlong>(buffer->size()));
    RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
            env, false, "JNI exception in NewDirectByteBuffer for PaimonJniWriter::write: "));

    env->CallVoidMethod(_jni_writer_obj, _write_id, direct_buffer);
    Status write_status =
            Jni::Env::GetJniExceptionMsg(env, false, "JNI exception in JniPaimonWriter::write: ");
    env->DeleteLocalRef(direct_buffer);
    return write_status;
}

Status JniPaimonWriter::write(RuntimeState* state, Block& block) {
    return _write_projected_block(state, block);
}

Status JniPaimonWriter::prepare_commit(std::vector<TPaimonCommitMessage>& messages) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    // Call PaimonJniWriter.prepareCommit() which returns byte[][] —
    // each element is a DPCM-framed serialized CommitMessage chunk produced
    // by PaimonCommitCodec.encode().
    jobject j_payloads_obj = env->CallObjectMethod(_jni_writer_obj, _prepare_commit_id);
    Status st = Jni::Env::GetJniExceptionMsg(env, false, "JNI exception in prepareCommit: ");
    if (!st.ok()) {
        return st;
    }

    if (j_payloads_obj == nullptr) {
        return Status::InternalError("PaimonJniWriter.prepareCommit returned null");
    }

    // Unpack the byte[][] into TPaimonCommitMessage structs for FE transport.
    auto* j_payloads = static_cast<jobjectArray>(j_payloads_obj);
    jsize num_payloads = env->GetArrayLength(j_payloads);

    for (jsize i = 0; i < num_payloads; ++i) {
        auto j_bytes = static_cast<jbyteArray>(env->GetObjectArrayElement(j_payloads, i));
        if (j_bytes == nullptr) {
            env->DeleteLocalRef(j_payloads);
            return Status::InternalError("PaimonJniWriter.prepareCommit returned a null payload");
        }
        jsize len = env->GetArrayLength(j_bytes);
        if (len == 0) {
            env->DeleteLocalRef(j_bytes);
            env->DeleteLocalRef(j_payloads);
            return Status::InternalError("PaimonJniWriter.prepareCommit returned an empty payload");
        }
        jbyte* bytes = env->GetByteArrayElements(j_bytes, nullptr);
        if (bytes == nullptr) {
            env->DeleteLocalRef(j_bytes);
            env->DeleteLocalRef(j_payloads);
            RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
                    env, false, "JNI exception while reading Paimon commit payload: "));
            return Status::InternalError("Failed to read Paimon commit payload");
        }
        std::string payload(reinterpret_cast<char*>(bytes), static_cast<size_t>(len));
        TPaimonCommitMessage msg;
        msg.__set_payload(payload);
        messages.emplace_back(std::move(msg));
        env->ReleaseByteArrayElements(j_bytes, bytes, JNI_ABORT);
        env->DeleteLocalRef(j_bytes);
    }
    env->DeleteLocalRef(j_payloads);
    return Status::OK();
}

Status JniPaimonWriter::abort() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));
    env->CallVoidMethod(_jni_writer_obj, _abort_id);
    return Jni::Env::GetJniExceptionMsg(env, true, "JNI exception in abort: ");
}

} // namespace doris
