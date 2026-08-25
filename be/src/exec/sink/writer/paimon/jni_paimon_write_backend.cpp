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
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string_view>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"
#include "exec/spill/spill_file_manager.h"
#include "format/arrow/arrow_block_convertor.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/jni-util.h"
#include "util/pretty_printer.h"

namespace doris {

namespace {
constexpr std::string_view PAIMON_JNI_WRITER_IO_TMP_DIR = "paimon_jni_writer_io_tmp";

void throw_java_io_exception(JNIEnv* env, const std::string& message) {
    jclass exception_class = env->FindClass("java/io/IOException");
    env->ThrowNew(exception_class, message.c_str());
    env->DeleteLocalRef(exception_class);
}

jobjectArray get_paimon_spill_directories(JNIEnv* env, jclass, jlong spill_session_handle) {
    auto* spill_session = reinterpret_cast<ExternalSpillSession*>(spill_session_handle);
    if (spill_session == nullptr) {
        throw_java_io_exception(env, "Paimon external spill session is null");
        return nullptr;
    }

    std::vector<std::string> paths;
    Status st = spill_session->get_paths(&paths);
    if (!st.ok()) {
        throw_java_io_exception(env, st.to_string());
        return nullptr;
    }
    jclass string_class = env->FindClass("java/lang/String");
    if (string_class == nullptr) {
        return nullptr;
    }
    jobjectArray result =
            env->NewObjectArray(static_cast<jsize>(paths.size()), string_class, nullptr);
    env->DeleteLocalRef(string_class);
    if (result == nullptr) {
        return nullptr;
    }
    for (jsize i = 0; i < static_cast<jsize>(paths.size()); ++i) {
        jstring path = env->NewStringUTF(paths[i].c_str());
        if (path == nullptr) {
            return nullptr;
        }
        env->SetObjectArrayElement(result, i, path);
        env->DeleteLocalRef(path);
        if (env->ExceptionCheck()) {
            return nullptr;
        }
    }
    return result;
}

void reserve_paimon_spill(JNIEnv* env, jclass, jlong spill_session_handle, jstring path,
                          jlong bytes) {
    auto* spill_session = reinterpret_cast<ExternalSpillSession*>(spill_session_handle);
    if (spill_session == nullptr || path == nullptr) {
        throw_java_io_exception(env, "Paimon external spill session or path is null");
        return;
    }
    const char* path_chars = env->GetStringUTFChars(path, nullptr);
    if (path_chars == nullptr) {
        return;
    }
    std::string native_path(path_chars);
    env->ReleaseStringUTFChars(path, path_chars);
    Status st = spill_session->reserve(native_path, bytes);
    if (!st.ok()) {
        throw_java_io_exception(env, st.to_string());
    }
}

void update_paimon_spill_accounting(JNIEnv* env, jclass, jlong spill_session_handle, jstring path,
                                    jlong current_bytes_delta, jlong write_bytes,
                                    jlong read_bytes) {
    auto* spill_session = reinterpret_cast<ExternalSpillSession*>(spill_session_handle);
    if (spill_session == nullptr || path == nullptr) {
        return;
    }
    const char* path_chars = env->GetStringUTFChars(path, nullptr);
    if (path_chars == nullptr) {
        return;
    }
    std::string native_path(path_chars);
    env->ReleaseStringUTFChars(path, path_chars);
    spill_session->update_accounting(native_path, current_bytes_delta, write_bytes, read_bytes);
}

void reconcile_paimon_spill(JNIEnv* env, jclass, jlong spill_session_handle,
                            jboolean allow_release) {
    auto* spill_session = reinterpret_cast<ExternalSpillSession*>(spill_session_handle);
    if (spill_session == nullptr) {
        throw_java_io_exception(env, "Paimon external spill session is null");
        return;
    }
    Status st = spill_session->reconcile_direct_file_usage(allow_release == JNI_TRUE);
    if (!st.ok()) {
        throw_java_io_exception(env, st.to_string());
    }
}

Status register_paimon_spill_natives(JNIEnv* env, jclass writer_class) {
    static char get_spill_directories_name[] = "getPaimonSpillDirectories";
    static char get_spill_directories_signature[] = "(J)[Ljava/lang/String;";
    static char reserve_spill_name[] = "reservePaimonSpill";
    static char reserve_spill_signature[] = "(JLjava/lang/String;J)V";
    static char update_spill_name[] = "updatePaimonSpillAccounting";
    static char update_spill_signature[] = "(JLjava/lang/String;JJJ)V";
    static char reconcile_spill_name[] = "reconcilePaimonSpill";
    static char reconcile_spill_signature[] = "(JZ)V";
    static ::JNINativeMethod methods[] = {
            {get_spill_directories_name, get_spill_directories_signature,
             reinterpret_cast<void*>(&get_paimon_spill_directories)},
            {reserve_spill_name, reserve_spill_signature,
             reinterpret_cast<void*>(&reserve_paimon_spill)},
            {update_spill_name, update_spill_signature,
             reinterpret_cast<void*>(&update_paimon_spill_accounting)},
            {reconcile_spill_name, reconcile_spill_signature,
             reinterpret_cast<void*>(&reconcile_paimon_spill)},
    };
    if (env->RegisterNatives(writer_class, methods,
                             static_cast<jint>(sizeof(methods) / sizeof(methods[0]))) != JNI_OK) {
        RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
                env, true, "JNI exception registering Paimon spill native methods: "));
        return Status::JniError("Failed to register Paimon spill native methods");
    }
    return Status::OK();
}

std::atomic<bool>& paimon_jni_close_failed() {
    static std::atomic<bool> failed {false};
    return failed;
}

struct RetainedPaimonResources {
    std::unique_ptr<PaimonJniMemoryManager> memory_manager;
    std::unique_ptr<ExternalSpillSession> spill_session;
};

std::mutex& retained_resources_mutex() {
    static auto* mutex = new std::mutex();
    return *mutex;
}

std::vector<RetainedPaimonResources>& retained_resources() {
    static auto* resources = new std::vector<RetainedPaimonResources>();
    return *resources;
}

void retain_resources_after_failed_close(std::unique_ptr<PaimonJniMemoryManager> memory_manager,
                                         std::unique_ptr<ExternalSpillSession> spill_session) {
    // An unconfirmed Java close means a background Paimon task may still reference this manager's
    // native pages or spill callbacks. Quarantine both resources and stop admitting new writers so
    // repeated failures cannot accumulate process-lifetime resources without a bound.
    paimon_jni_close_failed().store(true, std::memory_order_release);
    if (memory_manager == nullptr && spill_session == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(retained_resources_mutex());
    retained_resources().emplace_back(RetainedPaimonResources {
            .memory_manager = std::move(memory_manager),
            .spill_session = std::move(spill_session),
    });
}

} // namespace

// ────────────────────────────────────────────────────────────
// JNI helpers — class loading
// ────────────────────────────────────────────────────────────

static constexpr const char* PAIMON_JNI_WRITER_CLASS = "org/apache/doris/paimon/PaimonJniWriter";
const char* const PAIMON_JNI_WRITER_OPEN_SIGNATURE =
        "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;JLjava/lang/String;ZZLjava/lang/"
        "String;JJJ)V";

PaimonJniWriterOpenMode PaimonJniWriterOpenMode::from_write_mode(
        TPaimonWriteMode::type write_mode) {
    return {static_cast<jboolean>(write_mode == TPaimonWriteMode::OVERWRITE),
            static_cast<jboolean>(write_mode == TPaimonWriteMode::CHANGELOG)};
}

JniPaimonWriteBackend::JniPaimonWriteBackend() = default;

JniPaimonWriteBackend::~JniPaimonWriteBackend() {
    Status st = close();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to close Paimon JNI backend during destruction: " << st.to_string();
    }
}

Status JniPaimonWriteBackend::close() {
    if (_jni_writer_obj == nullptr && _jni_writer_cls == nullptr) {
        _memory_manager.reset();
        _arrow_schema.reset();
        _spill_session.reset();
        _opened = false;
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    Status env_status = Jni::Env::Get(&env);
    if (!env_status.ok()) {
        bool java_users_may_exist = _jni_writer_obj != nullptr;
        // JNI global references cannot be released without an environment.
        // Deliberately abandon the handles so the Java writer remains alive.
        _jni_writer_obj = nullptr;
        _jni_writer_cls = nullptr;
        if (java_users_may_exist) {
            retain_resources_after_failed_close(std::move(_memory_manager),
                                                std::move(_spill_session));
        } else {
            _memory_manager.reset();
            _spill_session.reset();
        }
        _arrow_schema.reset();
        _opened = false;
        return env_status;
    }

    Status close_status = Status::OK();
    if (_jni_writer_obj != nullptr) {
        _refresh_memory_profile();
        if (_close_id == nullptr) {
            close_status = Status::InternalError("PaimonJniWriter.close method is unavailable");
        } else {
            env->CallVoidMethod(_jni_writer_obj, _close_id);
            close_status = _check_jni_exception(env, "close PaimonJniWriter");
        }
        env->DeleteGlobalRef(_jni_writer_obj);
        _jni_writer_obj = nullptr;
    }
    if (_jni_writer_cls != nullptr) {
        env->DeleteGlobalRef(_jni_writer_cls);
        _jni_writer_cls = nullptr;
    }

    if (close_status.ok()) {
        _memory_manager.reset();
        _spill_session.reset();
    } else {
        if (_memory_manager != nullptr) {
            LOG(WARNING)
                    << "Retaining Paimon JNI native memory after an unconfirmed Java close: limit="
                    << PrettyPrinter::print_bytes(_memory_manager->memory_limit()) << ", peak="
                    << PrettyPrinter::print_bytes(_memory_manager->native_peak_allocated_bytes());
        }
        // Paimon may still have asynchronous tasks using Doris-backed pages or spill callbacks.
        // Retain ownership until process exit and fence subsequent writer admission.
        retain_resources_after_failed_close(std::move(_memory_manager), std::move(_spill_session));
    }
    _arrow_schema.reset();
    _opened = false;
    return close_status;
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

static Status _get_paimon_arrow_schema(JNIEnv* env, jobject writer, jmethodID get_schema_id,
                                       std::shared_ptr<arrow::Schema>* schema) {
    auto schema_bytes = static_cast<jbyteArray>(env->CallObjectMethod(writer, get_schema_id));
    RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
            env, false, "JNI exception in PaimonJniWriter.getArrowSchema: "));
    if (schema_bytes == nullptr) {
        return Status::InternalError("PaimonJniWriter.getArrowSchema returned null");
    }

    const jsize size = env->GetArrayLength(schema_bytes);
    if (size <= 0) {
        env->DeleteLocalRef(schema_bytes);
        return Status::InternalError("PaimonJniWriter.getArrowSchema returned empty data");
    }
    std::string serialized_schema(static_cast<size_t>(size), '\0');
    env->GetByteArrayRegion(schema_bytes, 0, size,
                            reinterpret_cast<jbyte*>(serialized_schema.data()));
    env->DeleteLocalRef(schema_bytes);
    RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
            env, false, "JNI exception while reading Paimon Arrow schema: "));

    auto input = std::make_shared<arrow::io::BufferReader>(
            arrow::Buffer::FromString(std::move(serialized_schema)));
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    if (!reader_result.ok()) {
        return Status::InternalError("Failed to deserialize Paimon Arrow schema: {}",
                                     reader_result.status().ToString());
    }
    *schema = reader_result.ValueOrDie()->schema();
    return Status::OK();
}
Status JniPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                   RuntimeProfile* profile) {
    if (paimon_jni_close_failed().load(std::memory_order_acquire)) {
        return Status::InternalError(
                "Paimon JNI writes are disabled on this BE because a previous Java writer close "
                "could not be confirmed; restart the BE to reclaim retained native memory safely");
    }
    _arrow_schema.reset();
    DORIS_CHECK(sink.__isset.column_names);
    DORIS_CHECK(sink.__isset.write_mode);
    DORIS_CHECK(sink.__isset.serialized_table);
    DORIS_CHECK(!sink.serialized_table.empty());
    DORIS_CHECK(sink.__isset.transaction_id);
    DORIS_CHECK(sink.transaction_id > 0);
    DORIS_CHECK(sink.__isset.commit_user);
    DORIS_CHECK(!sink.commit_user.empty());
    DORIS_CHECK(profile != nullptr);

    RETURN_IF_ERROR(PaimonJniMemoryManager::create(state, &_memory_manager));
    RuntimeProfile* jni_profile = profile->create_child("JniPaimonWriteBackend", true, true);
    _native_page_memory_limit = ADD_COUNTER(jni_profile, "NativePageMemoryLimit", TUnit::BYTES);
    _native_page_memory_peak = ADD_COUNTER(jni_profile, "NativePageMemoryPeak", TUnit::BYTES);

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    if (env->PushLocalFrame(32) != JNI_OK) {
        Status st = _check_jni_exception(env, "create PaimonJniWriter open local reference frame");
        return st.ok() ? Status::InternalError("Failed to create JNI local reference frame") : st;
    }
    Defer pop_local_frame([&]() { env->PopLocalFrame(nullptr); });

    // Step 1: Load PaimonJniWriter class through ScannerLoader (Paimon jars are
    // not on the default application classpath, so FindClass won't work).
    Jni::LocalObject local_writer_class;
    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, PAIMON_JNI_WRITER_CLASS, &local_writer_class));
    auto writer_class = static_cast<jclass>(local_writer_class.get());
    _jni_writer_cls = static_cast<jclass>(env->NewGlobalRef(writer_class));
    RETURN_IF_ERROR(_check_jni_exception(env, "create global PaimonJniWriter class reference"));
    if (_jni_writer_cls == nullptr) {
        return Status::JniError("Failed to create global PaimonJniWriter class reference");
    }
    RETURN_IF_ERROR(PaimonJniMemoryManager::register_natives(env, _jni_writer_cls));
    RETURN_IF_ERROR(register_paimon_spill_natives(env, _jni_writer_cls));

    // Step 2: Cache JNI method IDs for write, prepareCommit, abort, close.
    jmethodID open_id = env->GetMethodID(_jni_writer_cls, "open", PAIMON_JNI_WRITER_OPEN_SIGNATURE);
    jmethodID get_arrow_schema_id = env->GetMethodID(_jni_writer_cls, "getArrowSchema", "()[B");
    _write_id = env->GetMethodID(_jni_writer_cls, "writeArrow", "(JJ)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _close_id = env->GetMethodID(_jni_writer_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "resolve PaimonJniWriter methods"));

    // Step 3: Create the Java PaimonJniWriter instance.
    jmethodID ctor_id = env->GetMethodID(_jni_writer_cls, "<init>", "()V");
    jobject local_obj = env->NewObject(_jni_writer_cls, ctor_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "create PaimonJniWriter"));
    _jni_writer_obj = env->NewGlobalRef(local_obj);
    RETURN_IF_ERROR(_check_jni_exception(env, "create global PaimonJniWriter object reference"));
    if (_jni_writer_obj == nullptr) {
        return Status::JniError("Failed to create global PaimonJniWriter object reference");
    }

    // Step 4: Create a lazy query-scoped spill session. Java requests its path only when Paimon
    // first uses the IOManager, so a memory-only writer does not depend on spill storage.
    auto* spill_file_manager = state->exec_env()->spill_file_mgr();
    if (spill_file_manager != nullptr) {
        auto spill_relative_path =
                fmt::format("{}-{}", PAIMON_JNI_WRITER_IO_TMP_DIR, spill_file_manager->next_id());
        RETURN_IF_ERROR(spill_file_manager->create_external_spill_session(
                spill_relative_path, state->get_query_ctx(), &_spill_session));
    }

    // Step 5: Build Java arguments and call PaimonJniWriter.open().
    const std::map<std::string, std::string> empty_config;
    jstring j_serialized_table = env->NewStringUTF(sink.serialized_table.c_str());
    Jni::LocalObject j_hadoop_config;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(
            env, sink.__isset.hadoop_config ? sink.hadoop_config : empty_config, &j_hadoop_config));
    jstring j_commit_user = env->NewStringUTF(sink.commit_user.c_str());
    jstring j_time_zone = env->NewStringUTF(state->timezone().c_str());

    jclass string_cls = env->FindClass("java/lang/String");
    jobjectArray j_cols =
            env->NewObjectArray(static_cast<jsize>(sink.column_names.size()), string_cls, nullptr);
    for (size_t i = 0; i < sink.column_names.size(); ++i) {
        jstring column_name = env->NewStringUTF(sink.column_names[i].c_str());
        env->SetObjectArrayElement(j_cols, static_cast<jsize>(i), column_name);
        env->DeleteLocalRef(column_name);
    }
    RETURN_IF_ERROR(_check_jni_exception(env, "build PaimonJniWriter open arguments"));

    PaimonJniWriterOpenMode open_mode = PaimonJniWriterOpenMode::from_write_mode(sink.write_mode);
    env->CallVoidMethod(
            _jni_writer_obj, open_id, j_serialized_table, j_hadoop_config.get(), j_cols,
            static_cast<jlong>(sink.transaction_id), j_commit_user, open_mode.overwrite,
            open_mode.changelog, j_time_zone, static_cast<jlong>(_memory_manager->memory_limit()),
            reinterpret_cast<jlong>(_memory_manager.get()),
            _spill_session == nullptr ? 0 : reinterpret_cast<jlong>(_spill_session.get()));
    Status st = _check_jni_exception(env, "open PaimonJniWriter");

    if (st.ok()) {
        st = _get_paimon_arrow_schema(env, _jni_writer_obj, get_arrow_schema_id, &_arrow_schema);
    }
    if (st.ok()) {
        _opened = true;
        _refresh_memory_profile();
        LOG(INFO) << "Paimon JNI writer memory limit: "
                  << PrettyPrinter::print_bytes(_memory_manager->memory_limit())
                  << ", sink_pipeline_task_count=" << std::max(1, state->task_num());
    }
    return st;
}

// Writer creation stays non-const because the backend interface also supports future stateful FFI
// implementations.
Status JniPaimonWriteBackend::create_writer( // NOLINT(readability-make-member-function-const)
        std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_opened);
    DORIS_CHECK(_arrow_schema != nullptr);
    *writer = std::make_unique<JniPaimonWriter>(_jni_writer_obj, _write_id, _prepare_commit_id,
                                                _abort_id, _arrow_schema);
    return Status::OK();
}

JniPaimonWriter::JniPaimonWriter(jobject jni_writer_obj, jmethodID write_id,
                                 jmethodID prepare_commit_id, jmethodID abort_id,
                                 std::shared_ptr<arrow::Schema> arrow_schema)
        : _jni_writer_obj(jni_writer_obj),
          _write_id(write_id),
          _prepare_commit_id(prepare_commit_id),
          _abort_id(abort_id),
          _arrow_schema(std::move(arrow_schema)) {}

Status JniPaimonWriter::write(RuntimeState* state, Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    if (_arrow_schema == nullptr || _arrow_schema->num_fields() != block.columns()) {
        return Status::InvalidArgument(
                "Paimon Arrow schema column count does not match Doris Block: schema={}, block={}",
                _arrow_schema == nullptr ? 0 : _arrow_schema->num_fields(), block.columns());
    }

    // The schema comes from the pinned Paimon table, so timestamp timezone, nested nullability and
    // Variant layout are fixed before the first write. Arrow builders remain on the Doris side and
    // are charged to the current query's MemTracker through ArrowMemoryPool.
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, _arrow_schema, &_arrow_pool, &record_batch,
                                           state->timezone_obj()));

    ArrowArray c_array {};
    ArrowSchema c_schema {};
    auto arrow_status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema);
    if (!arrow_status.ok()) {
        return Status::InternalError("Failed to export Paimon Arrow RecordBatch: {}",
                                     arrow_status.ToString());
    }
    // Java consumes both C Data release callbacks on a successful import. On every exit, release
    // whichever struct still retains its callback; this covers partial imports and JNI failures
    // without double release.
    Defer release_c_data {[&] {
        if (c_array.release != nullptr) {
            c_array.release(&c_array);
        }
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
    }};
    // writeArrow is synchronous and this operator runs on the blocking scheduler. The exported
    // RecordBatch therefore stays alive until Paimon has consumed all rows; Java never owns an IPC
    // copy, and any synchronous SDK flush or memory wait occupies only a blocking-scheduler worker.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    env->CallVoidMethod(_jni_writer_obj, _write_id, reinterpret_cast<jlong>(&c_array),
                        reinterpret_cast<jlong>(&c_schema));
    return Jni::Env::GetJniExceptionMsg(env, false,
                                        "JNI exception in JniPaimonWriter::writeArrow: ");
}

Status JniPaimonWriter::prepare_commit(std::vector<TPaimonCommitMessage>& messages) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

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
        TPaimonCommitMessage msg;
        msg.payload.resize(static_cast<size_t>(len));
        env->GetByteArrayRegion(j_bytes, 0, len, reinterpret_cast<jbyte*>(msg.payload.data()));
        Status copy_status = Jni::Env::GetJniExceptionMsg(
                env, false, "JNI exception while reading Paimon commit payload: ");
        if (!copy_status.ok()) {
            env->DeleteLocalRef(j_bytes);
            env->DeleteLocalRef(j_payloads);
            return copy_status;
        }
        msg.__isset.payload = true;
        messages.emplace_back(std::move(msg));
        env->DeleteLocalRef(j_bytes);
    }
    env->DeleteLocalRef(j_payloads);
    return Status::OK();
}

Status JniPaimonWriter::abort() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    env->CallVoidMethod(_jni_writer_obj, _abort_id);
    return Jni::Env::GetJniExceptionMsg(env, true, "JNI exception in abort: ");
}

void JniPaimonWriteBackend::_refresh_memory_profile() {
    if (_memory_manager == nullptr) {
        return;
    }
    COUNTER_SET(_native_page_memory_limit, _memory_manager->memory_limit());
    COUNTER_SET(_native_page_memory_peak, _memory_manager->native_peak_allocated_bytes());
}

} // namespace doris
