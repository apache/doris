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

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string_view>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"
#include "format/arrow/arrow_block_convertor.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/jni-util.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"

namespace doris {

namespace {
constexpr std::string_view PAIMON_JNI_WRITER_IO_TMP_DIR = "paimon_jni_writer_io_tmp";

std::atomic<bool>& paimon_jni_close_failed() {
    static std::atomic<bool> failed {false};
    return failed;
}

std::mutex& retained_memory_managers_mutex() {
    static auto* mutex = new std::mutex();
    return *mutex;
}

std::vector<std::unique_ptr<PaimonJniMemoryManager>>& retained_memory_managers() {
    static auto* managers = new std::vector<std::unique_ptr<PaimonJniMemoryManager>>();
    return *managers;
}

void retain_memory_after_failed_close(std::unique_ptr<PaimonJniMemoryManager> manager) {
    // An unconfirmed Java close means a background Paimon task may still reference this manager's
    // native pages. Quarantine the manager and stop admitting new writers so repeated failures
    // cannot accumulate process-lifetime native memory without a bound.
    paimon_jni_close_failed().store(true, std::memory_order_release);
    if (manager == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(retained_memory_managers_mutex());
    retained_memory_managers().emplace_back(std::move(manager));
}

} // namespace

// ────────────────────────────────────────────────────────────
// JNI helpers — class loading
// ────────────────────────────────────────────────────────────

static constexpr const char* PAIMON_JNI_WRITER_CLASS = "org/apache/doris/paimon/PaimonJniWriter";
static constexpr const char* SCANNER_LOADER_CLASS =
        "org/apache/doris/common/classloader/ScannerLoader";

const char* const PAIMON_JNI_WRITER_OPEN_SIGNATURE =
        "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;JLjava/lang/String;ZZLjava/lang/"
        "String;Ljava/lang/String;JJ)V";

PaimonJniWriterOpenMode PaimonJniWriterOpenMode::from_write_mode(
        TPaimonWriteMode::type write_mode) {
    return {static_cast<jboolean>(write_mode == TPaimonWriteMode::OVERWRITE),
            static_cast<jboolean>(write_mode == TPaimonWriteMode::CHANGELOG)};
}

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
            retain_memory_after_failed_close(std::move(_memory_manager));
        } else {
            _memory_manager.reset();
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
    } else {
        if (_memory_manager != nullptr) {
            LOG(WARNING)
                    << "Retaining Paimon JNI native memory after an unconfirmed Java close: limit="
                    << PrettyPrinter::print_bytes(_memory_manager->memory_limit()) << ", peak="
                    << PrettyPrinter::print_bytes(_memory_manager->native_peak_allocated_bytes());
        }
        // Paimon may still have asynchronous flush or compaction tasks using MemorySegments backed
        // by these pages. Retain this failed writer's ownership until process exit to prevent UAF;
        // retain_memory_after_failed_close also fences subsequent Paimon JNI writer admission.
        retain_memory_after_failed_close(std::move(_memory_manager));
    }
    _arrow_schema.reset();
    _opened = false;
    return close_status;
}

Status JniPaimonWriteBackend::prepare_close_for_commit() {
    JNIEnv* env = nullptr;
    // Use the shared JNI attachment path so this method follows the branch-wide environment
    // lifetime invariant instead of relying on a backend helper that does not exist here.
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    if (_jni_writer_obj == nullptr || _prepare_close_for_commit_id == nullptr) {
        return Status::InternalError("Paimon prepared writer close method is unavailable");
    }
    env->CallVoidMethod(_jni_writer_obj, _prepare_close_for_commit_id);
    Status status = _check_jni_exception(env, "prepare-close PaimonJniWriter");
    _refresh_memory_profile();
    return status;
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

    // Step 1: Load PaimonJniWriter class through ScannerLoader (Paimon jars are
    // not on the default application classpath, so FindClass won't work).
    jclass local_cls = nullptr;
    RETURN_IF_ERROR(_load_writer_class(env, &local_cls));
    _jni_writer_cls = static_cast<jclass>(env->NewGlobalRef(local_cls));
    env->DeleteLocalRef(local_cls);
    RETURN_IF_ERROR(PaimonJniMemoryManager::register_natives(env, _jni_writer_cls));

    // Step 2: Cache JNI method IDs for write, prepareCommit, abort, close.
    jmethodID open_id = env->GetMethodID(_jni_writer_cls, "open", PAIMON_JNI_WRITER_OPEN_SIGNATURE);
    jmethodID get_arrow_schema_id = env->GetMethodID(_jni_writer_cls, "getArrowSchema", "()[B");
    _write_id = env->GetMethodID(_jni_writer_cls, "writeArrow", "(JJ)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _prepare_close_for_commit_id =
            env->GetMethodID(_jni_writer_cls, "prepareCloseForCommit", "()V");
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

    PaimonJniWriterOpenMode open_mode = PaimonJniWriterOpenMode::from_write_mode(sink.write_mode);
    env->CallVoidMethod(_jni_writer_obj, open_id, j_serialized_table, j_hadoop_config, j_cols,
                        static_cast<jlong>(sink.transaction_id), j_commit_user, open_mode.overwrite,
                        open_mode.changelog, j_time_zone, j_spill_directories,
                        static_cast<jlong>(_memory_manager->memory_limit()),
                        reinterpret_cast<jlong>(_memory_manager.get()));
    Status st = _check_jni_exception(env, "open");

    env->DeleteLocalRef(j_serialized_table);
    env->DeleteLocalRef(j_hadoop_config);
    env->DeleteLocalRef(j_commit_user);
    env->DeleteLocalRef(j_time_zone);
    env->DeleteLocalRef(j_spill_directories);
    env->DeleteLocalRef(j_cols);
    env->DeleteLocalRef(string_cls);

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
