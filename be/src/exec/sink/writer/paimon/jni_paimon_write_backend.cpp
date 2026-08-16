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

#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/block_budget.h"
#include "util/jni-util.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"

namespace doris {

namespace {
constexpr std::string_view PAIMON_JNI_WRITER_IO_TMP_DIR = "paimon_jni_writer_io_tmp";

std::atomic<bool>& paimon_jni_close_failed() {
    static auto* failed = new std::atomic<bool>(false);
    return *failed;
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
    paimon_jni_close_failed().store(true, std::memory_order_release);
    if (manager == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(retained_memory_managers_mutex());
    retained_memory_managers().emplace_back(std::move(manager));
}

Status convert_to_paimon_arrow_type(const DataTypePtr& origin_type,
                                    std::shared_ptr<arrow::DataType>* result,
                                    const std::string& timezone) {
    const DataTypePtr type = get_serialized_type(origin_type);
    switch (type->get_primitive_type()) {
    case TYPE_VARIANT:
        // Paimon consumes the lossless Variant V2 representation. Keeping both children non-null
        // distinguishes a SQL NULL struct from a non-null Variant value.
        *result = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                  arrow::field("metadata", arrow::binary(), false)});
        return Status::OK();
    case TYPE_ARRAY: {
        const auto& array_type = assert_cast<const DataTypeArray&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> element_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(array_type.get_nested_type(), &element_type,
                                                     timezone));
        *result = std::make_shared<arrow::ListType>(element_type);
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> value_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(map_type.get_key_type(), &key_type, timezone));
        RETURN_IF_ERROR(
                convert_to_paimon_arrow_type(map_type.get_value_type(), &value_type, timezone));
        *result = std::make_shared<arrow::MapType>(key_type, value_type);
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*remove_nullable(type));
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(struct_type.get_elements().size());
        for (size_t i = 0; i < struct_type.get_elements().size(); ++i) {
            const DataTypePtr& element = struct_type.get_element(i);
            std::shared_ptr<arrow::DataType> field_type;
            RETURN_IF_ERROR(convert_to_paimon_arrow_type(element, &field_type, timezone));
            fields.push_back(arrow::field(struct_type.get_element_name(i), field_type,
                                          element->is_nullable()));
        }
        *result = arrow::struct_(std::move(fields));
        return Status::OK();
    }
    default:
        return convert_to_arrow_type(origin_type, result, timezone);
    }
}

Status get_paimon_arrow_schema_from_block(const Block& block,
                                          std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(block.columns());
    for (const auto& type_and_name : block) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(type_and_name.type, &arrow_type, ""));
        fields.push_back(create_arrow_field_with_metadata(
                type_and_name.name, arrow_type, type_and_name.type->is_nullable(),
                type_and_name.type->get_primitive_type()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
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

enum class PaimonMemoryErrorType : jint {
    // Keep these values in sync with PaimonJniWriter.MEMORY_ERROR_*.
    NONE = 0,
    ARROW = 1,
    PAIMON_PAGE = 2,
    JVM_HEAP = 3,
    COMMIT_PAYLOAD = 4,
};

Status translate_paimon_memory_error(JNIEnv* env, jobject writer, jmethodID consume_memory_error_id,
                                     Status status, RuntimeProfile::Counter* arrow_memory_exceeded,
                                     RuntimeProfile::Counter* paimon_page_memory_exceeded,
                                     RuntimeProfile::Counter* jvm_heap_memory_exceeded,
                                     RuntimeProfile::Counter* commit_payload_memory_exceeded) {
    if (status.ok() || writer == nullptr || consume_memory_error_id == nullptr) {
        return status;
    }

    auto error_type =
            static_cast<PaimonMemoryErrorType>(env->CallIntMethod(writer, consume_memory_error_id));
    Status consume_status =
            Jni::Env::GetJniExceptionMsg(env, false, "read Paimon memory failure type: ");
    if (!consume_status.ok()) {
        LOG(WARNING) << consume_status.to_string();
        return status;
    }

    const char* source = nullptr;
    RuntimeProfile::Counter* counter = nullptr;
    switch (error_type) {
    case PaimonMemoryErrorType::ARROW:
        source = "Arrow decode";
        counter = arrow_memory_exceeded;
        break;
    case PaimonMemoryErrorType::PAIMON_PAGE:
        source = "Paimon native page";
        counter = paimon_page_memory_exceeded;
        break;
    case PaimonMemoryErrorType::JVM_HEAP:
        source = "JVM heap";
        counter = jvm_heap_memory_exceeded;
        break;
    case PaimonMemoryErrorType::COMMIT_PAYLOAD:
        source = "commit payload";
        counter = commit_payload_memory_exceeded;
        break;
    case PaimonMemoryErrorType::NONE:
        return status;
    default:
        LOG(WARNING) << "Unknown Paimon memory failure type: " << static_cast<jint>(error_type);
        return status;
    }

    if (counter != nullptr) {
        COUNTER_UPDATE(counter, 1);
    }
    return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
            "Paimon JNI {} memory failure; canceling the current query: {}", source,
            status.to_string());
}

Status convert_cpp_arrow_status(const arrow::Status& arrow_status, std::string_view operation,
                                RuntimeProfile::Counter* memory_error_counter) {
    if (arrow_status.IsOutOfMemory()) {
        COUNTER_UPDATE(memory_error_counter, 1);
        return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                "Paimon C++ Arrow {} ran out of query memory: {}", operation,
                arrow_status.ToString());
    }
    return Status::InternalError("Paimon C++ Arrow {} failed: {}", operation,
                                 arrow_status.ToString());
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
        _opened = false;
        return env_status;
    }

    Status close_status = Status::OK();
    if (_jni_writer_obj != nullptr) {
        _refresh_memory_profile(env);
        if (_close_id == nullptr) {
            close_status = Status::InternalError("PaimonJniWriter.close method is unavailable");
        } else {
            env->CallVoidMethod(_jni_writer_obj, _close_id);
            close_status = _check_jni_exception(env, "close PaimonJniWriter");
            if (close_status.ok()) {
                _refresh_memory_profile(env);
            }
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
        // Paimon may still have asynchronous flush or compaction tasks using
        // MemorySegments backed by these pages. Retain ownership until process
        // exit and reject new writers below. Retention is therefore limited to
        // writers which were already open when the first close failure occurred.
        retain_memory_after_failed_close(std::move(_memory_manager));
    }
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

Status JniPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                   RuntimeProfile* profile) {
    if (paimon_jni_close_failed().load(std::memory_order_acquire)) {
        return Status::InternalError(
                "Paimon JNI writes are disabled on this BE because a previous Java writer close "
                "failed; restart the BE to reclaim retained native memory safely");
    }
    _sink = sink;
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
    _jni_profile = jni_profile;
    // This is the managed envelope shared by the Paimon page pool and Java Arrow allocator.
    // C++ Arrow conversion buffers and other Paimon/JVM object memory are accounted separately.
    _writer_memory_limit = ADD_COUNTER(jni_profile, "ManagedWriterMemoryLimit", TUnit::BYTES);
    _native_page_memory_limit = ADD_COUNTER(jni_profile, "NativePageMemoryLimit", TUnit::BYTES);
    _native_page_memory_peak = ADD_COUNTER(jni_profile, "NativePageMemoryPeak", TUnit::BYTES);
    _arrow_memory_limit = ADD_COUNTER(jni_profile, "ArrowMemoryLimit", TUnit::BYTES);
    _arrow_memory_current = ADD_COUNTER(jni_profile, "ArrowMemoryCurrent", TUnit::BYTES);
    _arrow_memory_peak = ADD_COUNTER(jni_profile, "ArrowMemoryPeak", TUnit::BYTES);
    _arrow_memory_exceeded = ADD_COUNTER(jni_profile, "ArrowMemoryLimitExceededCount", TUnit::UNIT);
    _paimon_page_memory_exceeded =
            ADD_COUNTER(jni_profile, "PaimonPageMemoryErrorCount", TUnit::UNIT);
    _jvm_heap_memory_exceeded = ADD_COUNTER(jni_profile, "JvmHeapMemoryErrorCount", TUnit::UNIT);
    _commit_payload_memory_exceeded =
            ADD_COUNTER(jni_profile, "CommitPayloadMemoryLimitExceededCount", TUnit::UNIT);

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
    _write_id = env->GetMethodID(_jni_writer_cls, "write", "(Ljava/nio/ByteBuffer;)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _close_id = env->GetMethodID(_jni_writer_cls, "close", "()V");
    _get_arrow_memory_current_id =
            env->GetMethodID(_jni_writer_cls, "getArrowMemoryCurrentBytes", "()J");
    _get_arrow_memory_peak_id = env->GetMethodID(_jni_writer_cls, "getArrowMemoryPeakBytes", "()J");
    _get_arrow_memory_limit_id =
            env->GetMethodID(_jni_writer_cls, "getArrowMemoryLimitBytes", "()J");
    _get_paimon_page_memory_limit_id =
            env->GetMethodID(_jni_writer_cls, "getPaimonPageMemoryLimitBytes", "()J");
    _consume_last_memory_error_id =
            env->GetMethodID(_jni_writer_cls, "consumeLastMemoryErrorType", "()I");
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

    if (!st.ok()) {
        return translate_paimon_memory_error(
                env, _jni_writer_obj, _consume_last_memory_error_id, std::move(st),
                _arrow_memory_exceeded, _paimon_page_memory_exceeded, _jvm_heap_memory_exceeded,
                _commit_payload_memory_exceeded);
    }
    RETURN_IF_ERROR(_load_writer_memory_limits(env));
    _opened = true;
    _refresh_memory_profile(env);
    LOG(INFO) << "Paimon JNI writer memory limit: "
              << PrettyPrinter::print_bytes(_memory_manager->memory_limit())
              << ", local_sink_count=" << std::max(1, state->num_local_sink());
    return Status::OK();
}

// Writer creation stays non-const because the backend interface also supports future stateful FFI
// implementations.
Status JniPaimonWriteBackend::create_writer( // NOLINT(readability-make-member-function-const)
        std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_opened);
    if (_arrow_memory_limit_bytes <= 0) {
        return Status::InternalError("Paimon JNI writer cannot determine its Arrow memory limit");
    }
    *writer = std::make_unique<JniPaimonWriter>(_jni_writer_obj, _write_id, _prepare_commit_id,
                                                _abort_id, _consume_last_memory_error_id,
                                                std::make_unique<ArrowMemoryPool<>>(), _sink,
                                                _arrow_memory_limit_bytes, _jni_profile);
    return Status::OK();
}

JniPaimonWriter::JniPaimonWriter(jobject jni_writer_obj, jmethodID write_id,
                                 jmethodID prepare_commit_id, jmethodID abort_id,
                                 jmethodID consume_last_memory_error_id,
                                 std::unique_ptr<ArrowMemoryPool<>> arrow_pool,
                                 TPaimonTableSink sink, int64_t arrow_memory_limit_bytes,
                                 RuntimeProfile* profile)
        : _jni_writer_obj(jni_writer_obj),
          _write_id(write_id),
          _prepare_commit_id(prepare_commit_id),
          _abort_id(abort_id),
          _consume_last_memory_error_id(consume_last_memory_error_id),
          _arrow_pool(std::move(arrow_pool)),
          _sink(std::move(sink)),
          _arrow_memory_limit_bytes(arrow_memory_limit_bytes) {
    DORIS_CHECK_GT(_arrow_memory_limit_bytes, 0);
    DORIS_CHECK(profile != nullptr);
    _cpp_arrow_memory_peak = ADD_COUNTER(profile, "CppArrowMemoryPeak", TUnit::BYTES);
    _arrow_ipc_batch_count = ADD_COUNTER(profile, "ArrowIpcBatchCount", TUnit::UNIT);
    _arrow_ipc_bytes = ADD_COUNTER(profile, "ArrowIpcBytes", TUnit::BYTES);
    _arrow_ipc_batch_bytes_peak = ADD_COUNTER(profile, "ArrowIpcBatchBytesPeak", TUnit::BYTES);
    _arrow_batch_rows_peak = ADD_COUNTER(profile, "ArrowBatchRowsPeak", TUnit::UNIT);
    _cpp_arrow_memory_error = ADD_COUNTER(profile, "CppArrowMemoryErrorCount", TUnit::UNIT);
    _arrow_memory_exceeded = ADD_COUNTER(profile, "ArrowMemoryLimitExceededCount", TUnit::UNIT);
    _paimon_page_memory_exceeded = ADD_COUNTER(profile, "PaimonPageMemoryErrorCount", TUnit::UNIT);
    _jvm_heap_memory_exceeded = ADD_COUNTER(profile, "JvmHeapMemoryErrorCount", TUnit::UNIT);
    _commit_payload_memory_exceeded =
            ADD_COUNTER(profile, "CommitPayloadMemoryLimitExceededCount", TUnit::UNIT);
}

Status JniPaimonWriter::_write_projected_block(RuntimeState* state, Block& block) {
    const size_t block_rows = block.rows();
    if (block_rows == 0) {
        return Status::OK();
    }

    // Use Thrift column_names as the authoritative schema source for both
    // Arrow schema construction and Java-side write type derivation.
    DORIS_CHECK(_sink.__isset.column_names);
    DORIS_CHECK_EQ(_sink.column_names.size(), block.columns());
    for (size_t i = 0; i < _sink.column_names.size(); ++i) {
        block.get_by_position(i).name = _sink.column_names[i];
    }

    // Build the schema once, then convert row ranges independently. Each range owns its
    // RecordBatch and IPC buffer, so those transient allocations are released before the next
    // range is converted.
    // Paimon write timestamps are transported as civil-time fields. The Java writer uses the
    // pinned Paimon target type to preserve NTZ values or convert LTZ values with the session zone.
    // Variant V2 is transported losslessly as its value/metadata pair, including nested Variant.
    std::shared_ptr<arrow::Schema> arrow_schema;
    RETURN_IF_ERROR(get_paimon_arrow_schema_from_block(block, &arrow_schema));

    // This is a best-effort batch-size target, not a hard native-memory limit. The finite Java
    // allocator remains the hard decode boundary. Half of its limit leaves room for Arrow offsets,
    // validity buffers, allocator rounding and representation expansion during decode.
    const size_t arrow_batch_memory_budget =
            std::max<size_t>(1, static_cast<size_t>(_arrow_memory_limit_bytes) / 2);
    const size_t target_batch_bytes =
            std::min(state->preferred_block_size_bytes(), arrow_batch_memory_budget);
    const size_t block_bytes = block.bytes();
    const size_t average_row_bytes =
            std::max<size_t>(1, block_bytes / block_rows + (block_bytes % block_rows != 0));
    const BlockBudget batch_budget(static_cast<size_t>(state->batch_size()), target_batch_bytes);
    const size_t rows_per_batch = batch_budget.effective_max_rows(average_row_bytes);

    for (size_t start_row = 0; start_row < block_rows;) {
        const size_t range_rows = std::min(rows_per_batch, block_rows - start_row);
        const size_t end_row = start_row + range_rows;
        const size_t estimated_ipc_bytes = average_row_bytes > target_batch_bytes / range_rows
                                                   ? target_batch_bytes
                                                   : average_row_bytes * range_rows;
        RETURN_IF_ERROR(_write_row_range(state, block, arrow_schema, start_row, end_row,
                                         estimated_ipc_bytes));
        start_row = end_row;
    }
    return Status::OK();
}

Status JniPaimonWriter::_write_row_range(RuntimeState* state, const Block& block,
                                         const std::shared_ptr<arrow::Schema>& arrow_schema,
                                         size_t start_row, size_t end_row,
                                         size_t estimated_ipc_bytes) {
    std::shared_ptr<arrow::RecordBatch> record_batch;
    RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow_schema, _arrow_pool.get(), &record_batch,
                                           state->timezone_obj(), start_row, end_row));

    // Preallocate from the row-range estimate instead of repeatedly growing from 4KB. Arrow can
    // still expand the stream when variable-length or nested encodings exceed the estimate.
    constexpr size_t MIN_IPC_CAPACITY = 4096;
    const int64_t initial_ipc_capacity =
            static_cast<int64_t>(std::max(MIN_IPC_CAPACITY, estimated_ipc_bytes));
    auto out_stream_res =
            arrow::io::BufferOutputStream::Create(initial_ipc_capacity, _arrow_pool.get());
    if (!out_stream_res.ok()) {
        return convert_cpp_arrow_status(out_stream_res.status(), "BufferOutputStream creation",
                                        _cpp_arrow_memory_error);
    }
    auto out_stream = *out_stream_res;

    auto writer_res = arrow::ipc::MakeStreamWriter(out_stream, arrow_schema);
    if (!writer_res.ok()) {
        return convert_cpp_arrow_status(writer_res.status(), "StreamWriter creation",
                                        _cpp_arrow_memory_error);
    }
    auto ipc_writer = *writer_res;
    auto arrow_write_status = ipc_writer->WriteRecordBatch(*record_batch);
    if (!arrow_write_status.ok()) {
        return convert_cpp_arrow_status(arrow_write_status, "record batch serialization",
                                        _cpp_arrow_memory_error);
    }
    auto close_status = ipc_writer->Close();
    if (!close_status.ok()) {
        return convert_cpp_arrow_status(close_status, "StreamWriter close",
                                        _cpp_arrow_memory_error);
    }

    auto buffer_res = out_stream->Finish();
    if (!buffer_res.ok()) {
        return convert_cpp_arrow_status(buffer_res.status(), "output stream finish",
                                        _cpp_arrow_memory_error);
    }
    std::shared_ptr<arrow::Buffer> buffer = *buffer_res;
    COUNTER_SET(_cpp_arrow_memory_peak, _arrow_pool->max_memory());
    COUNTER_UPDATE(_arrow_ipc_batch_count, 1);
    COUNTER_UPDATE(_arrow_ipc_bytes, buffer->size());
    COUNTER_SET(_arrow_ipc_batch_bytes_peak,
                std::max<int64_t>(_arrow_ipc_batch_bytes_peak->value(), buffer->size()));
    COUNTER_SET(_arrow_batch_rows_peak,
                std::max<int64_t>(_arrow_batch_rows_peak->value(),
                                  static_cast<int64_t>(end_row - start_row)));

    // Wrap the IPC buffer in a JNI direct ByteBuffer (zero-copy). The synchronous Java call
    // finishes decoding and writing this range before the native buffer is released.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    jobject direct_buffer =
            env->NewDirectByteBuffer(buffer->mutable_data(), static_cast<jlong>(buffer->size()));
    Status direct_buffer_status = Jni::Env::GetJniExceptionMsg(
            env, false, "JNI exception in NewDirectByteBuffer for PaimonJniWriter::write: ");
    if (!direct_buffer_status.ok() || direct_buffer == nullptr) {
        if (direct_buffer != nullptr) {
            env->DeleteLocalRef(direct_buffer);
        }
        COUNTER_UPDATE(_jvm_heap_memory_exceeded, 1);
        if (direct_buffer_status.ok()) {
            return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                    "Paimon JNI NewDirectByteBuffer returned null; canceling the current query");
        }
        return Status::Error<ErrorCode::QUERY_MEMORY_EXCEEDED>(
                "Paimon JNI direct buffer creation failed; canceling the current query: {}",
                direct_buffer_status.to_string());
    }

    env->CallVoidMethod(_jni_writer_obj, _write_id, direct_buffer);
    Status jni_write_status =
            Jni::Env::GetJniExceptionMsg(env, false, "JNI exception in JniPaimonWriter::write: ");
    env->DeleteLocalRef(direct_buffer);
    return _get_jni_call_status(env, std::move(jni_write_status));
}

Status JniPaimonWriter::write(RuntimeState* state, Block& block) {
    return _write_projected_block(state, block);
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
        return _get_jni_call_status(env, std::move(st));
    }

    if (j_payloads_obj == nullptr) {
        return Status::InternalError("PaimonJniWriter.prepareCommit returned null");
    }

    // Unpack the byte[][] into TPaimonCommitMessage structs for FE transport.
    auto* j_payloads = static_cast<jobjectArray>(j_payloads_obj);
    jsize num_payloads = env->GetArrayLength(j_payloads);
    messages.reserve(messages.size() + static_cast<size_t>(num_payloads));

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

        // Copy directly into the final Thrift string. GetByteArrayElements may pin the Java
        // array and hold the GCLocker until ReleaseByteArrayElements; it also required another
        // copy from the temporary view into TPaimonCommitMessage::payload.
        TPaimonCommitMessage msg;
        msg.payload.resize(static_cast<size_t>(len));
        env->GetByteArrayRegion(j_bytes, 0, len, reinterpret_cast<jbyte*>(msg.payload.data()));
        if (env->ExceptionCheck()) {
            env->DeleteLocalRef(j_bytes);
            env->DeleteLocalRef(j_payloads);
            RETURN_IF_ERROR(Jni::Env::GetJniExceptionMsg(
                    env, false, "JNI exception while reading Paimon commit payload: "));
            return Status::InternalError("Failed to read Paimon commit payload");
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

Status JniPaimonWriter::_get_jni_call_status(JNIEnv* env, Status status) {
    return translate_paimon_memory_error(env, _jni_writer_obj, _consume_last_memory_error_id,
                                         std::move(status), _arrow_memory_exceeded,
                                         _paimon_page_memory_exceeded, _jvm_heap_memory_exceeded,
                                         _commit_payload_memory_exceeded);
}

Status JniPaimonWriteBackend::_load_writer_memory_limits(JNIEnv* env) {
    jlong page_memory_limit =
            env->CallLongMethod(_jni_writer_obj, _get_paimon_page_memory_limit_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "read Paimon page memory limit"));
    if (page_memory_limit <= 0) {
        return Status::InternalError("Paimon JNI writer returned an invalid page memory limit: {}",
                                     page_memory_limit);
    }

    jlong arrow_memory_limit = env->CallLongMethod(_jni_writer_obj, _get_arrow_memory_limit_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "read Paimon Arrow memory limit"));
    if (arrow_memory_limit <= 0) {
        return Status::InternalError("Paimon JNI writer returned an invalid Arrow memory limit: {}",
                                     arrow_memory_limit);
    }

    COUNTER_SET(_native_page_memory_limit, page_memory_limit);
    COUNTER_SET(_arrow_memory_limit, arrow_memory_limit);
    _arrow_memory_limit_bytes = arrow_memory_limit;
    return Status::OK();
}

void JniPaimonWriteBackend::_refresh_memory_profile(JNIEnv* env) {
    if (_memory_manager == nullptr) {
        return;
    }
    COUNTER_SET(_writer_memory_limit, _memory_manager->memory_limit());
    COUNTER_SET(_native_page_memory_peak, _memory_manager->native_peak_allocated_bytes());
    if (_jni_writer_obj == nullptr || env == nullptr) {
        return;
    }

    jlong arrow_memory_current = env->CallLongMethod(_jni_writer_obj, _get_arrow_memory_current_id);
    Status profile_status = _check_jni_exception(env, "read current Paimon Arrow memory");
    if (!profile_status.ok()) {
        LOG(WARNING) << "Failed to refresh Paimon JNI memory profile: "
                     << profile_status.to_string();
        return;
    }

    jlong arrow_memory_peak = env->CallLongMethod(_jni_writer_obj, _get_arrow_memory_peak_id);
    profile_status = _check_jni_exception(env, "read peak Paimon Arrow memory");
    if (!profile_status.ok()) {
        LOG(WARNING) << "Failed to refresh Paimon JNI memory profile: "
                     << profile_status.to_string();
        return;
    }
    COUNTER_SET(_arrow_memory_current, arrow_memory_current);
    COUNTER_SET(_arrow_memory_peak, arrow_memory_peak);
}

} // namespace doris
