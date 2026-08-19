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
#include <vector>

#include "common/check.h"
#include "common/config.h"
#include "common/logging.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"
#include "exec/sink/writer/paimon/paimon_sink_memory_allocator.h"
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
        "String;Ljava/lang/String;JJJ)V";

PaimonJniWriterOpenMode PaimonJniWriterOpenMode::from_write_mode(
        TPaimonWriteMode::type write_mode) {
    return {static_cast<jboolean>(write_mode == TPaimonWriteMode::OVERWRITE),
            static_cast<jboolean>(write_mode == TPaimonWriteMode::CHANGELOG)};
}

JniPaimonWriteBackend::JniPaimonWriteBackend(std::unique_ptr<PaimonWriterMemoryLease> memory_lease)
        : _memory_lease(std::move(memory_lease)) {}

JniPaimonWriteBackend::~JniPaimonWriteBackend() {
    Status st = close();
    if (!st.ok()) {
        LOG(WARNING) << "Failed to close Paimon JNI backend during destruction: " << st.to_string();
    }
}

Status JniPaimonWriteBackend::close() {
    if (_jni_writer_obj == nullptr && _jni_writer_cls == nullptr) {
        _memory_manager.reset();
        _memory_lease.reset();
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
            _memory_manager->poison(env_status);
            retain_memory_after_failed_close(std::move(_memory_manager));
        } else {
            _memory_manager.reset();
        }
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
            _memory_manager->poison(close_status);
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

    _arrow_memory_limit_bytes = config::paimon_jni_writer_arrow_memory_limit_bytes;
    DORIS_CHECK(_memory_lease != nullptr);
    RETURN_IF_ERROR(
            PaimonJniMemoryManager::create(state, std::move(_memory_lease), &_memory_manager));
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

    PaimonJniWriterOpenMode open_mode = PaimonJniWriterOpenMode::from_write_mode(sink.write_mode);
    env->CallVoidMethod(_jni_writer_obj, open_id, j_serialized_table, j_hadoop_config, j_cols,
                        static_cast<jlong>(sink.transaction_id), j_commit_user, open_mode.overwrite,
                        open_mode.changelog, j_time_zone, j_spill_directories,
                        static_cast<jlong>(_memory_manager->memory_limit()),
                        static_cast<jlong>(_arrow_memory_limit_bytes),
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
        _opened = true;
        _refresh_memory_profile();
        LOG(INFO) << "Paimon JNI writer memory limit: "
                  << PrettyPrinter::print_bytes(_memory_manager->memory_limit())
                  << ", Arrow direct memory limit="
                  << PrettyPrinter::print_bytes(_arrow_memory_limit_bytes)
                  << ", local_sink_count=" << std::max(1, state->num_local_sink());
    }
    return st;
}

// Writer creation stays non-const because the backend interface also supports future stateful FFI
// implementations.
Status JniPaimonWriteBackend::create_writer( // NOLINT(readability-make-member-function-const)
        std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_opened);
    // Target half of the Java allocator for one encoded batch so Arrow decoding and metadata have
    // headroom. The value is captured at backend open and remains stable for this writer.
    const size_t arrow_batch_size_bytes = static_cast<size_t>(_arrow_memory_limit_bytes / 2);
    *writer = std::make_unique<JniPaimonWriter>(_jni_writer_obj, _write_id, _prepare_commit_id,
                                                _abort_id, std::make_unique<ArrowMemoryPool<>>(),
                                                _sink, arrow_batch_size_bytes);
    return Status::OK();
}

JniPaimonWriter::JniPaimonWriter(jobject jni_writer_obj, jmethodID write_id,
                                 jmethodID prepare_commit_id, jmethodID abort_id,
                                 std::unique_ptr<ArrowMemoryPool<>> arrow_pool,
                                 TPaimonTableSink sink, size_t arrow_batch_size_bytes)
        : _jni_writer_obj(jni_writer_obj),
          _write_id(write_id),
          _prepare_commit_id(prepare_commit_id),
          _abort_id(abort_id),
          _arrow_pool(std::move(arrow_pool)),
          _sink(std::move(sink)),
          _arrow_batch_size_bytes(arrow_batch_size_bytes) {}

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

    // Build the Arrow schema once. Common blocks stay on the one-block/one-JNI-call fast path;
    // only unusually wide blocks are divided into bounded row ranges.
    // Paimon write timestamps are transported as civil-time fields. The Java writer uses the
    // pinned Paimon target type to preserve NTZ values or convert LTZ values with the session zone.
    // Variant V2 is transported losslessly as its value/metadata pair, including nested Variant.
    std::shared_ptr<arrow::Schema> arrow_schema;
    RETURN_IF_ERROR(get_paimon_arrow_schema_from_block(block, &arrow_schema));

    const size_t block_rows = block.rows();
    const size_t block_bytes = block.bytes();
    if (block_bytes <= _arrow_batch_size_bytes) {
        return _write_row_range(state, block, arrow_schema, 0, block_rows,
                                std::max<size_t>(1, block_bytes));
    }

    // Use Doris bytes only to choose an inexpensive initial range size. The encoded IPC size is
    // checked by _write_row_range(), which adaptively splits an oversized range. This keeps a
    // single unusually large row from being grouped with otherwise small rows based on an average.
    const size_t average_row_bytes =
            std::max<size_t>(1, block_bytes / block_rows + (block_bytes % block_rows != 0));
    const size_t rows_per_batch = BlockBudget(state->batch_size(), _arrow_batch_size_bytes)
                                          .effective_max_rows(average_row_bytes);

    for (size_t start_row = 0; start_row < block_rows; start_row += rows_per_batch) {
        const size_t end_row = std::min(start_row + rows_per_batch, block_rows);
        const size_t range_rows = end_row - start_row;
        const size_t estimated_ipc_bytes = average_row_bytes > _arrow_batch_size_bytes / range_rows
                                                   ? _arrow_batch_size_bytes
                                                   : average_row_bytes * range_rows;
        RETURN_IF_ERROR(_write_row_range(state, block, arrow_schema, start_row, end_row,
                                         estimated_ipc_bytes));
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

    // Reserve approximately the row range's Doris size to reduce buffer growth copies. The
    // estimate is capped by the IPC batching target; Arrow may still grow it when encoding expands.
    auto out_stream_res = arrow::io::BufferOutputStream::Create(
            std::max<size_t>(4096, estimated_ipc_bytes), _arrow_pool.get());
    if (!out_stream_res.ok()) {
        return Status::InternalError("Arrow BufferOutputStream create failed: {}",
                                     out_stream_res.status().ToString());
    }
    auto out_stream = std::move(out_stream_res).ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_stream, arrow_schema);
    if (!writer_res.ok()) {
        return Status::InternalError("Arrow StreamWriter create failed: {}",
                                     writer_res.status().ToString());
    }
    auto ipc_writer = std::move(writer_res).ValueOrDie();
    auto arrow_status = ipc_writer->WriteRecordBatch(*record_batch);
    if (!arrow_status.ok()) {
        return Status::InternalError("Arrow WriteRecordBatch failed: {}", arrow_status.ToString());
    }
    arrow_status = ipc_writer->Close();
    if (!arrow_status.ok()) {
        return Status::InternalError("Arrow StreamWriter close failed: {}",
                                     arrow_status.ToString());
    }

    auto buffer_res = out_stream->Finish();
    if (!buffer_res.ok()) {
        return Status::InternalError("Arrow output stream finish failed: {}",
                                     buffer_res.status().ToString());
    }
    std::shared_ptr<arrow::Buffer> buffer = std::move(buffer_res).ValueOrDie();

    // Only the finalized IPC buffer must stay alive during the synchronous JNI call. Releasing the
    // Arrow arrays and builders first keeps the C++ peak from spanning Java decoding and writing.
    record_batch.reset();
    ipc_writer.reset();
    out_stream.reset();

    const size_t range_rows = end_row - start_row;
    const size_t serialized_bytes = static_cast<size_t>(buffer->size());
    if (serialized_bytes > _arrow_batch_size_bytes && range_rows > 1) {
        // Doris column bytes are only an estimate of Arrow IPC size and may hide skew. Once the
        // actual encoded size is known, discard the oversized candidate and retry its two halves.
        // A one-row range is intentionally not split: it is the smallest writable unit and the
        // Java allocator still has the other half of its configured limit as headroom.
        buffer.reset();
        const size_t middle_row = start_row + range_rows / 2;
        const size_t average_serialized_row_bytes =
                serialized_bytes / range_rows + (serialized_bytes % range_rows != 0);
        const auto estimate_range_bytes = [&](size_t rows) {
            return average_serialized_row_bytes > _arrow_batch_size_bytes / rows
                           ? _arrow_batch_size_bytes
                           : average_serialized_row_bytes * rows;
        };
        RETURN_IF_ERROR(_write_row_range(state, block, arrow_schema, start_row, middle_row,
                                         estimate_range_bytes(middle_row - start_row)));
        return _write_row_range(state, block, arrow_schema, middle_row, end_row,
                                estimate_range_bytes(end_row - middle_row));
    }

    // Wrap the IPC buffer in a JNI direct ByteBuffer (zero-copy). Java consumes it synchronously.
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

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
