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

#include "exec/sink/writer/paimon/vpaimon_jni_table_writer.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "core/block/block.h"
#include "exec/sink/writer/paimon/paimon_writer_utils.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/jni-util.h"
#include "util/jni_native_method.h"

namespace doris {

const std::string PAIMON_JNI_CLASS = "org/apache/doris/paimon/PaimonJniWriter";

VPaimonJniTableWriter::VPaimonJniTableWriter(const TDataSink& t_sink,
                                             const VExprContextSPtrs& output_exprs)
        : VPaimonJniTableWriter(t_sink, output_exprs, nullptr, nullptr) {}

VPaimonJniTableWriter::VPaimonJniTableWriter(const TDataSink& t_sink,
                                             const VExprContextSPtrs& output_exprs,
                                             std::shared_ptr<Dependency> dep,
                                             std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, std::move(dep), std::move(fin_dep)), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

VPaimonJniTableWriter::~VPaimonJniTableWriter() {
    JNIEnv* env = nullptr;
    if (_get_jni_env(&env).ok()) {
        if (_jni_writer_obj != nullptr) {
            env->DeleteGlobalRef(_jni_writer_obj);
        }
        if (_jni_writer_cls != nullptr) {
            env->DeleteGlobalRef(_jni_writer_cls);
        }
    }
}

Status VPaimonJniTableWriter::init_properties(ObjectPool* pool) {
    return Status::OK();
}

Status VPaimonJniTableWriter::_get_jni_env(JNIEnv** env) {
    JavaVM* jvm = nullptr;
    jsize n_vms = 0;
    jint result = JNI_GetCreatedJavaVMs(&jvm, 1, &n_vms);
    if (result != JNI_OK || n_vms == 0) {
        return Status::InternalError("Failed to get created JavaVM");
    }

    result = jvm->GetEnv((void**)env, JNI_VERSION_1_8);
    if (result == JNI_EDETACHED) {
        result = jvm->AttachCurrentThread((void**)env, nullptr);
        if (result != JNI_OK) {
            return Status::InternalError("Failed to attach current thread to JVM");
        }
    } else if (result != JNI_OK) {
        return Status::InternalError("Failed to get JNIEnv");
    }
    return Status::OK();
}

Status VPaimonJniTableWriter::_check_jni_exception(JNIEnv* env, const std::string& method_name) {
    if (env->ExceptionCheck()) {
        Status st = Jni::Env::GetJniExceptionMsg(env, true,
                                                 "JNI exception occurred in " + method_name + ": ");
        LOG(WARNING) << st.to_string();
        return st;
    }
    return Status::OK();
}

Status VPaimonJniTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;

    _written_rows_counter = ADD_COUNTER(_profile, "WrittenRows", TUnit::UNIT);
    _written_bytes_counter = ADD_COUNTER(_profile, "WrittenBytes", TUnit::BYTES);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _project_timer = ADD_CHILD_TIMER(_profile, "ProjectTime", "SendDataTime");
    _arrow_convert_timer = ADD_CHILD_TIMER(_profile, "ArrowConvertTime", "SendDataTime");
    _file_store_write_timer = ADD_CHILD_TIMER(_profile, "FileStoreWriteTime", "SendDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseTime");
    _prepare_commit_timer = ADD_TIMER(_profile, "PrepareCommitTime");
    _serialize_commit_messages_timer = ADD_TIMER(_profile, "SerializeCommitMessagesTime");
    _commit_payload_bytes_counter = ADD_COUNTER(_profile, "CommitPayloadBytes", TUnit::BYTES);
    _buffer_flush_count = ADD_COUNTER(_profile, "BufferFlushCount", TUnit::UNIT);

    SCOPED_TIMER(_open_timer);

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));
    _arrow_pool = std::make_unique<ArrowMemoryPool<>>();

    Jni::LocalClass local_cls;
    Status find_class_st =
            Jni::Util::get_jni_scanner_class(env, PAIMON_JNI_CLASS.c_str(), &local_cls);
    if (!find_class_st.ok()) {
        RETURN_IF_ERROR(_check_jni_exception(env, "get_scanner_class"));
        return Status::InternalError("Failed to load scanner class {}: {}", PAIMON_JNI_CLASS,
                                     find_class_st.to_string());
    }

    _jni_writer_cls = (jclass)env->NewGlobalRef(local_cls.get());
    if (_jni_writer_cls == nullptr) {
        return Status::InternalError("Failed to create global ref for class {}", PAIMON_JNI_CLASS);
    }

    _ctor_id = env->GetMethodID(_jni_writer_cls, "<init>", "()V");
    _open_id = env->GetMethodID(_jni_writer_cls, "open",
                                "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;)V");
    _write_id = env->GetMethodID(_jni_writer_cls, "write", "(JI)V");
    _prepare_commit_id = env->GetMethodID(_jni_writer_cls, "prepareCommit", "()[[B");
    _abort_id = env->GetMethodID(_jni_writer_cls, "abort", "()V");
    _close_id = env->GetMethodID(_jni_writer_cls, "close", "()V");
    RETURN_IF_ERROR(_check_jni_exception(env, "GetMethodID"));

    jobject local_obj = env->NewObject(_jni_writer_cls, _ctor_id);
    RETURN_IF_ERROR(_check_jni_exception(env, "NewObject"));
    _jni_writer_obj = env->NewGlobalRef(local_obj);
    env->DeleteLocalRef(local_obj);

    const auto& paimon_sink = _t_sink.paimon_table_sink;
    LOG(INFO) << "paimon: table location: " << paimon_sink.table_location;

    jstring j_location = env->NewStringUTF(paimon_sink.table_location.c_str());
    std::map<std::string, std::string> jni_options = paimon_sink.options;
    jni_options["db_name"] = paimon_sink.db_name;
    jni_options["table_name"] = paimon_sink.tb_name;
    if (paimon_sink.__isset.serialized_table && !paimon_sink.serialized_table.empty()) {
        jni_options["serialized_table"] = paimon_sink.serialized_table;
    }
    int64_t buffer_size = 256 * 1024 * 1024L; // Default 256MB

    if (_state->query_options().__isset.paimon_write_buffer_size &&
        _state->query_options().paimon_write_buffer_size > 0) {
        buffer_size = _state->query_options().paimon_write_buffer_size;
    }

    bool enable_adaptive = true;
    if (_state->query_options().__isset.enable_paimon_adaptive_buffer_size) {
        enable_adaptive = _state->query_options().enable_paimon_adaptive_buffer_size;
    }

    if (enable_adaptive && paimon_sink.__isset.bucket_num && paimon_sink.bucket_num > 0) {
        int bucket_num = paimon_sink.bucket_num;
        buffer_size = get_paimon_write_buffer_size(buffer_size, true, bucket_num);
        LOG(INFO) << "Adaptive Paimon JNI Buffer Size: bucket_num=" << bucket_num
                  << ", adjusted_buffer_size=" << buffer_size;
    }
    LOG(INFO) << "Paimon JNI Writer Final Buffer Size: " << buffer_size
              << " (enable_adaptive=" << enable_adaptive << ")";

    jni_options["write-buffer-size"] = std::to_string(buffer_size);

    if (_state->query_options().__isset.paimon_target_file_size &&
        _state->query_options().paimon_target_file_size > 0) {
        jni_options["target-file-size"] =
                std::to_string(_state->query_options().paimon_target_file_size);
        LOG(INFO) << "Paimon JNI Writer Target File Size: "
                  << _state->query_options().paimon_target_file_size;
    }

    bool enable_spill = false;
    if (_state->query_options().__isset.enable_paimon_jni_spill) {
        enable_spill = _state->query_options().enable_paimon_jni_spill;
    }

    if (enable_spill) {
        jni_options["write-buffer-spillable"] = "true";
        LOG(INFO) << "Paimon JNI Writer Spill Enabled";

        // Pass Spill Options
        if (_state->query_options().__isset.paimon_spill_max_disk_size) {
            jni_options["write-buffer-spill.max-disk-size"] =
                    std::to_string(_state->query_options().paimon_spill_max_disk_size);
        }
        if (_state->query_options().__isset.paimon_spill_sort_buffer_size) {
            jni_options["sort-spill-buffer-size"] =
                    std::to_string(_state->query_options().paimon_spill_sort_buffer_size);
        }
        if (_state->query_options().__isset.paimon_spill_sort_threshold) {
            jni_options["sort-spill-threshold"] =
                    std::to_string(_state->query_options().paimon_spill_sort_threshold);
        }
        if (_state->query_options().__isset.paimon_spill_compression) {
            jni_options["spill-compression"] = _state->query_options().paimon_spill_compression;
        }
        if (_state->query_options().__isset.paimon_global_memory_pool_size) {
            jni_options["paimon_global_memory_pool_size"] =
                    std::to_string(_state->query_options().paimon_global_memory_pool_size);
        }

        // Auto-detect Spill Directory from BE Storage
        std::string spill_dir = "/tmp/paimon_spill";
        jni_options["paimon_jni_spill_dir"] = spill_dir;
        LOG(INFO) << "Paimon JNI Spill Directory: " << spill_dir;
    }

    jobject j_options = _to_java_options(env, jni_options);

    jobjectArray j_cols = nullptr;
    jclass string_cls = env->FindClass("java/lang/String");
    if (paimon_sink.__isset.column_names) {
        j_cols = env->NewObjectArray(static_cast<jsize>(paimon_sink.column_names.size()),
                                     string_cls, nullptr);
        for (size_t i = 0; i < paimon_sink.column_names.size(); ++i) {
            jstring str = env->NewStringUTF(paimon_sink.column_names[i].c_str());
            env->SetObjectArrayElement(j_cols, static_cast<jsize>(i), str);
            env->DeleteLocalRef(str);
        }
    } else {
        j_cols = env->NewObjectArray(0, string_cls, nullptr);
    }

    env->CallVoidMethod(_jni_writer_obj, _open_id, j_location, j_options, j_cols);
    Status st = _check_jni_exception(env, "open");

    env->DeleteLocalRef(j_location);
    env->DeleteLocalRef(j_options);
    env->DeleteLocalRef(j_cols);
    env->DeleteLocalRef(string_cls);

    return st;
}

Status VPaimonJniTableWriter::write(RuntimeState* state, ::doris::Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    SCOPED_TIMER(_send_data_timer);

    ::doris::Block output_block;
    {
        SCOPED_TIMER(_project_timer);
        RETURN_IF_ERROR(_projection_block(block, &output_block));
    }

    RETURN_IF_ERROR(_append_to_buffer(output_block));
    if (_buffered_rows >= _batch_max_rows || _buffered_bytes >= _batch_max_bytes) {
        return _flush_buffer();
    }
    return Status::OK();
}

Status VPaimonJniTableWriter::_write_projected_block(::doris::Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    _row_count += block.rows();

    COUNTER_UPDATE(_written_rows_counter, block.rows());
    COUNTER_UPDATE(_written_bytes_counter, block.bytes());

    _state->update_num_rows_load_total(block.rows());
    _state->update_num_bytes_load_total(block.bytes());

    std::shared_ptr<arrow::Buffer> buffer;
    std::shared_ptr<arrow::Schema> arrow_schema;
    {
        SCOPED_TIMER(_arrow_convert_timer);

        RETURN_IF_ERROR(get_arrow_schema_from_block(block, &arrow_schema, _state->timezone()));

        std::shared_ptr<arrow::RecordBatch> record_batch;
        RETURN_IF_ERROR(convert_to_arrow_batch(block, arrow_schema, _arrow_pool.get(),
                                               &record_batch, _state->timezone_obj()));

        auto out_stream_res = arrow::io::BufferOutputStream::Create();
        if (!out_stream_res.ok()) {
            return Status::InternalError("Arrow BufferOutputStream create failed");
        }
        auto out_stream = *out_stream_res;

        auto writer_res = arrow::ipc::MakeStreamWriter(out_stream, arrow_schema);
        if (!writer_res.ok()) {
            return Status::InternalError("Arrow StreamWriter create failed");
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
            return Status::InternalError("Arrow output stream finish failed");
        }
        buffer = *buffer_res;
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(_get_jni_env(&env));

    auto address = reinterpret_cast<jlong>(buffer->data());
    jint length = static_cast<jint>(buffer->size());

    {
        SCOPED_TIMER(_file_store_write_timer);
        env->CallVoidMethod(_jni_writer_obj, _write_id, address, length);
    }
    return _check_jni_exception(env, "write");
}

Status VPaimonJniTableWriter::_append_to_buffer(const ::doris::Block& block) {
    if (!_buffer) {
        _buffer = ::doris::Block::create_unique(block.clone_empty());
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }
    auto columns = _buffer->mutate_columns();
    const int cols = block.columns();
    const size_t rows = block.rows();
    for (int col = 0; col < cols; ++col) {
        columns[col]->insert_range_from(*block.get_by_position(col).column, 0, rows);
    }
    _buffer->set_columns(std::move(columns));
    _buffered_rows += rows;
    _buffered_bytes += block.bytes();
    return Status::OK();
}

Status VPaimonJniTableWriter::_flush_buffer() {
    if (!_buffer || _buffered_rows == 0) {
        return Status::OK();
    }
    Status st = _write_projected_block(*_buffer);
    COUNTER_UPDATE(_buffer_flush_count, 1);
    _buffer.reset();
    _buffered_rows = 0;
    _buffered_bytes = 0;
    return st;
}

Status VPaimonJniTableWriter::close(Status status) {
    JNIEnv* env = nullptr;
    if (!_get_jni_env(&env).ok()) {
        return status;
    }

    SCOPED_TIMER(_close_timer);

    if (status.ok()) {
        Status flush_st = _flush_buffer();
        if (!flush_st.ok()) {
            status = flush_st;
        }
    } else {
        _buffer.reset();
        _buffered_rows = 0;
        _buffered_bytes = 0;
    }

    std::vector<TPaimonCommitMessage> pending_commit_messages;
    if (status.ok()) {
        jobject j_payloads_obj = nullptr;
        {
            SCOPED_TIMER(_prepare_commit_timer);
            j_payloads_obj = env->CallObjectMethod(_jni_writer_obj, _prepare_commit_id);
        }
        Status st = _check_jni_exception(env, "prepareCommit");

        if (st.ok() && j_payloads_obj != nullptr) {
            auto* j_payloads = (jobjectArray)j_payloads_obj;
            jsize num_payloads = env->GetArrayLength(j_payloads);

            pending_commit_messages.reserve(static_cast<size_t>(num_payloads));
            {
                SCOPED_TIMER(_serialize_commit_messages_timer);

                for (jsize i = 0; i < num_payloads; ++i) {
                    jbyteArray j_bytes = (jbyteArray)env->GetObjectArrayElement(j_payloads, i);
                    if (j_bytes == nullptr) {
                        continue;
                    }
                    jsize len = env->GetArrayLength(j_bytes);
                    if (len > 0) {
                        jbyte* bytes = env->GetByteArrayElements(j_bytes, nullptr);
                        if (bytes != nullptr) {
                            std::string payload(reinterpret_cast<char*>(bytes),
                                                static_cast<size_t>(len));
                            TPaimonCommitMessage msg;
                            msg.__set_payload(payload);
                            pending_commit_messages.emplace_back(std::move(msg));
                            env->ReleaseByteArrayElements(j_bytes, bytes, JNI_ABORT);
                        }
                    }
                    env->DeleteLocalRef(j_bytes);
                }
            }

            env->DeleteLocalRef(j_payloads);
        } else if (!st.ok()) {
            status = st;
            Status abort_st = _abort(env);
            if (!abort_st.ok()) {
                LOG(WARNING) << "paimon: failed to abort writer during cleanup: "
                             << abort_st.to_string();
            }
        }
    } else {
        LOG(WARNING) << "paimon: writer closing with error: " << status.to_string();
        Status abort_st = _abort(env);
        if (!abort_st.ok()) {
            LOG(WARNING) << "paimon: failed to abort writer on error: " << abort_st.to_string();
        }
    }

    if (_jni_writer_obj != nullptr && _close_id != nullptr) {
        env->CallVoidMethod(_jni_writer_obj, _close_id);
        Status close_st = _check_jni_exception(env, "close");
        if (!close_st.ok()) {
            LOG(WARNING) << "paimon: failed to close writer in Java: " << close_st.to_string();
            if (status.ok()) {
                status = close_st;
            }
        }
    } else {
        LOG(INFO) << "paimon: skip Java close because writer object or close method is not "
                     "initialized";
    }

    if (status.ok()) {
        if (!pending_commit_messages.empty()) {
            _state->add_paimon_commit_messages(pending_commit_messages);
            LOG(INFO) << "paimon: added " << pending_commit_messages.size()
                      << " commit messages to state";
        } else {
            LOG(INFO) << "paimon: no commit messages to add to state";
        }
    }

    return status;
}

Status VPaimonJniTableWriter::_abort(JNIEnv* env) {
    if (_jni_writer_obj != nullptr && _abort_id != nullptr) {
        env->CallVoidMethod(_jni_writer_obj, _abort_id);
        if (env->ExceptionCheck()) {
            return Jni::Env::GetJniExceptionMsg(env, true, "JNI exception occurred in abort: ");
        }
    }
    return Status::OK();
}

jobject VPaimonJniTableWriter::_to_java_options(JNIEnv* env,
                                                const std::map<std::string, std::string>& options) {
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

} // namespace doris
