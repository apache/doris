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

#include "format/transformer/vjni_format_transformer.h"

#include "exec/connector/jni_connector.h"
#include "runtime/runtime_state.h"
#include "util/time.h"

namespace doris {

namespace {

std::string mc_diag_param(const std::map<std::string, std::string>& params, const std::string& key) {
    auto it = params.find(key);
    return it == params.end() ? "" : it->second;
}

} // namespace

VJniFormatTransformer::VJniFormatTransformer(RuntimeState* state,
                                             const VExprContextSPtrs& output_vexpr_ctxs,
                                             std::string writer_class,
                                             std::map<std::string, std::string> writer_params)
        : VFileFormatTransformer(state, output_vexpr_ctxs, false),
          _writer_class(std::move(writer_class)),
          _writer_params(std::move(writer_params)) {}

Status VJniFormatTransformer::_init_jni_writer(JNIEnv* env, int batch_size) {
    // Load writer class via the same class loader as JniScanner
    LOG(INFO) << "MC_DIAG stage=BE_JNI_INIT_WRITER_ENTER"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << mc_diag_param(_writer_params, "partition_spec")
              << ", batch_size=" << batch_size;
    Jni::GlobalClass jni_writer_cls;
    int64_t start_ms = MonotonicMillis();
    Status status = Jni::Util::get_jni_scanner_class(env, _writer_class.c_str(), &jni_writer_cls);
    LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_CLASS_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    RETURN_IF_ERROR(status);

    // Get constructor: (int batchSize, Map<String,String> params)
    Jni::MethodId writer_constructor;
    start_ms = MonotonicMillis();
    status = jni_writer_cls.get_method(env, "<init>", "(ILjava/util/Map;)V", &writer_constructor);
    LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_CONSTRUCTOR_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    RETURN_IF_ERROR(status);

    // Convert C++ params map to Java HashMap
    Jni::LocalObject hashmap_object;
    start_ms = MonotonicMillis();
    status = Jni::Util::convert_to_java_map(env, _writer_params, &hashmap_object);
    LOG(INFO) << "MC_DIAG stage=BE_JNI_CONVERT_PARAMS_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    RETURN_IF_ERROR(status);

    // Create writer instance
    start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_NEW_WRITER_BEFORE"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id");
    status = jni_writer_cls.new_object(env, writer_constructor)
                     .with_arg((jint)batch_size)
                     .with_arg(hashmap_object)
                     .call(&_jni_writer_obj);
    LOG(INFO) << "MC_DIAG stage=BE_JNI_NEW_WRITER_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    RETURN_IF_ERROR(status);

    // Resolve method IDs
    status = jni_writer_cls.get_method(env, "open", "()V", &_jni_writer_open);
    RETURN_IF_ERROR(status);
    status = jni_writer_cls.get_method(env, "write", "(Ljava/util/Map;)V", &_jni_writer_write);
    RETURN_IF_ERROR(status);
    status = jni_writer_cls.get_method(env, "close", "()V", &_jni_writer_close);
    RETURN_IF_ERROR(status);
    status = jni_writer_cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                       &_jni_writer_get_statistics);
    RETURN_IF_ERROR(status);
    LOG(INFO) << "MC_DIAG stage=BE_JNI_INIT_WRITER_EXIT"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id");
    return Status::OK();
}

Status VJniFormatTransformer::open() {
    int64_t open_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_OPEN_ENTER"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", partition_spec=" << mc_diag_param(_writer_params, "partition_spec");
    JNIEnv* env = nullptr;
    Status status = Jni::Env::Get(&env);
    if (!status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_OPEN_GET_ENV_AFTER"
                  << ", status=" << status.to_string();
        return status;
    }

    int batch_size = _state->batch_size();
    status = _init_jni_writer(env, batch_size);
    if (!status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_OPEN_INIT_AFTER"
                  << ", status=" << status.to_string()
                  << ", cost_ms=" << MonotonicMillis() - open_start_ms;
        return status;
    }

    int64_t java_open_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_OPEN_BEFORE"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id");
    status = _jni_writer_obj.call_void_method(env, _jni_writer_open).call();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_OPEN_CALL_RETURNED"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - java_open_start_ms;
    RETURN_IF_ERROR(status);
    RETURN_ERROR_IF_EXC(env);

    _opened = true;
    LOG(INFO) << "MC_DIAG stage=BE_JNI_OPEN_EXIT"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", cost_ms=" << MonotonicMillis() - open_start_ms;
    return Status::OK();
}

Status VJniFormatTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    int64_t write_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_WRITE_ENTER"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", rows=" << block.rows() << ", columns=" << block.columns();
    JNIEnv* env = nullptr;
    Status status = Jni::Env::Get(&env);
    if (!status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_WRITE_GET_ENV_AFTER"
                  << ", status=" << status.to_string();
        return status;
    }

    // 1. Convert Block to Java table metadata (column addresses)
    Block* mutable_block = const_cast<Block*>(&block);
    std::unique_ptr<long[]> input_table;
    int64_t convert_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_TO_JAVA_TABLE_BEFORE"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", rows=" << block.rows() << ", columns=" << block.columns();
    status = JniConnector::to_java_table(mutable_block, input_table);
    LOG(INFO) << "MC_DIAG stage=BE_TO_JAVA_TABLE_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - convert_start_ms;
    RETURN_IF_ERROR(status);

    // 2. Cache schema on first call
    if (!_schema_cached) {
        int64_t schema_start_ms = MonotonicMillis();
        LOG(INFO) << "MC_DIAG stage=BE_PARSE_SCHEMA_BEFORE"
                  << ", writer_class=" << _writer_class
                  << ", table=" << mc_diag_param(_writer_params, "table");
        auto schema = JniConnector::parse_table_schema(mutable_block);
        _cached_required_fields = schema.first;
        _cached_columns_types = schema.second;
        _schema_cached = true;
        LOG(INFO) << "MC_DIAG stage=BE_PARSE_SCHEMA_AFTER"
                  << ", writer_class=" << _writer_class
                  << ", required_fields_length=" << _cached_required_fields.size()
                  << ", columns_types_length=" << _cached_columns_types.size()
                  << ", cost_ms=" << MonotonicMillis() - schema_start_ms;
    }

    // 3. Build input params map for Java writer
    std::map<std::string, std::string> input_params = {
            {"meta_address", std::to_string((long)input_table.get())},
            {"required_fields", _cached_required_fields},
            {"columns_types", _cached_columns_types}};

    // 4. Convert to Java Map and call writer.write(inputParams)
    Jni::LocalObject input_map;
    int64_t map_start_ms = MonotonicMillis();
    status = Jni::Util::convert_to_java_map(env, input_params, &input_map);
    LOG(INFO) << "MC_DIAG stage=BE_CONVERT_INPUT_MAP_AFTER"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - map_start_ms;
    RETURN_IF_ERROR(status);

    int64_t java_write_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_WRITE_BEFORE"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", rows=" << block.rows() << ", columns=" << block.columns();
    status = _jni_writer_obj.call_void_method(env, _jni_writer_write).with_arg(input_map).call();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_WRITE_CALL_RETURNED"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - java_write_start_ms;
    RETURN_IF_ERROR(status);
    RETURN_ERROR_IF_EXC(env);

    _cur_written_rows += block.rows();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_WRITE_EXIT"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", rows=" << block.rows()
              << ", cur_written_rows=" << _cur_written_rows
              << ", cost_ms=" << MonotonicMillis() - write_start_ms;
    return Status::OK();
}

Status VJniFormatTransformer::close() {
    if (_closed || !_opened) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_CLOSE_SKIP"
                  << ", writer_class=" << _writer_class
                  << ", table=" << mc_diag_param(_writer_params, "table")
                  << ", opened=" << _opened << ", closed=" << _closed;
        return Status::OK();
    }
    _closed = true;

    int64_t close_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_CLOSE_ENTER"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", cur_written_rows=" << _cur_written_rows;
    JNIEnv* env = nullptr;
    Status status = Jni::Env::Get(&env);
    if (!status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_CLOSE_GET_ENV_AFTER"
                  << ", status=" << status.to_string();
        return status;
    }

    int64_t java_close_start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_CLOSE_BEFORE"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id");
    status = _jni_writer_obj.call_void_method(env, _jni_writer_close).call();
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_CLOSE_CALL_RETURNED"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - java_close_start_ms;
    RETURN_IF_ERROR(status);
    RETURN_ERROR_IF_EXC(env);

    LOG(INFO) << "MC_DIAG stage=BE_JNI_CLOSE_EXIT"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id")
              << ", cost_ms=" << MonotonicMillis() - close_start_ms;
    return Status::OK();
}

int64_t VJniFormatTransformer::written_len() {
    // JNI writer manages file size on Java side; return 0 to disable C++ auto-split.
    return 0;
}

std::map<std::string, std::string> VJniFormatTransformer::get_statistics() {
    std::map<std::string, std::string> result;
    if (!_opened) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_STATS_SKIP_NOT_OPENED"
                  << ", writer_class=" << _writer_class
                  << ", table=" << mc_diag_param(_writer_params, "table");
        return result;
    }

    int64_t start_ms = MonotonicMillis();
    LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_STATS_ENTER"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", txn_id=" << mc_diag_param(_writer_params, "txn_id")
              << ", write_session_id=" << mc_diag_param(_writer_params, "write_session_id");
    JNIEnv* env = nullptr;
    Status status = Jni::Env::Get(&env);
    if (!status.ok()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_STATS_GET_ENV_AFTER"
                  << ", status=" << status.to_string();
        return result;
    }

    Jni::LocalObject stats_map;
    status = _jni_writer_obj.call_object_method(env, _jni_writer_get_statistics).call(&stats_map);
    LOG(INFO) << "MC_DIAG stage=BE_JAVA_GET_STATS_CALL_RETURNED"
              << ", writer_class=" << _writer_class << ", status=" << status.to_string()
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    if (!status.ok()) {
        return result;
    }
    if (stats_map.uninitialized()) {
        LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_STATS_EMPTY"
                  << ", writer_class=" << _writer_class
                  << ", table=" << mc_diag_param(_writer_params, "table");
        return result;
    }

    // Convert Java Map<String,String> to C++ map
    static_cast<void>(Jni::Util::convert_to_cpp_map(env, stats_map, &result));
    auto commit_it = result.find("mc_commit_message");
    auto written_rows_it = result.find("counter:WrittenRows");
    auto written_bytes_it = result.find("bytes:WrittenBytes");
    LOG(INFO) << "MC_DIAG stage=BE_JNI_GET_STATS_EXIT"
              << ", writer_class=" << _writer_class
              << ", table=" << mc_diag_param(_writer_params, "table")
              << ", stats_size=" << result.size()
              << ", commit_message_length="
              << (commit_it == result.end() ? 0 : commit_it->second.size())
              << ", written_rows="
              << (written_rows_it == result.end() ? "" : written_rows_it->second)
              << ", written_bytes="
              << (written_bytes_it == result.end() ? "" : written_bytes_it->second)
              << ", cost_ms=" << MonotonicMillis() - start_ms;
    return result;
}

} // namespace doris
