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

#include "vec/runtime/vjni_format_transformer.h"

#include "runtime/runtime_state.h"
#include "vec/exec/jni_connector.h"

namespace doris::vectorized {

VJniFormatTransformer::VJniFormatTransformer(RuntimeState* state,
                                             const VExprContextSPtrs& output_vexpr_ctxs,
                                             std::string writer_class,
                                             std::map<std::string, std::string> writer_params)
        : VFileFormatTransformer(state, output_vexpr_ctxs, false),
          _writer_class(std::move(writer_class)),
          _writer_params(std::move(writer_params)) {}

Status VJniFormatTransformer::_init_jni_writer(JNIEnv* env, int batch_size) {
    // Load writer class via the same class loader as JniScanner
    Jni::GlobalClass jni_writer_cls;
    RETURN_IF_ERROR(Jni::Util::get_jni_scanner_class(env, _writer_class.c_str(), &jni_writer_cls));

    // Get constructor: (int batchSize, Map<String,String> params)
    Jni::MethodId writer_constructor;
    RETURN_IF_ERROR(
            jni_writer_cls.get_method(env, "<init>", "(ILjava/util/Map;)V", &writer_constructor));

    // Convert C++ params map to Java HashMap
    Jni::LocalObject hashmap_object;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, _writer_params, &hashmap_object));

    // Create writer instance
    RETURN_IF_ERROR(jni_writer_cls.new_object(env, writer_constructor)
                            .with_arg((jint)batch_size)
                            .with_arg(hashmap_object)
                            .call(&_jni_writer_obj));

    // Resolve method IDs
    RETURN_IF_ERROR(jni_writer_cls.get_method(env, "open", "()V", &_jni_writer_open));
    RETURN_IF_ERROR(
            jni_writer_cls.get_method(env, "write", "(Ljava/util/Map;)V", &_jni_writer_write));
    RETURN_IF_ERROR(jni_writer_cls.get_method(env, "close", "()V", &_jni_writer_close));
    RETURN_IF_ERROR(jni_writer_cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                              &_jni_writer_get_statistics));
    return Status::OK();
}

Status VJniFormatTransformer::open() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    int batch_size = _state->batch_size();
    RETURN_IF_ERROR(_init_jni_writer(env, batch_size));

    RETURN_IF_ERROR(_jni_writer_obj.call_void_method(env, _jni_writer_open).call());
    RETURN_ERROR_IF_EXC(env);

    _opened = true;
    return Status::OK();
}

Status VJniFormatTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    // 1. Convert Block to Java table metadata (column addresses)
    Block* mutable_block = const_cast<Block*>(&block);
    std::unique_ptr<long[]> input_table;
    RETURN_IF_ERROR(JniConnector::to_java_table(mutable_block, input_table));

    // 2. Cache schema on first call
    if (!_schema_cached) {
        auto schema = JniConnector::parse_table_schema(mutable_block);
        _cached_required_fields = schema.first;
        _cached_columns_types = schema.second;
        _schema_cached = true;
    }

    // 3. Build input params map for Java writer
    std::map<std::string, std::string> input_params = {
            {"meta_address", std::to_string((long)input_table.get())},
            {"required_fields", _cached_required_fields},
            {"columns_types", _cached_columns_types}};

    // 4. Convert to Java Map and call writer.write(inputParams)
    Jni::LocalObject input_map;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, input_params, &input_map));

    RETURN_IF_ERROR(
            _jni_writer_obj.call_void_method(env, _jni_writer_write).with_arg(input_map).call());
    RETURN_ERROR_IF_EXC(env);

    _cur_written_rows += block.rows();
    return Status::OK();
}

Status VJniFormatTransformer::close() {
    if (_closed || !_opened) {
        return Status::OK();
    }
    _closed = true;

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    RETURN_IF_ERROR(_jni_writer_obj.call_void_method(env, _jni_writer_close).call());
    RETURN_ERROR_IF_EXC(env);

    return Status::OK();
}

int64_t VJniFormatTransformer::written_len() {
    // JNI writer manages file size on Java side; return 0 to disable C++ auto-split.
    return 0;
}

std::map<std::string, std::string> VJniFormatTransformer::get_statistics() {
    std::map<std::string, std::string> result;
    if (!_opened) {
        return result;
    }

    JNIEnv* env = nullptr;
    if (!Jni::Env::Get(&env).ok()) {
        return result;
    }

    Jni::LocalObject stats_map;
    if (!_jni_writer_obj.call_object_method(env, _jni_writer_get_statistics)
                 .call(&stats_map)
                 .ok()) {
        return result;
    }
    if (stats_map.uninitialized()) {
        return result;
    }

    // Convert Java Map<String,String> to C++ map
    static_cast<void>(Jni::Util::convert_to_cpp_map(env, stats_map, &result));
    return result;
}

} // namespace doris::vectorized
