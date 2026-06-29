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

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/sink/writer/async_result_writer.h"
#include "exprs/vexpr_fwd.h"
#include "format/parquet/arrow_memory_pool.h"

namespace doris {

class ObjectPool;

class VPaimonJniTableWriter final : public AsyncResultWriter {
public:
    VPaimonJniTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);
    VPaimonJniTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                          std::shared_ptr<Dependency> dep, std::shared_ptr<Dependency> fin_dep);
    ~VPaimonJniTableWriter() override;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, ::doris::Block& block) override;

    Status close(Status status) override;

private:
    Status _get_jni_env(JNIEnv** env);

    Status _check_jni_exception(JNIEnv* env, const std::string& method_name);
    jobject _to_java_options(JNIEnv* env, const std::map<std::string, std::string>& options);
    Status _write_projected_block(::doris::Block& block);
    Status _append_to_buffer(const ::doris::Block& block);
    Status _flush_buffer();

    Status _abort(JNIEnv* env);

    TDataSink _t_sink;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

    // JNI References
    jclass _jni_writer_cls = nullptr;
    jobject _jni_writer_obj = nullptr;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_bytes_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _project_timer = nullptr;
    RuntimeProfile::Counter* _arrow_convert_timer = nullptr;
    RuntimeProfile::Counter* _file_store_write_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _prepare_commit_timer = nullptr;
    RuntimeProfile::Counter* _serialize_commit_messages_timer = nullptr;
    RuntimeProfile::Counter* _commit_payload_bytes_counter = nullptr;
    RuntimeProfile::Counter* _buffer_flush_count = nullptr;

    // Cached Method IDs
    jmethodID _ctor_id = nullptr;
    jmethodID _open_id = nullptr;
    jmethodID _write_id = nullptr;
    jmethodID _prepare_commit_id = nullptr;
    jmethodID _abort_id = nullptr;
    jmethodID _close_id = nullptr;

    std::unique_ptr<ArrowMemoryPool<>> _arrow_pool;
    std::unique_ptr<::doris::Block> _buffer;
    size_t _batch_max_rows = 32768;
    size_t _batch_max_bytes = 4 * 1024 * 1024;
    size_t _buffered_rows = 0;
    size_t _buffered_bytes = 0;
    size_t _row_count = 0;
};

} // namespace doris
