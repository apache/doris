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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {

class ObjectPool;
class RuntimeState;

namespace vectorized {

class VIcebergDeleteSink;
class VIcebergTableWriter;

class VIcebergMergeSink final : public AsyncResultWriter {
public:
    VIcebergMergeSink(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                      std::shared_ptr<pipeline::Dependency> dep,
                      std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VIcebergMergeSink() override;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status) override;

#ifdef BE_TEST
    void set_skip_io(bool skip) { _skip_io = skip; }
#endif

private:
    Status _build_inner_sinks();
    Status _prepare_output_layout();
    bool _is_delete_op(int8_t op) const;
    bool _is_insert_op(int8_t op) const;

    TDataSink _t_sink;
    TDataSink _table_sink;
    TDataSink _delete_sink;

    std::unique_ptr<VIcebergTableWriter> _table_writer;
    std::unique_ptr<VIcebergDeleteSink> _delete_writer;

    RuntimeState* _state = nullptr;

    int _operation_idx = -1;
    int _row_id_idx = -1;
    std::vector<int> _data_column_indices;

    VExprContextSPtrs _table_output_expr_ctxs;
    VExprContextSPtrs _delete_output_expr_ctxs;

    size_t _row_count = 0;
    size_t _insert_row_count = 0;
    size_t _delete_row_count = 0;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _insert_rows_counter = nullptr;
    RuntimeProfile::Counter* _delete_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

#ifdef BE_TEST
    bool _skip_io = false;
#endif
};

} // namespace vectorized
} // namespace doris
