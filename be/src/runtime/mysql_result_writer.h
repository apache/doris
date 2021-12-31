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

#include "primitive_type.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

#include "vec/data_types/data_type.h"

namespace doris {

class TupleRow;
class RowBatch;
class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;

namespace vectorized {
class VExprContext;
}

// convert the row batch to mysql protocol row
class MysqlResultWriter final : public ResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                      RuntimeProfile* parent_profile, bool output_object_data);

    virtual ~MysqlResultWriter();

    virtual Status init(RuntimeState* state) override;
    // convert one row batch to mysql result and
    // append this batch to the result sink
    virtual Status append_row_batch(const RowBatch* batch) override;

    virtual Status close() override;

private:
    void _init_profile();
    // convert one tuple row
    Status _add_one_row(TupleRow* row);
    int _add_row_value(int index, const TypeDescriptor& type, void* item);

private:
    BufferControlBlock* _sinker;
    const std::vector<ExprContext*>& _output_expr_ctxs;

    std::vector<int> _result_column_ids;

    MysqlRowBuffer* _row_buffer;
    std::vector<MysqlRowBuffer*> _vec_buffers;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
};

} // namespace doris
