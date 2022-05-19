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

#include "vec/sink/thrift_result_writer.h"

#include <arrow/memory_pool.h>
#include <string>

#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
VThriftResultWriter::VThriftResultWriter(BufferControlBlock* sinker,
                                         const std::vector<VExprContext*>& output_vexpr_ctxs,
                                         RuntimeProfile* parent_profile)
        : VResultWriter(),
          _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

Status VThriftResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    return Status::OK();
}

void VThriftResultWriter::_init_profile() {
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status VThriftResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::RuntimeError("Not Implemented MysqlResultWriter::append_row_batch scalar");
}

template <PrimitiveType type, bool is_nullable>
Status VThriftResultWriter::_add_one_column(const ColumnPtr& column_ptr,
                                            TThriftIPCColumn& thrift_column_data) {
    // Init column nullable property, it is used to indicate whether the column at specified row
    // is null.
    std::vector<bool> is_null_vec;
    const auto row_size = column_ptr->size();
    doris::vectorized::ColumnPtr column;
    if constexpr (is_nullable) {
        column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
    } else {
        column = column_ptr;
    }
    // Currently, only support varchar type, not other types
    if constexpr (type == TYPE_VARCHAR) {
        std::vector<std::string> string_vec;
        for (int i = 0; i < row_size; ++i) {
            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(i)) {
                    is_null_vec.push_back(true);
                    continue;
                } else {
                    is_null_vec.push_back(false);
                }
            } else {
                is_null_vec.push_back(false);
            }
            const auto string_val = column->get_data_at(i);
            string_vec.emplace_back(string_val.data, string_val.size);
        }
        thrift_column_data.__set_is_null(is_null_vec);
        thrift_column_data.__set_string_vals(string_vec);
    }
    return Status::OK();
}

Status VThriftResultWriter::append_block(Block& input_block) {
    Status status = Status::OK();
    auto result = std::make_unique<TFetchDataResult>();
    size_t num_rows = input_block.rows();
    TThriftIPCRowBatch thrift_row_batch;
    thrift_row_batch.__set_num_rows(num_rows);
    std::vector<TThriftIPCColumn> thrift_columns;
    std::vector<TThriftIPCColumnDesc> thrift_column_defs;
    for (int i = 0; i < _output_vexpr_ctxs.size(); ++i) {
        auto column_ptr = input_block.get_by_position(i).column->convert_to_full_column_if_const();
        auto type_ptr = input_block.get_by_position(i).type;

        TThriftIPCColumn thrift_column_data;
        switch (_output_vexpr_ctxs[i]->root()->result_type()) {
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            TThriftIPCColumnDesc col_def;
            col_def.__set_type(TPrimitiveType::VARCHAR);
            thrift_column_defs.push_back(col_def);
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, true>(column_ptr,
                                                                            thrift_column_data);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, false>(column_ptr,
                                                                             thrift_column_data);
            }
            break;
        }
        default: {
            LOG(WARNING) << "can't convert this type to thrift . type = "
                         << _output_vexpr_ctxs[i]->root()->type();
            return Status::InternalError("vec block pack thrift failed.");
        }
        }
        thrift_columns.push_back(thrift_column_data);
        if (!status) {
            LOG(WARNING) << "convert row to thrift result failed.";
            return status;
        }
    }
    thrift_row_batch.__set_cols(thrift_columns);
    result->result_batch.__set_thrift_row_batch(thrift_row_batch);
    // push this batch to back
    status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += input_block.rows();
    } else {
        LOG(WARNING) << "append result batch to sink failed.";
    }
    return status;
}

Status VThriftResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
