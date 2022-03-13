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

#include "runtime/mysql_result_writer.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/primitive_type.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/date_func.h"
#include "util/mysql_row_buffer.h"
#include "util/types.h"

#include "vec/core/block.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker,
                                     const std::vector<ExprContext*>& output_expr_ctxs,
                                     RuntimeProfile* parent_profile, bool output_object_data)
        : ResultWriter(output_object_data),
          _sinker(sinker),
          _output_expr_ctxs(output_expr_ctxs),
          _row_buffer(nullptr),
          _parent_profile(parent_profile) {}

MysqlResultWriter::~MysqlResultWriter() {
    delete _row_buffer;
}

Status MysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr pointer.");
    }

    _row_buffer = new (std::nothrow) MysqlRowBuffer();
    if (nullptr == _row_buffer) {
        return Status::InternalError("no memory to alloc.");
    }

    return Status::OK();
}

void MysqlResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

int MysqlResultWriter::_add_row_value(int index, const TypeDescriptor& type, void* item) {
    int buf_ret = 0;
    if (item == nullptr) {
        return _row_buffer->push_null();
    }

    switch (type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        buf_ret = _row_buffer->push_tinyint(*static_cast<int8_t*>(item));
        break;

    case TYPE_SMALLINT:
        buf_ret = _row_buffer->push_smallint(*static_cast<int16_t*>(item));
        break;

    case TYPE_INT:
        buf_ret = _row_buffer->push_int(*static_cast<int32_t*>(item));
        break;

    case TYPE_BIGINT:
        buf_ret = _row_buffer->push_bigint(*static_cast<int64_t*>(item));
        break;

    case TYPE_LARGEINT: {
        buf_ret = _row_buffer->push_largeint(reinterpret_cast<const PackedInt128*>(item)->value);
        break;
    }

    case TYPE_FLOAT:
        buf_ret = _row_buffer->push_float(*static_cast<float*>(item));
        break;

    case TYPE_DOUBLE:
        buf_ret = _row_buffer->push_double(*static_cast<double*>(item));
        break;

    case TYPE_TIME: {
        buf_ret = _row_buffer->push_time(*static_cast<double*>(item));
        break;
    }

    case TYPE_DATE:
    case TYPE_DATETIME: {
        buf_ret = _row_buffer->push_datetime(*static_cast<DateTimeValue*>(item));
        break;
    }

    case TYPE_HLL:
    case TYPE_OBJECT: {
        if (_output_object_data) {
            const StringValue* string_val = (const StringValue*)(item);

            if (string_val->ptr == nullptr) {
                buf_ret = _row_buffer->push_null();
            } else {
                buf_ret = _row_buffer->push_string(string_val->ptr, string_val->len);
            }
        } else {
            buf_ret = _row_buffer->push_null();
        }

        break;
    }

    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        const StringValue* string_val = (const StringValue*)(item);

        if (string_val->ptr == nullptr) {
            if (string_val->len == 0) {
                // 0x01 is a magic num, not useful actually, just for present ""
                char* tmp_val = reinterpret_cast<char*>(0x01);
                buf_ret = _row_buffer->push_string(tmp_val, string_val->len);
            } else {
                buf_ret = _row_buffer->push_null();
            }
        } else {
            buf_ret = _row_buffer->push_string(string_val->ptr, string_val->len);
        }

        break;
    }

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
        // TODO: Support decimal output_scale after we support FE can sure
        // accuracy of output_scale
        // int output_scale = _output_expr_ctxs[index]->root()->output_scale();
        buf_ret = _row_buffer->push_decimal(decimal_val, -1);
        break;
    }

    case TYPE_ARRAY: {
        auto children_type = type.children[0].type;
        auto array_value = (const CollectionValue*)(item);

        ArrayIterator iter = array_value->iterator(children_type);

        _row_buffer->open_dynamic_mode();

        buf_ret = _row_buffer->push_string("[", 1);

        int begin = 0;
        while (iter.has_next() && !buf_ret) {
            if (begin != 0) {
                buf_ret = _row_buffer->push_string(", ", 2);
            }

            if (children_type == TYPE_CHAR || children_type == TYPE_VARCHAR) {
                buf_ret = _row_buffer->push_string("'", 1);
                buf_ret = _add_row_value(index, children_type, iter.value());
                buf_ret = _row_buffer->push_string("'", 1);
            } else {
                buf_ret = _add_row_value(index, children_type, iter.value());
            }

            iter.next();
            begin++;
        }

        if (!buf_ret) {
            buf_ret = _row_buffer->push_string("]", 1);
        }

        _row_buffer->close_dynamic_mode();
        break;
    }

    default:
        LOG(WARNING) << "can't convert this type to mysql type. type = "
                     << _output_expr_ctxs[index]->root()->type();
        buf_ret = -1;
        break;
    }

    return buf_ret;
}

Status MysqlResultWriter::_add_one_row(TupleRow* row) {
    _row_buffer->reset();
    int num_columns = _output_expr_ctxs.size();
    int buf_ret = 0;

    for (int i = 0; 0 == buf_ret && i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);

        buf_ret = _add_row_value(i, _output_expr_ctxs[i]->root()->type(), item);
    }

    if (0 != buf_ret) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

Status MysqlResultWriter::append_row_batch(const RowBatch* batch) {
    SCOPED_TIMER(_append_row_batch_timer);
    if (nullptr == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }

    Status status;
    // convert one batch
    std::unique_ptr<TFetchDataResult> result = std::make_unique<TFetchDataResult>();
    int num_rows = batch->num_rows();
    result->result_batch.rows.resize(num_rows);

    {
        SCOPED_TIMER(_convert_tuple_timer);
        for (int i = 0; status.ok() && i < num_rows; ++i) {
            TupleRow* row = batch->get_row(i);
            status = _add_one_row(row);

            if (status.ok()) {
                result->result_batch.rows[i].assign(_row_buffer->buf(), _row_buffer->length());
            } else {
                LOG(WARNING) << "convert row to mysql result failed.";
                break;
            }
        }
    }

    if (status.ok()) {
        SCOPED_TIMER(_result_send_timer);
        // push this batch to back
        RETURN_NOT_OK_STATUS_WITH_WARN(_sinker->add_batch(result),
                                       "fappend result batch to sink failed.");
        _written_rows += num_rows;
    }
    return Status::OK();
}

Status MysqlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace doris
