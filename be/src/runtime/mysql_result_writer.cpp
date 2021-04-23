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

namespace doris {

MysqlResultWriter::MysqlResultWriter(BufferControlBlock* sinker,
                                     const std::vector<ExprContext*>& output_expr_ctxs,
                                     RuntimeProfile* parent_profile)
        : _sinker(sinker),
          _output_expr_ctxs(output_expr_ctxs),
          _row_buffer(NULL),
          _parent_profile(parent_profile) {}

MysqlResultWriter::~MysqlResultWriter() {
    delete _row_buffer;
}

Status MysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (NULL == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _row_buffer = new (std::nothrow) MysqlRowBuffer();

    if (NULL == _row_buffer) {
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

Status MysqlResultWriter::_add_one_row(TupleRow* row) {
    _row_buffer->reset();
    int num_columns = _output_expr_ctxs.size();
    int buf_ret = 0;

    for (int i = 0; 0 == buf_ret && i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);

        if (NULL == item) {
            buf_ret = _row_buffer->push_null();
            continue;
        }

        switch (_output_expr_ctxs[i]->root()->type().type) {
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
            char buf[48];
            int len = 48;
            char* v = LargeIntValue::to_string(reinterpret_cast<const PackedInt128*>(item)->value,
                                               buf, &len);
            buf_ret = _row_buffer->push_string(v, len);
            break;
        }

        case TYPE_FLOAT:
            buf_ret = _row_buffer->push_float(*static_cast<float*>(item));
            break;

        case TYPE_DOUBLE:
            buf_ret = _row_buffer->push_double(*static_cast<double*>(item));
            break;

        case TYPE_TIME: {
            double time = *static_cast<double*>(item);
            std::string time_str = time_str_from_double(time);
            buf_ret = _row_buffer->push_string(time_str.c_str(), time_str.size());
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            char buf[64];
            const DateTimeValue* time_val = (const DateTimeValue*)(item);
            // TODO(zhaochun), this function has core risk
            char* pos = time_val->to_string(buf);
            buf_ret = _row_buffer->push_string(buf, pos - buf - 1);
            break;
        }

        case TYPE_HLL:
        case TYPE_OBJECT: {
            buf_ret = _row_buffer->push_null();
            break;
        }

        case TYPE_VARCHAR:
        case TYPE_CHAR: {
            const StringValue* string_val = (const StringValue*)(item);

            if (string_val->ptr == NULL) {
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

        case TYPE_DECIMAL: {
            const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
            std::string decimal_str;
            int output_scale = _output_expr_ctxs[i]->root()->output_scale();

            if (output_scale > 0 && output_scale <= 30) {
                decimal_str = decimal_val->to_string(output_scale);
            } else {
                decimal_str = decimal_val->to_string();
            }

            buf_ret = _row_buffer->push_string(decimal_str.c_str(), decimal_str.length());
            break;
        }

        case TYPE_DECIMALV2: {
            DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
            std::string decimal_str;
            int output_scale = _output_expr_ctxs[i]->root()->output_scale();

            if (output_scale > 0 && output_scale <= 30) {
                decimal_str = decimal_val.to_string(output_scale);
            } else {
                decimal_str = decimal_val.to_string();
            }

            buf_ret = _row_buffer->push_string(decimal_str.c_str(), decimal_str.length());
            break;
        }

        default:
            LOG(WARNING) << "can't convert this type to mysql type. type = "
                         << _output_expr_ctxs[i]->root()->type();
            buf_ret = -1;
            break;
        }
    }

    if (0 != buf_ret) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

Status MysqlResultWriter::append_row_batch(const RowBatch* batch) {
    SCOPED_TIMER(_append_row_batch_timer);
    if (NULL == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }

    Status status;
    // convert one batch
    TFetchDataResult* result = new (std::nothrow) TFetchDataResult();
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
        status = _sinker->add_batch(result);

        if (status.ok()) {
            result = NULL;
            _written_rows += num_rows;
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }

    delete result;
    result = NULL;

    return status;
}

Status MysqlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace doris
