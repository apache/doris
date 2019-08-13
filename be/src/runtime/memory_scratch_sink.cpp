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

#include "runtime/memory_scratch_sink.h"

#include <sstream>

#include "common/logging.h"
#include "exprs/expr.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/row_batch.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/date_func.h"
#include "util/types.h"

namespace doris {

MemoryScratchSink::MemoryScratchSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr,
                       const TMemoryScratchSink& sink) : _row_desc(row_desc),_t_output_expr(t_output_expr) {
}

MemoryScratchSink::~MemoryScratchSink() {
}

Status MemoryScratchSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(
            state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(
            _output_expr_ctxs, state, _row_desc, _expr_mem_tracker.get()));
    return Status::OK();
}

Status MemoryScratchSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));
    // create queue
    TUniqueId fragment_instance_id = state->fragment_instance_id();
    state->exec_env()->result_queue_mgr()->create_queue(fragment_instance_id, &_queue);
    std::stringstream title;
    title << "MemoryScratchSink (frag_id=" << fragment_instance_id << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(state->obj_pool(), title.str()));

    return Status::OK();
}

Status MemoryScratchSink::send(RuntimeState* state, RowBatch* batch) {
    if (NULL == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }
    Status status;
    // convert one batch
    std::shared_ptr<TScanRowBatch> t_scan_row_batch = std::make_shared<TScanRowBatch>();
    t_scan_row_batch->__set_num_rows(batch->num_rows());
    int num_columns = _output_expr_ctxs.size();
    std::vector<TScanColumnData> cols;
    for (int i =0 ; i < num_columns; i++) {
        TScanColumnData all_col_data;
        cols.push_back(all_col_data);
    }
    t_scan_row_batch->__set_cols(std::move(cols));
    for (int i = 0; status.ok() && i < batch->num_rows(); ++i) {
        TupleRow* row = batch->get_row(i);
        status = add_per_col(state, row, t_scan_row_batch);
        if (!status.ok()) {
            return Status::InternalError("convert row failed");
        }
    }
    _queue->blocking_put(t_scan_row_batch);
    return Status::OK();
}

// add all col data for TScanRowBatch
Status MemoryScratchSink::add_per_col(RuntimeState* state, TupleRow* row, std::shared_ptr<TScanRowBatch> result) {
    int num_cols = _output_expr_ctxs.size();
    for (int i = 0; i < num_cols; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);
        if (item == nullptr) {
            result->cols[i].is_null.push_back(true);
            continue;
        } else {
            result->cols[i].is_null.push_back(false);
        }
        switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                result->cols[i].__isset.byte_vals = true;
                result->cols[i].byte_vals.push_back(*static_cast<int8_t*>(item));
                break;
            case TYPE_SMALLINT:
                result->cols[i].__isset.short_vals = true;
                result->cols[i].short_vals.push_back(*static_cast<int16_t*>(item));
                break;
            case TYPE_INT:
                result->cols[i].__isset.int_vals = true;
                result->cols[i].int_vals.push_back(*static_cast<int32_t*>(item));
                break;
            case TYPE_BIGINT:
                result->cols[i].__isset.long_vals = true;
                result->cols[i].long_vals.push_back(*static_cast<int64_t*>(item));
                break;
            case TYPE_LARGEINT: {
                char buf[48];
                int len = 48;
                char* v = LargeIntValue::to_string(reinterpret_cast<const PackedInt128*>(item)->value, buf, &len);
                std::string temp(v, len);
                result->cols[i].__isset.string_vals = true;
                result->cols[i].string_vals.push_back(std::move(temp));
                break;
            }
            case TYPE_FLOAT:
                result->cols[i].__isset.double_vals = true;
                result->cols[i].double_vals.push_back(*static_cast<float*>(item));
                break;
            case TYPE_DOUBLE:
                result->cols[i].__isset.double_vals = true;
                result->cols[i].double_vals.push_back(*static_cast<double*>(item));         
                break;
            case TYPE_TIME: {
                double time = *static_cast<double *>(item);
                std::string time_str = time_str_from_int((int) time);
                result->cols[i].__isset.string_vals = true;
                result->cols[i].string_vals.push_back(std::move(time_str));
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                int64_t ts = 0;
                if (time_val->unix_timestamp(&ts, state->timezone())) {
                    result->cols[i].__isset.long_vals = true;
                    result->cols[i].long_vals.push_back(ts);
                } else {
                    result->cols[i].is_null.back() = true;
                }
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_HLL:
            case TYPE_CHAR: {
                const StringValue* string_val = (const StringValue*)(item);
                if (string_val->ptr == NULL) {
                    if (string_val->len == 0) {
                        // 0x01 is a magic num, not usefull actually, just for present ""
                        //char* tmp_val = reinterpret_cast<char*>(0x01);
                        result->cols[i].__isset.string_vals = true;
                        result->cols[i].string_vals.push_back("");          
                    } else {
                        result->cols[i].is_null[i] = true;
                    }
                } else {
                    result->cols[i].__isset.string_vals = true;
                    result->cols[i].string_vals.push_back(std::move(string_val->to_string()));
                }
                break;
            }
            case TYPE_DECIMAL: {
                result->cols[i].__isset.string_vals = true;
                const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val->to_string(output_scale);
                } else {
                    decimal_str = decimal_val->to_string();
                }
                result->cols[i].string_vals.push_back(std::move(decimal_str));          
                break;
            }
            case TYPE_DECIMALV2: {
                result->cols[i].__isset.string_vals = true;
                DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();

                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val.to_string(output_scale);
                } else {
                    decimal_str = decimal_val.to_string();
                }
                result->cols[i].string_vals.push_back(std::move(decimal_str));          
                break;
            }
            default:{
                LOG(WARNING) << "can't convert this type. type = " <<
                            _output_expr_ctxs[i]->root()->type();
                return Status::InternalError("unsupported column type");
            }
        }
    }
    return Status::OK();
}

Status MemoryScratchSink::open(RuntimeState* state) {
    return Expr::open(_output_expr_ctxs, state);
}

Status MemoryScratchSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    // shutdown queue, then blocking_get return false, put sentinel
    if (_queue) {
        _queue->blocking_put(nullptr);
    }
    Expr::close(_output_expr_ctxs, state);
    _closed = true;
    return Status::OK();
}

}
