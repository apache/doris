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

#include "vec/sink/vmysql_result_writer.h"

#include "runtime/buffer_control_block.h"
#include "runtime/jsonb_value.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
VMysqlResultWriter::VMysqlResultWriter(BufferControlBlock* sinker,
                                       const std::vector<VExprContext*>& output_vexpr_ctxs,
                                       RuntimeProfile* parent_profile)
        : VResultWriter(),
          _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

Status VMysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    return Status::OK();
}

void VMysqlResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

template <PrimitiveType type, bool is_nullable>
Status VMysqlResultWriter::_add_one_column(const ColumnPtr& column_ptr,
                                           std::unique_ptr<TFetchDataResult>& result,
                                           const DataTypePtr& nested_type_ptr, int scale) {
    SCOPED_TIMER(_convert_tuple_timer);

    const auto row_size = column_ptr->size();

    doris::vectorized::ColumnPtr column;
    if constexpr (is_nullable) {
        column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
    } else {
        column = column_ptr;
    }

    MysqlRowBuffer _buffer;
    int buf_ret = 0;

    if constexpr (type == TYPE_OBJECT || type == TYPE_VARCHAR || type == TYPE_JSONB) {
        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }
            _buffer.reset();

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(i)) {
                    buf_ret = _buffer.push_null();
                    result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
                    continue;
                }
            }

            if constexpr (type == TYPE_OBJECT) {
                buf_ret = _buffer.push_null();
            }
            if constexpr (type == TYPE_VARCHAR) {
                const auto string_val = column->get_data_at(i);

                if (string_val.data == nullptr) {
                    if (string_val.size == 0) {
                        // 0x01 is a magic num, not useful actually, just for present ""
                        char* tmp_val = reinterpret_cast<char*>(0x01);
                        buf_ret = _buffer.push_string(tmp_val, string_val.size);
                    } else {
                        buf_ret = _buffer.push_null();
                    }
                } else {
                    buf_ret = _buffer.push_string(string_val.data, string_val.size);
                }
            }
            if constexpr (type == TYPE_JSONB) {
                const auto json_val = column->get_data_at(i);
                if (json_val.data == nullptr) {
                    if (json_val.size == 0) {
                        // 0x01 is a magic num, not useful actually, just for present ""
                        char* tmp_val = reinterpret_cast<char*>(0x01);
                        buf_ret = _buffer.push_string(tmp_val, json_val.size);
                    } else {
                        buf_ret = _buffer.push_null();
                    }
                } else {
                    std::string json_str =
                            JsonbToJson::jsonb_to_json_string(json_val.data, json_val.size);
                    buf_ret = _buffer.push_string(json_str.c_str(), json_str.size());
                }
            }

            result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
        }
    } else if constexpr (type == TYPE_ARRAY) {
        auto& column_array = assert_cast<const ColumnArray&>(*column);
        auto& offsets = column_array.get_offsets();
        for (ssize_t i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }
            _buffer.reset();

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(i)) {
                    buf_ret = _buffer.push_null();
                    result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
                    continue;
                }
            }

            _buffer.open_dynamic_mode();
            buf_ret = _buffer.push_string("[", 1);
            bool begin = true;
            for (auto j = offsets[i - 1]; j < offsets[i]; ++j) {
                if (!begin) {
                    buf_ret = _buffer.push_string(", ", 2);
                }
                const auto& data = column_array.get_data_ptr();
                if (data->is_null_at(j)) {
                    buf_ret = _buffer.push_string("NULL", strlen("NULL"));
                } else {
                    if (WhichDataType(remove_nullable(nested_type_ptr)).is_string()) {
                        buf_ret = _buffer.push_string("'", 1);
                        buf_ret = _add_one_cell(data, j, nested_type_ptr, _buffer);
                        buf_ret = _buffer.push_string("'", 1);
                    } else {
                        buf_ret = _add_one_cell(data, j, nested_type_ptr, _buffer);
                    }
                }
                begin = false;
            }
            buf_ret = _buffer.push_string("]", 1);
            _buffer.close_dynamic_mode();
            result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
        }
    } else if constexpr (type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                         type == TYPE_DECIMAL128) {
        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }
            _buffer.reset();

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(i)) {
                    buf_ret = _buffer.push_null();
                    result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
                    continue;
                }
            }
            std::string decimal_str = nested_type_ptr->to_string(*column, i);
            buf_ret = _buffer.push_string(decimal_str.c_str(), decimal_str.length());
            result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
        }
    } else {
        using ColumnType = typename PrimitiveTypeTraits<type>::ColumnType;
        auto& data = assert_cast<const ColumnType&>(*column).get_data();

        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }
            _buffer.reset();

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(i)) {
                    buf_ret = _buffer.push_null();
                    result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
                    continue;
                }
            }

            if constexpr (type == TYPE_BOOLEAN) {
                //todo here need to using uint after MysqlRowBuffer support it
                buf_ret = _buffer.push_tinyint(data[i]);
            }
            if constexpr (type == TYPE_TINYINT) {
                buf_ret = _buffer.push_tinyint(data[i]);
            }
            if constexpr (type == TYPE_SMALLINT) {
                buf_ret = _buffer.push_smallint(data[i]);
            }
            if constexpr (type == TYPE_INT) {
                buf_ret = _buffer.push_int(data[i]);
            }
            if constexpr (type == TYPE_BIGINT) {
                buf_ret = _buffer.push_bigint(data[i]);
            }
            if constexpr (type == TYPE_LARGEINT) {
                auto v = LargeIntValue::to_string(data[i]);
                buf_ret = _buffer.push_string(v.c_str(), v.size());
            }
            if constexpr (type == TYPE_FLOAT) {
                buf_ret = _buffer.push_float(data[i]);
            }
            if constexpr (type == TYPE_DOUBLE) {
                buf_ret = _buffer.push_double(data[i]);
            }
            if constexpr (type == TYPE_TIME || type == TYPE_TIMEV2) {
                buf_ret = _buffer.push_time(data[i]);
            }
            if constexpr (type == TYPE_DATETIME) {
                char buf[64];
                auto time_num = data[i];
                VecDateTimeValue time_val;
                memcpy(static_cast<void*>(&time_val), &time_num, sizeof(Int64));
                // TODO(zhaochun), this function has core risk
                char* pos = time_val.to_string(buf);
                buf_ret = _buffer.push_string(buf, pos - buf - 1);
            }
            if constexpr (type == TYPE_DATEV2) {
                char buf[64];
                auto time_num = data[i];
                doris::vectorized::DateV2Value<DateV2ValueType> date_val;
                memcpy(static_cast<void*>(&date_val), &time_num, sizeof(UInt32));
                char* pos = date_val.to_string(buf);
                buf_ret = _buffer.push_string(buf, pos - buf - 1);
            }
            if constexpr (type == TYPE_DATETIMEV2) {
                char buf[64];
                auto time_num = data[i];
                doris::vectorized::DateV2Value<DateTimeV2ValueType> date_val;
                memcpy(static_cast<void*>(&date_val), &time_num, sizeof(UInt64));
                char* pos = date_val.to_string(buf, scale);
                buf_ret = _buffer.push_string(buf, pos - buf - 1);
            }
            if constexpr (type == TYPE_DECIMALV2) {
                DecimalV2Value decimal_val(data[i]);
                auto decimal_str = decimal_val.to_string(scale);
                buf_ret = _buffer.push_string(decimal_str.c_str(), decimal_str.length());
            }

            result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
        }
    }
    if (0 != buf_ret) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

int VMysqlResultWriter::_add_one_cell(const ColumnPtr& column_ptr, size_t row_idx,
                                      const DataTypePtr& type, MysqlRowBuffer& buffer) {
    WhichDataType which(type->get_type_id());
    if (which.is_nullable() && column_ptr->is_null_at(row_idx)) {
        return buffer.push_null();
    }

    ColumnPtr column;
    if (which.is_nullable()) {
        column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
        which = WhichDataType(assert_cast<const DataTypeNullable&>(*type).get_nested_type());
    } else {
        column = column_ptr;
    }

    if (which.is_uint8()) {
        auto& data = assert_cast<const ColumnUInt8&>(*column).get_data();
        return buffer.push_tinyint(data[row_idx]);
    } else if (which.is_int8()) {
        auto& data = assert_cast<const ColumnInt8&>(*column).get_data();
        return buffer.push_tinyint(data[row_idx]);
    } else if (which.is_int16()) {
        auto& data = assert_cast<const ColumnInt16&>(*column).get_data();
        return buffer.push_smallint(data[row_idx]);
    } else if (which.is_int32()) {
        auto& data = assert_cast<const ColumnInt32&>(*column).get_data();
        return buffer.push_int(data[row_idx]);
    } else if (which.is_int64()) {
        auto& data = assert_cast<const ColumnInt64&>(*column).get_data();
        return buffer.push_bigint(data[row_idx]);
    } else if (which.is_int128()) {
        auto& data = assert_cast<const ColumnInt128&>(*column).get_data();
        auto v = LargeIntValue::to_string(data[row_idx]);
        return buffer.push_string(v.c_str(), v.size());
    } else if (which.is_float32()) {
        auto& data = assert_cast<const ColumnFloat32&>(*column).get_data();
        return buffer.push_float(data[row_idx]);
    } else if (which.is_float64()) {
        auto& data = assert_cast<const ColumnFloat64&>(*column).get_data();
        return buffer.push_double(data[row_idx]);
    } else if (which.is_string()) {
        int buf_ret = 0;
        const auto string_val = column->get_data_at(row_idx);
        if (string_val.data == nullptr) {
            if (string_val.size == 0) {
                // 0x01 is a magic num, not useful actually, just for present ""
                char* tmp_val = reinterpret_cast<char*>(0x01);
                buf_ret = buffer.push_string(tmp_val, string_val.size);
            } else {
                buf_ret = buffer.push_null();
            }
        } else {
            buf_ret = buffer.push_string(string_val.data, string_val.size);
        }
        return buf_ret;
    } else if (which.is_date_or_datetime()) {
        auto& column_vector = assert_cast<const ColumnVector<Int64>&>(*column);
        auto value = column_vector[row_idx].get<Int64>();
        VecDateTimeValue datetime;
        memcpy(static_cast<void*>(&datetime), static_cast<void*>(&value), sizeof(value));
        if (which.is_date()) {
            datetime.cast_to_date();
        }
        char buf[64];
        char* pos = datetime.to_string(buf);
        return buffer.push_string(buf, pos - buf - 1);
    } else if (which.is_date_v2()) {
        auto& column_vector = assert_cast<const ColumnVector<UInt32>&>(*column);
        auto value = column_vector[row_idx].get<UInt32>();
        DateV2Value<DateV2ValueType> datev2;
        memcpy(static_cast<void*>(&datev2), static_cast<void*>(&value), sizeof(value));
        char buf[64];
        char* pos = datev2.to_string(buf);
        return buffer.push_string(buf, pos - buf - 1);
    } else if (which.is_decimal32()) {
        DataTypePtr nested_type = type;
        if (type->is_nullable()) {
            nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
        }
        auto decimal_str = assert_cast<const DataTypeDecimal<Decimal32>*>(nested_type.get())
                                   ->to_string(*column, row_idx);
        return buffer.push_string(decimal_str.c_str(), decimal_str.length());
    } else if (which.is_decimal64()) {
        DataTypePtr nested_type = type;
        if (type->is_nullable()) {
            nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
        }
        auto decimal_str = assert_cast<const DataTypeDecimal<Decimal64>*>(nested_type.get())
                                   ->to_string(*column, row_idx);
        return buffer.push_string(decimal_str.c_str(), decimal_str.length());
    } else if (which.is_decimal128()) {
        if (config::enable_decimalv3) {
            DataTypePtr nested_type = type;
            if (type->is_nullable()) {
                nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
            }
            auto decimal_str = assert_cast<const DataTypeDecimal<Decimal128>*>(nested_type.get())
                                       ->to_string(*column, row_idx);
            return buffer.push_string(decimal_str.c_str(), decimal_str.length());
        } else {
            auto& column_data =
                    static_cast<const ColumnDecimal<vectorized::Decimal128>&>(*column).get_data();
            DecimalV2Value decimal_val(column_data[row_idx]);
            auto decimal_str = decimal_val.to_string();
            return buffer.push_string(decimal_str.c_str(), decimal_str.length());
        }
    } else if (which.is_array()) {
        auto& column_array = assert_cast<const ColumnArray&>(*column);
        auto& offsets = column_array.get_offsets();
        DataTypePtr sub_type;
        if (type->is_nullable()) {
            auto& nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
            sub_type = assert_cast<const DataTypeArray&>(*nested_type).get_nested_type();
        } else {
            sub_type = assert_cast<const DataTypeArray&>(*type).get_nested_type();
        }

        int start = offsets[row_idx - 1];
        int length = offsets[row_idx] - start;
        const auto& data = column_array.get_data_ptr();

        int buf_ret = buffer.push_string("[", strlen("["));
        bool begin = true;
        for (int i = 0; i < length; ++i) {
            int position = start + i;
            if (begin) {
                begin = false;
            } else {
                buf_ret = buffer.push_string(", ", strlen(", "));
            }
            if (data->is_null_at(position)) {
                buf_ret = buffer.push_string("NULL", strlen("NULL"));
            } else {
                buf_ret = _add_one_cell(data, position, sub_type, buffer);
            }
        }
        buf_ret = buffer.push_string("]", strlen("]"));
        return buf_ret;
    } else {
        LOG(WARNING) << "sub TypeIndex(" << (int)which.idx << "not supported yet";
        return -1;
    }
}

Status VMysqlResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::RuntimeError("Not Implemented MysqlResultWriter::append_row_batch scalar");
}

Status VMysqlResultWriter::append_block(Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    auto block = VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs, input_block,
                                                                    status);
    auto num_rows = block.rows();
    if (UNLIKELY(num_rows == 0)) {
        return status;
    }

    // convert one batch
    auto result = std::make_unique<TFetchDataResult>();
    result->result_batch.rows.resize(num_rows);
    for (int i = 0; status.ok() && i < _output_vexpr_ctxs.size(); ++i) {
        auto column_ptr = block.get_by_position(i).column->convert_to_full_column_if_const();
        auto type_ptr = block.get_by_position(i).type;

        int scale = _output_vexpr_ctxs[i]->root()->type().scale;
        switch (_output_vexpr_ctxs[i]->root()->result_type()) {
        case TYPE_BOOLEAN:
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_BOOLEAN, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_BOOLEAN, false>(column_ptr, result);
            }
            break;
        case TYPE_TINYINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_TINYINT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_TINYINT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_SMALLINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_SMALLINT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_SMALLINT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_INT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_INT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_INT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_BIGINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_BIGINT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_BIGINT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_LARGEINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_LARGEINT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_LARGEINT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_FLOAT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_FLOAT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_FLOAT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_DOUBLE: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DOUBLE, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DOUBLE, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_TIME: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_TIME, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_TIME, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_TIMEV2: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_TIMEV2, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_TIMEV2, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_DECIMALV2: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMALV2, true>(column_ptr, result,
                                                                              nested_type, scale);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMALV2, false>(column_ptr, result,
                                                                               type_ptr, scale);
            }
            break;
        }
        case TYPE_DECIMAL32: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL32, true>(column_ptr, result,
                                                                              nested_type, scale);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL32, false>(column_ptr, result,
                                                                               type_ptr, scale);
            }
            break;
        }
        case TYPE_DECIMAL64: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL64, true>(column_ptr, result,
                                                                              nested_type, scale);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL64, false>(column_ptr, result,
                                                                               type_ptr, scale);
            }
            break;
        }
        case TYPE_DECIMAL128: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL128, true>(column_ptr, result,
                                                                               nested_type, scale);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL128, false>(column_ptr, result,
                                                                                type_ptr, scale);
            }
            break;
        }
        case TYPE_JSONB: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_JSONB, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_JSONB, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_DATE:
        case TYPE_DATETIME: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DATETIME, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DATETIME, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_DATEV2: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DATEV2, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DATEV2, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_DATETIMEV2: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DATETIMEV2, true>(column_ptr, result,
                                                                               nullptr, scale);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DATETIMEV2, false>(column_ptr, result,
                                                                                nullptr, scale);
            }
            break;
        }
        case TYPE_HLL:
        case TYPE_OBJECT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_OBJECT, true>(column_ptr, result);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_OBJECT, false>(column_ptr, result);
            }
            break;
        }
        case TYPE_ARRAY: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                auto& sub_type = assert_cast<const DataTypeArray&>(*nested_type).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_ARRAY, true>(column_ptr, result,
                                                                          sub_type);
            } else {
                auto& sub_type = assert_cast<const DataTypeArray&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_ARRAY, false>(column_ptr, result,
                                                                           sub_type);
            }
            break;
        }
        default: {
            LOG(WARNING) << "can't convert this type to mysql type. type = "
                         << _output_vexpr_ctxs[i]->root()->type();
            return Status::InternalError("vec block pack mysql buffer failed.");
        }
        }

        if (!status) {
            LOG(WARNING) << "convert row to mysql result failed.";
            break;
        }
    }
    if (status) {
        SCOPED_TIMER(_result_send_timer);
        // push this batch to back
        status = _sinker->add_batch(result);

        if (status.ok()) {
            _written_rows += num_rows;
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }

    return status;
}

Status VMysqlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
