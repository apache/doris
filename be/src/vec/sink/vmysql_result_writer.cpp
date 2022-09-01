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

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
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
        : _sinker(sinker), _output_vexpr_ctxs(output_vexpr_ctxs), _parent_profile(parent_profile) {}

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

    if constexpr (type == TYPE_OBJECT || type == TYPE_VARCHAR) {
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
            const auto& [json, status] = _convert_array_to_json_string(
                    column_array, offsets[i - 1], offsets[i] - offsets[i - 1], nested_type_ptr);
            if (status.ok()) {
                buf_ret = _buffer.push_string(json.c_str(), json.length());
                _buffer.close_dynamic_mode();
                result->result_batch.rows[i].append(_buffer.buf(), _buffer.length());
            } else {
                _buffer.close_dynamic_mode();
                buf_ret = -1;
            }
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
                auto decimal_str = decimal_val.to_string();
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

const std::pair<std::string, Status> VMysqlResultWriter::_convert_array_to_json_string(
        const ColumnArray& column_array, uint64_t start, uint64_t size,
        const DataTypePtr& nested_type_ptr) {
    std::ostringstream out;
    out << "[";
    bool is_first = true;
    for (uint64_t i = 0, offset = start + i; i < size; ++i, ++offset) {
        if (is_first) {
            is_first = false;
        } else {
            out << ", ";
        }
        auto& data_ptr = column_array.get_data_ptr();
        if (data_ptr->is_null_at(offset)) {
            out << "null";
            continue;
        }

        const auto& column_item = remove_nullable(data_ptr);
        WhichDataType which(remove_nullable(nested_type_ptr));
        if (which.is_uint8()) {
            auto value = assert_cast<const ColumnUInt8&>(*column_item).get_data()[offset];
            out << (value ? "true" : "false");
        } else if (which.is_int8()) {
            auto value = assert_cast<const ColumnInt8&>(*column_item).get_data()[offset];
            out << static_cast<int>(value);
        } else if (which.is_int16()) {
            auto value = assert_cast<const ColumnInt16&>(*column_item).get_data()[offset];
            out << value;
        } else if (which.is_int32()) {
            auto value = assert_cast<const ColumnInt32&>(*column_item).get_data()[offset];
            out << value;
        } else if (which.is_int64()) {
            auto value = assert_cast<const ColumnInt64&>(*column_item).get_data()[offset];
            out << value;
        } else if (which.is_int128()) {
            auto value = assert_cast<const ColumnInt128&>(*column_item).get_data()[offset];
            out << std::quoted(LargeIntValue::to_string(value));
        } else if (which.is_float32()) {
            auto value = assert_cast<const ColumnFloat32&>(*column_item).get_data()[offset];
            out << value;
        } else if (which.is_float64()) {
            auto value = assert_cast<const ColumnFloat64&>(*column_item).get_data()[offset];
            out << value;
        } else if (which.is_decimal128()) {
            auto value = assert_cast<const DataTypeDecimal<Decimal128>&>(
                                 *remove_nullable(nested_type_ptr))
                                 .to_string(*column_item, offset);
            out << std::quoted(value);
        } else if (which.is_date_or_datetime()) {
            auto value = assert_cast<const ColumnVector<Int64>&>(*column_item)[offset].get<Int64>();
            VecDateTimeValue datetime;
            memcpy(static_cast<void*>(&datetime), static_cast<void*>(&value), sizeof(value));
            if (which.is_date()) {
                datetime.cast_to_date();
            }
            char buf[64];
            datetime.to_string(buf);
            out << std::quoted(buf);
        } else if (which.is_string()) {
            const auto string_val = column_item->get_data_at(offset);
            if (string_val.size == 0) {
                out << R"("")";
            } else {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                writer.String(string_val.data, string_val.size);
                out << buffer.GetString();
            }
        } else if (which.is_array()) {
            const auto& sub_column_array = assert_cast<const ColumnArray&>(*column_item);
            const auto& sub_offsets = sub_column_array.get_offsets();
            const auto& [json, status] = _convert_array_to_json_string(
                    sub_column_array, sub_offsets[offset - 1],
                    sub_offsets[offset] - sub_offsets[offset - 1],
                    assert_cast<const DataTypeArray&>(*remove_nullable(nested_type_ptr))
                            .get_nested_type());
            if (status.ok()) {
                out << json;
            } else {
                return {json, status};
            }
        } else {
            LOG(WARNING) << "sub TypeIndex(" << (int)which.idx << "not supported yet";
            return {"", Status::InternalError("Invalid type")};
        }
    }
    out << "]";
    return {out.str(), Status::OK()};
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
                                                                              nested_type);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMALV2, false>(column_ptr, result,
                                                                               type_ptr);
            }
            break;
        }
        case TYPE_DECIMAL32: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL32, true>(column_ptr, result,
                                                                              nested_type);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL32, false>(column_ptr, result,
                                                                               type_ptr);
            }
            break;
        }
        case TYPE_DECIMAL64: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL64, true>(column_ptr, result,
                                                                              nested_type);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL64, false>(column_ptr, result,
                                                                               type_ptr);
            }
            break;
        }
        case TYPE_DECIMAL128: {
            if (type_ptr->is_nullable()) {
                auto& nested_type =
                        assert_cast<const DataTypeNullable&>(*type_ptr).get_nested_type();
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL128, true>(column_ptr, result,
                                                                               nested_type);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMAL128, false>(column_ptr, result,
                                                                                type_ptr);
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
            int scale = _output_vexpr_ctxs[i]->root()->type().scale;
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
