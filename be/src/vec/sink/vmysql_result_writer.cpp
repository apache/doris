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

#include <fmt/core.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "gutil/integral_types.h"
#include "olap/hll.h"
#include "runtime/buffer_control_block.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/bitmap_value.h"
#include "util/jsonb_utils.h"
#include "util/quantile_state.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {

template <bool is_binary_format>
VMysqlResultWriter<is_binary_format>::VMysqlResultWriter(BufferControlBlock* sinker,
                                                         const VExprContextSPtrs& output_vexpr_ctxs,
                                                         RuntimeProfile* parent_profile)
        : ResultWriter(),
          _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    set_output_object_data(state->return_object_data_as_binary());
    _is_dry_run = state->query_options().dry_run_query;
    _enable_faster_float_convert = state->enable_faster_float_convert();

    return Status::OK();
}

template <bool is_binary_format>
void VMysqlResultWriter<is_binary_format>::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultSendTime", "AppendBatchTime");
    _copy_buffer_timer = ADD_CHILD_TIMER(_parent_profile, "CopyBufferTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
    _bytes_sent_counter = ADD_COUNTER(_parent_profile, "BytesSent", TUnit::BYTES);
}

// (TODO Amory: do not need this function)
template <bool is_binary_format>
template <PrimitiveType type, bool is_nullable>
Status VMysqlResultWriter<is_binary_format>::_add_one_column(
        const ColumnPtr& column_ptr, std::unique_ptr<TFetchDataResult>& result,
        std::vector<MysqlRowBuffer<is_binary_format>>& rows_buffer, bool arg_const, int scale,
        const DataTypes& sub_types) {
    SCOPED_TIMER(_convert_tuple_timer);

    //if arg_const is true, the column_ptr is already expanded to one row
    const auto row_size = rows_buffer.size();

    doris::vectorized::ColumnPtr column;
    if constexpr (is_nullable) {
        column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
    } else {
        column = column_ptr;
    }

    int buf_ret = 0;

    if constexpr (type == TYPE_OBJECT || type == TYPE_QUANTILE_STATE || type == TYPE_VARCHAR ||
                  type == TYPE_JSONB) {
        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }

            if constexpr (type == TYPE_OBJECT) {
                if (column->is_bitmap() && output_object_data()) {
                    const vectorized::ColumnComplexType<BitmapValue>* pColumnComplexType =
                            assert_cast<const vectorized::ColumnComplexType<BitmapValue>*>(
                                    column.get());
                    BitmapValue bitmapValue = pColumnComplexType->get_element(col_index);
                    size_t size = bitmapValue.getSizeInBytes();
                    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
                    bitmapValue.write_to(buf.get());
                    buf_ret = rows_buffer[i].push_string(buf.get(), size);
                } else if (column->is_hll() && output_object_data()) {
                    const vectorized::ColumnComplexType<HyperLogLog>* pColumnComplexType =
                            assert_cast<const vectorized::ColumnComplexType<HyperLogLog>*>(
                                    column.get());
                    HyperLogLog hyperLogLog = pColumnComplexType->get_element(col_index);
                    size_t size = hyperLogLog.max_serialized_size();
                    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
                    hyperLogLog.serialize((uint8*)buf.get());
                    buf_ret = rows_buffer[i].push_string(buf.get(), size);

                } else if (column->is_quantile_state() && output_object_data()) {
                    const vectorized::ColumnComplexType<QuantileStateDouble>* pColumnComplexType =
                            assert_cast<const vectorized::ColumnComplexType<QuantileStateDouble>*>(
                                    column.get());
                    QuantileStateDouble quantileValue = pColumnComplexType->get_element(col_index);
                    size_t size = quantileValue.get_serialized_size();
                    std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
                    quantileValue.serialize((uint8_t*)buf.get());
                    buf_ret = rows_buffer[i].push_string(buf.get(), size);
                } else {
                    buf_ret = rows_buffer[i].push_null();
                }
            }
            if constexpr (type == TYPE_VARCHAR) {
                const auto string_val = column->get_data_at(col_index);

                if (string_val.data == nullptr) {
                    if (string_val.size == 0) {
                        // 0x01 is a magic num, not useful actually, just for present ""
                        char* tmp_val = reinterpret_cast<char*>(0x01);
                        buf_ret = rows_buffer[i].push_string(tmp_val, string_val.size);
                    } else {
                        buf_ret = rows_buffer[i].push_null();
                    }
                } else {
                    buf_ret = rows_buffer[i].push_string(string_val.data, string_val.size);
                }
            }
            if constexpr (type == TYPE_JSONB) {
                const auto jsonb_val = column->get_data_at(col_index);
                // jsonb size == 0 is NULL
                if (jsonb_val.data == nullptr || jsonb_val.size == 0) {
                    buf_ret = rows_buffer[i].push_null();
                } else {
                    std::string json_str =
                            JsonbToJson::jsonb_to_json_string(jsonb_val.data, jsonb_val.size);
                    buf_ret = rows_buffer[i].push_string(json_str.c_str(), json_str.size());
                }
            }
        }
    } else if constexpr (type == TYPE_ARRAY) {
        DCHECK_EQ(sub_types.size(), 1);
        auto& column_array = assert_cast<const ColumnArray&>(*column);
        auto& offsets = column_array.get_offsets();
        for (ssize_t i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }

            rows_buffer[i].open_dynamic_mode();
            buf_ret = rows_buffer[i].push_string("[", 1);
            bool begin = true;
            for (auto j = offsets[col_index - 1]; j < offsets[col_index]; ++j) {
                if (!begin) {
                    buf_ret = rows_buffer[i].push_string(", ", 2);
                }
                const auto& data = column_array.get_data_ptr();
                if (data->is_null_at(j)) {
                    buf_ret = rows_buffer[i].push_string("NULL", strlen("NULL"));
                } else {
                    if (WhichDataType(remove_nullable(sub_types[0])).is_string()) {
                        buf_ret = rows_buffer[i].push_string("'", 1);
                        buf_ret = _add_one_cell(data, j, sub_types[0], rows_buffer[i], scale);
                        buf_ret = rows_buffer[i].push_string("'", 1);
                    } else {
                        buf_ret = _add_one_cell(data, j, sub_types[0], rows_buffer[i], scale);
                    }
                }
                begin = false;
            }
            buf_ret = rows_buffer[i].push_string("]", 1);
            rows_buffer[i].close_dynamic_mode();
        }
    } else if constexpr (type == TYPE_MAP) {
        DCHECK_GE(sub_types.size(), 1);
        auto& map_type = assert_cast<const DataTypeMap&>(*sub_types[0]);
        for (ssize_t i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }
            rows_buffer[i].open_dynamic_mode();
            std::string cell_str = map_type.to_string(*column, col_index);
            buf_ret = rows_buffer[i].push_string(cell_str.c_str(), strlen(cell_str.c_str()));

            rows_buffer[i].close_dynamic_mode();
        }
    } else if constexpr (type == TYPE_STRUCT) {
        DCHECK_GE(sub_types.size(), 1);
        auto& column_struct = assert_cast<const ColumnStruct&>(*column);
        for (ssize_t i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }

            rows_buffer[i].open_dynamic_mode();
            buf_ret = rows_buffer[i].push_string("{", 1);
            bool begin = true;
            for (size_t j = 0; j < sub_types.size(); ++j) {
                if (!begin) {
                    buf_ret = rows_buffer[i].push_string(", ", 2);
                }
                const auto& data = column_struct.get_column_ptr(j);
                if (data->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_string("NULL", strlen("NULL"));
                } else {
                    if (WhichDataType(remove_nullable(sub_types[j])).is_string()) {
                        buf_ret = rows_buffer[i].push_string("'", 1);
                        buf_ret = _add_one_cell(data, col_index, sub_types[j], rows_buffer[i]);
                        buf_ret = rows_buffer[i].push_string("'", 1);
                    } else {
                        buf_ret = _add_one_cell(data, col_index, sub_types[j], rows_buffer[i]);
                    }
                }
                begin = false;
            }
            buf_ret = rows_buffer[i].push_string("}", 1);
            rows_buffer[i].close_dynamic_mode();
        }
    } else if constexpr (type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
                         type == TYPE_DECIMAL128I) {
        DCHECK_EQ(sub_types.size(), 1);
        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }
            std::string decimal_str = sub_types[0]->to_string(*column, col_index);
            buf_ret = rows_buffer[i].push_string(decimal_str.c_str(), decimal_str.length());
        }
    } else {
        using ColumnType = typename PrimitiveTypeTraits<type>::ColumnType;
        auto& data = assert_cast<const ColumnType&>(*column).get_data();

        for (int i = 0; i < row_size; ++i) {
            if (0 != buf_ret) {
                return Status::InternalError("pack mysql buffer failed.");
            }

            const auto col_index = index_check_const(i, arg_const);

            if constexpr (is_nullable) {
                if (column_ptr->is_null_at(col_index)) {
                    buf_ret = rows_buffer[i].push_null();
                    continue;
                }
            }

            if constexpr (type == TYPE_BOOLEAN) {
                //todo here need to using uint after MysqlRowBuffer support it
                buf_ret = rows_buffer[i].push_tinyint(data[col_index]);
            }
            if constexpr (type == TYPE_TINYINT) {
                buf_ret = rows_buffer[i].push_tinyint(data[col_index]);
            }
            if constexpr (type == TYPE_SMALLINT) {
                buf_ret = rows_buffer[i].push_smallint(data[col_index]);
            }
            if constexpr (type == TYPE_INT) {
                buf_ret = rows_buffer[i].push_int(data[col_index]);
            }
            if constexpr (type == TYPE_BIGINT) {
                buf_ret = rows_buffer[i].push_bigint(data[col_index]);
            }
            if constexpr (type == TYPE_LARGEINT) {
                auto v = LargeIntValue::to_string(data[col_index]);
                buf_ret = rows_buffer[i].push_string(v.c_str(), v.size());
            }
            if constexpr (type == TYPE_FLOAT) {
                buf_ret = rows_buffer[i].push_float(data[col_index]);
            }
            if constexpr (type == TYPE_DOUBLE) {
                buf_ret = rows_buffer[i].push_double(data[col_index]);
            }
            if constexpr (type == TYPE_TIME) {
                buf_ret = rows_buffer[i].push_time(data[col_index]);
            }
            if constexpr (type == TYPE_TIMEV2) {
                buf_ret = rows_buffer[i].push_timev2(data[col_index]);
            }
            if constexpr (type == TYPE_DATETIME) {
                auto time_num = data[col_index];
                VecDateTimeValue time_val = binary_cast<Int64, VecDateTimeValue>(time_num);
                buf_ret = rows_buffer[i].push_vec_datetime(time_val);
            }
            if constexpr (type == TYPE_DATEV2) {
                auto time_num = data[col_index];
                DateV2Value<DateV2ValueType> date_val =
                        binary_cast<UInt32, DateV2Value<DateV2ValueType>>(time_num);
                buf_ret = rows_buffer[i].push_vec_datetime(date_val);
            }
            if constexpr (type == TYPE_DATETIMEV2) {
                auto time_num = data[col_index];
                char buf[64];
                DateV2Value<DateTimeV2ValueType> date_val =
                        binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(time_num);
                char* pos = date_val.to_string(buf, scale);
                buf_ret = rows_buffer[i].push_string(buf, pos - buf - 1);
            }
            if constexpr (type == TYPE_DECIMALV2) {
                DecimalV2Value decimal_val(data[col_index]);
                auto decimal_str = decimal_val.to_string(scale);
                buf_ret = rows_buffer[i].push_string(decimal_str.c_str(), decimal_str.length());
            }
        }
    }
    if (0 != buf_ret) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

template <bool is_binary_format>
int VMysqlResultWriter<is_binary_format>::_add_one_cell(const ColumnPtr& column_ptr, size_t row_idx,
                                                        const DataTypePtr& type,
                                                        MysqlRowBuffer<is_binary_format>& buffer,
                                                        int scale) {
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
        VecDateTimeValue datetime = binary_cast<Int64, VecDateTimeValue>(value);
        if (which.is_date()) {
            datetime.cast_to_date();
        }
        char buf[64];
        char* pos = datetime.to_string(buf);
        return buffer.push_string(buf, pos - buf - 1);
    } else if (which.is_date_v2()) {
        auto& column_vector = assert_cast<const ColumnVector<UInt32>&>(*column);
        auto value = column_vector[row_idx].get<UInt32>();
        DateV2Value<DateV2ValueType> datev2 =
                binary_cast<UInt32, DateV2Value<DateV2ValueType>>(value);
        char buf[64];
        char* pos = datev2.to_string(buf);
        return buffer.push_string(buf, pos - buf - 1);
    } else if (which.is_date_time_v2()) {
        auto& column_vector = assert_cast<const ColumnVector<UInt64>&>(*column);
        auto value = column_vector[row_idx].get<UInt64>();
        DateV2Value<DateTimeV2ValueType> datetimev2 =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(value);
        char buf[64];
        char* pos = datetimev2.to_string(buf, scale);
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
        auto& column_data =
                static_cast<const ColumnDecimal<vectorized::Decimal128>&>(*column).get_data();
        DecimalV2Value decimal_val(column_data[row_idx]);
        auto decimal_str = decimal_val.to_string();
        return buffer.push_string(decimal_str.c_str(), decimal_str.length());
    } else if (which.is_decimal128i()) {
        DataTypePtr nested_type = type;
        if (type->is_nullable()) {
            nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
        }
        auto decimal_str = assert_cast<const DataTypeDecimal<Decimal128I>*>(nested_type.get())
                                   ->to_string(*column, row_idx);
        return buffer.push_string(decimal_str.c_str(), decimal_str.length());
        // TODO(xy): support nested struct
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
    } else if (which.is_struct()) {
        auto& column_struct = assert_cast<const ColumnStruct&>(*column);

        DataTypePtr nested_type = type;
        if (type->is_nullable()) {
            nested_type = assert_cast<const DataTypeNullable&>(*type).get_nested_type();
        }

        size_t tuple_size = column_struct.tuple_size();

        int buf_ret = buffer.push_string("{", strlen("{"));
        bool begin = true;
        for (int i = 0; i < tuple_size; ++i) {
            const auto& data = column_struct.get_column_ptr(i);
            const auto& sub_type = assert_cast<const DataTypeStruct&>(*nested_type).get_element(i);

            if (begin) {
                begin = false;
            } else {
                buf_ret = buffer.push_string(", ", strlen(", "));
            }

            if (data->is_null_at(row_idx)) {
                buf_ret = buffer.push_string("NULL", strlen("NULL"));
            } else {
                if (WhichDataType(remove_nullable(sub_type)).is_string()) {
                    buf_ret = buffer.push_string("'", 1);
                    buf_ret = _add_one_cell(data, row_idx, sub_type, buffer, scale);
                    buf_ret = buffer.push_string("'", 1);
                } else {
                    buf_ret = _add_one_cell(data, row_idx, sub_type, buffer, scale);
                }
            }
        }
        buf_ret = buffer.push_string("}", strlen("}"));
        return buf_ret;
    } else {
        LOG(WARNING) << "sub TypeIndex(" << (int)which.idx << "not supported yet";
        return -1;
    }
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::append_block(Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) {
        return status;
    }

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    Block block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
                                                                       input_block, &block));

    // convert one batch
    auto result = std::make_unique<TFetchDataResult>();
    auto num_rows = block.rows();
    result->result_batch.rows.resize(num_rows);

    uint64_t bytes_sent = 0;
    {
        SCOPED_TIMER(_convert_tuple_timer);
        MysqlRowBuffer<is_binary_format> row_buffer;
        row_buffer.set_faster_float_convert(_enable_faster_float_convert);
        if constexpr (is_binary_format) {
            row_buffer.start_binary_row(_output_vexpr_ctxs.size());
        }

        struct Arguments {
            const IColumn* column;
            bool is_const;
            DataTypeSerDeSPtr serde;
        };

        std::vector<Arguments> arguments;
        for (int i = 0; i < _output_vexpr_ctxs.size(); ++i) {
            const auto& [column_ptr, col_const] = unpack_if_const(block.get_by_position(i).column);
            int scale = _output_vexpr_ctxs[i]->root()->type().scale;
            // decimalv2 scale and precision is hard code, so we should get real scale and precision
            // from expr
            DataTypeSerDeSPtr serde;
            if (_output_vexpr_ctxs[i]->root()->type().is_decimal_v2_type()) {
                serde = std::make_shared<DataTypeDecimalSerDe<vectorized::Decimal128>>(scale, 27);
            } else {
                serde = block.get_by_position(i).type->get_serde();
            }
            serde->set_return_object_as_string(output_object_data());
            arguments.emplace_back(column_ptr.get(), col_const, serde);
        }

        for (size_t row_idx = 0; row_idx != num_rows; ++row_idx) {
            for (int i = 0; i < _output_vexpr_ctxs.size(); ++i) {
                RETURN_IF_ERROR(arguments[i].serde->write_column_to_mysql(
                        *(arguments[i].column), row_buffer, row_idx, arguments[i].is_const));
            }

            // copy MysqlRowBuffer to Thrift
            result->result_batch.rows[row_idx].append(row_buffer.buf(), row_buffer.length());
            bytes_sent += row_buffer.length();
            row_buffer.reset();
            if constexpr (is_binary_format) {
                row_buffer.start_binary_row(_output_vexpr_ctxs.size());
            }
        }
    }
    {
        SCOPED_TIMER(_result_send_timer);
        // If this is a dry run task, no need to send data block
        if (!_is_dry_run) {
            if (_sinker) {
                status = _sinker->add_batch(result);
            } else {
                _results.push_back(std::move(result));
            }
        }
        if (status.ok()) {
            _written_rows += num_rows;
            if (!_is_dry_run) {
                _bytes_sent += bytes_sent;
            }
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }
    return status;
}

template <bool is_binary_format>
bool VMysqlResultWriter<is_binary_format>::can_sink() {
    return _sinker->can_sink();
}

template <bool is_binary_format>
Status VMysqlResultWriter<is_binary_format>::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    COUNTER_UPDATE(_bytes_sent_counter, _bytes_sent);
    return Status::OK();
}

template class VMysqlResultWriter<true>;
template class VMysqlResultWriter<false>;

} // namespace vectorized
} // namespace doris
