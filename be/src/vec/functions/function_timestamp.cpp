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

#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

template <typename DateType, typename NativeType>
struct StrToDate {
    static constexpr auto name = "str_to_date";
    using ReturnType = DateType;
    using ColumnType = ColumnVector<NativeType>;

    static void vector_vector(FunctionContext* context, const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets,
                              PaddedPODArray<NativeType>& res, NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1] - 1;

            if constexpr (std::is_same_v<DateType, DataTypeDateTime> ||
                          std::is_same_v<DateType, DataTypeDate>) {
                auto& ts_val = *reinterpret_cast<VecDateTimeValue*>(&res[i]);
                if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size)) {
                    null_map[i] = 1;
                }
                if (context->impl()->get_return_type().type ==
                    doris_udf::FunctionContext::Type::TYPE_DATETIME) {
                    ts_val.to_datetime();
                } else {
                    ts_val.cast_to_date();
                }
            } else {
                auto& ts_val = *reinterpret_cast<DateV2Value<DateV2ValueType>*>(&res[i]);
                if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size)) {
                    null_map[i] = 1;
                }
            }
        }
    }
};

struct NameMakeDate {
    static constexpr auto name = "makedate";
};

template <typename LeftDataType, typename RightDataType, typename ResultDateType,
          typename ReturnType>
struct MakeDateImpl {
    using ResultDataType = ResultDateType;
    using LeftDataColumnType = ColumnVector<typename LeftDataType::FieldType>;
    using RightDataColumnType = ColumnVector<typename RightDataType::FieldType>;
    using ColumnType = ColumnVector<ReturnType>;

    static void vector_vector(const PaddedPODArray<typename LeftDataType::FieldType>& ldata,
                              const PaddedPODArray<typename RightDataType::FieldType>& rdata,
                              PaddedPODArray<ReturnType>& res, NullMap& null_map) {
        auto len = ldata.size();
        res.resize(len);

        for (size_t i = 0; i < len; ++i) {
            const auto& l = ldata[i];
            const auto& r = rdata[i];
            if (r <= 0 || l < 0 || l > 9999) {
                null_map[i] = 1;
                continue;
            }

            if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime> ||
                          std::is_same_v<ResultDataType, DataTypeDate>) {
                auto& res_val = *reinterpret_cast<VecDateTimeValue*>(&res[i]);

                VecDateTimeValue ts_value = VecDateTimeValue();
                ts_value.set_time(l, 1, 1, 0, 0, 0);

                DateTimeVal ts_val;
                ts_value.to_datetime_val(&ts_val);
                if (ts_val.is_null) {
                    null_map[i] = 1;
                    continue;
                }

                TimeInterval interval(DAY, r - 1, false);
                res_val = VecDateTimeValue::from_datetime_val(ts_val);
                if (!res_val.date_add_interval(interval, DAY)) {
                    null_map[i] = 1;
                    continue;
                }
                res_val.cast_to_date();
            } else {
                DateV2Value<DateV2ValueType>* value = new (&res[i]) DateV2Value<DateV2ValueType>();
                value->set_time(l, 1, 1);
                TimeInterval interval(DAY, r - 1, false);
                if (!value->date_add_interval(interval, DAY, *value)) {
                    null_map[i] = 1;
                }
            }
        }
    }
};

template <typename DateType>
class FromDays : public IFunction {
public:
    static constexpr auto name = "from_days";

    static FunctionPtr create() { return std::make_shared<FromDays>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DateType>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_column = ColumnInt64::create(input_rows_count);
        auto& res_data = assert_cast<ColumnInt64&>(*res_column).get_data();
        ColumnPtr argument_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();

        auto data_col = assert_cast<const ColumnVector<Int32>*>(argument_column.get());
        for (int i = 0; i < input_rows_count; i++) {
            if constexpr (std::is_same_v<DateType, DataTypeDate>) {
                const auto& cur_data = data_col->get_data()[i];
                auto& ts_value = *reinterpret_cast<VecDateTimeValue*>(&res_data[i]);
                if (!ts_value.from_date_daynr(cur_data)) {
                    null_map->get_data()[i] = 1;
                    continue;
                }
                DateTimeVal ts_val;
                ts_value.to_datetime_val(&ts_val);
                ts_value = VecDateTimeValue::from_datetime_val(ts_val);
            } else {
                const auto& cur_data = data_col->get_data()[i];
                auto& ts_value = *reinterpret_cast<DateV2Value<DateV2ValueType>*>(&res_data[i]);
                if (!ts_value.get_date_from_daynr(cur_data)) {
                    null_map->get_data()[i] = 1;
                }
            }
        }
        block.replace_by_position(
                result, ColumnNullable::create(std::move(res_column), std::move(null_map)));
        return Status::OK();
    }
};

using FunctionStrToDate = FunctionBinaryStringOperateToNullType<StrToDate<DataTypeDateTime, Int64>>;

using FunctionMakeDate = FunctionBinaryToNullType<DataTypeInt32, DataTypeInt32, DataTypeDateTime,
                                                  Int64, MakeDateImpl, NameMakeDate>;

struct UnixTimeStampImpl {
    static Int32 trim_timestamp(Int64 timestamp) {
        if (timestamp < 0 || timestamp > INT_MAX) {
            timestamp = 0;
        }
        return (Int32)timestamp;
    }

    static DataTypes get_variadic_argument_types() { return {}; }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataTypeInt32>();
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        auto col_result = ColumnVector<Int32>::create();
        col_result->resize(input_rows_count);
        // TODO: use a const column to store this value
        auto& col_result_data = col_result->get_data();
        auto res_value = context->impl()->state()->timestamp_ms() / 1000;
        for (int i = 0; i < input_rows_count; i++) {
            col_result_data[i] = res_value;
        }
        block.replace_by_position(result, std::move(col_result));
        return Status::OK();
    }
};

template <typename DateType>
struct UnixTimeStampDateImpl {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const ColumnPtr col_source = block.get_by_position(arguments[0]).column;

        auto col_result = ColumnVector<Int32>::create();
        auto null_map = ColumnVector<UInt8>::create();

        col_result->resize(input_rows_count);
        null_map->resize(input_rows_count);

        auto& col_result_data = col_result->get_data();
        auto& null_map_data = null_map->get_data();

        for (int i = 0; i < input_rows_count; i++) {
            if (col_source->is_null_at(i)) {
                null_map_data[i] = true;
                continue;
            }

            StringRef source = col_source->get_data_at(i);
            if constexpr (std::is_same_v<DateType, DataTypeDate>) {
                const VecDateTimeValue& ts_value =
                        reinterpret_cast<const VecDateTimeValue&>(*source.data);
                int64_t timestamp;
                if (!ts_value.unix_timestamp(&timestamp,
                                             context->impl()->state()->timezone_obj())) {
                    null_map_data[i] = true;
                } else {
                    null_map_data[i] = false;
                    col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                }
            } else {
                const DateV2Value<DateV2ValueType>& ts_value =
                        reinterpret_cast<const DateV2Value<DateV2ValueType>&>(*source.data);
                int64_t timestamp;
                if (!ts_value.unix_timestamp(&timestamp,
                                             context->impl()->state()->timezone_obj())) {
                    null_map_data[i] = true;
                } else {
                    null_map_data[i] = false;
                    col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
                }
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_result), std::move(null_map)));

        return Status::OK();
    }
};

template <typename DateType>
struct UnixTimeStampDatetimeImpl : public UnixTimeStampDateImpl<DateType> {
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DateType>()}; }
};

struct UnixTimeStampStrImpl {
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return make_nullable(std::make_shared<DataTypeInt32>());
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) {
        const ColumnPtr col_source = block.get_by_position(arguments[0]).column;
        const ColumnPtr col_format = block.get_by_position(arguments[1]).column;

        auto col_result = ColumnVector<Int32>::create();
        auto null_map = ColumnVector<UInt8>::create();

        col_result->resize(input_rows_count);
        null_map->resize(input_rows_count);

        auto& col_result_data = col_result->get_data();
        auto& null_map_data = null_map->get_data();

        for (int i = 0; i < input_rows_count; i++) {
            if (col_source->is_null_at(i) || col_format->is_null_at(i)) {
                null_map_data[i] = true;
                continue;
            }

            StringRef source = col_source->get_data_at(i);
            StringRef fmt = col_format->get_data_at(i);

            VecDateTimeValue ts_value;
            if (!ts_value.from_date_format_str(fmt.data, fmt.size, source.data, source.size)) {
                null_map_data[i] = true;
                continue;
            }

            int64_t timestamp;
            if (!ts_value.unix_timestamp(&timestamp, context->impl()->state()->timezone_obj())) {
                null_map_data[i] = true;
            } else {
                null_map_data[i] = false;
                col_result_data[i] = UnixTimeStampImpl::trim_timestamp(timestamp);
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(col_result), std::move(null_map)));

        return Status::OK();
    }
};

template <typename Impl>
class FunctionUnixTimestamp : public IFunction {
public:
    static constexpr auto name = "unix_timestamp";
    static FunctionPtr create() { return std::make_shared<FunctionUnixTimestamp<Impl>>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

void register_function_timestamp(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
    factory.register_function<FunctionMakeDate>();
    factory.register_function<FromDays<DataTypeDate>>();

    factory.register_function<FunctionUnixTimestamp<UnixTimeStampImpl>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDateImpl<DataTypeDateV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDatetimeImpl<DataTypeDate>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampDatetimeImpl<DataTypeDateV2>>>();
    factory.register_function<FunctionUnixTimestamp<UnixTimeStampStrImpl>>();
}

} // namespace doris::vectorized
