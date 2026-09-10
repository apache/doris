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

#include <glog/logging.h>
#include <stddef.h>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_uuid.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/string_utils/string_utils.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/function_totype.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {
namespace {

template <bool Version7>
class FunctionGenerateUUID : public IFunction {
public:
    static constexpr auto name = Version7 ? "uuid_v7" : "uuid_v4";

    static FunctionPtr create() { return std::make_shared<FunctionGenerateUUID>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUUID>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto column = ColumnUUID::create(input_rows_count);
        auto& data = column->get_data();
        UUIDValue::generate(data.data(), input_rows_count, Version7);
        block.replace_by_position(result, std::move(column));
        return Status::OK();
    }
};

class FunctionUUIDVersion : public IFunction {
public:
    static constexpr auto name = "uuid_version";

    static FunctionPtr create() { return std::make_shared<FunctionUUIDVersion>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& input =
                assert_cast<const ColumnUUID&>(*block.get_by_position(arguments[0]).column);
        auto output = ColumnInt8::create(input_rows_count);
        auto& output_data = output->get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            output_data[i] = static_cast<Int8>(UUIDValue::version(input.get_element(i)));
        }
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

// NULL input is propagated by OrZero/OrNull, but selects the fallback in OrDefault.
enum class UUIDParseMode { Zero, Null, Default };

template <UUIDParseMode Mode>
class FunctionToUUID : public IFunction {
public:
    static constexpr auto name = Mode == UUIDParseMode::Zero   ? "to_uuid_or_zero"
                                 : Mode == UUIDParseMode::Null ? "to_uuid_or_null"
                                                               : "to_uuid_or_default";
    static FunctionPtr create() { return std::make_shared<FunctionToUUID>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override {
        return Mode == UUIDParseMode::Default ? 0 : 1;
    }
    bool is_variadic() const override { return Mode == UUIDParseMode::Default; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        bool nullable = Mode == UUIDParseMode::Null ||
                        (Mode == UUIDParseMode::Zero && arguments[0]->is_nullable()) ||
                        (Mode == UUIDParseMode::Default && arguments.size() == 2 &&
                         arguments[1]->is_nullable());
        DataTypePtr type = std::make_shared<DataTypeUUID>();
        return nullable ? make_nullable(type) : type;
    }
    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t rows) const override {
        auto input = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* nullable_input = check_and_get_column<ColumnNullable>(*input);
        const auto& strings = assert_cast<const ColumnString&>(
                nullable_input ? nullable_input->get_nested_column() : *input);
        ColumnPtr fallback;
        const ColumnNullable* nullable_fallback = nullptr;
        const ColumnUUID* fallback_values = nullptr;
        if constexpr (Mode == UUIDParseMode::Default) {
            if (arguments.size() == 2) {
                fallback = block.get_by_position(arguments[1])
                                   .column->convert_to_full_column_if_const();
                nullable_fallback = check_and_get_column<ColumnNullable>(*fallback);
                fallback_values = &assert_cast<const ColumnUUID&>(
                        nullable_fallback ? nullable_fallback->get_nested_column() : *fallback);
            }
        }
        auto output = ColumnUUID::create(rows, 0);
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t i = 0; i < rows; ++i) {
            bool source_null = nullable_input && nullable_input->is_null_at(i);
            UUIDValueType value = 0;
            auto text = strings.get_data_at(i);
            bool parsed = !source_null && UUIDValue::from_string(value, text.data, text.size);
            if (parsed) {
                output->get_data()[i] = value;
            } else if constexpr (Mode == UUIDParseMode::Default) {
                if (fallback_values) {
                    output->get_data()[i] = fallback_values->get_element(i);
                    nulls->get_data()[i] = nullable_fallback && nullable_fallback->is_null_at(i);
                }
            } else {
                nulls->get_data()[i] = Mode == UUIDParseMode::Null || source_null;
            }
        }
        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result,
                                      ColumnNullable::create(std::move(output), std::move(nulls)));
        } else {
            block.replace_by_position(result, std::move(output));
        }
        return Status::OK();
    }
};

class FunctionUUIDv7ToDateTime : public IFunction {
public:
    static constexpr auto name = "uuid_v7_to_datetime";
    static FunctionPtr create() { return std::make_shared<FunctionUUIDv7ToDateTime>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return true; }
    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeDateTimeV2>(3));
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t rows) const override {
        cctz::time_zone timezone = context->state()->timezone_obj();
        if (arguments.size() == 2) {
            auto zone_column =
                    block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
            if (rows > 0) {
                auto zone = assert_cast<const ColumnString&>(*zone_column).get_data_at(0);
                if (!TimezoneUtils::find_cctz_time_zone(zone.to_string(), timezone)) {
                    return Status::InvalidArgument("Invalid timezone: {}", zone.to_string());
                }
            }
        }
        const auto& input =
                assert_cast<const ColumnUUID&>(*block.get_by_position(arguments[0]).column);
        auto output = ColumnDateTimeV2::create(rows);
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t i = 0; i < rows; ++i) {
            auto uuid = input.get_element(i);
            uint64_t millis = UUIDValue::version(uuid) == 7 ? static_cast<uint64_t>(uuid >> 80) : 0;
            DateV2Value<DateTimeV2ValueType> datetime;
            datetime.from_unixtime(millis / 1000, (millis % 1000) * 1000000, timezone, 3);
            // The UUID timestamp domain extends beyond Doris DATETIME's year 9999.
            if (!datetime.is_valid_date()) {
                nulls->get_data()[i] = 1;
            } else {
                output->get_data()[i] = datetime;
            }
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(output), std::move(nulls)));
        return Status::OK();
    }
};

// The caller supplies a timestamp, so a previous wall-clock UUID must not replace it.
// Keep a separate counter for this generator; a change of input timestamp resets the counter.
struct UUIDTimestampCounter {
    uint64_t input_timestamp = 0;
    uint64_t timestamp = 0;
    uint64_t counter = 0;
    UUIDValueType next(UUIDValueType random, uint64_t millis) {
        constexpr uint64_t counter_limit = uint64_t {1} << 42;
        if (input_timestamp == millis && counter != 0) {
            ++counter;
        } else {
            input_timestamp = millis;
            timestamp = millis;
            counter = (static_cast<uint64_t>(random >> 32) & ((uint64_t {1} << 41) - 1)) + 1;
        }
        if (counter == counter_limit) {
            ++timestamp;
            counter = 1;
        }
        return (static_cast<UUIDValueType>(timestamp) << 80) | (UUIDValueType {7} << 76) |
               (static_cast<UUIDValueType>(counter >> 30) << 64) | (UUIDValueType {2} << 62) |
               (static_cast<UUIDValueType>(counter & ((uint64_t {1} << 30) - 1)) << 32) |
               (random & 0xffffffff);
    }
};

class FunctionDateTimeToUUIDv7 : public IFunction {
public:
    static constexpr auto name = "datetime_to_uuid_v7";
    static FunctionPtr create() { return std::make_shared<FunctionDateTimeToUUIDv7>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_constants() const override { return false; }
    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeUUID>());
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t rows) const override {
        auto input = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& dates = assert_cast<const ColumnDateTimeV2&>(*input).get_data();
        auto output = ColumnUUID::create(rows, 0);
        auto nulls = ColumnUInt8::create(rows, 0);
        std::vector<uint64_t> timestamps(rows);
        UUIDValue::generate(output->get_data().data(), rows, false);
        for (size_t i = 0; i < rows; ++i) {
            auto datetime = dates[i];
            int64_t seconds = -1;
            if (datetime.is_valid_date()) {
                datetime.unix_timestamp(&seconds, context->state()->timezone_obj());
            }
            if (seconds < 0) {
                nulls->get_data()[i] = 1;
                output->get_data()[i] = 0;
            } else {
                timestamps[i] = seconds * 1000 + datetime.microsecond() / 1000;
            }
        }
        static std::mutex mutex;
        static UUIDTimestampCounter counter;
        // Only counter and UUID bit assembly run under the lock, once per batch.
        {
            std::lock_guard lock(mutex);
            for (size_t i = 0; i < rows; ++i) {
                if (!nulls->get_data()[i]) {
                    output->get_data()[i] = counter.next(output->get_data()[i], timestamps[i]);
                }
            }
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(output), std::move(nulls)));
        return Status::OK();
    }
};

} // namespace

class Uuid : public IFunction {
public:
    static constexpr auto name = "uuid";
    static constexpr size_t uuid_length = 36; //uuid fixed length

    static FunctionPtr create() { return std::make_shared<Uuid>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto col_res = ColumnString::create();
        col_res->get_offsets().reserve(input_rows_count);
        col_res->get_chars().reserve(input_rows_count * uuid_length);

        boost::uuids::random_generator generator;
        for (int i = 0; i < input_rows_count; i++) {
            std::string uuid = boost::uuids::to_string(generator());
            DCHECK(uuid.length() == uuid_length);
            col_res->insert_data_without_reserve(uuid.c_str(), uuid.length());
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

struct NameIsUuid {
    static constexpr auto name = "is_uuid";
};

struct IsUuidImpl {
    using ReturnType = DataTypeBool;
    using ReturnColumnType = ColumnUInt8;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_STRING;
    static constexpr size_t uuid_without_dash_length = 32;
    static constexpr size_t uuid_with_dash_length = 36;
    static constexpr size_t uuid_with_braces_and_dash_length = 38;
    static constexpr size_t dash_positions[4] = {8, 13, 18, 23};

    static bool is_uuid_with_dash(const char* src, const char* end) {
        size_t str_size = end - src;
        for (int i = 0; i < str_size; ++i) {
            if (!is_hex_ascii(src[i])) {
                if (i == dash_positions[0] || i == dash_positions[1] || i == dash_positions[2] ||
                    i == dash_positions[3]) {
                    if (src[i] != '-') {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<UInt8>& res) {
        size_t rows_count = offsets.size();
        res.resize(rows_count);
        for (size_t i = 0; i < rows_count; ++i) {
            const char* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1];
            if (str_size == uuid_without_dash_length) {
                bool is_valid = true;
                for (int j = 0; j < str_size; ++j) {
                    if (!is_hex_ascii(source[j])) {
                        is_valid = false;
                        break;
                    }
                }
                res[i] = is_valid;
            } else if (str_size == uuid_with_dash_length) {
                res[i] = is_uuid_with_dash(source, source + str_size);
            } else if (str_size == uuid_with_braces_and_dash_length) {
                if (source[0] != '{' || source[str_size - 1] != '}') {
                    res[i] = 0;
                    continue;
                }
                res[i] = is_uuid_with_dash(source + 1, source + str_size - 1);
            } else {
                res[i] = 0;
            }
        }
        return Status::OK();
    }
};

using FunctionIsUuid = FunctionUnaryToType<IsUuidImpl, NameIsUuid>;

void register_function_uuid(SimpleFunctionFactory& factory) {
    factory.register_function<Uuid>();
    factory.register_function<FunctionIsUuid>();
    factory.register_function<FunctionGenerateUUID<false>>();
    factory.register_function<FunctionGenerateUUID<true>>();
    factory.register_function<FunctionUUIDVersion>();
    factory.register_function<FunctionToUUID<UUIDParseMode::Zero>>();
    factory.register_function<FunctionToUUID<UUIDParseMode::Null>>();
    factory.register_function<FunctionToUUID<UUIDParseMode::Default>>();
    factory.register_function<FunctionUUIDv7ToDateTime>();
    factory.register_function<FunctionDateTimeToUUIDv7>();
    factory.register_alias("to_uuid_or_zero", "touuidorzero");
    factory.register_alias("to_uuid_or_null", "touuidornull");
    factory.register_alias("to_uuid_or_default", "touuidordefault");
    factory.register_alias("uuid_v7_to_datetime", "uuidv7todatetime");
    factory.register_alias("datetime_to_uuid_v7", "datetimetouuidv7");
    factory.register_alias(FunctionGenerateUUID<false>::name, "generate_uuid_v4");
    factory.register_alias(FunctionGenerateUUID<false>::name, "generateuuidv4");
    factory.register_alias(FunctionGenerateUUID<true>::name, "generate_uuid_v7");
    factory.register_alias(FunctionGenerateUUID<true>::name, "generateuuidv7");
}

} // namespace doris
