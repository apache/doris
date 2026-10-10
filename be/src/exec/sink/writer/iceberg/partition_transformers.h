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

#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/value/uuid_value.h"
#include "exec/common/stringop_substring.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "util/bit_util.h"

namespace doris {

namespace iceberg {
class Type;
class PartitionField;
}; // namespace iceberg

class IColumn;
class PartitionColumnTransform;

class PartitionColumnTransforms {
private:
    PartitionColumnTransforms();

public:
    static std::unique_ptr<PartitionColumnTransform> create(
            const doris::iceberg::PartitionField& field, const DataTypePtr& source_type);
};

class PartitionColumnTransformUtils {
public:
    static const DateV2Value<DateV2ValueType>& epoch_date() {
        // Function-local initialization is synchronized; a separate mutable flag races across writers.
        static const auto epoch_date = [] {
            DateV2Value<DateV2ValueType> value;
            value.unchecked_set_time(1970, 1, 1, 0, 0, 0);
            return value;
        }();
        return epoch_date;
    }

    static const DateV2Value<DateTimeV2ValueType>& epoch_datetime() {
        static const auto epoch_datetime = [] {
            DateV2Value<DateTimeV2ValueType> value;
            value.unchecked_set_time(1970, 1, 1, 0, 0, 0);
            return value;
        }();
        return epoch_datetime;
    }

    static DateV2Value<DateTimeV2ValueType> timestamp_value(UInt64 value) {
        return DateV2Value<DateTimeV2ValueType>(value);
    }

    static DateV2Value<DateTimeV2ValueType> timestamp_value(const TimestampTzValue& value) {
        // TIMESTAMPTZ's calendar fields are already UTC. Never project through the session zone.
        return value.utc_dt();
    }

    static std::string human_year(int year_ordinal) {
        auto ymd = std::chrono::year_month_day {EPOCH} + std::chrono::years(year_ordinal);
        return std::to_string(static_cast<int>(ymd.year()));
    }

    static std::string human_month(int month_ordinal) {
        auto ymd = std::chrono::year_month_day {EPOCH} + std::chrono::months(month_ordinal);
        return fmt::format("{:04d}-{:02d}", static_cast<int>(ymd.year()),
                           static_cast<unsigned>(ymd.month()));
    }

    static std::string human_day(int day_ordinal) {
        auto ymd = std::chrono::year_month_day(std::chrono::sys_days(
                std::chrono::floor<std::chrono::days>(EPOCH + std::chrono::days(day_ordinal))));
        return fmt::format("{:04d}-{:02d}-{:02d}", static_cast<int>(ymd.year()),
                           static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()));
    }

    static std::string human_hour(int hour_ordinal) {
        // Iceberg ordinals floor toward negative infinity, including the hour before the epoch.
        int day_value = hour_ordinal / 24 - (hour_ordinal % 24 < 0);
        int hour_value = hour_ordinal - day_value * 24;
        auto ymd = std::chrono::year_month_day(std::chrono::sys_days(
                std::chrono::floor<std::chrono::days>(EPOCH + std::chrono::days(day_value))));
        return fmt::format("{:04d}-{:02d}-{:02d}-{:02d}", static_cast<int>(ymd.year()),
                           static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()),
                           hour_value);
    }

private:
    static const std::chrono::sys_days EPOCH;
    PartitionColumnTransformUtils() = default;
};

class PartitionColumnTransform {
public:
    PartitionColumnTransform() = default;

    virtual ~PartitionColumnTransform() = default;

    virtual std::string name() const;

    virtual DataTypePtr get_result_type() const = 0;

    virtual ColumnWithTypeAndName apply(const Block& block, int column_pos) = 0;

    virtual std::string to_human_string(const DataTypePtr type, const std::any& value) const;

    virtual std::string get_partition_value(const DataTypePtr type, const std::any& value) const;
};

class IdentityPartitionColumnTransform : public PartitionColumnTransform {
public:
    IdentityPartitionColumnTransform(const DataTypePtr source_type) : _source_type(source_type) {}

    std::string name() const override { return "Identity"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        return {column_with_type_and_name.column, column_with_type_and_name.type,
                column_with_type_and_name.name};
    }

private:
    DataTypePtr _source_type;
};

class StringTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    StringTruncatePartitionColumnTransform(const DataTypePtr source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "StringTruncate"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        static_cast<void>(_width);
        auto int_type = std::make_shared<DataTypeInt32>();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr string_column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column.get())) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            string_column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            string_column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }

        // Create a temp_block to execute substring function.
        Block temp_block;
        // Substring requires the physical ColumnString; preserve nullability separately and
        // restore the original null map after transforming the nested values.
        temp_block.insert({string_column_ptr, remove_nullable(column_with_type_and_name.type),
                           column_with_type_and_name.name});
        temp_block.insert({int_type->create_column_const(temp_block.rows(), to_field<TYPE_INT>(1)),
                           int_type, "const 1"});
        temp_block.insert(
                {int_type->create_column_const(temp_block.rows(), to_field<TYPE_INT>(_width)),
                 int_type, fmt::format("const {}", _width)});
        temp_block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = 0; // str column
        temp_arguments[1] = 1; // pos
        temp_arguments[2] = 2; // width
        uint32_t result_column_id = 3;

        SubstringUtil::substring_execute(temp_block, temp_arguments, result_column_id,
                                         temp_block.rows());
        if (is_nullable) {
            auto res_column = ColumnNullable::create(
                    temp_block.get_by_position(result_column_id).column, null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            auto res_column = temp_block.get_by_position(result_column_id).column;
            return {std::move(res_column), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    DataTypePtr _source_type;
    int _width;
};

class BinaryTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    BinaryTruncatePartitionColumnTransform(const DataTypePtr source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "BinaryTruncate"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const auto& source = block.get_by_position(column_pos);
        ColumnPtr column = source.column->convert_to_full_column_if_const();
        ColumnPtr null_map;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            null_map = nullable->get_null_map_column_ptr();
            column = nullable->get_nested_column_ptr();
        }
        const auto& binary = assert_cast<const ColumnVarbinary&>(*column);
        auto result = ColumnVarbinary::create();
        result->reserve(binary.size());
        for (size_t row = 0; row < binary.size(); ++row) {
            const auto bytes = binary.get_data_at(row);
            // Iceberg binary prefixes count bytes, including partial UTF-8 sequences and NULs.
            result->insert_data(bytes.data, std::min(bytes.size, static_cast<size_t>(_width)));
        }
        if (null_map) {
            return {ColumnNullable::create(std::move(result), null_map),
                    make_nullable(get_result_type()), source.name};
        }
        return {std::move(result), remove_nullable(get_result_type()), source.name};
    }

private:
    DataTypePtr _source_type;
    int _width;
};

class IntegerTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    IntegerTruncatePartitionColumnTransform(const DataTypePtr source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "IntegerTruncate"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnInt32*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const int* end_in = in_data.data() + in_data.size();
        const Int32* __restrict p_in = in_data.data();
        Int32* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            *p_out = *p_in - ((*p_in % _width) + _width) % _width;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    DataTypePtr _source_type;
    int _width;
};

class BigintTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    BigintTruncatePartitionColumnTransform(const DataTypePtr source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "BigintTruncate"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnInt64*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt64::create();
        ColumnInt64::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const Int64* end_in = in_data.data() + in_data.size();
        const Int64* __restrict p_in = in_data.data();
        Int64* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            *p_out = *p_in - ((*p_in % _width) + _width) % _width;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    DataTypePtr _source_type;
    int _width;
};

template <PrimitiveType PT>
class DecimalTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    using T = typename PrimitiveTypeTraits<PT>::CppType;
    DecimalTruncatePartitionColumnTransform(const DataTypePtr source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "DecimalTruncate"; }

    DataTypePtr get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column.get())) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }

        const auto* const decimal_col = assert_cast<const ColumnDecimal<PT>*>(column_ptr.get());
        const auto& vec_src = decimal_col->get_data();

        auto col_res = ColumnDecimal<PT>::create(vec_src.size(), decimal_col->get_scale());
        auto& vec_res = col_res->get_data();

        const auto* __restrict p_in = reinterpret_cast<const T::NativeType*>(vec_src.data());
        const auto* end_in =
                reinterpret_cast<const T::NativeType*>(vec_src.data()) + vec_src.size();
        auto* __restrict p_out = reinterpret_cast<T::NativeType*>(vec_res.data());

        while (p_in < end_in) {
            typename T::NativeType remainder = ((*p_in % _width) + _width) % _width;
            *p_out = *p_in - remainder;
            ++p_in;
            ++p_out;
        }

        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    DataTypePtr _source_type;
    int _width;
};

class IntBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    IntBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "IntBucket"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnInt32*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const int* end_in = in_data.data() + in_data.size();
        const Int32* __restrict p_in = in_data.data();
        Int32* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            Int64 long_value = static_cast<Int64>(*p_in);
            uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);
            //            *p_out = ((hash_value >> 1) & INT32_MAX) % _bucket_num;
            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

class BigintBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    BigintBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "BigintBucket"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnInt64*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const Int64* end_in = in_data.data() + in_data.size();
        const Int64* __restrict p_in = in_data.data();
        Int32* __restrict p_out = out_data.data();
        while (p_in < end_in) {
            Int64 long_value = static_cast<Int64>(*p_in);
            uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);
            //            int value = ((hash_value >> 1) & INT32_MAX) % _bucket_num;
            int value = (hash_value & INT32_MAX) % _bucket_num;
            *p_out = value;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

template <PrimitiveType PT>
class DecimalBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    using T = typename PrimitiveTypeTraits<PT>::CppType;
    DecimalBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "DecimalBucket"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDecimal<PT>*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* __restrict p_in = reinterpret_cast<const T::NativeType*>(in_data.data());
        const auto* end_in =
                reinterpret_cast<const T::NativeType*>(in_data.data()) + in_data.size();
        Int32* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            std::string buffer = BitUtil::IntToByteBuffer(*p_in);
            uint32_t hash_value = HashUtil::murmur_hash3_32(buffer.data(), buffer.size(), 0);
            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return std::to_string(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

class DateBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "DateBucket"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const auto* end_in = in_data.data() + in_data.size();

        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateV2ValueType> value =
                    binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(UInt32*)p_in);

            int64_t days_from_unix_epoch = value.daynr() - 719528;
            uint32_t hash_value = HashUtil::murmur_hash3_32(&days_from_unix_epoch,
                                                            sizeof(days_from_unix_epoch), 0);

            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

template <PrimitiveType P>
class TimestampBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "TimestampBucket"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnVector<P>&>(*column_ptr).get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const auto* end_in = in_data.data() + in_data.size();

        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            if (is_nullable && assert_cast<const ColumnUInt8&>(*null_map_column_ptr)
                                       .get_data()[p_in - in_data.data()]) {
                *p_out++ = 0;
                ++p_in;
                continue;
            }
            auto value = PartitionColumnTransformUtils::timestamp_value(*p_in);

            // Iceberg hashes the entire signed epoch-microsecond value, not truncated seconds.
            Int64 long_value = value.datetime_diff_in_microseconds(
                    PartitionColumnTransformUtils::epoch_datetime());
            uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);

            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return std::to_string(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

template <typename ColumnType>
class ByteBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    ByteBucketPartitionColumnTransform(const DataTypePtr source_type, int bucket_num)
            : _bucket_num(bucket_num),
              _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override {
        if constexpr (std::is_same_v<ColumnType, ColumnVarbinary>) {
            return "BinaryBucket";
        } else if constexpr (std::is_same_v<ColumnType, ColumnUUID>) {
            return "UuidBucket";
        }
        return "StringBucket";
    }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto* str_col = assert_cast<const ColumnType*>(column_ptr.get());

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        const size_t row_count = str_col->size();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(row_count);
        for (size_t row = 0; row < row_count; ++row) {
            // Iceberg hashes raw bytes for both strings and binary, without text decoding.
            uint32_t hash_value;
            if constexpr (std::is_same_v<ColumnType, ColumnUUID>) {
                // Iceberg hashes UUID network-order bytes, never the native integer layout.
                const auto bytes = UUIDValue::to_big_endian(str_col->get_data()[row]);
                hash_value = HashUtil::murmur_hash3_32(bytes.data(), bytes.size(), 0);
            } else {
                const auto bytes = str_col->get_data_at(row);
                hash_value = HashUtil::murmur_hash3_32(bytes.data, bytes.size, 0);
            }
            out_data[row] = (hash_value & INT32_MAX) % _bucket_num;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

private:
    int _bucket_num;
    DataTypePtr _target_type;
};

using StringBucketPartitionColumnTransform = ByteBucketPartitionColumnTransform<ColumnString>;
using BinaryBucketPartitionColumnTransform = ByteBucketPartitionColumnTransform<ColumnVarbinary>;

class DateYearPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateYearPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "DateYear"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateV2ValueType> value =
                    binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(UInt32*)p_in);
            // Partition ordinals identify calendar units, not complete elapsed years.
            *p_out = static_cast<Int32>(value.year()) - 1970;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_year(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

template <PrimitiveType P>
class TimestampYearPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampYearPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "TimestampYear"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnVector<P>&>(*column_ptr).get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            if (is_nullable && assert_cast<const ColumnUInt8&>(*null_map_column_ptr)
                                       .get_data()[p_in - in_data.data()]) {
                *p_out++ = 0;
                ++p_in;
                continue;
            }
            auto value = PartitionColumnTransformUtils::timestamp_value(*p_in);
            *p_out = static_cast<Int32>(value.year()) - 1970;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_year(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

class DateMonthPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateMonthPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "DateMonth"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateV2ValueType> value =
                    binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(UInt32*)p_in);
            *p_out = (static_cast<Int32>(value.year()) - 1970) * 12 + value.month() - 1;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_month(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

template <PrimitiveType P>
class TimestampMonthPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampMonthPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "TimestampMonth"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnVector<P>&>(*column_ptr).get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            if (is_nullable && assert_cast<const ColumnUInt8&>(*null_map_column_ptr)
                                       .get_data()[p_in - in_data.data()]) {
                *p_out++ = 0;
                ++p_in;
                continue;
            }
            auto value = PartitionColumnTransformUtils::timestamp_value(*p_in);
            *p_out = (static_cast<Int32>(value.year()) - 1970) * 12 + value.month() - 1;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_month(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

class DateDayPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateDayPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "DateDay"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateV2ValueType> value =
                    binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(UInt32*)p_in);
            // datetime_diff<DAY> actually returns int
            *p_out = cast_set<int, int64_t, false>(
                    datetime_diff<DAY>(PartitionColumnTransformUtils::epoch_date(), value));
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            int day_value = std::any_cast<Int32>(value);
            return PartitionColumnTransformUtils::human_day(day_value);
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

template <PrimitiveType P>
class TimestampDayPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampDayPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "TimestampDay"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnVector<P>&>(*column_ptr).get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            if (is_nullable && assert_cast<const ColumnUInt8&>(*null_map_column_ptr)
                                       .get_data()[p_in - in_data.data()]) {
                *p_out++ = 0;
                ++p_in;
                continue;
            }
            auto value = PartitionColumnTransformUtils::timestamp_value(*p_in);
            *p_out = value.date_diff_in_days(PartitionColumnTransformUtils::epoch_datetime());
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_day(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

template <PrimitiveType P>
class TimestampHourPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampHourPartitionColumnTransform(const DataTypePtr source_type)
            : _target_type(DataTypeFactory::instance().create_data_type(TYPE_INT, false)) {}

    std::string name() const override { return "TimestampHour"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (is_column_nullable(*column_ptr)) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnVector<P>&>(*column_ptr).get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            if (is_nullable && assert_cast<const ColumnUInt8&>(*null_map_column_ptr)
                                       .get_data()[p_in - in_data.data()]) {
                *p_out++ = 0;
                ++p_in;
                continue;
            }
            auto value = PartitionColumnTransformUtils::timestamp_value(*p_in);
            // Calendar partitions floor to the containing hour rather than truncate toward zero.
            *p_out = value.date_diff_in_days(PartitionColumnTransformUtils::epoch_datetime()) * 24 +
                     value.hour();
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column), make_nullable(get_result_type()),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res), remove_nullable(get_result_type()),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const DataTypePtr type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_hour(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    DataTypePtr _target_type;
};

class VoidPartitionColumnTransform : public PartitionColumnTransform {
public:
    VoidPartitionColumnTransform(const DataTypePtr source_type) : _target_type(source_type) {}

    std::string name() const override { return "Void"; }

    DataTypePtr get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column.get())) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        } else {
            column_ptr = column_with_type_and_name.column;
        }
        auto res_column = ColumnNullable::create(std::move(column_ptr),
                                                 ColumnUInt8::create(column_ptr->size(), 1));
        return {std::move(res_column), make_nullable(get_result_type()),
                column_with_type_and_name.name};
    }

private:
    DataTypePtr _target_type;
};

} // namespace doris
