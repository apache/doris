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

#include "runtime/types.h"
#include "util/bit_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/function_string.h"

namespace doris {

namespace iceberg {
class Type;
class PartitionField;
}; // namespace iceberg

namespace vectorized {

class IColumn;
class PartitionColumnTransform;

class PartitionColumnTransforms {
private:
    PartitionColumnTransforms();

public:
    static std::unique_ptr<PartitionColumnTransform> create(
            const doris::iceberg::PartitionField& field, const TypeDescriptor& source_type);
};

class PartitionColumnTransformUtils {
public:
    static DateV2Value<DateV2ValueType>& epoch_date() {
        static DateV2Value<DateV2ValueType> epoch_date;
        static bool initialized = false;
        if (!initialized) {
            epoch_date.from_date_str("1970-01-01 00:00:00", 19);
            initialized = true;
        }
        return epoch_date;
    }

    static DateV2Value<DateTimeV2ValueType>& epoch_datetime() {
        static DateV2Value<DateTimeV2ValueType> epoch_datetime;
        static bool initialized = false;
        if (!initialized) {
            epoch_datetime.from_date_str("1970-01-01 00:00:00", 19);
            initialized = true;
        }
        return epoch_datetime;
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
        int day_value = hour_ordinal / 24;
        int housr_value = hour_ordinal % 24;
        auto ymd = std::chrono::year_month_day(std::chrono::sys_days(
                std::chrono::floor<std::chrono::days>(EPOCH + std::chrono::days(day_value))));
        return fmt::format("{:04d}-{:02d}-{:02d}-{:02d}", static_cast<int>(ymd.year()),
                           static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()),
                           housr_value);
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

    virtual const TypeDescriptor& get_result_type() const = 0;

    virtual ColumnWithTypeAndName apply(const Block& block, int column_pos) = 0;

    virtual std::string to_human_string(const TypeDescriptor& type, const std::any& value) const;

    virtual std::string get_partition_value(const TypeDescriptor& type,
                                            const std::any& value) const;
};

class IdentityPartitionColumnTransform : public PartitionColumnTransform {
public:
    IdentityPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type) {}

    std::string name() const override { return "Identity"; }

    const TypeDescriptor& get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        return {column_with_type_and_name.column, column_with_type_and_name.type,
                column_with_type_and_name.name};
    }

private:
    TypeDescriptor _source_type;
};

class StringTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    StringTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "StringTruncate"; }

    const TypeDescriptor& get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        static_cast<void>(_width);
        auto int_type = std::make_shared<DataTypeInt32>();
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr string_column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            string_column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            string_column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }

        // Create a temp_block to execute substring function.
        Block temp_block;
        temp_block.insert(column_with_type_and_name);
        temp_block.insert({int_type->create_column_const(temp_block.rows(), to_field(1)), int_type,
                           "const 1"});
        temp_block.insert({int_type->create_column_const(temp_block.rows(), to_field(_width)),
                           int_type, fmt::format("const {}", _width)});
        temp_block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
        ColumnNumbers temp_arguments(3);
        temp_arguments[0] = 0; // str column
        temp_arguments[1] = 1; // pos
        temp_arguments[2] = 2; // width
        size_t result_column_id = 3;

        SubstringUtil::substring_execute(temp_block, temp_arguments, result_column_id,
                                         temp_block.rows());
        if (is_nullable) {
            auto res_column = ColumnNullable::create(
                    temp_block.get_by_position(result_column_id).column, null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            auto res_column = temp_block.get_by_position(result_column_id).column;
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

class IntegerTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    IntegerTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "IntegerTruncate"; }

    const TypeDescriptor& get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            return {res_column,
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

class BigintTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    BigintTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "BigintTruncate"; }

    const TypeDescriptor& get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            return {res_column,
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

template <typename T>
class DecimalTruncatePartitionColumnTransform : public PartitionColumnTransform {
public:
    DecimalTruncatePartitionColumnTransform(const TypeDescriptor& source_type, int width)
            : _source_type(source_type), _width(width) {}

    std::string name() const override { return "DecimalTruncate"; }

    const TypeDescriptor& get_result_type() const override { return _source_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
            is_nullable = true;
        } else {
            column_ptr = column_with_type_and_name.column;
            is_nullable = false;
        }

        const auto* const decimal_col = check_and_get_column<ColumnDecimal<T>>(column_ptr);
        const auto& vec_src = decimal_col->get_data();

        auto col_res = ColumnDecimal<T>::create(vec_src.size(), decimal_col->get_scale());
        auto& vec_res = col_res->get_data();

        const typename T::NativeType* __restrict p_in =
                reinterpret_cast<const T::NativeType*>(vec_src.data());
        const typename T::NativeType* end_in =
                reinterpret_cast<const T::NativeType*>(vec_src.data()) + vec_src.size();
        typename T::NativeType* __restrict p_out = reinterpret_cast<T::NativeType*>(vec_res.data());

        while (p_in < end_in) {
            typename T::NativeType remainder = ((*p_in % _width) + _width) % _width;
            *p_out = *p_in - remainder;
            ++p_in;
            ++p_out;
        }

        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {res_column,
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _width;
};

class IntBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    IntBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "IntBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class BigintBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    BigintBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "BigintBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

template <typename T>
class DecimalBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    DecimalBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "DecimalBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDecimal<T>*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        auto& vec_res = col_res->get_data();

        const typename T::NativeType* __restrict p_in =
                reinterpret_cast<const T::NativeType*>(in_data.data());
        const typename T::NativeType* end_in =
                reinterpret_cast<const T::NativeType*>(in_data.data()) + in_data.size();
        typename T::NativeType* __restrict p_out = reinterpret_cast<T::NativeType*>(vec_res.data());

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
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const TypeDescriptor& type,
                                    const std::any& value) const override {
        if (value.has_value()) {
            return std::to_string(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class DateBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "DateBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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

            int32_t days_from_unix_epoch = value.daynr() - 719528;
            Int64 long_value = static_cast<Int64>(days_from_unix_epoch);
            uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);

            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class TimestampBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "TimestampBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateTimeV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());
        const auto* end_in = in_data.data() + in_data.size();

        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateTimeV2ValueType> value =
                    binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(UInt64*)p_in);

            int64_t timestamp;
            if (!value.unix_timestamp(&timestamp, "UTC")) {
                LOG(WARNING) << "Failed to call unix_timestamp :" << value.debug_string();
                timestamp = 0;
            }
            Int64 long_value = static_cast<Int64>(timestamp) * 1000000;
            uint32_t hash_value = HashUtil::murmur_hash3_32(&long_value, sizeof(long_value), 0);

            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return std::to_string(std::any_cast<Int32>(value));
            ;
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class StringBucketPartitionColumnTransform : public PartitionColumnTransform {
public:
    StringBucketPartitionColumnTransform(const TypeDescriptor& source_type, int bucket_num)
            : _source_type(source_type), _bucket_num(bucket_num), _target_type(TYPE_INT) {}

    std::string name() const override { return "StringBucket"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto* str_col = assert_cast<const ColumnString*>(column_ptr.get());

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        const auto& data = str_col->get_chars();
        const auto& offsets = str_col->get_offsets();

        size_t offset_size = offsets.size();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(offset_size);
        auto* __restrict p_out = out_data.data();

        for (int i = 0; i < offset_size; i++) {
            const unsigned char* raw_str = &data[offsets[i - 1]];
            ColumnString::Offset size = offsets[i] - offsets[i - 1];
            uint32_t hash_value = HashUtil::murmur_hash3_32(raw_str, size, 0);

            *p_out = (hash_value & INT32_MAX) % _bucket_num;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

private:
    TypeDescriptor _source_type;
    int _bucket_num;
    TypeDescriptor _target_type;
};

class DateYearPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateYearPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "DateYear"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            *p_out = datetime_diff<YEAR>(PartitionColumnTransformUtils::epoch_date(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_year(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class TimestampYearPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampYearPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "TimestampYear"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateTimeV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateTimeV2ValueType> value =
                    binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(UInt64*)p_in);
            *p_out = datetime_diff<YEAR>(PartitionColumnTransformUtils::epoch_datetime(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_year(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class DateMonthPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateMonthPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "DateMonth"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            *p_out = datetime_diff<MONTH>(PartitionColumnTransformUtils::epoch_date(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_month(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class TimestampMonthPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampMonthPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "TimestampMonth"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateTimeV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateTimeV2ValueType> value =
                    binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(UInt64*)p_in);
            *p_out = datetime_diff<MONTH>(PartitionColumnTransformUtils::epoch_datetime(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_month(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class DateDayPartitionColumnTransform : public PartitionColumnTransform {
public:
    DateDayPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "DateDay"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
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
            *p_out = datetime_diff<DAY>(PartitionColumnTransformUtils::epoch_date(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const TypeDescriptor& type,
                                    const std::any& value) const override {
        if (value.has_value()) {
            int day_value = std::any_cast<Int32>(value);
            return PartitionColumnTransformUtils::human_day(day_value);
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class TimestampDayPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampDayPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "TimestampDay"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateTimeV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateTimeV2ValueType> value =
                    binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(UInt64*)p_in);
            *p_out = datetime_diff<DAY>(PartitionColumnTransformUtils::epoch_datetime(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        return get_partition_value(type, value);
    }

    std::string get_partition_value(const TypeDescriptor& type,
                                    const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_day(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class TimestampHourPartitionColumnTransform : public PartitionColumnTransform {
public:
    TimestampHourPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(TYPE_INT) {}

    std::string name() const override { return "TimestampHour"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        //1) get the target column ptr
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);
        ColumnPtr column_ptr = column_with_type_and_name.column->convert_to_full_column_if_const();
        CHECK(column_ptr != nullptr);

        //2) get the input data from block
        ColumnPtr null_map_column_ptr;
        bool is_nullable = false;
        if (column_ptr->is_nullable()) {
            const ColumnNullable* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            is_nullable = true;
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        }
        const auto& in_data = assert_cast<const ColumnDateTimeV2*>(column_ptr.get())->get_data();

        //3) do partition routing
        auto col_res = ColumnInt32::create();
        ColumnInt32::Container& out_data = col_res->get_data();
        out_data.resize(in_data.size());

        const auto* end_in = in_data.data() + in_data.size();
        const auto* __restrict p_in = in_data.data();
        auto* __restrict p_out = out_data.data();

        while (p_in < end_in) {
            DateV2Value<DateTimeV2ValueType> value =
                    binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(UInt64*)p_in);
            *p_out = datetime_diff<HOUR>(PartitionColumnTransformUtils::epoch_datetime(), value);
            ++p_in;
            ++p_out;
        }

        //4) create the partition column and return
        if (is_nullable) {
            auto res_column = ColumnNullable::create(std::move(col_res), null_map_column_ptr);
            return {std::move(res_column),
                    DataTypeFactory::instance().create_data_type(get_result_type(), true),
                    column_with_type_and_name.name};
        } else {
            return {std::move(col_res),
                    DataTypeFactory::instance().create_data_type(get_result_type(), false),
                    column_with_type_and_name.name};
        }
    }

    std::string to_human_string(const TypeDescriptor& type, const std::any& value) const override {
        if (value.has_value()) {
            return PartitionColumnTransformUtils::human_hour(std::any_cast<Int32>(value));
        } else {
            return "null";
        }
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

class VoidPartitionColumnTransform : public PartitionColumnTransform {
public:
    VoidPartitionColumnTransform(const TypeDescriptor& source_type)
            : _source_type(source_type), _target_type(source_type) {}

    std::string name() const override { return "Void"; }

    const TypeDescriptor& get_result_type() const override { return _target_type; }

    ColumnWithTypeAndName apply(const Block& block, int column_pos) override {
        const ColumnWithTypeAndName& column_with_type_and_name = block.get_by_position(column_pos);

        ColumnPtr column_ptr;
        ColumnPtr null_map_column_ptr;
        if (auto* nullable_column =
                    check_and_get_column<ColumnNullable>(column_with_type_and_name.column)) {
            null_map_column_ptr = nullable_column->get_null_map_column_ptr();
            column_ptr = nullable_column->get_nested_column_ptr();
        } else {
            column_ptr = column_with_type_and_name.column;
        }
        auto res_column = ColumnNullable::create(std::move(column_ptr),
                                                 ColumnUInt8::create(column_ptr->size(), 1));
        return {std::move(res_column),
                DataTypeFactory::instance().create_data_type(get_result_type(), true),
                column_with_type_and_name.name};
    }

private:
    TypeDescriptor _source_type;
    TypeDescriptor _target_type;
};

} // namespace vectorized
} // namespace doris
