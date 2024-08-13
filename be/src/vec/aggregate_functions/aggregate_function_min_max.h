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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionMinMaxAny.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <string.h>

#include <memory>
#include <vector>

#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {
class Arena;
template <typename T>
class ColumnDecimal;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/// For numeric values.
template <typename T>
struct SingleValueDataFixed {
private:
    using Self = SingleValueDataFixed;

    bool has_value =
            false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

public:
    SingleValueDataFixed() = default;
    SingleValueDataFixed(bool has_value_, T value_) : has_value(has_value_), value(value_) {}
    bool has() const { return has_value; }

    constexpr static bool IsFixedLength = true;

    void set_to_min_max(bool max) { value = max ? type_limit<T>::max() : type_limit<T>::min(); }

    void change_if(const IColumn& column, size_t row_num, bool less) {
        has_value = true;
        value = less ? std::min(assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(
                                        column)
                                        .get_data()[row_num],
                                value)
                     : std::max(assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(
                                        column)
                                        .get_data()[row_num],
                                value);
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnVector<T>&>(to).get_data().push_back(value);
        } else {
            assert_cast<ColumnVector<T>&>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(has(), buf);
        if (has()) {
            write_binary(value, buf);
        }
    }

    void read(BufferReadable& buf, Arena* arena) {
        read_binary(has_value, buf);
        if (has()) {
            read_binary(value, buf);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena*) {
        has_value = true;
        value = assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    /// Assuming to.has()
    void change(const Self& to, Arena*) {
        has_value = true;
        value = to.value;
    }

    bool change_if_less(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] < value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] > value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena* arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena* arena) {
        if (UNLIKELY(!has() && to.has())) {
            change(to, arena);
        }
    }
};

/// For decimal values.
template <typename T>
struct SingleValueDataDecimal {
private:
    using Self = SingleValueDataDecimal;

    bool has_value =
            false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

public:
    SingleValueDataDecimal() = default;
    SingleValueDataDecimal(bool has_value_, T value_) : has_value(has_value_), value(value_) {}
    bool has() const { return has_value; }

    constexpr static bool IsFixedLength = true;

    void set_to_min_max(bool max) { value = max ? type_limit<T>::max() : type_limit<T>::min(); }

    void change_if(const IColumn& column, size_t row_num, bool less) {
        has_value = true;
        value = less ? std::min(assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(
                                        column)
                                        .get_data()[row_num],
                                value)
                     : std::max(assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(
                                        column)
                                        .get_data()[row_num],
                                value);
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnDecimal<T>&>(to).insert_data((const char*)&value, 0);
        } else {
            assert_cast<ColumnDecimal<T>&>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(has(), buf);
        if (has()) {
            write_binary(value, buf);
        }
    }

    void read(BufferReadable& buf, Arena* arena) {
        read_binary(has_value, buf);
        if (has()) {
            read_binary(value, buf);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena*) {
        has_value = true;
        value = assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    /// Assuming to.has()
    void change(const Self& to, Arena*) {
        has_value = true;
        value = to.value;
    }

    bool change_if_less(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] < value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnDecimal<T>&, TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] > value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena* arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena* arena) {
        if (UNLIKELY(!has() && to.has())) {
            change(to, arena);
        }
    }
};

/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
struct SingleValueDataString {
private:
    using Self = SingleValueDataString;

    Int32 size = -1;    /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    std::unique_ptr<char[]> large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE =
            AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

public:
    ~SingleValueDataString() = default;

    constexpr static bool IsFixedLength = false;

    bool has() const { return size >= 0; }

    const char* get_data() const {
        return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data.get();
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnString&>(to).insert_data(get_data(), size);
        } else {
            assert_cast<ColumnString&>(to).insert_default();
        }
    }

    void reset() {
        if (size != -1) {
            size = -1;
            capacity = 0;
            large_data = nullptr;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(size, buf);
        if (has()) {
            buf.write(get_data(), size);
        }
    }

    void read(BufferReadable& buf, Arena* arena) {
        Int32 rhs_size;
        read_binary(rhs_size, buf);

        if (rhs_size >= 0) {
            if (rhs_size <= MAX_SMALL_STRING_SIZE) {
                /// Don't free large_data here.

                size = rhs_size;

                if (size > 0) {
                    buf.read(small_data, size);
                }
            } else {
                if (capacity < rhs_size) {
                    capacity = round_up_to_power_of_two_or_zero(rhs_size);
                    large_data.reset(new char[capacity]);
                }

                size = rhs_size;
                buf.read(large_data.get(), size);
            }
        } else {
            /// Don't free large_data here.
            size = rhs_size;
        }
    }

    StringRef get_string_ref() const { return StringRef(get_data(), size); }

    /// Assuming to.has()
    void change_impl(StringRef value, Arena* arena) {
        Int32 value_size = value.size;

        if (value_size <= MAX_SMALL_STRING_SIZE) {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0) {
                memcpy(small_data, value.data, size);
            }
        } else {
            if (capacity < value_size) {
                /// Don't free large_data here.
                capacity = round_up_to_power_of_two_or_zero(value_size);
                large_data.reset(new char[capacity]);
            }

            size = value_size;
            memcpy(large_data.get(), value.data, size);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena* arena) {
        change_impl(
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                        row_num),
                arena);
    }

    void change(const Self& to, Arena* arena) { change_impl(to.get_string_ref(), arena); }

    bool change_if_less(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() ||
            assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num) < get_string_ref()) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() ||
            assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num) > get_string_ref()) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.get_string_ref() < get_string_ref())) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.get_string_ref() > get_string_ref())) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena* arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena* arena) {
        if (UNLIKELY(!has() && to.has())) {
            change(to, arena);
        }
    }
};

template <typename Data>
struct AggregateFunctionMaxData : public Data {
    using Self = AggregateFunctionMaxData;
    using Data::IsFixedLength;
    constexpr static bool IS_ANY = false;

    AggregateFunctionMaxData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena* arena) {
        if constexpr (Data::IsFixedLength) {
            this->change_if(column, row_num, false);
        } else {
            this->change_if_greater(column, row_num, arena);
        }
    }

    void change_if_better(const Self& to, Arena* arena) { this->change_if_greater(to, arena); }

    void reset() {
        if constexpr (Data::IsFixedLength) {
            this->set_to_min_max(false);
        }
        Data::reset();
    }

    static const char* name() { return "max"; }
};

template <typename Data>
struct AggregateFunctionMinData : Data {
    using Self = AggregateFunctionMinData;
    using Data::IsFixedLength;
    constexpr static bool IS_ANY = false;

    AggregateFunctionMinData() { reset(); }

    void change_if_better(const IColumn& column, size_t row_num, Arena* arena) {
        if constexpr (Data::IsFixedLength) {
            this->change_if(column, row_num, true);
        } else {
            this->change_if_less(column, row_num, arena);
        }
    }
    void change_if_better(const Self& to, Arena* arena) { this->change_if_less(to, arena); }

    void reset() {
        if constexpr (Data::IsFixedLength) {
            this->set_to_min_max(true);
        }
        Data::reset();
    }

    static const char* name() { return "min"; }
};

template <typename Data>
struct AggregateFunctionAnyData : Data {
    using Self = AggregateFunctionAnyData;
    using Data::IsFixedLength;
    constexpr static bool IS_ANY = true;

    void change_if_better(const IColumn& column, size_t row_num, Arena* arena) {
        this->change_first_time(column, row_num, arena);
    }
    void change_if_better(const Self& to, Arena* arena) { this->change_first_time(to, arena); }

    static const char* name() { return "any"; }
};

template <typename Data>
class AggregateFunctionsSingleValue final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>> {
private:
    DataTypePtr& type;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>;

public:
    AggregateFunctionsSingleValue(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>(arguments),
              type(this->argument_types[0]) {
        if (StringRef(Data::name()) == StringRef("min") ||
            StringRef(Data::name()) == StringRef("max")) {
            if (!type->is_comparable()) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                       "Illegal type {} of argument of aggregate function {} "
                                       "because the values of that data type are not comparable",
                                       type->get_name(), get_name());
            }
        }
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).change_if_better(*columns[0], row_num, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        if constexpr (Data::IS_ANY) {
            DCHECK_GT(batch_size, 0);
            this->data(place).change_if_better(*columns[0], 0, arena);
        } else {
            Base::add_batch_single_place(batch_size, place, columns, arena);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).read(buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        if constexpr (Data::IsFixedLength) {
            const auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
            auto* column_data = reinterpret_cast<const Data*>(col.get_data().data());
            Data* data = reinterpret_cast<Data*>(places);
            for (size_t i = 0; i != num_rows; ++i) {
                data[i] = column_data[i];
            }
        } else {
            Base::deserialize_from_column(places, column, arena, num_rows);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        if constexpr (Data::IsFixedLength) {
            auto& dst_column = assert_cast<ColumnFixedLengthObject&>(*dst);
            dst_column.resize(num_rows);
            auto* dst_data = reinterpret_cast<Data*>(dst_column.get_data().data());
            for (size_t i = 0; i != num_rows; ++i) {
                dst_data[i] = this->data(places[i] + offset);
            }
        } else {
            Base::serialize_to_column(places, offset, dst, num_rows);
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        if constexpr (Data::IsFixedLength) {
            auto& dst_column = assert_cast<ColumnFixedLengthObject&>(*dst);
            dst_column.resize(num_rows);
            auto* dst_data = reinterpret_cast<Data*>(dst_column.get_data().data());
            for (size_t i = 0; i != num_rows; ++i) {
                dst_data[i].change(*columns[0], i, arena);
            }
        } else {
            Base::streaming_agg_serialize_to_column(columns, dst, num_rows, arena);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        if constexpr (Data::IsFixedLength) {
            const auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
            auto* column_data = reinterpret_cast<const Data*>(col.get_data().data());
            const size_t num_rows = column.size();
            for (size_t i = 0; i != num_rows; ++i) {
                this->data(place).change_if_better(column_data[i], arena);
            }
        } else {
            Base::deserialize_and_merge_from_column(place, column, arena);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        if constexpr (Data::IsFixedLength) {
            DCHECK(end <= column.size() && begin <= end) << ", begin:" << begin << ", end:" << end
                                                         << ", column.size():" << column.size();
            auto& col = assert_cast<const ColumnFixedLengthObject&>(column);
            auto* data = reinterpret_cast<const Data*>(col.get_data().data());
            for (size_t i = begin; i <= end; ++i) {
                this->data(place).change_if_better(data[i], arena);
            }
        } else {
            Base::deserialize_and_merge_from_column_range(place, column, begin, end, arena);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena* arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena* arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        if constexpr (Data::IsFixedLength) {
            auto& col = assert_cast<ColumnFixedLengthObject&>(to);
            size_t old_size = col.size();
            col.resize(old_size + 1);
            *(reinterpret_cast<Data*>(col.get_data().data()) + old_size) = this->data(place);
        } else {
            Base::serialize_without_key_to_column(place, to);
        }
    }

    MutableColumnPtr create_serialize_column() const override {
        if constexpr (Data::IsFixedLength) {
            return ColumnFixedLengthObject::create(sizeof(Data));
        } else {
            return ColumnString::create();
        }
    }

    DataTypePtr get_serialized_type() const override {
        if constexpr (Data::IsFixedLength) {
            return std::make_shared<DataTypeFixedLengthObject>();
        } else {
            return std::make_shared<DataTypeString>();
        }
    }
};

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value(const String& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable);
} // namespace doris::vectorized
