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
#include <glog/logging.h>
#include <string.h>

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "common/cast_set.h"
#include "common/logging.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
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
#include "common/compile_check_begin.h"
namespace vectorized {
class Arena;
template <PrimitiveType T>
class ColumnDecimal;
template <PrimitiveType T>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/// For numeric values.
template <PrimitiveType T>
struct SingleValueDataFixed {
private:
    using Self = SingleValueDataFixed;

    bool has_value =
            false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    typename PrimitiveTypeTraits<T>::ColumnItemType value;

public:
    SingleValueDataFixed() = default;
    SingleValueDataFixed(bool has_value_, typename PrimitiveTypeTraits<T>::ColumnItemType value_)
            : has_value(has_value_), value(value_) {}
    bool has() const { return has_value; }

    constexpr static bool IsFixedLength = true;

    void set_to_min_max(bool max) {
        value = max ? type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::max()
                    : type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::min();
    }

    void change_if(const IColumn& column, size_t row_num, bool less) {
        has_value = true;
        value = less ? std::min(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                            TypeCheckOnRelease::DISABLE>(column)
                                        .get_data()[row_num],
                                value)
                     : std::max(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                            TypeCheckOnRelease::DISABLE>(column)
                                        .get_data()[row_num],
                                value);
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&>(to).get_data().push_back(
                    value);
        } else {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(has());
        if (has()) {
            buf.write_binary(value);
        }
    }

    void read(BufferReadable& buf, Arena&) {
        buf.read_binary(has_value);
        if (has()) {
            buf.read_binary(value);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena&) {
        has_value = true;
        value = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                            TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    /// Assuming to.has()
    void change(const Self& to, Arena&) {
        has_value = true;
        value = to.value;
    }

    bool change_if_less(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                  TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] < value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                  TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] > value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool check_if_equal(const IColumn& column, size_t row_num) const {
        if (!has()) {
            return false;
        }
        return assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                           TypeCheckOnRelease::DISABLE>(column)
                       .get_data()[row_num] == value;
    }

    bool change_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena& arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena& arena) {
        if (UNLIKELY(!has() && to.has())) {
            change(to, arena);
        }
    }
};

/// For decimal values.
template <PrimitiveType T>
struct SingleValueDataDecimal {
private:
    using Self = SingleValueDataDecimal;

    bool has_value =
            false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    typename PrimitiveTypeTraits<T>::ColumnItemType value;

public:
    SingleValueDataDecimal() = default;
    SingleValueDataDecimal(bool has_value_, typename PrimitiveTypeTraits<T>::ColumnItemType value_)
            : has_value(has_value_), value(value_) {}
    bool has() const { return has_value; }

    constexpr static bool IsFixedLength = true;

    void set_to_min_max(bool max) {
        value = max ? type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::max()
                    : type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::min();
    }

    void change_if(const IColumn& column, size_t row_num, bool less) {
        has_value = true;
        value = less ? std::min(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                            TypeCheckOnRelease::DISABLE>(column)
                                        .get_data()[row_num],
                                value)
                     : std::max(assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                            TypeCheckOnRelease::DISABLE>(column)
                                        .get_data()[row_num],
                                value);
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&>(to).insert_data(
                    (const char*)&value, 0);
        } else {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(has());
        if (has()) {
            buf.write_binary(value);
        }
    }

    void read(BufferReadable& buf, Arena&) {
        buf.read_binary(has_value);
        if (has()) {
            buf.read_binary(value);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena&) {
        has_value = true;
        value = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                            TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    /// Assuming to.has()
    void change(const Self& to, Arena&) {
        has_value = true;
        value = to.value;
    }

    bool change_if_less(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                  TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] < value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                  TypeCheckOnRelease::DISABLE>(column)
                                      .get_data()[row_num] > value) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool check_if_equal(const IColumn& column, size_t row_num) const {
        if (!has()) {
            return false;
        }
        return assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                           TypeCheckOnRelease::DISABLE>(column)
                       .get_data()[row_num] == value;
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena& arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena& arena) {
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
    // This function uses int32 for storage, which triggers a 64-bit to 32-bit conversion warning.
    // However, considering compatibility with future upgrades, no changes will be made here.
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
        buf.write_binary(size);
        if (has()) {
            buf.write(get_data(), size);
        }
    }

    void read(BufferReadable& buf, Arena&) {
        Int32 rhs_size;
        buf.read_binary(rhs_size);

        if (rhs_size >= 0) {
            if (rhs_size <= MAX_SMALL_STRING_SIZE) {
                /// Don't free large_data here.

                size = rhs_size;

                if (size > 0) {
                    buf.read(small_data, size);
                }
            } else {
                if (capacity < rhs_size) {
                    capacity = (Int32)round_up_to_power_of_two_or_zero(rhs_size);
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
    void change_impl(StringRef value, Arena&) {
        Int32 value_size = cast_set<Int32>(value.size);
        if (value_size <= MAX_SMALL_STRING_SIZE) {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0) {
                memcpy(small_data, value.data, size);
            }
        } else {
            if (capacity < value_size) {
                /// Don't free large_data here.
                capacity = (Int32)round_up_to_power_of_two_or_zero(value_size);
                large_data.reset(new char[capacity]);
            }

            size = value_size;
            memcpy(large_data.get(), value.data, size);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena& arena) {
        change_impl(
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                        row_num),
                arena);
    }

    void change(const Self& to, Arena& arena) { change_impl(to.get_string_ref(), arena); }

    bool change_if_less(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() ||
            assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num) < get_string_ref()) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() ||
            assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                    row_num) > get_string_ref()) {
            change(column, row_num, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.get_string_ref() < get_string_ref())) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to.get_string_ref() > get_string_ref())) {
            change(to, arena);
            return true;
        } else {
            return false;
        }
    }

    bool check_if_equal(const IColumn& column, size_t row_num) const {
        if (!has()) {
            return false;
        }
        return assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                       row_num) == get_string_ref();
    }

    void change_first_time(const IColumn& column, size_t row_num, Arena& arena) {
        if (UNLIKELY(!has())) {
            change(column, row_num, arena);
        }
    }

    void change_first_time(const Self& to, Arena& arena) {
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

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        if constexpr (Data::IsFixedLength) {
            this->change_if(column, row_num, false);
        } else {
            this->change_if_greater(column, row_num, arena);
        }
    }

    void change_if_better(const Self& to, Arena& arena) { this->change_if_greater(to, arena); }

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

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        if constexpr (Data::IsFixedLength) {
            this->change_if(column, row_num, true);
        } else {
            this->change_if_less(column, row_num, arena);
        }
    }
    void change_if_better(const Self& to, Arena& arena) { this->change_if_less(to, arena); }

    void reset() {
        if constexpr (Data::IsFixedLength) {
            this->set_to_min_max(true);
        }
        Data::reset();
    }

    static const char* name() { return "min"; }
};

// this is used for plain type about any_value function
template <typename Data>
struct AggregateFunctionAnyData : Data {
    using Self = AggregateFunctionAnyData;
    using Data::IsFixedLength;
    static const char* name() { return "any"; }
    constexpr static bool IS_ANY = true;
    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        this->change_first_time(column, row_num, arena);
    }

    void change_if_better(const Self& to, Arena& arena) { this->change_first_time(to, arena); }
};

// this is used for complex type about any_value function
struct SingleValueDataComplexType {
    static const char* name() { return "any"; }
    constexpr static bool IS_ANY = true;
    constexpr static bool IsFixedLength = false;
    using Self = SingleValueDataComplexType;

    SingleValueDataComplexType() = default;

    SingleValueDataComplexType(const DataTypes& argument_types, int be_version) {
        column_type = argument_types[0];
        column_data = column_type->create_column();
        be_exec_version = be_version;
    }

    bool has() const { return has_value; }

    void change_first_time(const IColumn& column, size_t row_num, Arena&) {
        if (UNLIKELY(!has())) {
            change_impl(column, row_num);
        }
    }

    void change_first_time(const Self& to, Arena&) {
        if (UNLIKELY(!has() && to.has())) {
            change_impl(*to.column_data, 0);
        }
    }

    void change_impl(const IColumn& column, size_t row_num) {
        DCHECK_EQ(column_data->size(), 0);
        column_data->insert_from(column, row_num);
        has_value = true;
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            to.insert_from(*column_data, 0);
        } else {
            to.insert_default();
        }
    }

    void reset() {
        column_data->clear();
        has_value = false;
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(has());
        if (!has()) {
            return;
        }
        auto size_bytes =
                column_type->get_uncompressed_serialized_bytes(*column_data, be_exec_version);
        std::string memory_buffer(size_bytes, '0');
        auto* p = column_type->serialize(*column_data, memory_buffer.data(), be_exec_version);
        buf.write_binary(memory_buffer);
        DCHECK_EQ(p, memory_buffer.data() + size_bytes);
    }

    void read(BufferReadable& buf, Arena& arena) {
        buf.read_binary(has_value);
        if (!has()) {
            return;
        }
        std::string memory_buffer;
        buf.read_binary(memory_buffer);
        const auto* p =
                column_type->deserialize(memory_buffer.data(), &column_data, be_exec_version);
        DCHECK_EQ(p, memory_buffer.data() + memory_buffer.size());
    }

    void change_if_better(const IColumn& column, size_t row_num, Arena& arena) {
        this->change_first_time(column, row_num, arena);
    }

    void change_if_better(const Self& to, Arena& arena) { this->change_first_time(to, arena); }

    bool check_if_equal(const IColumn& column, size_t row_num) const { return false; }

private:
    bool has_value = false;
    MutableColumnPtr column_data;
    DataTypePtr column_type;
    int be_exec_version = -1;
};

template <typename Data>
class AggregateFunctionsSingleValue final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>> {
private:
    DataTypePtr& type;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionsSingleValue<Data>>;
    using IAggregateFunction::argument_types;

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

    void create(AggregateDataPtr __restrict place) const override {
        if constexpr (std::is_same_v<Data, SingleValueDataComplexType>) {
            new (place) Data(argument_types, IAggregateFunction::version);
        } else {
            new (place) Data;
        }
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).change_if_better(*columns[0], row_num, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        if constexpr (Data::IS_ANY) {
            DCHECK_GT(batch_size, 0);
            this->data(place).change_if_better(*columns[0], 0, arena);
        } else {
            Base::add_batch_single_place(batch_size, place, columns, arena);
        }
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, AggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(AggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        this->data(place).read(buf, arena);
    }

    void insert_result_into(AggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena& arena,
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
                                           const size_t num_rows, Arena& arena) const override {
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
                                           Arena& arena) const override {
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
                                                 Arena& arena) const override {
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
                                   AggregateDataPtr rhs, const IColumn* column, Arena& arena,
                                   const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec(places, offset, rhs, arena, num_rows);
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column,
                                            Arena& arena, const size_t num_rows) const override {
        this->deserialize_from_column(rhs, *column, arena, num_rows);
        DEFER({ this->destroy_vec(rhs, num_rows); });
        this->merge_vec_selected(places, offset, rhs, arena, num_rows);
    }

    void serialize_without_key_to_column(AggregateDataPtr __restrict place,
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

    bool supported_incremental_mode() const override { return !(Data::IS_ANY); }

    void execute_function_with_incremental(int64_t partition_start, int64_t partition_end,
                                           int64_t frame_start, int64_t frame_end,
                                           AggregateDataPtr place, const IColumn** columns,
                                           Arena& arena, bool previous_is_nul, bool end_is_nul,
                                           bool has_null, UInt8* use_null_result,
                                           UInt8* could_use_previous_result) const override {
        int64_t current_frame_start = std::max<int64_t>(frame_start, partition_start);
        int64_t current_frame_end = std::min<int64_t>(frame_end, partition_end);
        if (current_frame_start >= current_frame_end) {
            *use_null_result = true;
            return;
        }
        if (*could_use_previous_result) {
            auto outcoming_pos = frame_start - 1;
            auto incoming_pos = frame_end - 1;
            if (!previous_is_nul && outcoming_pos >= partition_start &&
                outcoming_pos < partition_end) {
                if (this->data(place).check_if_equal(*columns[0], outcoming_pos)) {
                    this->data(place).reset();
                    if (has_null) {
                        const auto& null_map_data =
                                assert_cast<const ColumnUInt8*>(columns[1])->get_data();
                        for (size_t i = current_frame_start; i < current_frame_end; ++i) {
                            if (null_map_data[i] == 0) {
                                this->data(place).change_if_better(*columns[0], i, arena);
                            }
                        }
                    } else {
                        this->add_range_single_place(partition_start, partition_end,
                                                     current_frame_start, current_frame_end, place,
                                                     columns, arena, use_null_result,
                                                     could_use_previous_result);
                    }
                    return;
                }
            }
            if (!end_is_nul && incoming_pos >= partition_start && incoming_pos < partition_end) {
                this->data(place).change_if_better(*columns[0], incoming_pos, arena);
            }

        } else {
            this->add_range_single_place(partition_start, partition_end, frame_start, frame_end,
                                         place, columns, arena, use_null_result,
                                         could_use_previous_result);
        }
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena, UInt8* use_null_result,
                                UInt8* could_use_previous_result) const override {
        auto current_frame_start = std::max<int64_t>(frame_start, partition_start);
        auto current_frame_end = std::min<int64_t>(frame_end, partition_end);

        if (current_frame_start >= current_frame_end) {
            if (!*could_use_previous_result) {
                *use_null_result = true;
            }
        } else {
            for (size_t row_num = current_frame_start; row_num < current_frame_end; ++row_num) {
                this->data(place).change_if_better(*columns[0], row_num, arena);
            }
            *use_null_result = false;
            *could_use_previous_result = true;
        }
    }
};

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value(const String& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr = {});

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_single_value_any_value_function(
        const String& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr = {});
} // namespace doris::vectorized

#include "common/compile_check_end.h"
