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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupArrayIntersect.cpp
// and modified by Doris

#include <vec/aggregate_functions/factory_helpers.h>
#include <vec/common/assert_cast.h>
#include <vec/common/hash_table/hash_set.h>
#include <vec/core/field.h>

#include <cassert>
#include <memory>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionGroupArrayIntersectData {
    using Set = HashSet<T>;

    Set value;
    UInt64 version = 0;
};

/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T>
class AggregateFunctionGroupArrayIntersect
        : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
                                              AggregateFunctionGroupArrayIntersect<T>> {
private:
    using State = AggregateFunctionGroupArrayIntersectData<T>;
    DataTypePtr argument_type;

public:
    AggregateFunctionGroupArrayIntersect(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
                                           AggregateFunctionGroupArrayIntersect<T>>(
                      {argument_types_}),
              argument_type(this->argument_types[0]) {}

    AggregateFunctionGroupArrayIntersect(const DataTypes& argument_types_,
                                         const bool result_is_nullable)
            : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
                                           AggregateFunctionGroupArrayIntersect<T>>(
                      {argument_types_}),
              argument_type(this->argument_types[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    // DataTypePtr get_return_type() const override { return argument_type; }
    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(argument_type);
    }

    bool allocates_memory_in_arena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        auto& version = this->data(place).version;
        auto& set = this->data(place).value;

        const auto data_column = assert_cast<const ColumnArray&>(*columns[0]).get_data_ptr();
        const auto& offsets = assert_cast<const ColumnArray&>(*columns[0]).get_offsets();
        const size_t offset = offsets[row_num - 1];
        const auto arr_size = offsets[row_num] - offset;

        ++version;
        if (version == 1) {
            for (size_t i = 0; i < arr_size; ++i)
                set.insert(static_cast<T>((*data_column)[offset + i].get<T>()));
        } else if (!set.empty()) {
            typename State::Set new_set;
            for (size_t i = 0; i < arr_size; ++i) {
                typename State::Set::LookupResult set_value =
                        set.find(static_cast<T>((*data_column)[offset + i].get<T>()));
                if (set_value != nullptr)
                    new_set.insert(static_cast<T>((*data_column)[offset + i].get<T>()));
            }
            set = std::move(new_set);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        auto& set = this->data(place).value;
        const auto& rhs_set = this->data(rhs).value;

        if (this->data(rhs).version == 0) return;

        UInt64 version = this->data(place).version++;
        if (version == 0) {
            for (auto& rhs_elem : rhs_set) set.insert(rhs_elem.get_value());
            return;
        }

        if (!set.empty()) {
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_set;
                for (auto& lhs_elem : lhs_val) {
                    auto res = rhs_val.find(lhs_elem.get_value());
                    if (res != nullptr) new_set.insert(lhs_elem.get_value());
                }
                return new_set;
            };
            auto new_set = rhs_set.size() < set.size() ? create_new_set(rhs_set, set)
                                                       : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& set = this->data(place).value;
        auto version = this->data(place).version;

        write_var_uint(version, buf);
        write_var_uint(set.size(), buf);

        for (const auto& elem : set) write_int_binary(elem.get_value(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        read_var_uint(this->data(place).version, buf);
        this->data(place).value.read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        ColumnArray& arr_to = assert_cast<ColumnArray&>(to);
        auto& offsets_to = arr_to.get_offsets();

        const auto& set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        typename ColumnVector<T>::Container& data_to =
                assert_cast<ColumnVector<T>&>(arr_to.get_data()).get_data();
        size_t old_size = data_to.size();
        data_to.resize(old_size + set.size());

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->get_value();
    }
};

/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupArrayIntersectGenericData {
    using Set = HashSet<StringRef>;

    Set value;
    UInt64 version = 0;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns GroupArrayIntersect() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionGroupArrayIntersectGeneric
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionGroupArrayIntersectGenericData,
                  AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>> {
    const DataTypePtr& input_data_type;

    using State = AggregateFunctionGroupArrayIntersectGenericData;

public:
    AggregateFunctionGroupArrayIntersectGeneric(const DataTypes& input_data_type_)
            : IAggregateFunctionDataHelper<
                      AggregateFunctionGroupArrayIntersectGenericData,
                      AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>(
                      input_data_type_),
              input_data_type(this->argument_types[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(input_data_type);
    }

    bool allocates_memory_in_arena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        auto& set = this->data(place).value;
        auto& version = this->data(place).version;
        bool inserted;
        State::Set::LookupResult it;

        const auto data_column = assert_cast<const ColumnArray&>(*columns[0]).get_data_ptr();
        const auto& offsets = assert_cast<const ColumnArray&>(*columns[0]).get_offsets();
        const size_t offset = offsets[row_num - 1];
        const auto arr_size = offsets[row_num] - offset;

        ++version;
        if (version == 1) {
            for (size_t i = 0; i < arr_size; ++i) {
                if constexpr (is_plain_column) {
                    StringRef key = data_column->get_data_at(offset + i);
                    key.data = arena->insert(key.data, key.size);
                    set.emplace(key, it, inserted);
                } else {
                    const char* begin = nullptr;
                    StringRef serialized =
                            data_column->serialize_value_into_arena(offset + i, *arena, begin);
                    assert(serialized.data != nullptr);
                    serialized.data = arena->insert(serialized.data, serialized.size);
                    set.emplace(serialized, it, inserted);
                }
            }
        } else if (!set.empty()) {
            typename State::Set new_set;
            for (size_t i = 0; i < arr_size; ++i) {
                if constexpr (is_plain_column) {
                    it = set.find(data_column->get_data_at(offset + i));
                    if (it != nullptr) {
                        StringRef key = data_column->get_data_at(offset + i);
                        key.data = arena->insert(key.data, key.size);
                        new_set.emplace(key, it, inserted);
                    }
                } else {
                    const char* begin = nullptr;
                    StringRef serialized =
                            data_column->serialize_value_into_arena(offset + i, *arena, begin);
                    assert(serialized.data != nullptr);
                    it = set.find(serialized);

                    if (it != nullptr) {
                        serialized.data = arena->insert(serialized.data, serialized.size);
                        new_set.emplace(serialized, it, inserted);
                    }
                }
            }
            set = std::move(new_set);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        auto& set = this->data(place).value;
        const auto& rhs_value = this->data(rhs).value;

        if (this->data(rhs).version == 0) return;

        UInt64 version = this->data(place).version++;
        if (version == 0) {
            bool inserted;
            State::Set::LookupResult it;
            for (auto& rhs_elem : rhs_value) {
                StringRef key = rhs_elem.get_value();
                key.data = arena->insert(key.data, key.size);
                set.emplace(key, it, inserted);
            }
        } else if (!set.empty()) {
            auto create_new_map = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_map;
                for (auto& lhs_elem : lhs_val) {
                    auto val = rhs_val.find(lhs_elem.get_value());
                    if (val != nullptr) new_map.insert(lhs_elem.get_value());
                }
                return new_map;
            };
            auto new_map = rhs_value.size() < set.size() ? create_new_map(rhs_value, set)
                                                         : create_new_map(set, rhs_value);
            set = std::move(new_map);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& set = this->data(place).value;
        auto& version = this->data(place).version;
        write_var_uint(version, buf);
        write_var_uint(set.size(), buf);

        for (const auto& elem : set) write_string_binary(elem.get_value(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        auto& set = this->data(place).value;
        auto& version = this->data(place).version;
        size_t size;
        read_var_uint(version, buf);
        read_var_uint(size, buf);
        set.reserve(size);
        UInt64 elem_version;
        for (size_t i = 0; i < size; ++i) {
            auto key = read_string_binary_into(*arena, buf);
            read_var_uint(elem_version, buf);
            set.insert(key);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        ColumnArray& arr_to = assert_cast<ColumnArray&>(to);
        auto& offsets_to = arr_to.get_offsets();
        IColumn& data_to = arr_to.get_data();

        auto& set = this->data(place).value;

        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto& elem : set) {
            if constexpr (is_plain_column)
                data_to.insert_data(elem.get_value().data, elem.get_value().size);
            else
                std::ignore = data_to.deserialize_and_insert_from_arena(elem.get_value().data);
        }
    }
};

namespace {

/// Substitute return type for DateV2 and DateTimeV2
class AggregateFunctionGroupArrayIntersectDateV2
        : public AggregateFunctionGroupArrayIntersect<DataTypeDateV2::FieldType> {
public:
    explicit AggregateFunctionGroupArrayIntersectDateV2(const DataTypes& argument_types_)
            : AggregateFunctionGroupArrayIntersect<DataTypeDateV2::FieldType>(
                      DataTypes(argument_types_.begin(), argument_types_.end())) {}
};

class AggregateFunctionGroupArrayIntersectDateTimeV2
        : public AggregateFunctionGroupArrayIntersect<DataTypeDateTimeV2::FieldType> {
public:
    explicit AggregateFunctionGroupArrayIntersectDateTimeV2(const DataTypes& argument_types_)
            : AggregateFunctionGroupArrayIntersect<DataTypeDateTimeV2::FieldType>(
                      DataTypes(argument_types_.begin(), argument_types_.end())) {}
};

IAggregateFunction* create_with_extra_types(const DataTypes& argument_types) {
    WhichDataType which(argument_types[0]);
    if (which.idx == TypeIndex::DateV2)
        return new AggregateFunctionGroupArrayIntersectDateV2(argument_types);
    else if (which.idx == TypeIndex::DateTimeV2)
        return new AggregateFunctionGroupArrayIntersectDateTimeV2(argument_types);
    else {
        /// Check that we can use plain version of AggregateFunctionGroupArrayIntersectGeneric
        if (argument_types[0]->is_value_unambiguously_represented_in_contiguous_memory_region())
            return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_types);
        else
            return new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_types);
    }
}

inline AggregateFunctionPtr create_aggregate_function_group_array_intersect_impl(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable) {
    const auto& nested_type =
            dynamic_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type();
    DataTypes new_argument_types = {nested_type};
    AggregateFunctionPtr res(
            creator_with_numeric_type::creator<AggregateFunctionGroupArrayIntersect>(
                    "", new_argument_types, result_is_nullable));
    if (!res) {
        res = AggregateFunctionPtr(create_with_extra_types(argument_types));
    }

    if (!res)
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->get_name(), name);

    return res;
}
} // namespace

} // namespace doris::vectorized
