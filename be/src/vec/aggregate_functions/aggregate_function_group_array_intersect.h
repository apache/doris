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

#include <boost/core/demangle.hpp>
#include <cassert>
#include <memory>

#include "exprs/hybrid_set.h"
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

/// Only for changing Numeric type or Date(DateTime)V2 type to PrimitiveType so that to inherit HybridSet
template <typename T>
constexpr PrimitiveType TypeToPrimitiveType() {
    if constexpr (std::is_same_v<T, UInt8> || std::is_same_v<T, Int8>) {
        return TYPE_TINYINT;
    } else if constexpr (std::is_same_v<T, Int16>) {
        return TYPE_SMALLINT;
    } else if constexpr (std::is_same_v<T, Int32>) {
        return TYPE_INT;
    } else if constexpr (std::is_same_v<T, Int64>) {
        return TYPE_BIGINT;
    } else if constexpr (std::is_same_v<T, Int128>) {
        return TYPE_LARGEINT;
    } else if constexpr (std::is_same_v<T, Float32>) {
        return TYPE_FLOAT;
    } else if constexpr (std::is_same_v<T, Float64>) {
        return TYPE_DOUBLE;
    } else if constexpr (std::is_same_v<T, DateV2>) {
        return TYPE_DATEV2;
    } else if constexpr (std::is_same_v<T, DateTimeV2>) {
        return TYPE_DATETIMEV2;
    } else {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "Only for changing Numeric type or Date(DateTime)V2 type to PrimitiveType");
    }
}

template <typename T>
class NullableNumericOrDateSet
        : public HybridSet<TypeToPrimitiveType<T>(), DynamicContainer<typename PrimitiveTypeTraits<
                                                             TypeToPrimitiveType<T>()>::CppType>> {
public:
    NullableNumericOrDateSet() { this->_null_aware = true; }

    void change_contains_null_value(bool target_value) { this->_contains_null = target_value; }
};

template <typename T>
struct AggregateFunctionGroupArrayIntersectData {
    using NullableNumericOrDateSetType = NullableNumericOrDateSet<T>;
    using Set = std::shared_ptr<NullableNumericOrDateSetType>;

    AggregateFunctionGroupArrayIntersectData()
            : value(std::make_shared<NullableNumericOrDateSetType>()) {}

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
                      argument_types_),
              argument_type(argument_types_[0]) {}

    AggregateFunctionGroupArrayIntersect(const DataTypes& argument_types_,
                                         const bool result_is_nullable)
            : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
                                           AggregateFunctionGroupArrayIntersect<T>>(
                      argument_types_),
              argument_type(argument_types_[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    DataTypePtr get_return_type() const override {
        std::string demangled_name = boost::core::demangle(typeid(argument_type).name());
        LOG(INFO) << "in the get_return_type, name of argument_type: " << demangled_name;
        std::string demangled_name_T = boost::core::demangle(typeid(T).name());
        LOG(INFO) << "in the get_return_type, name of T: " << demangled_name_T;
        return argument_type;
    }

    bool allocates_memory_in_arena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        auto& data = this->data(place);
        auto& version = data.version;
        auto& set = data.value;
        CHECK(set != nullptr);

        LOG(INFO) << "Input row_num: " << row_num; // 输出输入的 row_num
        const bool col_is_nullable = (*columns[0]).is_nullable();
        const ColumnArray& column =
                col_is_nullable ? assert_cast<const ColumnArray&>(
                                          assert_cast<const ColumnNullable&>(*columns[0])
                                                  .get_nested_column())
                                : assert_cast<const ColumnArray&>(*columns[0]);

        const auto data_column = column.get_data_ptr();
        const auto& offsets = column.get_offsets();
        const size_t offset = offsets[static_cast<ssize_t>(row_num) - 1];
        const auto arr_size = offsets[row_num] - offset;

        using ColVecType = ColumnVector<T>;
        LOG(INFO) << "the name of column is: " << column.get_name();

        const auto& column_data = column.get_data();

        bool is_column_data_nullable = column_data.is_nullable();
        ColumnNullable* col_null = nullptr;
        const ColVecType* nested_column_data = nullptr;

        if (is_column_data_nullable) {
            LOG(INFO) << "nested_col is nullable. ";
            auto const_col_data = const_cast<IColumn*>(&column_data);
            col_null = static_cast<ColumnNullable*>(const_col_data);
            nested_column_data = &assert_cast<const ColVecType&>(col_null->get_nested_column());
        } else {
            LOG(INFO) << "nested_col is not nullable. ";
            nested_column_data = &static_cast<const ColVecType&>(column_data);
        }

        ++version;
        if (version == 1) {
            LOG(INFO) << "version is 1.";
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);
                const T* src_data =
                        is_null_element ? nullptr : &(nested_column_data->get_element(offset + i));

                if (is_null_element) {
                    LOG(INFO) << "src_data is null!!!!";
                    // src_data.data = nullptr;
                } else {
                    LOG(INFO) << "src_data is not null";
                    LOG(INFO) << "Inserting value: " << *(src_data);
                }

                if (src_data == nullptr) {
                    LOG(INFO) << "src_data==nullptr is true. ";
                }

                // set->insert(src_data.data);
                set->insert(src_data);
                LOG(INFO) << "After inserting value.";
            }

            if (set->contain_null()) {
                LOG(INFO) << "in the last of version==1, the set contains null.";
            }

        } else if (set->size() != 0 || set->contain_null()) {
            // typename State::Set new_set;
            typename State::Set new_set =
                    std::make_shared<typename State::NullableNumericOrDateSetType>();

            CHECK(new_set != nullptr);
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);
                const T* src_data =
                        is_null_element ? nullptr : &(nested_column_data->get_element(offset + i));

                if (is_null_element) {
                    LOG(INFO) << "src_data is null again ~";
                    // src_data.data = nullptr;
                } else {
                    LOG(INFO) << "Inserting value: " << *src_data;
                }

                if (src_data == nullptr) {
                    LOG(INFO) << "src_data==nullptr is true. ";
                }
                if (set->find(src_data) || (set->contain_null() && src_data == nullptr)) {
                    if (set->find(src_data)) {
                        LOG(INFO) << "we can find set's value: " << *src_data;
                    } else {
                        LOG(INFO) << "src_data is null again ";
                    }
                    new_set->insert(src_data);
                    LOG(INFO) << "After inserting value.";
                }
            }

            if (set->contain_null()) {
                LOG(INFO) << "before swap between set and new_set, the set contains null.";
            }
            if (new_set->contain_null()) {
                LOG(INFO) << "before swap between set and new_set, the new_set contains null.";
            }
            set = std::move(new_set);

            if (set->contain_null()) {
                LOG(INFO) << "after swap between set and new_set, the set contains null.";
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& rhs_set = this->data(rhs).value;
        set->change_contains_null_value(rhs_set->contain_null());

        LOG(INFO) << "merge: place set size = " << set->size();

        if (this->data(rhs).version == 0) {
            LOG(INFO) << "rhs version is 0, skipping merge";
            return;
        }

        UInt64 version = data.version++;
        LOG(INFO) << "merge: version = " << version;

        if (version == 0) {
            LOG(INFO) << "Copying rhs set to place set";
            const auto& rhs_set = this->data(rhs).value;
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const void* value = it->get_value();
                set->insert(value);
                it->next();
            }
            return;
        }

        if (set->size() != 0) {
            LOG(INFO) << "Merging place set and rhs set";
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_set;
                HybridSetBase::IteratorBase* it = lhs_val->begin();
                while (it->has_next()) {
                    const void* value = it->get_value();
                    bool found = (rhs_val->find(value) || (rhs_val->contain_null() && value == nullptr));
                    if (found) {
                        new_set->insert(value);
                    }
                    it->next();
                }
                return new_set;
            };
            auto new_set = rhs_set->size() < set->size() ? create_new_set(rhs_set, set)
                                                         : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }

        LOG(INFO) << "After merge: set = {";
        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            T value = *reinterpret_cast<const T*>(it->get_value());
            LOG(INFO) << value << " ";
            it->next();
        }
        LOG(INFO) << "}";
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto version = data.version;
        CHECK(set != nullptr);

        LOG(INFO) << "serialize: version = " << version << ", set size = " << set->size();

        bool is_set_contains_null = set->contain_null();
        if (is_set_contains_null) {
            LOG(INFO) << "Before writing of serialize, the set contains null.";
        }

        write_pod_binary(is_set_contains_null, buf);

        write_var_uint(version, buf);
        write_var_uint(set->size(), buf);
        HybridSetBase::IteratorBase* it = set->begin();

        if (it == nullptr) {
            LOG(INFO) << "Before writing of serialize, the set->begin() is nullptr.";
        }

        if (it->has_next()) {
            LOG(INFO) << "Before writing of serialize, the it->has_next() is true.";
        }

        while (it->has_next()) {
            if (it->get_value() == nullptr) {
                LOG(INFO) << "during writing of serialize, the it->get_value() is nullptr.";
            }
            const T* value_ptr = static_cast<const T*>(it->get_value());
            // const StringRef* str_ref = reinterpret_cast<const StringRef*>(value_ptr);
            LOG(INFO) << "Serializing element: " << (*value_ptr);
            LOG(INFO) << "after writing element... ";
            write_int_binary((*value_ptr), buf);
            it->next();
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        auto& data = this->data(place);
        bool is_set_contains_null;

        LOG(INFO) << "deserialize";

        read_pod_binary(is_set_contains_null, buf);
        data.value->change_contains_null_value(is_set_contains_null);

        read_var_uint(data.version, buf);
        LOG(INFO) << "Deserialized version: " << data.version;
        // this->data(place).value.read(buf);
        size_t size;
        read_var_uint(size, buf);
        LOG(INFO) << "Deserialized size: " << size;

        LOG(INFO) << "Deserialized set: {";
        T element;
        for (size_t i = 0; i < size; ++i) {
            read_int_binary(element, buf);
            LOG(INFO) << "derializing element: " << element;
            data.value->insert(static_cast<void*>(&element));
        }
        LOG(INFO) << "}";

        if (data.value->contain_null()) {
            LOG(INFO) << "After reading of deserialize, the set contains null.";
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        LOG(INFO) << "in the insert_result_into, name of T: "
                  << boost::core::demangle(typeid(T).name());
        LOG(INFO) << "in the start of insert, Type name: " << typeid(T).name();
        LOG(INFO) << "the name of to is: " << to.get_name();

        ColumnArray& arr_to = assert_cast<ColumnArray&>(to);
        LOG(INFO) << "the name of arr_to is: " << arr_to.get_name();

        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();

        auto& to_nested_col = arr_to.get_data();
        using ElementType = T;
        using ColVecType = ColumnVector<ElementType>;

        bool is_nullable = to_nested_col.is_nullable();

        if (is_nullable) {
            LOG(INFO) << "nested_col is nullable. ";
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            auto& nested_col = assert_cast<ColVecType&>(col_null->get_nested_column());
            auto& null_map_data = col_null->get_null_map_data();

            size_t old_size = nested_col.get_data().size();
            LOG(INFO) << "old_size of data_to: " << old_size;

            const auto& set = this->data(place).value;
            LOG(INFO) << "insert_result_into: set size = " << set->size();

            auto res_size = set->size();
            size_t i = 0;

            if (set->contain_null()) {
                LOG(INFO) << "We have null, insert it!";
                col_null->insert_data(nullptr, 0);
                res_size += 1;
                i = 1;
            }

            if (offsets_to.size() == 0) {
                offsets_to.push_back(res_size);
            } else {
                offsets_to.push_back(offsets_to.back() + res_size);
            }

            nested_col.get_data().resize(old_size + res_size);

            HybridSetBase::IteratorBase* it = set->begin();
            while (it->has_next()) {
                T value = *reinterpret_cast<const T*>(it->get_value());
                LOG(INFO) << "Inserting value: " << value << " at index " << (old_size + i);
                nested_col.get_data()[old_size + i] = value;
                null_map_data.push_back(0);
                it->next();
                ++i;
            }
            // }
        } else {
            LOG(INFO) << "nested_col is not nullable. ";
            auto& nested_col = static_cast<ColVecType&>(to_nested_col);
            size_t old_size = nested_col.get_data().size();
            LOG(INFO) << "old_size of data_to: " << old_size;

            const auto& set = this->data(place).value;
            LOG(INFO) << "insert_result_into: set size = " << set->size();

            if (offsets_to.size() == 0) {
                offsets_to.push_back(set->size());
            } else {
                offsets_to.push_back(offsets_to.back() + set->size());
            }

            nested_col.get_data().resize(old_size + set->size());

            size_t i = 0;
            HybridSetBase::IteratorBase* it = set->begin();
            while (it->has_next()) {
                T value = *reinterpret_cast<const T*>(it->get_value());
                LOG(INFO) << "Inserting value: " << value << " at index " << (old_size + i);
                nested_col.get_data()[old_size + i] = value;
                it->next();
                ++i;
            }
        }
        LOG(INFO) << "After making value to data_to, leaving...";
    }
};

/// Generic implementation, it uses serialized representation as object descriptor.
class NullableStringSet : public StringValueSet<DynamicContainer<StringRef>> {
public:
    NullableStringSet() { this->_null_aware = true; }

    void change_contains_null_value(bool target_value) { this->_contains_null = target_value; }
};

struct AggregateFunctionGroupArrayIntersectGenericData {
    using Set = std::shared_ptr<NullableStringSet>;

    AggregateFunctionGroupArrayIntersectGenericData()
            : value(std::make_shared<NullableStringSet>()) {}
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
              input_data_type(input_data_type_[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    // DataTypePtr get_return_type() const override { return input_data_type; }
    DataTypePtr get_return_type() const override {
        // return std::make_shared<DataTypeArray>(make_nullable(input_data_type));
        return input_data_type;
    }

    bool allocates_memory_in_arena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        auto& data = this->data(place);
        auto& version = data.version;
        auto& set = data.value;
        CHECK(set != nullptr);

        LOG(INFO) << "Input row_num: " << row_num; // 输出输入的 row_num
        const bool col_is_nullable = (*columns[0]).is_nullable();
        const ColumnArray& column =
                col_is_nullable ? assert_cast<const ColumnArray&>(
                                          assert_cast<const ColumnNullable&>(*columns[0])
                                                  .get_nested_column())
                                : assert_cast<const ColumnArray&>(*columns[0]);

        const auto data_column = column.get_data_ptr();
        const auto& offsets = column.get_offsets();
        const size_t offset = offsets[static_cast<ssize_t>(row_num) - 1];
        const auto arr_size = offsets[row_num] - offset;

        LOG(INFO) << "the name of column is: " << column.get_name();

        const auto& column_data = column.get_data();

        bool is_column_data_nullable = column_data.is_nullable();
        ColumnNullable* col_null = nullptr;
        const ColumnArray* nested_column_data = nullptr;

        if (is_column_data_nullable) {
            LOG(INFO) << "nested_col is nullable. ";
            auto const_col_data = const_cast<IColumn*>(&column_data);
            col_null = static_cast<ColumnNullable*>(const_col_data);
            nested_column_data = &assert_cast<const ColumnArray&>(col_null->get_nested_column());
        } else {
            LOG(INFO) << "nested_col is not nullable. ";
            nested_column_data = &static_cast<const ColumnArray&>(column_data);
        }

        ++version;
        if (version == 1) {
            LOG(INFO) << "version is 1.";
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);

                StringRef src = StringRef();
                if constexpr (is_plain_column) {
                    src = nested_column_data->get_data_at(offset + i);
                    src.data = arena->insert(src.data, src.size);
                } else {
                    const char* begin = nullptr;
                    src = nested_column_data->serialize_value_into_arena(offset + i, *arena, begin);
                    src.data = arena->insert(src.data, src.size);
                }

                const auto* src_data = is_null_element ? nullptr : src.data;

                if (is_null_element) {
                    LOG(INFO) << "src_data is null!!!!";
                    // src_data.data = nullptr;
                } else {
                    LOG(INFO) << "src_data is not null";
                    LOG(INFO) << "Inserting value: " << *(src_data);
                }

                if (src_data == nullptr) {
                    LOG(INFO) << "src_data==nullptr is true. ";
                }

                // set->insert(src_data.data);
                set->insert(src_data);
                LOG(INFO) << "After inserting value.";
            }

            if (set->contain_null()) {
                LOG(INFO) << "in the last of version==1, the set contains null.";
            }

        } else if (set->size() != 0 || set->contain_null()) {
            // typename State::Set new_set;
            typename State::Set new_set = std::make_shared<NullableStringSet>();

            CHECK(new_set != nullptr);
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);
                StringRef src = StringRef();
                if constexpr (is_plain_column) {
                    src = nested_column_data->get_data_at(offset + i);
                    src.data = arena->insert(src.data, src.size);
                } else {
                    const char* begin = nullptr;
                    src = nested_column_data->serialize_value_into_arena(offset + i, *arena, begin);
                    src.data = arena->insert(src.data, src.size);
                }

                const auto* src_data = is_null_element ? nullptr : src.data;

                if (is_null_element) {
                    LOG(INFO) << "src_data is null again ~";
                    // src_data.data = nullptr;
                } else {
                    LOG(INFO) << "Inserting value: " << *src_data;
                }

                if (src_data == nullptr) {
                    LOG(INFO) << "src_data==nullptr is true. ";
                }
                if (set->find(src_data) || (set->contain_null() && src_data == nullptr)) {
                    if (set->find(src_data)) {
                        LOG(INFO) << "we can find set's value: " << *src_data;
                    } else {
                        LOG(INFO) << "src_data is null again ";
                    }
                    new_set->insert(src_data);
                    LOG(INFO) << "After inserting value.";
                }
            }

            if (set->contain_null()) {
                LOG(INFO) << "before swap between set and new_set, the set contains null.";
            }
            if (new_set->contain_null()) {
                LOG(INFO) << "before swap between set and new_set, the new_set contains null.";
            }
            set = std::move(new_set);

            if (set->contain_null()) {
                LOG(INFO) << "after swap between set and new_set, the set contains null.";
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& rhs_set = this->data(rhs).value;
        set->change_contains_null_value(rhs_set->contain_null());

        LOG(INFO) << "merge: place set size = " << set->size();

        if (this->data(rhs).version == 0) {
            LOG(INFO) << "rhs version is 0, skipping merge";
            return;
        }

        UInt64 version = data.version++;
        LOG(INFO) << "merge: version = " << version;

        if (version == 0) {
            LOG(INFO) << "Copying rhs set to place set";
            const auto& rhs_set = this->data(rhs).value;
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const void* value = it->get_value();
                set->insert(value);
                it->next();
            }
            return;
        }

        if (set->size() != 0) {
            LOG(INFO) << "Merging place set and rhs set";
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_set;
                HybridSetBase::IteratorBase* it = lhs_val->begin();
                while (it->has_next()) {
                    const void* value = it->get_value();
                    bool found =
                            (rhs_val->find(value) || (rhs_val->contain_null() && value == nullptr));
                    if (found) {
                        new_set->insert(value);
                    }
                    it->next();
                }
                return new_set;
            };
            auto new_set = rhs_set->size() < set->size() ? create_new_set(rhs_set, set)
                                                         : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }

        LOG(INFO) << "After merge: set = {";
        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            auto* value = it->get_value();
            LOG(INFO) << value << " ";
            it->next();
        }
        LOG(INFO) << "}";
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto version = data.version;
        CHECK(set != nullptr);

        LOG(INFO) << "serialize: version = " << version << ", set size = " << set->size();

        bool is_set_contains_null = set->contain_null();
        if (is_set_contains_null) {
            LOG(INFO) << "Before writing of serialize, the set contains null.";
        }

        write_pod_binary(is_set_contains_null, buf);

        write_var_uint(version, buf);
        write_var_uint(set->size(), buf);
        HybridSetBase::IteratorBase* it = set->begin();

        if (it == nullptr) {
            LOG(INFO) << "Before writing of serialize, the set->begin() is nullptr.";
        }

        if (it->has_next()) {
            LOG(INFO) << "Before writing of serialize, the it->has_next() is true.";
        }

        while (it->has_next()) {
            if (it->get_value() == nullptr) {
                LOG(INFO) << "during writing of serialize, the it->get_value() is nullptr.";
            }
            const auto* value_ptr = reinterpret_cast<const StringRef*>(it->get_value());
            // const StringRef* str_ref = reinterpret_cast<const StringRef*>(value_ptr);
            LOG(INFO) << "Serializing element: " << *(value_ptr->data);
            LOG(INFO) << "after writing element... ";
            write_string_binary(*(value_ptr), buf);
            it->next();
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        auto& data = this->data(place);
        bool is_set_contains_null;

        LOG(INFO) << "deserialize";

        read_pod_binary(is_set_contains_null, buf);
        data.value->change_contains_null_value(is_set_contains_null);

        read_var_uint(data.version, buf);
        LOG(INFO) << "Deserialized version: " << data.version;
        // this->data(place).value.read(buf);
        size_t size;
        read_var_uint(size, buf);
        LOG(INFO) << "Deserialized size: " << size;

        LOG(INFO) << "Deserialized set: {";
        StringRef element;
        for (size_t i = 0; i < size; ++i) {
            element = read_string_binary_into(*arena, buf);
            LOG(INFO) << "derializing element: " << element.to_string();
            data.value->insert(element.data);
        }
        LOG(INFO) << "}";

        if (data.value->contain_null()) {
            LOG(INFO) << "After reading of deserialize, the set contains null.";
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        LOG(INFO) << "the name of to is: " << to.get_name();

        ColumnArray& arr_to = assert_cast<ColumnArray&>(to);
        LOG(INFO) << "the name of arr_to is: " << arr_to.get_name();

        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();

        LOG(INFO) << "nested_col is nullable. ";
        auto& data_to = arr_to.get_data();
        auto col_null = reinterpret_cast<ColumnNullable*>(&data_to);
        auto& null_map_data = col_null->get_null_map_data();

        const auto& set = this->data(place).value;
        LOG(INFO) << "insert_result_into: set size = " << set->size();

        auto res_size = set->size();

        if (set->contain_null()) {
            LOG(INFO) << "We have null, insert it!";
            col_null->insert_data(nullptr, 0);
            res_size += 1;
        }

        if (offsets_to.size() == 0) {
            offsets_to.push_back(res_size);
        } else {
            offsets_to.push_back(offsets_to.back() + res_size);
        }

        data_to.resize(res_size);

        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            const auto* value =
                    reinterpret_cast<const StringRef*>(it->get_value());
            if constexpr (is_plain_column) {
                data_to.insert_data(value->data, value->size);
            } else {
                std::ignore = data_to.deserialize_and_insert_from_arena(value->data);
            }
            null_map_data.push_back(0);
            it->next();
        }
        LOG(INFO) << "After making value to data_to, leaving...";
    }
};

namespace {

/// Substitute return type for DateV2 and DateTimeV2
class AggregateFunctionGroupArrayIntersectDateV2
        : public AggregateFunctionGroupArrayIntersect<DateV2> {
public:
    explicit AggregateFunctionGroupArrayIntersectDateV2(const DataTypes& argument_types_)
            : AggregateFunctionGroupArrayIntersect<DateV2>(
                      DataTypes(argument_types_.begin(), argument_types_.end())) {}
};

class AggregateFunctionGroupArrayIntersectDateTimeV2
        : public AggregateFunctionGroupArrayIntersect<DateTimeV2> {
public:
    explicit AggregateFunctionGroupArrayIntersectDateTimeV2(const DataTypes& argument_types_)
            : AggregateFunctionGroupArrayIntersect<DateTimeV2>(
                      DataTypes(argument_types_.begin(), argument_types_.end())) {}
};

IAggregateFunction* create_with_extra_types(const DataTypePtr& nested_type,
                                            const DataTypes& argument_types) {
    WhichDataType which(nested_type);
    if (which.idx == TypeIndex::Date || which.idx == TypeIndex::DateV2)
        return new AggregateFunctionGroupArrayIntersectDateV2(argument_types);
    else if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::DateTimeV2)
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
    const auto& nested_type = remove_nullable(
            dynamic_cast<const DataTypeArray&>(*(argument_types[0])).get_nested_type());
    WhichDataType which_type(nested_type);
    if (which_type.is_int()) {
        LOG(INFO) << "nested_type is int";
    } else {
        LOG(INFO) << "nested_type is not int";
    }
    DataTypes new_argument_types = {nested_type};
    LOG(INFO) << "in the create_aggregate_function_group_array_intersect_impl, name of "
                 "remove_nullable(nested_type): "
              << boost::core::demangle(typeid(nested_type).name());

    const auto& argument_type = dynamic_cast<const DataTypeArray&>(*(argument_types[0]));
    LOG(INFO) << "in the create_aggregate_function_group_array_intersect_impl, name of "
                 "argument_type: "
              << boost::core::demangle(typeid(argument_type).name());

    // AggregateFunctionPtr res(
    //         creator_with_numeric_type::creator<AggregateFunctionGroupArrayIntersect>(
    //                 "", new_argument_types, result_is_nullable));
    AggregateFunctionPtr res = nullptr;
    WhichDataType which(nested_type);
#define DISPATCH(TYPE)                                                                  \
    if (which.idx == TypeIndex::TYPE)                                                   \
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<TYPE>>( \
                argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (!res) {
        res = AggregateFunctionPtr(create_with_extra_types(nested_type, argument_types));
    }

    if (!res)
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->get_name(), name);

    return res;
}
} // namespace

} // namespace doris::vectorized
