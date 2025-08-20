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

#include <memory>

#include "exprs/hybrid_set.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
} // namespace doris::vectorized

namespace doris::vectorized {

template <PrimitiveType T>
class NullableNumericOrDateSet
        : public HybridSet<T == TYPE_BOOLEAN ? TYPE_TINYINT : T,
                           DynamicContainer<typename PrimitiveTypeTraits<
                                   T == TYPE_BOOLEAN ? TYPE_TINYINT : T>::CppType>> {
public:
    NullableNumericOrDateSet()
            : HybridSet < T
                    == TYPE_BOOLEAN
            ? TYPE_TINYINT
            : T,
    DynamicContainer < typename PrimitiveTypeTraits < T == TYPE_BOOLEAN ? TYPE_TINYINT
                                                                        : T > ::CppType >> (true) {}

    void change_contain_null_value(bool target_value) { this->_contain_null = target_value; }
};

template <PrimitiveType T>
struct AggregateFunctionGroupArrayIntersectData {
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using NullableNumericOrDateSetType = NullableNumericOrDateSet<T>;
    using Set = std::unique_ptr<NullableNumericOrDateSetType>;

    AggregateFunctionGroupArrayIntersectData()
            : value(std::make_unique<NullableNumericOrDateSetType>()) {}

    Set value;
    bool init = false;

    void reset() {
        init = false;
        value = std::make_unique<NullableNumericOrDateSetType>();
    }

    void process_col_data(auto& column_data, size_t offset, size_t arr_size, Set& set) {
        const bool is_column_data_nullable = column_data.is_nullable();

        const ColumnNullable* col_null = nullptr;
        const ColVecType* nested_column_data = nullptr;

        if (is_column_data_nullable) {
            auto* const_col_data = const_cast<IColumn*>(&column_data);
            col_null = static_cast<ColumnNullable*>(const_col_data);
            nested_column_data = &assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                    col_null->get_nested_column());
        } else {
            nested_column_data = &static_cast<const ColVecType&>(column_data);
        }

        if (!init) {
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);
                const typename PrimitiveTypeTraits<T>::ColumnItemType* src_data =
                        is_null_element ? nullptr : &(nested_column_data->get_element(offset + i));

                set->insert(src_data);
            }
            init = true;
        } else if (!set->empty()) {
            Set new_set = std::make_unique<NullableNumericOrDateSetType>();

            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element =
                        is_column_data_nullable && col_null->is_null_at(offset + i);
                const typename PrimitiveTypeTraits<T>::ColumnItemType* src_data =
                        is_null_element ? nullptr : &(nested_column_data->get_element(offset + i));

                if ((!is_null_element && set->find(src_data)) ||
                    (set->contain_null() && is_null_element)) {
                    new_set->insert(src_data);
                }
            }
            set = std::move(new_set);
        }
    }
};

/// Puts all values to the hybrid set. Returns an array of unique values. Implemented for numeric/date types.
template <PrimitiveType T>
class AggregateFunctionGroupArrayIntersect
        : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
                                              AggregateFunctionGroupArrayIntersect<T>>,
          UnaryExpression,
          NotNullableAggregateFunction {
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

    DataTypePtr get_return_type() const override { return argument_type; }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        auto& data = this->data(place);
        auto& set = data.value;

        const bool col_is_nullable = (*columns[0]).is_nullable();
        const ColumnArray& column =
                col_is_nullable
                        ? assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(
                                  assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                          *columns[0])
                                          .get_nested_column())
                        : assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(*columns[0]);

        const auto& offsets = column.get_offsets();
        const auto offset = offsets[row_num - 1];
        const auto arr_size = offsets[row_num] - offset;
        const auto& column_data = column.get_data();

        data.process_col_data(column_data, offset, arr_size, set);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& rhs_set = this->data(rhs).value;

        if (!this->data(rhs).init) {
            return;
        }

        auto& init = data.init;
        if (!init) {
            set->change_contain_null_value(rhs_set->contain_null());
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const void* value = it->get_value();
                set->insert(value);
                it->next();
            }
            init = true;
        } else if (!set->empty()) {
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_set =
                        std::make_unique<typename State::NullableNumericOrDateSetType>();
                HybridSetBase::IteratorBase* it = lhs_val->begin();
                while (it->has_next()) {
                    const void* value = it->get_value();
                    if ((rhs_val->find(value))) {
                        new_set->insert(value);
                    }
                    it->next();
                }
                new_set->change_contain_null_value(lhs_val->contain_null() &&
                                                   rhs_val->contain_null());
                return new_set;
            };
            auto new_set = rhs_set->size() < set->size() ? create_new_set(rhs_set, set)
                                                         : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& init = data.init;
        const bool is_set_contain_null = set->contain_null();

        buf.write_binary(is_set_contain_null);
        buf.write_binary(init);
        buf.write_var_uint(set->size());
        HybridSetBase::IteratorBase* it = set->begin();

        while (it->has_next()) {
            const typename PrimitiveTypeTraits<T>::CppType* value_ptr =
                    static_cast<const typename PrimitiveTypeTraits<T>::CppType*>(it->get_value());
            buf.write_binary((*value_ptr));
            it->next();
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        auto& data = this->data(place);
        bool is_set_contain_null;

        buf.read_binary(is_set_contain_null);
        data.value->change_contain_null_value(is_set_contain_null);
        buf.read_binary(data.init);
        UInt64 size;
        buf.read_var_uint(size);

        typename PrimitiveTypeTraits<T>::CppType element;
        for (UInt64 i = 0; i < size; ++i) {
            buf.read_binary(element);
            data.value->insert(static_cast<void*>(&element));
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        ColumnArray& arr_to = assert_cast<ColumnArray&>(to);
        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();
        auto& to_nested_col = arr_to.get_data();
        const bool is_nullable = to_nested_col.is_nullable();

        auto insert_values = [](typename State::ColVecType& nested_col, auto& set,
                                bool is_nullable = false, ColumnNullable* col_null = nullptr) {
            size_t old_size = nested_col.get_data().size();
            size_t res_size = set->size();
            size_t i = 0;

            if (is_nullable && set->contain_null()) {
                col_null->insert_data(nullptr, 0);
                res_size += 1;
                i = 1;
            }

            nested_col.get_data().resize(old_size + res_size);

            HybridSetBase::IteratorBase* it = set->begin();
            while (it->has_next()) {
                const auto value =
                        *reinterpret_cast<const typename PrimitiveTypeTraits<T>::ColumnItemType*>(
                                it->get_value());
                nested_col.get_data()[old_size + i] = value;
                if (is_nullable) {
                    col_null->get_null_map_data().push_back(0);
                }
                it->next();
                ++i;
            }
        };

        const auto& set = this->data(place).value;
        if (is_nullable) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            auto& nested_col =
                    assert_cast<typename State::ColVecType&>(col_null->get_nested_column());
            offsets_to.push_back(offsets_to.back() + set->size() + (set->contain_null() ? 1 : 0));
            insert_values(nested_col, set, true, col_null);
        } else {
            auto& nested_col = static_cast<typename State::ColVecType&>(to_nested_col);
            offsets_to.push_back(offsets_to.back() + set->size());
            insert_values(nested_col, set);
        }
    }
};

/// Generic implementation, it uses serialized representation as object descriptor.
class NullableStringSet : public StringValueSet<DynamicContainer<StringRef>> {
public:
    NullableStringSet() : StringValueSet<DynamicContainer<StringRef>>(true) {}

    void change_contain_null_value(bool target_value) { this->_contain_null = target_value; }
};

struct AggregateFunctionGroupArrayIntersectGenericData {
    using Set = std::unique_ptr<NullableStringSet>;

    AggregateFunctionGroupArrayIntersectGenericData()
            : value(std::make_unique<NullableStringSet>()) {}
    Set value;
    bool init = false;

    void reset() {
        init = false;
        value = std::make_unique<NullableStringSet>();
    }
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns group_array_intersect() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionGroupArrayIntersectGeneric
        : public IAggregateFunctionDataHelper<
                  AggregateFunctionGroupArrayIntersectGenericData,
                  AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>> {
private:
    using State = AggregateFunctionGroupArrayIntersectGenericData;
    DataTypePtr input_data_type;

public:
    AggregateFunctionGroupArrayIntersectGeneric(const DataTypes& input_data_type_)
            : IAggregateFunctionDataHelper<
                      AggregateFunctionGroupArrayIntersectGenericData,
                      AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>(
                      input_data_type_),
              input_data_type(input_data_type_[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    DataTypePtr get_return_type() const override { return input_data_type; }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        auto& data = this->data(place);
        auto& init = data.init;
        auto& set = data.value;

        const bool col_is_nullable = (*columns[0]).is_nullable();
        const ColumnArray& column =
                col_is_nullable
                        ? assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(
                                  assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                                          *columns[0])
                                          .get_nested_column())
                        : assert_cast<const ColumnArray&, TypeCheckOnRelease::DISABLE>(*columns[0]);

        const auto nested_column_data = column.get_data_ptr();
        const auto& offsets = column.get_offsets();
        const auto offset = offsets[row_num - 1];
        const auto arr_size = offsets[row_num] - offset;
        const auto& column_data = column.get_data();
        const bool is_column_data_nullable = column_data.is_nullable();
        ColumnNullable* col_null = nullptr;

        if (is_column_data_nullable) {
            auto const_col_data = const_cast<IColumn*>(&column_data);
            col_null = static_cast<ColumnNullable*>(const_col_data);
        }

        auto process_element = [&](size_t i) {
            const bool is_null_element =
                    is_column_data_nullable && col_null->is_null_at(offset + i);

            StringRef src = StringRef();
            if constexpr (is_plain_column) {
                src = nested_column_data->get_data_at(offset + i);
            } else {
                const char* begin = nullptr;
                src = nested_column_data->serialize_value_into_arena(offset + i, arena, begin);
            }

            src.data = is_null_element ? nullptr : arena.insert(src.data, src.size);
            return src;
        };

        if (!init) {
            for (size_t i = 0; i < arr_size; ++i) {
                StringRef src = process_element(i);
                set->insert((void*)src.data, src.size);
            }
            init = true;
        } else if (!set->empty()) {
            typename State::Set new_set = std::make_unique<NullableStringSet>();

            for (size_t i = 0; i < arr_size; ++i) {
                StringRef src = process_element(i);
                if ((set->find(src.data, src.size) && src.data != nullptr) ||
                    (set->contain_null() && src.data == nullptr)) {
                    new_set->insert((void*)src.data, src.size);
                }
            }
            set = std::move(new_set);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& rhs_set = this->data(rhs).value;

        if (!this->data(rhs).init) {
            return;
        }

        auto& init = data.init;
        if (!init) {
            set->change_contain_null_value(rhs_set->contain_null());
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const auto* value = reinterpret_cast<const StringRef*>(it->get_value());
                set->insert((void*)(value->data), value->size);
                it->next();
            }
            init = true;
        } else if (!set->empty()) {
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                typename State::Set new_set = std::make_unique<NullableStringSet>();
                HybridSetBase::IteratorBase* it = lhs_val->begin();
                while (it->has_next()) {
                    const auto* value = reinterpret_cast<const StringRef*>(it->get_value());
                    if (rhs_val->find(value)) {
                        new_set->insert((void*)value->data, value->size);
                    }
                    it->next();
                }
                new_set->change_contain_null_value(lhs_val->contain_null() &&
                                                   rhs_val->contain_null());
                return new_set;
            };
            auto new_set = rhs_set->size() < set->size() ? create_new_set(rhs_set, set)
                                                         : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& data = this->data(place);
        auto& set = data.value;
        auto& init = data.init;
        const bool is_set_contain_null = set->contain_null();

        buf.write_binary(is_set_contain_null);
        buf.write_binary(init);
        buf.write_var_uint(set->size());

        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            const auto* value = reinterpret_cast<const StringRef*>(it->get_value());
            buf.write_binary(*value);
            it->next();
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        auto& data = this->data(place);
        bool is_set_contain_null;

        buf.read_binary(is_set_contain_null);
        data.value->change_contain_null_value(is_set_contain_null);
        buf.read_binary(data.init);
        UInt64 size;
        buf.read_var_uint(size);

        StringRef element;
        for (UInt64 i = 0; i < size; ++i) {
            element = buf.read_binary_into(arena);
            data.value->insert((void*)element.data, element.size);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& arr_to = assert_cast<ColumnArray&>(to);
        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();
        auto& data_to = arr_to.get_data();
        auto col_null = reinterpret_cast<ColumnNullable*>(&data_to);

        const auto& set = this->data(place).value;
        auto res_size = set->size();

        if (set->contain_null()) {
            col_null->insert_data(nullptr, 0);
            res_size += 1;
        }

        offsets_to.push_back(offsets_to.back() + res_size);

        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            const auto* value = reinterpret_cast<const StringRef*>(it->get_value());
            if constexpr (is_plain_column) {
                data_to.insert_data(value->data, value->size);
            } else {
                std::ignore = data_to.deserialize_and_insert_from_arena(value->data);
            }
            it->next();
        }
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
