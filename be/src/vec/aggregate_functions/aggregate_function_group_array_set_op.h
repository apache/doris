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
#include <string>

#include "exprs/hybrid_set.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
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
struct GroupArraySetOpNumericBaseData {
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
    using ColumnItemType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using NullableNumericOrDateSetType = NullableNumericOrDateSet<T>;
    using Set = std::unique_ptr<NullableNumericOrDateSetType>;

    GroupArraySetOpNumericBaseData() : set(std::make_unique<NullableNumericOrDateSetType>()) {}

    Set set;
    bool init = false;

    void reset() {
        init = false;
        set = std::make_unique<NullableNumericOrDateSetType>();
    }

    void serialize(BufferWritable& buf) const {
        const bool is_set_contain_null = this->set->contain_null();
        buf.write_binary(is_set_contain_null);
        buf.write_binary(init);
        buf.write_var_uint(set->size());

        HybridSetBase::IteratorBase* it = set->begin();
        while (it->has_next()) {
            const auto* value_ptr = static_cast<const CppType*>(it->get_value());
            buf.write_binary((*value_ptr));
            it->next();
        }
    }

    void deserialize(BufferReadable& buf) {
        bool is_set_contain_null;
        buf.read_binary(is_set_contain_null);
        this->set->change_contain_null_value(is_set_contain_null);
        buf.read_binary(this->init);
        UInt64 size;
        buf.read_var_uint(size);

        CppType element;
        for (UInt64 i = 0; i < size; ++i) {
            buf.read_binary(element);
            this->set->insert(static_cast<void*>(&element));
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& arr_to = assert_cast<ColumnArray&>(to);
        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();
        auto& to_nested_col = arr_to.get_data();
        DCHECK(to_nested_col.is_nullable())
                << "should be array(nullable(column)) " << to.dump_structure();

        auto insert_values = [](ColVecType& nested_col, auto& set, ColumnNullable* col_null) {
            size_t old_size = nested_col.get_data().size();
            size_t res_size = set->size();
            size_t i = 0;

            if (set->contain_null()) {
                col_null->insert_default();
                res_size += 1;
                i = 1;
            }

            nested_col.get_data().resize(old_size + res_size);
            HybridSetBase::IteratorBase* it = set->begin();
            while (it->has_next()) {
                const auto value = *reinterpret_cast<const ColumnItemType*>(it->get_value());
                nested_col.get_data()[old_size + i] = value;
                col_null->get_null_map_data().push_back(0);
                it->next();
                ++i;
            }
        };

        auto* col_null = assert_cast<ColumnNullable*>(&to_nested_col);
        auto& nested_col = assert_cast<ColVecType&>(col_null->get_nested_column());
        offsets_to.push_back(offsets_to.back() + this->set->size() +
                             (this->set->contain_null() ? 1 : 0));
        insert_values(nested_col, this->set, col_null);
    }
};

template <PrimitiveType T>
struct GroupArrayNumericIntersectData : public GroupArraySetOpNumericBaseData<T> {
    using Base = GroupArraySetOpNumericBaseData<T>;
    using Set = Base::Set;
    using NullableNumericOrDateSetType = Base::NullableNumericOrDateSetType;
    using ColVecType = Base::ColVecType;

    static std::string get_name() { return "group_array_intersect"; }

    void process_col_data(const auto& column_data, size_t offset, size_t arr_size) {
        if (!this->init) {
            this->set->insert_range_from(column_data, offset, offset + arr_size);
            this->init = true;
        } else if (!this->set->empty()) {
            // for intersect, need to create a new set to store the intersection result
            Set new_set = std::make_unique<NullableNumericOrDateSetType>();
            const auto& col_nullable = assert_cast<const ColumnNullable&>(*column_data);
            const ColVecType& nested_column_data =
                    assert_cast<const ColVecType&>(col_nullable.get_nested_column());
            for (size_t i = 0; i < arr_size; ++i) {
                const bool is_null_element = col_nullable.is_null_at(offset + i);
                const typename PrimitiveTypeTraits<T>::ColumnItemType* src_data =
                        is_null_element ? nullptr : &(nested_column_data.get_element(offset + i));

                if ((!is_null_element && this->set->find(src_data)) ||
                    (this->set->contain_null() && is_null_element)) {
                    new_set->insert(src_data);
                }
            }
            this->set = std::move(new_set);
        }
    }

    void merge(const auto& rhs_set) {
        if (!this->init) {
            this->set->change_contain_null_value(rhs_set->contain_null());
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const void* value = it->get_value();
                this->set->insert(value);
                it->next();
            }
            this->init = true;
        } else if (!this->set->empty()) {
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                Set new_set = std::make_unique<NullableNumericOrDateSetType>();
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
            auto new_set = rhs_set->size() < this->set->size() ? create_new_set(rhs_set, this->set)
                                                               : create_new_set(this->set, rhs_set);
            this->set = std::move(new_set);
        }
    }
};

template <PrimitiveType T>
struct GroupArrayNumericUnionData : public GroupArraySetOpNumericBaseData<T> {
    using Base = GroupArraySetOpNumericBaseData<T>;
    using Set = Base::Set;

    static std::string get_name() { return "group_array_union"; }

    void process_col_data(const auto& column_data, size_t offset, size_t arr_size) {
        this->set->insert_range_from(column_data, offset, offset + arr_size);
        this->init = true;
    }

    void merge(const auto& rhs_set) {
        this->init = true;
        this->set->change_contain_null_value(this->set->contain_null() || rhs_set->contain_null());
        HybridSetBase::IteratorBase* it = rhs_set->begin();
        while (it->has_next()) {
            const void* value = it->get_value();
            this->set->insert(value);
            it->next();
        }
    }
};

class NullableStringSet : public StringSet<DynamicContainer<std::string>> {
public:
    NullableStringSet() : StringSet<DynamicContainer<std::string>>(true) {}

    void change_contain_null_value(bool target_value) { this->_contain_null = target_value; }
};

struct GroupArraySetOpStringBaseData {
    using Set = std::unique_ptr<NullableStringSet>;

    GroupArraySetOpStringBaseData() : set(std::make_unique<NullableStringSet>()) {}
    Set set;
    bool init = false;

    void reset() {
        init = false;
        set = std::make_unique<NullableStringSet>();
    }

    void serialize(BufferWritable& buf) const {
        const bool is_set_contain_null = this->set->contain_null();
        buf.write_binary(is_set_contain_null);
        buf.write_binary(this->init);
        buf.write_var_uint(this->set->size());

        HybridSetBase::IteratorBase* it = this->set->begin();
        while (it->has_next()) {
            const auto* value = reinterpret_cast<const std::string*>(it->get_value());
            buf.write_binary(*value);
            it->next();
        }
    }

    void deserialize(BufferReadable& buf) {
        bool is_set_contain_null;
        buf.read_binary(is_set_contain_null);
        this->set->change_contain_null_value(is_set_contain_null);
        buf.read_binary(this->init);
        UInt64 size;
        buf.read_var_uint(size);

        StringRef element;
        for (UInt64 i = 0; i < size; ++i) {
            buf.read_binary(element);
            this->set->insert((void*)element.data, element.size);
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& arr_to = assert_cast<ColumnArray&>(to);
        ColumnArray::Offsets64& offsets_to = arr_to.get_offsets();
        auto& data_to = arr_to.get_data();
        auto* col_null = assert_cast<ColumnNullable*>(&data_to);
        auto res_size = this->set->size();

        if (this->set->contain_null()) {
            col_null->insert_default();
            res_size += 1;
        }

        offsets_to.push_back(offsets_to.back() + res_size);
        HybridSetBase::IteratorBase* it = this->set->begin();
        while (it->has_next()) {
            const auto* value = reinterpret_cast<const std::string*>(it->get_value());
            data_to.insert_data(value->data(), value->size());
            it->next();
        }
    }
};

struct GroupArrayStringIntersectData : public GroupArraySetOpStringBaseData {
    using Base = GroupArraySetOpStringBaseData;
    using Set = Base::Set;

    static std::string get_name() { return "group_array_intersect"; }

    void process_col_data(const auto& column_data, size_t offset, size_t arr_size) {
        const auto* col_null = assert_cast<const ColumnNullable*>(column_data.get());
        const auto& nested_column_data =
                assert_cast<const ColumnString&>(col_null->get_nested_column());

        if (!init) {
            for (size_t i = 0; i < arr_size; ++i) {
                if (col_null->is_null_at(offset + i)) {
                    set->insert(nullptr);
                } else {
                    auto src = nested_column_data.get_data_at(offset + i);
                    set->insert((void*)src.data, src.size);
                }
            }
            init = true;
        } else if (!this->set->empty()) {
            Set new_set = std::make_unique<NullableStringSet>();
            for (size_t i = 0; i < arr_size; ++i) {
                if (col_null->is_null_at(offset + i) && this->set->contain_null()) {
                    new_set->insert(nullptr);
                } else {
                    auto src = nested_column_data.get_data_at(offset + i);
                    if (this->set->find((void*)src.data, src.size)) {
                        new_set->insert((void*)src.data, src.size);
                    }
                }
            }
            set = std::move(new_set);
        }
    }

    void merge(const auto& rhs_set) {
        if (!this->init) {
            this->set->change_contain_null_value(rhs_set->contain_null());
            HybridSetBase::IteratorBase* it = rhs_set->begin();
            while (it->has_next()) {
                const void* value = it->get_value();
                this->set->insert(value);
                it->next();
            }
            this->init = true;
        } else if (!this->set->empty()) {
            auto create_new_set = [](auto& lhs_val, auto& rhs_val) {
                Set new_set = std::make_unique<NullableStringSet>();
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
            auto new_set = rhs_set->size() < this->set->size() ? create_new_set(rhs_set, this->set)
                                                               : create_new_set(this->set, rhs_set);
            this->set = std::move(new_set);
        }
    }
};

struct GroupArrayStringUnionData : public GroupArraySetOpStringBaseData {
    using Base = GroupArraySetOpStringBaseData;
    using Set = Base::Set;

    static std::string get_name() { return "group_array_union"; }

    void process_col_data(const auto& column_data, size_t offset, size_t arr_size) {
        const auto* col_null = assert_cast<const ColumnNullable*>(column_data.get());
        const auto& nested_column_data =
                assert_cast<const ColumnString&>(col_null->get_nested_column());

        for (size_t i = 0; i < arr_size; ++i) {
            if (col_null->is_null_at(offset + i)) {
                set->insert(nullptr);
            } else {
                auto src = nested_column_data.get_data_at(offset + i);
                set->insert((void*)src.data, src.size);
            }
        }
        init = true;
    }

    void merge(const auto& rhs_set) {
        this->init = true;
        this->set->change_contain_null_value(this->set->contain_null() || rhs_set->contain_null());
        HybridSetBase::IteratorBase* it = rhs_set->begin();
        while (it->has_next()) {
            const void* value = it->get_value();
            this->set->insert(value);
            it->next();
        }
    }
};

/// Puts all values to the hybrid set. Returns an array of unique values
template <typename ImplData>
class AggregateFunctionGroupArraySetOp
        : public IAggregateFunctionDataHelper<ImplData, AggregateFunctionGroupArraySetOp<ImplData>>,
          UnaryExpression,
          NotNullableAggregateFunction {
private:
    DataTypePtr argument_type;

public:
    AggregateFunctionGroupArraySetOp(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<ImplData, AggregateFunctionGroupArraySetOp>(
                      argument_types_),
              argument_type(argument_types_[0]) {}

    AggregateFunctionGroupArraySetOp(const DataTypes& argument_types_,
                                     const bool result_is_nullable)
            : IAggregateFunctionDataHelper<ImplData, AggregateFunctionGroupArraySetOp>(
                      argument_types_),
              argument_type(argument_types_[0]) {}

    String get_name() const override { return ImplData::get_name(); }

    DataTypePtr get_return_type() const override { return argument_type; }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
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
        const auto& column_data = column.get_data_ptr();
        DCHECK(column_data->is_nullable())
                << "should be array(nullable(column)) " << column.dump_structure();
        this->data(place).process_col_data(column_data, offset, arr_size);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        if (!this->data(rhs).init) {
            return;
        }
        auto& rhs_set = this->data(rhs).set;
        this->data(place).merge(rhs_set);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        this->data(place).deserialize(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
