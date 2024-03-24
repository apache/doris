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
    UInt64 has_null = 0; // 添加一个私有变量has_null
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

    // DataTypePtr get_return_type() const override { return argument_type; }
    DataTypePtr get_return_type() const override {
        std::string demangled_name = boost::core::demangle(typeid(argument_type).name());
        LOG(INFO) << "in the get_return_type, name of argument_type: " << demangled_name;
        std::string demangled_name_T = boost::core::demangle(typeid(T).name());
        LOG(INFO) << "in the get_return_type, name of T: " << demangled_name_T;
        // return std::make_shared<DataTypeArray>(make_nullable(argument_type));
        return argument_type;

        // using ReturnDataType = DataTypeNumber<T>;
        // std::string demangled_name_re = boost::core::demangle(typeid(ReturnDataType).name());
        // LOG(INFO) << "in the get_return_type, name of ReturnDataType: " << demangled_name_re;
        // return std::make_shared<DataTypeArray>(std::make_shared<ReturnDataType>());
    }

    bool allocates_memory_in_arena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        auto& version = this->data(place).version;
        auto& set = this->data(place).value;
        auto& has_null = this->data(place).has_null;

        LOG(INFO) << "Input row_num: " << row_num; // 输出输入的 row_num
        const bool col_is_nullable = (*columns[0]).is_nullable();
        const ColumnArray& column =
                col_is_nullable ? assert_cast<const ColumnArray&>(
                                          assert_cast<const ColumnNullable&>(*columns[0])
                                                  .get_nested_column())
                                : assert_cast<const ColumnArray&>(*columns[0]);

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

        std::string demangled_name_column =
                boost::core::demangle(typeid(*nested_column_data).name());
        LOG(INFO) << "In add func, the name of nested_column_data: " << demangled_name_column;

        const auto& offsets = column.get_offsets();
        const size_t offset = offsets[row_num - 1];
        const auto arr_size = offsets[row_num] - offset;
        LOG(INFO) << "In add func, the name of *columns[0] is: " << (*columns[0]).get_name();
        LOG(INFO) << "In add func, offsets size: " << offsets.size();
        LOG(INFO) << "In add func, offsets capacity: " << offsets.capacity();

        LOG(INFO) << "Before update: version = " << version
                  << ", set = {"; // 输出更新前的 version 和 set
        for (const auto& elem : set) {
            LOG(INFO) << elem.get_value() << " "; // 逐个输出 set 中的元素
        }
        LOG(INFO) << "}";

        ++version;
        if (version == 1) {
            LOG(INFO) << "Inserting elements into an empty set...";
            for (size_t i = 0; i < arr_size; ++i) {
                if (col_null->is_null_at(offset + i)) {
                    LOG(INFO) << "Encounting null: ";
                    // this->data(place).null_set.insert(offset + i);
                    has_null = 1;
                    break;
                } else {
                    T value = nested_column_data->get_element(offset + i);
                    LOG(INFO) << "Inserting value: " << value;
                    set.insert(value);
                }
            }
        } else if (!set.empty()) {
            LOG(INFO) << "Updating an existing set...";
            typename State::Set new_set;
            bool found_null = false;
            for (size_t i = 0; i < arr_size; ++i) {
                T value; // 将value的声明移动到循环体的开始
                if (col_null && col_null->is_null_at(offset + i)) {
                    LOG(INFO) << "Encounting null: ";
                    if (!found_null) { // 如果还没有找到null值
                        // this->data(place).null_set.insert(offset + i);
                        has_null = 1;
                        found_null = true; // 更新标志
                    }
                    break; // 遇到null值，跳出循环
                } else {
                    value = nested_column_data->get_element(offset + i);
                }
                LOG(INFO) << "Checking value: " << value;
                typename State::Set::LookupResult set_value = set.find(value);
                if (set_value != nullptr) {
                    LOG(INFO) << "Value found in the set, inserting into new_set";
                    new_set.insert(value);
                } else {
                    LOG(INFO) << "Value not found in the set, skipping";
                }
            }
            set = std::move(new_set);
        }

        LOG(INFO) << "After update: set = {";
        for (const auto& elem : set) {
            LOG(INFO) << elem.get_value() << " ";
        }
        LOG(INFO) << "}";

        if (is_column_data_nullable) {
            auto& null_map_data = col_null->get_null_map_data();
            null_map_data.resize_fill(nested_column_data->size(), 0);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        auto place_has_null = this->data(place).has_null;
        auto rhs_has_null = this->data(rhs).has_null;
        LOG(INFO) << "before, merge: place_has_null = " << place_has_null
                  << ", rhs_has_null = " << rhs_has_null;
        this->data(place).has_null = rhs_has_null;

        auto& set = this->data(place).value;
        const auto& rhs_set = this->data(rhs).value;
        // auto& null_set = this->data(place).null_set;
        // const auto& rhs_null_set = this->data(rhs).null_set;

        LOG(INFO) << "merge: place set size = " << set.size()
                  << ", rhs set size = " << rhs_set.size();

        if (this->data(rhs).version == 0) {
            LOG(INFO) << "rhs version is 0, skipping merge";
            return;
        }

        UInt64 version = this->data(place).version++;
        LOG(INFO) << "merge: version = " << version;

        if (version == 0) {
            LOG(INFO) << "Copying rhs set to place set";
            for (auto& rhs_elem : rhs_set) set.insert(rhs_elem.get_value());
            return;
        }

        if (!set.empty()) {
            LOG(INFO) << "Merging place set and rhs set";
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

        LOG(INFO) << "After merge: set = {";
        for (const auto& elem : set) {
            LOG(INFO) << elem.get_value() << " ";
        }
        LOG(INFO) << "}";
        // null_set.insert(rhs_null_set.begin(), rhs_null_set.end());
        LOG(INFO) << "after, merge: has_null = " << this->data(place).has_null;
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        auto& set = this->data(place).value;
        auto version = this->data(place).version;
        auto has_null = this->data(place).has_null;

        LOG(INFO) << "before, serialize: has_null = " << has_null;

        LOG(INFO) << "serialize: version = " << version << ", set size = " << set.size();

        write_var_uint(has_null, buf);

        // if (has_null == 0) {
            write_var_uint(version, buf);
            write_var_uint(set.size(), buf);
            for (const auto& elem : set) {
                LOG(INFO) << "Serializing element: " << elem.get_value();
                write_int_binary(elem.get_value(), buf);
            }
        // }
        // } else {
        // }
        LOG(INFO) << "after, serialize: has_null = " << has_null;
        // // 序列化null_set
        // write_var_uint(null_set.size(), buf);
        // for (const auto& null_index : null_set) {
        //     write_var_uint(null_index, buf);
        // }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        LOG(INFO) << "deserialize";
        // auto& has_null = this->data(place).has_null;

        // LOG(INFO) << "before, deserialize: has_null = " << has_null;

        read_var_uint(this->data(place).has_null, buf);
        auto has_null = this->data(place).has_null;
        LOG(INFO) << "another way to read the has_null: " << this->data(place).has_null;
        LOG(INFO) << "another has_null value: " << has_null;

        // if (has_null == 0) {
            read_var_uint(this->data(place).version, buf);
            LOG(INFO) << "Deserialized version: " << this->data(place).version;
            this->data(place).value.read(buf);
            LOG(INFO) << "Deserialized set: {";
            for (const auto& elem : this->data(place).value) {
                LOG(INFO) << elem.get_value() << " ";
            }
            LOG(INFO) << "}";
        // }
        // 读取has_null的值
        // } else {
        //     read_var_uint(has_null, buf);
        //     LOG(INFO) << "Deserialized has_null: " << this->data(place).has_null;
        // }

        LOG(INFO) << "after, deserialize: has_null = " << this->data(place).has_null;

        // size_t null_set_size;
        // read_var_uint(null_set_size, buf);
        // for (size_t i = 0; i < null_set_size; ++i) {
        //     size_t null_index;
        //     read_var_uint(null_index, buf);
        //     this->data(place).null_set.insert(null_index);
        // }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        LOG(INFO) << "before, insert: has_null = " << this->data(place).has_null;

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

            if (this->data(place).has_null == 1) {
                LOG(INFO) << "We have null, pass!";
                // nested_col.insert_default();
                // if (offsets_to.size() == 0) {
                //     offsets_to.push_back(0);
                // } else {
                //     offsets_to.push_back(offsets_to.back());
                // }
                // nested_col.get_data().resize(1);
                // nested_col.insert_default();
                col_null->insert_data(nullptr, 0);
                offsets_to.push_back(to_nested_col.size());
                // null_map_data.push_back(1);
            } else {
                size_t old_size = nested_col.get_data().size();
                LOG(INFO) << "old_size of data_to: " << old_size;

                const auto& set = this->data(place).value;
                LOG(INFO) << "insert_result_into: set size = " << set.size();

                if (offsets_to.size() == 0) {
                    offsets_to.push_back(set.size());
                } else {
                    offsets_to.push_back(offsets_to.back() + set.size());
                }

                nested_col.get_data().resize(old_size + set.size());

                size_t i = 0;
                for (auto it = set.begin(); it != set.end(); ++it, ++i) {
                    T value = it->get_value();
                    LOG(INFO) << "Inserting value: " << value << " at index " << (old_size + i);
                    nested_col.get_data()[old_size + i] = value;
                    null_map_data.push_back(0);
                }
            }
        } else {
            LOG(INFO) << "nested_col is not nullable. ";
            auto& nested_col = static_cast<ColVecType&>(to_nested_col);
            size_t old_size = nested_col.get_data().size();
            LOG(INFO) << "old_size of data_to: " << old_size;

            const auto& set = this->data(place).value;
            LOG(INFO) << "insert_result_into: set size = " << set.size();

            if (offsets_to.size() == 0) {
                offsets_to.push_back(set.size());
            } else {
                offsets_to.push_back(offsets_to.back() + set.size());
            }

            nested_col.get_data().resize(old_size + set.size());

            size_t i = 0;
            for (auto it = set.begin(); it != set.end(); ++it, ++i) {
                T value = it->get_value();
                LOG(INFO) << "Inserting value: " << value << " at index " << (old_size + i);
                nested_col.get_data()[old_size + i] = value;
            }
        }
        LOG(INFO) << "After making value to data_to, leaving...";

        // auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
        // auto& data_to = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
        // // typename ColumnVector<T>::Container& data_to =
        // //         assert_cast<ColumnVector<T>&>(arr_to.get_data()).get_data();
        // std::string demangled_name = boost::core::demangle(typeid(data_to).name());
        // LOG(INFO) << "name of data_to: " << demangled_name << std::endl;
        // size_t old_size = data_to.size();
        // LOG(INFO) << "old_size of data_to: " << old_size;

        // data_to.resize(old_size + set.size());
        // LOG(INFO) << "after resize, size of data_to: " << data_to.size();

        // size_t i = 0;
        // for (auto it = set.begin(); it != set.end(); ++it, ++i) {
        //     T value = it->get_value();
        //     LOG(INFO) << "Inserting value: " << value << " at index " << (old_size + i);
        //     data_to[old_size + i] = value;
        // }
        // LOG(INFO) << "after for loop, size of data_to: " << data_to.size();
        // LOG(INFO) << "After making value to data_to, leaving...";
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
              input_data_type(input_data_type_[0]) {}

    String get_name() const override { return "group_array_intersect"; }

    // DataTypePtr get_return_type() const override { return input_data_type; }
    DataTypePtr get_return_type() const override {
        // return std::make_shared<DataTypeArray>(make_nullable(input_data_type));
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
    WhichDataType which_type(remove_nullable(nested_type));
    if (which_type.is_int()) {
        LOG(INFO) << "nested_type is int";
    } else {
        LOG(INFO) << "nested_type is not int";
    }
    DataTypes new_argument_types = {nested_type};
    LOG(INFO) << "in the create_aggregate_function_group_array_intersect_impl, name of "
                 "remove_nullable(nested_type): "
              << boost::core::demangle(typeid(remove_nullable(nested_type)).name());

    const auto& argument_type = dynamic_cast<const DataTypeArray&>(*(argument_types[0]));
    LOG(INFO) << "in the create_aggregate_function_group_array_intersect_impl, name of "
                 "argument_type: "
              << boost::core::demangle(typeid(argument_type).name());

    // AggregateFunctionPtr res(
    //         creator_with_numeric_type::creator<AggregateFunctionGroupArrayIntersect>(
    //                 "", new_argument_types, result_is_nullable));
    AggregateFunctionPtr res = nullptr;
    WhichDataType which(remove_nullable(nested_type));
#define DISPATCH(TYPE)                                                                  \
    if (which.idx == TypeIndex::TYPE)                                                   \
        res = creator_without_type::create<AggregateFunctionGroupArrayIntersect<TYPE>>( \
                argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (!res) {
        res = AggregateFunctionPtr(create_with_extra_types(remove_nullable(argument_types)));
    }

    if (!res)
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->get_name(), name);

    return res;
}
} // namespace

} // namespace doris::vectorized
