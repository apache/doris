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

#include "factory_helpers.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {

template <typename ColVecType, bool arg_is_nullable>
struct Value {
public:
    bool is_null() const {
        if (_ptr == nullptr) {
            return true;
        }
        if constexpr (arg_is_nullable) {
            return assert_cast<const ColumnNullable*>(_ptr)->is_null_at(_offset);
        }
        return false;
    }

    StringRef get_value() const {
        if constexpr (arg_is_nullable) {
            auto* col = assert_cast<const ColumnNullable*>(_ptr);
            return assert_cast<const ColVecType&>(col->get_nested_column()).get_data_at(_offset);
        } else {
            return assert_cast<const ColVecType*>(_ptr)->get_data_at(_offset);
        }
    }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }

    void reset() {
        _ptr = nullptr;
        _offset = 0;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
};

template <typename ColVecType, bool arg_is_nullable>
struct CopiedValue : public Value<ColVecType, arg_is_nullable> {
public:
    StringRef get_value() const { return _copied_value; }

    bool is_null() const { return this->_ptr == nullptr; }

    void set_value(const IColumn* column, size_t row) {
        // here _ptr, maybe null at row, so call reset to set nullptr
        // But we will use is_null() check first, others have set _ptr column to a meaningless address
        // because the address have meaningless, only need it to check is nullptr
        this->_ptr = (IColumn*)0x00000001;
        if constexpr (arg_is_nullable) {
            auto* col = assert_cast<const ColumnNullable*>(column);
            if (col->is_null_at(row)) {
                this->reset();
                return;
            } else {
                _copied_value = assert_cast<const ColVecType&>(col->get_nested_column())
                                        .get_data_at(row)
                                        .to_string();
            }
        } else {
            _copied_value = assert_cast<const ColVecType*>(column)->get_data_at(row).to_string();
        }
    }

private:
    std::string _copied_value;
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFirstAndLastData {
public:
    using StoreType = std::conditional_t<is_copy, CopiedValue<ColVecType, arg_is_nullable>,
                                         Value<ColVecType, arg_is_nullable>>;
    static constexpr bool nullable = arg_is_nullable;

    void reset() {
        _data_value.reset();
        _has_value = false;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) { //_ptr == nullptr || null data at row
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                //get_value will never get null value
                const StringRef& value = _data_value.get_value();
                col.get_null_map_data().push_back(0);
                assert_cast<ColVecType&>(col.get_nested_column())
                        .insert_data(value.data, value.size);
            }
        } else {
            const StringRef& value = _data_value.get_value();
            assert_cast<ColVecType&>(to).insert_data(value.data, value.size);
        }
    }

    // here not check the columns[0] is null at the row,
    // but it is need to check in other
    void set_value(const IColumn** columns, size_t pos) {
        _data_value.set_value(columns[0], pos);
        _has_value = true;
    }

    bool has_set_value() { return _has_value; }

protected:
    StoreType _data_value;
    bool _has_value = false;
};

template <typename Data>
struct ReaderFunctionFirstData : Data {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_value"; }
};

template <typename Data>
struct ReaderFunctionFirstNonNullData : Data {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if constexpr (Data::nullable) {
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_non_null_value"; }
};

template <typename Data>
struct ReaderFunctionLastData : Data {
    void add(int64_t row, const IColumn** columns) { this->set_value(columns, row); }
    static const char* name() { return "last_value"; }
};

template <typename Data>
struct ReaderFunctionLastNonNullData : Data {
    void add(int64_t row, const IColumn** columns) {
        if constexpr (Data::nullable) {
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }

    static const char* name() { return "last_non_null_value"; }
};

template <typename Data>
class ReaderFunctionData final
        : public IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>> {
public:
    ReaderFunctionData(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>>(argument_types, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return _argument_type; }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(row_num, columns);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        LOG(FATAL) << "ReaderFunctionData do not support add_range_single_place";
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        LOG(FATAL) << "ReaderFunctionData do not support merge";
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        LOG(FATAL) << "ReaderFunctionData do not support serialize";
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        LOG(FATAL) << "ReaderFunctionData do not support deserialize";
    }

private:
    DataTypePtr _argument_type;
};

template <template <typename> class AggregateFunctionTemplate, template <typename> class Impl,
          bool result_is_nullable, bool arg_is_nullable, bool is_copy = false>
static IAggregateFunction* create_function_single_value(const String& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    auto type = remove_nullable(argument_types[0]);
    WhichDataType which(*type);

#define DISPATCH(TYPE, COLUMN_TYPE)                                       \
    if (which.idx == TypeIndex::TYPE)                                     \
        return new AggregateFunctionTemplate<Impl<ReaderFirstAndLastData< \
                COLUMN_TYPE, result_is_nullable, arg_is_nullable, is_copy>>>(argument_types);
    TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH

    LOG(FATAL) << "with unknowed type, failed in  create_aggregate_function_" << name
               << " and type is: " << argument_types[0]->get_name();
    return nullptr;
}

#define CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(CREATE_FUNCTION_NAME, FUNCTION_DATA)            \
    template <bool is_copy>                                                                       \
    AggregateFunctionPtr CREATE_FUNCTION_NAME(const std::string& name,                            \
                                              const DataTypes& argument_types,                    \
                                              const Array& parameters, bool result_is_nullable) { \
        const bool arg_is_nullable = argument_types[0]->is_nullable();                            \
        AggregateFunctionPtr res = nullptr;                                                       \
        std::visit(                                                                               \
                [&](auto result_is_nullable, auto arg_is_nullable) {                              \
                    res = AggregateFunctionPtr(                                                   \
                            create_function_single_value<ReaderFunctionData, FUNCTION_DATA,       \
                                                         result_is_nullable, arg_is_nullable,     \
                                                         is_copy>(name, argument_types,           \
                                                                  parameters));                   \
                },                                                                                \
                make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));       \
        if (!res) {                                                                               \
            LOG(WARNING) << " failed in  create_aggregate_function_" << name                      \
                         << " and type is: " << argument_types[0]->get_name();                    \
        }                                                                                         \
        return res;                                                                               \
    }

CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_first, ReaderFunctionFirstData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_first_non_null_value,
                                          ReaderFunctionFirstNonNullData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_last, ReaderFunctionLastData);
CREATE_READER_FUNCTION_WITH_NAME_AND_DATA(create_aggregate_function_last_non_null_value,
                                          ReaderFunctionLastNonNullData);
} // namespace doris::vectorized