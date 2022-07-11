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

template <typename ColVecType, bool result_is_nullable>
struct Value {
public:
    bool is_null() const { return _is_null; }
    void set_null(bool is_null) { _is_null = is_null; }

    StringRef get_value() const {
        if constexpr (result_is_nullable) {
            return assert_cast<ColumnNullable*>(_ptr)->get_data_at(_offset);
        } else {
            return assert_cast<ColVecType*>(_ptr)->get_data_at(_offset);
        }
    }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
    bool _is_null;
};

template <typename ColVecType, bool result_is_nullable>
struct CopiedValue : public Value<ColVecType, result_is_nullable> {
public:
    StringRef get_value() const { return _copied_value; }

    void set_value(const IColumn* column, size_t row) {
        _copied_value = column->get_data_at(row).to_string();
    }

private:
    std::string _copied_value;
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct FirstAndLastData {
public:
    using StoreType = std::conditional_t<is_copy, CopiedValue<ColVecType, result_is_nullable>,
                                         Value<ColVecType, result_is_nullable>>;
    static constexpr bool nullable = arg_is_nullable;

    void set_null_if_need() {
        if (!_has_value) {
            this->set_is_null();
        }
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
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

    void set_value(const IColumn** columns, size_t pos) {
        if constexpr (arg_is_nullable) {
            if (assert_cast<const ColumnNullable*>(columns[0])->is_null_at(pos)) {
                _data_value.set_null(true);
                _has_value = true;
                return;
            }
        }
        _data_value.set_value(columns[0], pos);
        _data_value.set_null(false);
        _has_value = true;
    }

    void set_is_null() { _data_value.set_null(true); }

    bool has_set_value() { return _has_value; }

private:
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
            this->set_null_if_need();
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
            this->set_null_if_need();
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

    void reset(AggregateDataPtr place) const override {
        LOG(FATAL) << "ReaderFunctionData do not support reset";
    }
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

#define DISPATCH(TYPE, COLUMN_TYPE)                                                            \
    if (which.idx == TypeIndex::TYPE)                                                          \
        return new AggregateFunctionTemplate<Impl<                                             \
                FirstAndLastData<COLUMN_TYPE, result_is_nullable, arg_is_nullable, is_copy>>>( \
                argument_types);
    TYPE_TO_COLUMN_TYPE(DISPATCH)
#undef DISPATCH

    LOG(FATAL) << "with unknowed type, failed in  create_aggregate_function_" << name
               << " and type is: " << argument_types[0]->get_name();
    return nullptr;
}

template <bool is_copy>
AggregateFunctionPtr create_aggregate_function_first(const std::string& name,
                                                     const DataTypes& argument_types,
                                                     const Array& parameters,
                                                     bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    AggregateFunctionPtr res = nullptr;
    std::visit(
            [&](auto result_is_nullable, auto arg_is_nullable) {
                res = AggregateFunctionPtr(
                        create_function_single_value<ReaderFunctionData, ReaderFunctionFirstData,
                                                     result_is_nullable, arg_is_nullable, is_copy>(
                                name, argument_types, parameters));
            },
            make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));
    if (!res) {
        LOG(WARNING) << " failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
    }
    return res;
}

template <bool is_copy>
AggregateFunctionPtr create_aggregate_function_first_non_null_value(const std::string& name,
                                                                    const DataTypes& argument_types,
                                                                    const Array& parameters,
                                                                    bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    AggregateFunctionPtr res = nullptr;
    std::visit(
            [&](auto result_is_nullable, auto arg_is_nullable) {
                res = AggregateFunctionPtr(
                        create_function_single_value<ReaderFunctionData,
                                                     ReaderFunctionFirstNonNullData,
                                                     result_is_nullable, arg_is_nullable, is_copy>(
                                name, argument_types, parameters));
            },
            make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));
    if (!res) {
        LOG(WARNING) << " failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
    }
    return res;
}

template <bool is_copy>
AggregateFunctionPtr create_aggregate_function_last(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    AggregateFunctionPtr res = nullptr;

    std::visit(
            [&](auto result_is_nullable, auto arg_is_nullable) {
                res = AggregateFunctionPtr(
                        create_function_single_value<ReaderFunctionData, ReaderFunctionLastData,
                                                     result_is_nullable, arg_is_nullable, is_copy>(
                                name, argument_types, parameters));
            },
            make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));
    if (!res) {
        LOG(WARNING) << " failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
    }
    return res;
}

template <bool is_copy>
AggregateFunctionPtr create_aggregate_function_last_non_null_value(const std::string& name,
                                                                   const DataTypes& argument_types,
                                                                   const Array& parameters,
                                                                   bool result_is_nullable) {
    const bool arg_is_nullable = argument_types[0]->is_nullable();
    AggregateFunctionPtr res = nullptr;

    std::visit(
            [&](auto result_is_nullable, auto arg_is_nullable) {
                res = AggregateFunctionPtr(
                        create_function_single_value<ReaderFunctionData,
                                                     ReaderFunctionLastNonNullData,
                                                     result_is_nullable, arg_is_nullable, is_copy>(
                                name, argument_types, parameters));
            },
            make_bool_variant(result_is_nullable), make_bool_variant(arg_is_nullable));
    if (!res) {
        LOG(WARNING) << " failed in  create_aggregate_function_" << name
                     << " and type is: " << argument_types[0]->get_name();
    }
    return res;
}

} // namespace doris::vectorized